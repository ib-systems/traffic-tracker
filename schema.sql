-- ============================================================
-- 1. Kafka source (consumption starts once an MV is attached)
-- ============================================================
CREATE TABLE IF NOT EXISTS port_stats_queue
(
    ts            UInt32,
    host          String,
    instance_uuid UUID,
    port_uuid     UUID,
    rx            UInt64,
    tx            UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'port-stats',
    kafka_group_name = 'ch-port-stats',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- ============================================================
-- 2. Raw samples (poll-rate granularity). Short TTL: only needed
--    until the hour-close refresh turns them into volume rows,
--    plus margin for refresh lag / late Kafka delivery.
-- ============================================================
CREATE TABLE IF NOT EXISTS port_samples
(
    ts            DateTime CODEC(Delta, ZSTD),
    host          LowCardinality(String),
    instance_uuid UUID,
    port_uuid     UUID,
    rx            UInt64 CODEC(ZSTD),
    tx            UInt64 CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (instance_uuid, port_uuid, ts)
TTL ts + INTERVAL 6 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS port_samples_mv TO port_samples AS
SELECT
    toDateTime(ts)  AS ts,
    host,
    instance_uuid,
    port_uuid,
    rx,
    tx
FROM port_stats_queue;

-- ============================================================
-- 3. Processed volumes: reset-aware traffic per port per closed
--    hour, computed from minute-level samples (so mid-hour resets
--    are accounted exactly). This is the long-term billing store;
--    raw samples can expire once an hour is materialized here.
--
--    `samples` is the ReplacingMergeTree version: a recompute can
--    only replace a row if it saw at least as many samples, so a
--    recompute over partially-expired raw data never degrades a
--    row that was built from full data, while late-arriving
--    samples (more rows) still win.
-- ============================================================
CREATE TABLE IF NOT EXISTS port_volume_hourly
(
    hour          DateTime,
    instance_uuid UUID,
    port_uuid     UUID,
    rx_bytes      UInt64,
    tx_bytes      UInt64,
    samples       UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMM(hour)
ORDER BY (instance_uuid, port_uuid, hour)
TTL hour + INTERVAL 13 MONTH;

-- Hour-close job: recomputes the last 3 closed hours every refresh
-- (idempotent: same input -> same row; ReplacingMergeTree dedupes).
-- The 3-hour recompute window picks up late Kafka deliveries; the
-- extra baseline hour in the inner WHERE provides the previous
-- counter for the oldest recomputed hour. PoC refresh is 10 min;
-- prod would use EVERY 1 HOUR. Invariant: refresh lag must stay
-- well below the raw samples TTL.
CREATE MATERIALIZED VIEW IF NOT EXISTS port_volume_hourly_mv
REFRESH EVERY 10 MINUTE APPEND TO port_volume_hourly AS
WITH toStartOfHour(now()) AS cur_hour
SELECT
    toStartOfHour(ts) AS hour,
    instance_uuid,
    port_uuid,
    sum(if(rx < p_rx, rx, toUInt64(rx - p_rx))) AS rx_bytes,
    sum(if(tx < p_tx, tx, toUInt64(tx - p_tx))) AS tx_bytes,
    toUInt32(count()) AS samples
FROM
(
    SELECT
        instance_uuid,
        port_uuid,
        ts, rx, tx,
        lagInFrame(rx, 1, toUInt64(0)) OVER w AS p_rx,
        lagInFrame(tx, 1, toUInt64(0)) OVER w AS p_tx
    FROM
    (
        -- max() collapses same-second duplicates (agent bursts, Kafka
        -- redelivery) so the window order below is deterministic
        SELECT instance_uuid, port_uuid, ts, max(rx) AS rx, max(tx) AS tx
        FROM port_samples
        WHERE ts >= cur_hour - INTERVAL 4 HOUR
        GROUP BY instance_uuid, port_uuid, ts
    )
    WINDOW w AS (PARTITION BY port_uuid ORDER BY ts)
)
WHERE ts >= cur_hour - INTERVAL 3 HOUR AND ts < cur_hour
GROUP BY hour, instance_uuid, port_uuid;

-- ============================================================
-- 4. Period-to-date usage, fresh to the last agent poll.
--    = materialized volume rows for closed hours
--    + reset-aware delta-sum over raw samples newer than the
--      watermark (the first hour not yet materialized).
--    The watermark (not toStartOfHour(now())) keeps the live
--    window covering a just-closed hour until the refresh lands,
--    so usage never dips at hour boundaries.
--    Usage: SELECT * FROM port_usage(period_start='2026-06-01 00:00:00')
-- ============================================================
CREATE OR REPLACE VIEW port_usage AS
WITH
    (SELECT max(hour) + INTERVAL 1 HOUR FROM port_volume_hourly) AS watermark
SELECT
    instance_uuid,
    port_uuid,
    sum(rx)  AS rx_bytes,
    sum(tx)  AS tx_bytes,
    max(f)   AS fresh_as_of
FROM
(
    SELECT instance_uuid, port_uuid, rx_bytes AS rx, tx_bytes AS tx, hour AS f
    FROM port_volume_hourly FINAL
    WHERE hour >= {period_start:DateTime}

    UNION ALL

    SELECT
        instance_uuid,
        port_uuid,
        if(ts >= watermark AND ts >= {period_start:DateTime},
           if(rx < p_rx, rx, toUInt64(rx - p_rx)), 0) AS rx,
        if(ts >= watermark AND ts >= {period_start:DateTime},
           if(tx < p_tx, tx, toUInt64(tx - p_tx)), 0) AS tx,
        ts AS f
    FROM
    (
        SELECT
            instance_uuid,
            port_uuid,
            ts, rx, tx,
            -- default 0: a port's first-ever point counts in full,
            -- same as the counter-reset rule (rx < p_rx)
            lagInFrame(rx, 1, toUInt64(0)) OVER w AS p_rx,
            lagInFrame(tx, 1, toUInt64(0)) OVER w AS p_tx
        FROM
        (
            -- the extra hour below the watermark provides each port's
            -- baseline counter; those rows are excluded from the sum by
            -- the ts >= watermark filter above. max() collapses
            -- same-second duplicates so the window order is deterministic.
            SELECT instance_uuid, port_uuid, ts, max(rx) AS rx, max(tx) AS tx
            FROM port_samples
            WHERE ts >= watermark - INTERVAL 1 HOUR
            GROUP BY instance_uuid, port_uuid, ts
        )
        WINDOW w AS (PARTITION BY port_uuid ORDER BY ts)
    )
)
GROUP BY instance_uuid, port_uuid;
