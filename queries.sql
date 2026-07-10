-- Tail of raw samples (cumulative counters as received)
SELECT
    ts,
    region_name,
    right(toString(port_uuid), 4) AS port,
    right(toString(network_uuid), 4) AS network,
    rx, tx, is_baseline
FROM port_samples
ORDER BY ts DESC
LIMIT 10;

-- Materialized hourly volumes (what billing reads)
SELECT
    hour,
    region_name,
    right(toString(port_uuid), 4)    AS port,
    right(toString(network_uuid), 4) AS network,
    formatReadableSize(rx_bytes)     AS rx,
    formatReadableSize(tx_bytes)     AS tx,
    samples
FROM port_volume_hourly FINAL
ORDER BY hour DESC, port
LIMIT 10;

-- Billing sync: per-instance volume for the current period,
-- closed hours only (cheap: never touches raw samples)
SELECT
    region_name,
    instance_uuid,
    sum(rx_bytes) AS rx_bytes,
    sum(tx_bytes) AS tx_bytes
FROM port_volume_hourly FINAL
WHERE hour >= toDateTime('2026-06-01 00:00:00')
GROUP BY region_name, instance_uuid
LIMIT 10;

-- Period-to-date usage per port (delta-summed, reset-aware,
-- fresh to the last agent poll). Set period_start to your test window.
SELECT
    region_name,
    right(toString(instance_uuid), 4) AS instance,
    -- instance index + port index: the two ports of an instance share
    -- the UUID tail and differ only in the second group
    concat(right(toString(port_uuid), 4), '/',
           substring(toString(port_uuid), 10, 4)) AS port,
    right(toString(network_uuid), 4)  AS network,
    formatReadableSize(rx_bytes)      AS rx,
    formatReadableSize(tx_bytes)      AS tx,
    fresh_as_of
FROM port_usage(period_start = '2026-06-01 00:00:00')
ORDER BY region_name, instance, port;

-- Sanity check: usage from the view must equal delta-sum over raw samples
-- (while raw TTL still covers the whole period)
SELECT
    region_name,
    right(toString(port_uuid), 4) AS port,
    sum(if(is_baseline, 0, if(rx < p_rx, rx, toUInt64(rx - p_rx)))) AS rx_bytes
FROM
(
    -- A baseline establishes the predecessor but contributes no usage.
    -- max() per (port, ts) collapses same-second duplicates.
    SELECT region_name, port_uuid, rx, is_baseline,
           lagInFrame(rx, 1, toUInt64(0)) OVER w AS p_rx
    FROM
    (
        SELECT region_name, port_uuid, ts, max(rx) AS rx, max(is_baseline) AS is_baseline
        FROM port_samples
        WHERE ts >= toDateTime('2026-06-01 00:00:00')
        GROUP BY region_name, port_uuid, ts
    )
    WINDOW w AS (PARTITION BY region_name, port_uuid ORDER BY ts)
)
GROUP BY region_name, port_uuid
ORDER BY region_name, port;

-- Kafka consumer health
SELECT * FROM system.kafka_consumers FORMAT Vertical;
