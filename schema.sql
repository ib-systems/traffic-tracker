-- ============================================================
-- 1. Network port traffic. Agent sends per-interval deltas
--    (rx_bytes, tx_bytes) already computed from cumulative
--    counters, so no window-function delta logic is needed.
-- ============================================================
CREATE TABLE IF NOT EXISTS port_stats_queue
(
    ts            UInt32,
    host          String,
    region_name   String,
    instance_uuid UUID,
    port_uuid     UUID,
    network_uuid  UUID,
    rx_bytes      UInt64,
    tx_bytes      UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = 'port-stats',
    kafka_group_name = 'ch-port-stats',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'SCRAM-SHA-256',
    kafka_sasl_username = 'agent',
    kafka_sasl_password = 'agentpass';

CREATE TABLE IF NOT EXISTS port_samples
(
    ts            DateTime CODEC(Delta, ZSTD),
    host          LowCardinality(String),
    region_name   LowCardinality(String),
    instance_uuid UUID,
    port_uuid     UUID,
    network_uuid  UUID,
    rx_bytes      UInt64 CODEC(ZSTD),
    tx_bytes      UInt64 CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (region_name, instance_uuid, port_uuid, ts)
TTL ts + INTERVAL 6 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS port_samples_mv TO port_samples AS
SELECT
    toDateTime(ts) AS ts,
    host,
    region_name,
    instance_uuid,
    port_uuid,
    network_uuid,
    rx_bytes,
    tx_bytes
FROM port_stats_queue;

-- ============================================================
-- 2a. 5-minute network volumes (dashboard: 12h/24h view).
-- ============================================================
CREATE TABLE IF NOT EXISTS port_volume_5min
(
    ts5           DateTime,
    region_name   LowCardinality(String),
    instance_uuid UUID,
    port_uuid     UUID,
    network_uuid  UUID,
    rx_bytes      UInt64,
    tx_bytes      UInt64,
    samples       UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMMDD(ts5)
ORDER BY (region_name, instance_uuid, port_uuid, network_uuid, ts5)
TTL ts5 + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS port_volume_5min_mv
REFRESH EVERY 5 MINUTE APPEND TO port_volume_5min AS
WITH toStartOfFiveMinutes(now()) AS cur_bucket
SELECT
    toStartOfFiveMinutes(ts) AS ts5,
    region_name, instance_uuid, port_uuid, network_uuid,
    sum(rx_bytes) AS rx_bytes,
    sum(tx_bytes) AS tx_bytes,
    toUInt32(count()) AS samples
FROM port_samples
WHERE ts >= cur_bucket - INTERVAL 15 MINUTE AND ts < cur_bucket
GROUP BY ts5, region_name, instance_uuid, port_uuid, network_uuid;

-- ============================================================
-- 2b. Hourly network volumes (billing, 7d+ dashboard).
-- ============================================================
CREATE TABLE IF NOT EXISTS port_volume_hourly
(
    hour          DateTime,
    region_name   LowCardinality(String),
    instance_uuid UUID,
    port_uuid     UUID,
    network_uuid  UUID,
    rx_bytes      UInt64,
    tx_bytes      UInt64,
    samples       UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMM(hour)
ORDER BY (region_name, instance_uuid, port_uuid, network_uuid, hour)
TTL hour + INTERVAL 3 MONTH;

CREATE MATERIALIZED VIEW IF NOT EXISTS port_volume_hourly_mv
REFRESH EVERY 10 MINUTE APPEND TO port_volume_hourly AS
WITH toStartOfHour(now()) AS cur_hour
SELECT
    toStartOfHour(ts) AS hour,
    region_name, instance_uuid, port_uuid, network_uuid,
    sum(rx_bytes) AS rx_bytes,
    sum(tx_bytes) AS tx_bytes,
    toUInt32(count()) AS samples
FROM port_samples
WHERE ts >= cur_hour - INTERVAL 3 HOUR AND ts < cur_hour
GROUP BY hour, region_name, instance_uuid, port_uuid, network_uuid;

-- ============================================================
-- 3. Period-to-date usage view.
-- ============================================================
CREATE OR REPLACE VIEW port_usage AS
WITH
    (SELECT max(hour) + INTERVAL 1 HOUR FROM port_volume_hourly) AS watermark
SELECT
    region_name, instance_uuid, port_uuid, network_uuid,
    sum(rx) AS rx_bytes, sum(tx) AS tx_bytes, max(f) AS fresh_as_of
FROM
(
    SELECT region_name, instance_uuid, port_uuid, network_uuid,
        rx_bytes AS rx, tx_bytes AS tx, hour AS f
    FROM port_volume_hourly FINAL
    WHERE hour >= {period_start:DateTime}
    UNION ALL
    SELECT region_name, instance_uuid, port_uuid, network_uuid,
        rx_bytes AS rx, tx_bytes AS tx, ts AS f
    FROM port_samples
    WHERE ts >= watermark AND ts >= {period_start:DateTime}
)
GROUP BY region_name, instance_uuid, port_uuid, network_uuid;

-- ============================================================
-- 4. Instance CPU/RAM metrics.
-- ============================================================
CREATE TABLE IF NOT EXISTS instance_stats_queue
(
    ts                    UInt32,
    host                  String,
    region_name           String,
    instance_uuid         UUID,
    cpu_pct               Float64,
    memory_actual_bytes   Nullable(UInt64),
    memory_rss_bytes      Nullable(UInt64),
    memory_usable_bytes   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = 'instance-stats',
    kafka_group_name = 'ch-instance-stats',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'SCRAM-SHA-256',
    kafka_sasl_username = 'agent',
    kafka_sasl_password = 'agentpass';

CREATE TABLE IF NOT EXISTS instance_samples
(
    ts                    DateTime CODEC(Delta, ZSTD),
    host                  LowCardinality(String),
    region_name           LowCardinality(String),
    instance_uuid         UUID,
    cpu_pct               Float64 CODEC(ZSTD),
    memory_actual_bytes   Nullable(UInt64) CODEC(ZSTD),
    memory_rss_bytes      Nullable(UInt64) CODEC(ZSTD),
    memory_usable_bytes   Nullable(UInt64) CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (region_name, instance_uuid, ts)
TTL ts + INTERVAL 6 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS instance_samples_mv TO instance_samples AS
SELECT
    toDateTime(ts) AS ts, host, region_name, instance_uuid,
    cpu_pct, memory_actual_bytes, memory_rss_bytes, memory_usable_bytes
FROM instance_stats_queue;

-- ============================================================
-- 4a. 5-minute instance metrics (dashboard: 12h/24h view).
-- ============================================================
CREATE TABLE IF NOT EXISTS instance_metrics_5min
(
    ts5                        DateTime,
    host                       LowCardinality(String),
    region_name                LowCardinality(String),
    instance_uuid              UUID,
    cpu_avg_pct                Float64,
    cpu_max_pct                Float64,
    memory_actual_avg_bytes    Nullable(Float64),
    memory_rss_avg_bytes       Nullable(Float64),
    memory_usable_avg_bytes    Nullable(Float64),
    samples                    UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMMDD(ts5)
ORDER BY (region_name, instance_uuid, host, ts5)
TTL ts5 + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS instance_metrics_5min_mv
REFRESH EVERY 5 MINUTE APPEND TO instance_metrics_5min AS
WITH toStartOfFiveMinutes(now()) AS cur_bucket
SELECT
    toStartOfFiveMinutes(ts) AS ts5,
    host, region_name, instance_uuid,
    avg(cpu_pct) AS cpu_avg_pct,
    max(cpu_pct) AS cpu_max_pct,
    avg(memory_actual_bytes) AS memory_actual_avg_bytes,
    avg(memory_rss_bytes) AS memory_rss_avg_bytes,
    avg(memory_usable_bytes) AS memory_usable_avg_bytes,
    toUInt32(count()) AS samples
FROM instance_samples
WHERE ts >= cur_bucket - INTERVAL 15 MINUTE AND ts < cur_bucket
GROUP BY ts5, host, region_name, instance_uuid;

-- ============================================================
-- 4b. Hourly instance metrics (billing, 7d+ dashboard).
-- ============================================================
CREATE TABLE IF NOT EXISTS instance_metrics_hourly
(
    hour                       DateTime,
    host                       LowCardinality(String),
    region_name                LowCardinality(String),
    instance_uuid              UUID,
    cpu_avg_pct                Float64,
    cpu_max_pct                Float64,
    memory_actual_avg_bytes    Nullable(Float64),
    memory_actual_max_bytes    Nullable(UInt64),
    memory_actual_last_bytes   Nullable(UInt64),
    memory_rss_avg_bytes       Nullable(Float64),
    memory_rss_max_bytes       Nullable(UInt64),
    memory_rss_last_bytes      Nullable(UInt64),
    memory_usable_avg_bytes    Nullable(Float64),
    memory_usable_min_bytes    Nullable(UInt64),
    memory_usable_last_bytes   Nullable(UInt64),
    samples                    UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMM(hour)
ORDER BY (region_name, instance_uuid, host, hour)
TTL hour + INTERVAL 3 MONTH;

CREATE MATERIALIZED VIEW IF NOT EXISTS instance_metrics_hourly_mv
REFRESH EVERY 10 MINUTE APPEND TO instance_metrics_hourly AS
WITH toStartOfHour(now()) AS cur_hour
SELECT
    toStartOfHour(ts) AS hour,
    host, region_name, instance_uuid,
    avg(cpu_pct) AS cpu_avg_pct,
    max(cpu_pct) AS cpu_max_pct,
    avg(memory_actual_bytes) AS memory_actual_avg_bytes,
    max(memory_actual_bytes) AS memory_actual_max_bytes,
    argMax(memory_actual_bytes, ts) AS memory_actual_last_bytes,
    avg(memory_rss_bytes) AS memory_rss_avg_bytes,
    max(memory_rss_bytes) AS memory_rss_max_bytes,
    argMax(memory_rss_bytes, ts) AS memory_rss_last_bytes,
    avg(memory_usable_bytes) AS memory_usable_avg_bytes,
    min(memory_usable_bytes) AS memory_usable_min_bytes,
    argMax(memory_usable_bytes, ts) AS memory_usable_last_bytes,
    toUInt32(count()) AS samples
FROM instance_samples
WHERE ts >= cur_hour - INTERVAL 3 HOUR AND ts < cur_hour
GROUP BY hour, host, region_name, instance_uuid;

-- ============================================================
-- 5. Per-device disk metrics.
-- ============================================================
CREATE TABLE IF NOT EXISTS disk_stats_queue
(
    ts                    UInt32,
    host                  String,
    region_name           String,
    instance_uuid         UUID,
    device                String,
    read_bytes            Nullable(UInt64),
    write_bytes           Nullable(UInt64),
    read_requests         Nullable(UInt64),
    write_requests        Nullable(UInt64),
    capacity_bytes        Nullable(UInt64),
    allocation_bytes      Nullable(UInt64),
    physical_bytes        Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = 'disk-stats',
    kafka_group_name = 'ch-disk-stats',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'SCRAM-SHA-256',
    kafka_sasl_username = 'agent',
    kafka_sasl_password = 'agentpass';

CREATE TABLE IF NOT EXISTS disk_samples
(
    ts                    DateTime CODEC(Delta, ZSTD),
    host                  LowCardinality(String),
    region_name           LowCardinality(String),
    instance_uuid         UUID,
    device                LowCardinality(String),
    read_bytes            Nullable(UInt64) CODEC(ZSTD),
    write_bytes           Nullable(UInt64) CODEC(ZSTD),
    read_requests         Nullable(UInt64) CODEC(ZSTD),
    write_requests        Nullable(UInt64) CODEC(ZSTD),
    capacity_bytes        Nullable(UInt64) CODEC(ZSTD),
    allocation_bytes      Nullable(UInt64) CODEC(ZSTD),
    physical_bytes        Nullable(UInt64) CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (region_name, instance_uuid, device, ts)
TTL ts + INTERVAL 6 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS disk_samples_mv TO disk_samples AS
SELECT
    toDateTime(ts) AS ts, host, region_name, instance_uuid, device,
    read_bytes, write_bytes, read_requests, write_requests,
    capacity_bytes, allocation_bytes, physical_bytes
FROM disk_stats_queue;

-- ============================================================
-- 5a. 5-minute disk metrics (dashboard: 12h/24h view).
-- ============================================================
CREATE TABLE IF NOT EXISTS disk_metrics_5min
(
    ts5                        DateTime,
    host                       LowCardinality(String),
    region_name                LowCardinality(String),
    instance_uuid              UUID,
    device                     LowCardinality(String),
    read_bytes                 UInt64,
    write_bytes                UInt64,
    read_requests              UInt64,
    write_requests             UInt64,
    capacity_last_bytes        Nullable(UInt64),
    allocation_last_bytes      Nullable(UInt64),
    samples                    UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMMDD(ts5)
ORDER BY (region_name, instance_uuid, device, host, ts5)
TTL ts5 + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS disk_metrics_5min_mv
REFRESH EVERY 5 MINUTE APPEND TO disk_metrics_5min AS
WITH toStartOfFiveMinutes(now()) AS cur_bucket
SELECT
    toStartOfFiveMinutes(ts) AS ts5,
    host, region_name, instance_uuid, device,
    sum(ifNull(read_bytes, 0)) AS read_bytes,
    sum(ifNull(write_bytes, 0)) AS write_bytes,
    sum(ifNull(read_requests, 0)) AS read_requests,
    sum(ifNull(write_requests, 0)) AS write_requests,
    argMax(capacity_bytes, ts) AS capacity_last_bytes,
    argMax(allocation_bytes, ts) AS allocation_last_bytes,
    toUInt32(count()) AS samples
FROM disk_samples
WHERE ts >= cur_bucket - INTERVAL 15 MINUTE AND ts < cur_bucket
GROUP BY ts5, host, region_name, instance_uuid, device;

-- ============================================================
-- 5b. Hourly disk metrics (billing, 7d+ dashboard).
-- ============================================================
CREATE TABLE IF NOT EXISTS disk_metrics_hourly
(
    hour                       DateTime,
    host                       LowCardinality(String),
    region_name                LowCardinality(String),
    instance_uuid              UUID,
    device                     LowCardinality(String),
    read_bytes                 UInt64,
    write_bytes                UInt64,
    read_requests              UInt64,
    write_requests             UInt64,
    capacity_last_bytes        Nullable(UInt64),
    allocation_avg_bytes       Nullable(Float64),
    allocation_max_bytes       Nullable(UInt64),
    allocation_last_bytes      Nullable(UInt64),
    physical_avg_bytes         Nullable(Float64),
    physical_max_bytes         Nullable(UInt64),
    physical_last_bytes        Nullable(UInt64),
    samples                    UInt32
)
ENGINE = ReplacingMergeTree(samples)
PARTITION BY toYYYYMM(hour)
ORDER BY (region_name, instance_uuid, device, host, hour)
TTL hour + INTERVAL 3 MONTH;

CREATE MATERIALIZED VIEW IF NOT EXISTS disk_metrics_hourly_mv
REFRESH EVERY 10 MINUTE APPEND TO disk_metrics_hourly AS
WITH toStartOfHour(now()) AS cur_hour
SELECT
    toStartOfHour(ts) AS hour,
    host, region_name, instance_uuid, device,
    sum(ifNull(read_bytes, 0)) AS read_bytes,
    sum(ifNull(write_bytes, 0)) AS write_bytes,
    sum(ifNull(read_requests, 0)) AS read_requests,
    sum(ifNull(write_requests, 0)) AS write_requests,
    argMax(capacity_bytes, ts) AS capacity_last_bytes,
    avg(allocation_bytes) AS allocation_avg_bytes,
    max(allocation_bytes) AS allocation_max_bytes,
    argMax(allocation_bytes, ts) AS allocation_last_bytes,
    avg(physical_bytes) AS physical_avg_bytes,
    max(physical_bytes) AS physical_max_bytes,
    argMax(physical_bytes, ts) AS physical_last_bytes,
    toUInt32(count()) AS samples
FROM disk_samples
WHERE ts >= cur_hour - INTERVAL 3 HOUR AND ts < cur_hour
GROUP BY hour, host, region_name, instance_uuid, device;
