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

-- Raw CPU/RAM samples. Memory fields are NULL when libvirt does not expose
-- the corresponding balloon statistic.
SELECT
    ts,
    region_name,
    host,
    instance_uuid,
    cpu_time_ns,
    vcpus,
    formatReadableSize(memory_actual_bytes) AS memory_actual,
    formatReadableSize(memory_rss_bytes) AS memory_rss,
    formatReadableSize(memory_usable_bytes) AS memory_usable,
    is_baseline
FROM instance_samples
ORDER BY ts DESC
LIMIT 20;

-- Closed-hour CPU utilization and RAM gauges per instance.
SELECT
    hour,
    region_name,
    host,
    instance_uuid,
    round(cpu_utilization_pct, 2) AS cpu_pct,
    vcpus,
    formatReadableTimeDelta(cpu_time_ns / 1000000000) AS cpu_time,
    formatReadableSize(memory_actual_avg_bytes) AS memory_actual_avg,
    formatReadableSize(memory_rss_max_bytes) AS memory_rss_max,
    formatReadableSize(memory_usable_min_bytes) AS memory_usable_min,
    samples
FROM instance_metrics_hourly FINAL
ORDER BY hour DESC, region_name, instance_uuid
LIMIT 20;

-- Fleet CPU and RAM by region for the latest materialized hour.
SELECT
    region_name,
    hour,
    count() AS instances,
    round(avg(cpu_utilization_pct), 2) AS avg_instance_cpu_pct,
    round(sum(cpu_time_ns) / 1000000000, 2) AS cpu_seconds,
    formatReadableSize(sum(memory_actual_avg_bytes)) AS allocated_memory,
    formatReadableSize(sum(memory_rss_avg_bytes)) AS resident_memory
FROM instance_metrics_hourly FINAL
WHERE hour = (SELECT max(hour) FROM instance_metrics_hourly)
GROUP BY region_name, hour
ORDER BY region_name;

-- Raw per-device disk counters and gauges.
SELECT
    ts,
    region_name,
    host,
    instance_uuid,
    device,
    formatReadableSize(read_bytes) AS read_total,
    formatReadableSize(write_bytes) AS write_total,
    read_requests,
    write_requests,
    formatReadableSize(capacity_bytes) AS capacity,
    formatReadableSize(allocation_bytes) AS allocation,
    is_baseline
FROM disk_samples
ORDER BY ts DESC
LIMIT 20;

-- Closed-hour reset-aware disk I/O and latest allocation per device.
SELECT
    hour,
    region_name,
    host,
    instance_uuid,
    device,
    formatReadableSize(read_bytes) AS bytes_read,
    formatReadableSize(write_bytes) AS bytes_written,
    read_requests,
    write_requests,
    formatReadableSize(capacity_last_bytes) AS capacity,
    formatReadableSize(allocation_last_bytes) AS allocation,
    formatReadableSize(physical_last_bytes) AS physical,
    samples
FROM disk_metrics_hourly FINAL
ORDER BY hour DESC, region_name, instance_uuid, device
LIMIT 20;

-- Per-instance disk I/O over a requested period.
SELECT
    region_name,
    instance_uuid,
    sum(read_bytes) AS read_bytes,
    sum(write_bytes) AS write_bytes,
    sum(read_requests) AS read_requests,
    sum(write_requests) AS write_requests
FROM disk_metrics_hourly FINAL
WHERE hour >= toDateTime('2026-07-01 00:00:00')
GROUP BY region_name, instance_uuid
ORDER BY region_name, instance_uuid
LIMIT 20;
