# Per-port traffic accounting PoC

Stateless agent → Kafka (cumulative counters) → ClickHouse Kafka engine →
raw samples → refreshable MV materializes reset-aware hourly volumes →
usage view (closed-hour volumes + live tail over raw samples).

## Run

```bash
docker compose up -d

pip install confluent-kafka
python3 agent.py
```

Schema is applied automatically on first ClickHouse start
(`schema.sql` mounted into `/docker-entrypoint-initdb.d/`).

## Verify

```bash
docker exec -it poc-clickhouse clickhouse-client --password clickhouse
```

Then run queries from `queries.sql`. Within ~30s of starting the agent,
`port_samples` fills, and:

```sql
SELECT * FROM port_usage(period_start = '2026-06-01 00:00:00');
```

returns period-to-date volume per port. Watch the reset count in the
agent's cycle log, then confirm usage keeps growing monotonically
through resets — that's the delta/reset rule working.

`port_volume_hourly` stays empty until the first wall-clock hour closes
and the next refresh tick (≤10 min) materializes it; until then the
usage view serves everything from raw samples.

## Pipeline layout

| object                  | role                                       | TTL       |
|-------------------------|--------------------------------------------|-----------|
| `port_stats_queue`      | Kafka engine source                        | —         |
| `port_samples_mv`       | incremental MV: Kafka → raw samples        | —         |
| `port_samples`          | raw cumulative samples (poll-rate)         | 6 hours   |
| `port_volume_hourly_mv` | refreshable MV: hour-close job (10 min)    | —         |
| `port_volume_hourly`    | reset-aware volume per port per hour       | 13 months |
| `port_usage`            | parameterized view: period usage           | —         |

Hourly volumes are the only long-term data; raw samples and Kafka
segments are self-expiring buffers. Freshness: `port_volume_hourly`
lags up to one hour + refresh interval; `port_usage` adds a live
delta-sum over post-watermark raw samples and is fresh to the last
agent poll.

Billing sync = hourly per-instance `sum()` over `port_volume_hourly`
within the period window, overwrite-upserted into the billing DB.
Period rollover needs no reset: a new period is just a new window.

## Notes

- Kafka has two listeners: `localhost:9092` (agent on host),
  `kafka:19092` (ClickHouse inside the compose network).
- Kafka engine consumption starts only once an MV is attached — already
  the case here.
- Duplicates from Kafka redelivery are harmless: identical consecutive
  counters produce delta 0. Same-second rows are additionally collapsed
  with `max()` before delta computation so the window order stays
  deterministic.
- To test reset accounting deliberately, bump `RESET_CHANCE` in `agent.py`.
- The ClickHouse data dir lives on an anonymous Docker volume, so
  `docker compose up -d` after an image bump keeps data and does NOT
  re-run `schema.sql`. For a clean slate (and schema re-apply):
  `docker compose up -d --renew-anon-volumes clickhouse`.
