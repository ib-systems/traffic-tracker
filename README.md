# Per-port traffic accounting PoC

The primary PoC is a real ML2/OVN compute-node collector:

```text
libvirt cumulative vNIC counters -> ovn_agent.py -> Kafka -> ClickHouse
                                           |             |
                                           +-> NBDB      +-> hourly volumes
```

ClickHouse receives cumulative RX/TX counters and computes reset-aware usage
per region, instance, port, and network. It retains raw samples for six hours
and aggregated per-port hourly volume for three months.

## OVN PoC

`ovn_agent.py` is the real collector. One poll uses libvirt
`getAllDomainStats()` to retrieve all running-domain vNIC counters. It maps
each interface to a Neutron port and network without command-line clients:

1. Read `virtualport/parameters@interfaceid` from the libvirt domain XML.
2. If that field is absent, match `tap<port-UUID-prefix>` and the libvirt
   domain UUID against NBDB `Logical_Switch_Port.name` and
   `external_ids["neutron:device_id"]`.
3. Read `external_ids["neutron:network_name"]` from the matching logical
   switch port to derive `network_uuid`.

The agent talks directly to OVN Northbound through `ovsdbapp` IDL. It does not
invoke `virsh`, `ovs-vsctl`, or `ovn-nbctl`. It currently accounts only for
active Nova compute ports (`neutron:device_owner=compute:nova`).

### Run on a compute node

```bash
# The libvirt and Open vSwitch Python bindings are normally supplied by the
# host distribution. ovsdbapp is needed for native OVSDB access.
pip install ovsdbapp

# The default mode prints a reset-aware traffic delta every 10 seconds.
python3 ovn_agent.py \
  --libvirt-uri qemu+tcp://<libvirt-host>/system \
  --libvirt-username '<username>' \
  --libvirt-password '<password>' \
  --region-name '<openstack-region>' \
  --ovn-nb-db tcp:<nbdb-host-1>:6641,tcp:<nbdb-host-2>:6641
```

The first output for an interface is a zero-MiB baseline. Subsequent rows show
the RX, TX, and total MiB processed during the prior polling interval. When a
counter decreases because a vNIC is recreated or reset, the agent treats the
new counter value as the interval delta instead of emitting a negative value.

The host needs `python3-libvirt`, the Open vSwitch Python bindings, and network
access to the configured libvirt and NBDB endpoints. The shown `tcp:` NBDB
cluster does not require TLS options.

The agent reports per-poll query timing, split into libvirt/NBDB discovery and
NBDB port metadata resolution. A warm port/network cache makes the latter
effectively free during normal polling.

`--region-name` (or `REGION_NAME`) defaults to `RegionOne`. It is sent in every
Kafka record and is part of the ClickHouse aggregation key, allowing a shared
ClickHouse deployment to report usage independently for each OpenStack region.

For a SASL-protected remote libvirt endpoint, pass
`--libvirt-username` and `--libvirt-password`. Alternatively,
`--libvirt-auth-file` reads Nova's `credentials-default` section. The password
argument is visible to local process inspection, so the auth-file or
`LIBVIRT_PASSWORD` environment-variable form is preferable outside this PoC.

### Send samples to Kafka

The default mode only prints interval usage. Add `--publish-kafka` to send the
underlying cumulative counters to the billing pipeline; ClickHouse, not the
agent's printed delta, is the billing source of truth.

```bash
python3 ovn_agent.py \
  --libvirt-uri qemu+tcp://<libvirt-host>/system \
  --libvirt-username '<username>' \
  --libvirt-password '<password>' \
  --region-name '<openstack-region>' \
  --ovn-nb-db tcp:<nbdb-host-1>:6641,tcp:<nbdb-host-2>:6641 \
  --bootstrap-servers kafka.example:9092 \
  --publish-kafka
```

`is_baseline=true` is sent for the first observation of every port after an
agent starts. This prevents a process restart from charging all traffic that
predated it. Kafka redelivery is harmless because same-second counters are
collapsed before delta calculation. Each record includes `region_name`,
`instance_uuid`, `port_uuid`, and `network_uuid`.

### Current scope

- The supported OVN source is the Northbound database; Southbound DB is not
  queried.
- The collector reads libvirt counters, not OVS interface statistics.
- Port and network mappings are cached in memory. A restart is billing-safe
  because the next observation is a baseline.
- This is a single-process PoC. Production deployment still needs service
  management, Kafka authentication, monitoring, and a migration policy.

## Synthetic load generator

`agent.py` is retained solely to generate deterministic fake ports and counter
resets for ClickHouse throughput and schema tests. It is not used on compute
nodes.

```bash
docker compose up -d
pip install confluent-kafka
python3 agent.py
```

## ClickHouse setup

Schema is applied automatically on first ClickHouse start
(`schema.sql` mounted into `/docker-entrypoint-initdb.d/`).

The region dimension changes the Kafka table, materialized views, and sorting
keys. Recreate the PoC ClickHouse volume when upgrading an existing test stack
to this schema.

## Verify

```bash
docker exec -it poc-clickhouse clickhouse-client --password clickhouse
```

Then run queries from `queries.sql`. Within ~30s of starting the collector
with `--publish-kafka`, `port_samples` fills, and:

```sql
SELECT * FROM port_usage(period_start = '2026-06-01 00:00:00');
```

returns period-to-date volume per port. Confirm that a vNIC counter reset
produces a positive new-counter delta, rather than negative usage, and that a
new agent baseline contributes zero billed traffic.

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
| `port_volume_hourly`    | reset-aware volume per port per hour       | 3 months  |
| `port_usage`            | parameterized view: period usage           | —         |

Hourly volumes are the only long-term data; raw samples and Kafka
segments are self-expiring buffers. Freshness: `port_volume_hourly`
lags up to one hour + refresh interval; `port_usage` adds a live
delta-sum over post-watermark raw samples and is fresh to the last
agent poll.

Billing sync = hourly per-instance `sum()` over `port_volume_hourly`
within the period window, overwrite-upserted into the billing DB.
Period rollover needs no reset: a new period is just a new window.

Raw, hourly, and period-usage rows include `region_name` and `network_uuid`.
The network UUID is resolved from the OVN logical-switch port's
`neutron:network_name` external ID.

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
- For an existing ClickHouse deployment, change the aggregate retention with:

  ```sql
  ALTER TABLE port_volume_hourly MODIFY TTL hour + INTERVAL 3 MONTH;
  ```
