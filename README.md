# Compute metrics and per-port traffic accounting PoC

```text
libvirt bulk domain stats -> ovn_agent.py -> Redpanda -> ClickHouse
                               |              |          |
                               +-> OVN NBDB    |          +-> 5-min / hourly aggregates
                                              +-> three Kafka-compatible topics
```

The agent computes CPU utilization, network deltas, and disk I/O deltas
locally, then publishes ready-to-aggregate values to Redpanda. ClickHouse
stores raw samples (6h TTL), 5-minute aggregates (7d TTL), and hourly
aggregates (3 months TTL). No window functions or delta computation in
ClickHouse — MVs are simple `sum()`/`avg()` GROUP BY.

## Motivation

Classical OpenStack metrics stack is Ceilometer, Gnocchi (+ Cloudkitty for rating optionally).
It was hard for me to find out what are pros and cons of this stack. So I decided to implement my own metrics stack for OpenStack.

## Architecture

### What the agent sends

All delta/utilization computation happens in the agent, not ClickHouse:

- **CPU**: `cpu_pct` — computed from consecutive `cpu_time_ns` readings
- **Network**: `rx_bytes`, `tx_bytes` — per-interval deltas (not cumulative)
- **Disk I/O**: `read_bytes`, `write_bytes`, `read_requests`, `write_requests` — per-interval deltas
- **Gauges**: `memory_actual_bytes`, `memory_rss_bytes`, `memory_usable_bytes`, `capacity_bytes`, `allocation_bytes`, `physical_bytes` — sent as-is

The first sample after agent start is skipped (no previous value to diff). No `is_baseline` flag — the agent simply doesn't emit until it has two consecutive readings.

### Aggregation tiers

| Tier | Granularity | TTL | Source | Dashboard use |
|------|-------------|-----|--------|---------------|
| Raw samples | ~1 min | 6 hours | Kafka engine MVs | 6h view |
| 5-min aggregates | 5 min | 7 days | Refreshable MV every 5 min | 12h/24h view |
| Hourly aggregates | 1 hour | 3 months | Refreshable MV every 10 min | 7d view, billing |

All tiers aggregate directly from raw samples (parallel, not chained).

## OVN agent

`ovn_agent.py` is the production collector. One poll uses libvirt
`getAllDomainStats()` to retrieve CPU, balloon-memory, block-device, and vNIC
statistics for all running domains. It maps each interface to a Neutron port
and network:

1. Read `virtualport/parameters@interfaceid` from the libvirt domain XML.
2. If absent, match `tap<port-UUID-prefix>` against NBDB
   `Logical_Switch_Port.name` and `external_ids["neutron:device_id"]`.
3. Read `external_ids["neutron:network_name"]` to derive `network_uuid`.

The agent talks directly to OVN Northbound through `ovsdbapp` IDL. It does not
invoke `virsh`, `ovs-vsctl`, or `ovn-nbctl`. Only active Nova compute ports
(`neutron:device_owner=compute:nova`) are collected.

### Docker image

```bash
docker build -t ovn-traffic-agent:latest .
```

### Run on a compute node

```bash
docker compose -f deploy/docker-compose.agent.yml up -d
```

Configure via `.env` (see `deploy/.env.example`):

```env
KAFKA_BOOTSTRAP_SERVERS=metrics.example.com:9092
KAFKA_SASL_USERNAME=agent
KAFKA_SASL_PASSWORD=agentpass
LIBVIRT_URI=qemu:///system
LIBVIRT_USERNAME=nova
LIBVIRT_PASSWORD=<password>
OVN_NB_DB=tcp:1.1.0.1:6641
REGION_NAME=RegionOne
POLL_INTERVAL=60
```

Or run directly:

```bash
python3 ovn_agent.py \
  --libvirt-uri qemu+tcp://<libvirt-host>/system \
  --libvirt-username '<username>' \
  --libvirt-password '<password>' \
  --ovn-nb-db tcp:<nbdb-host>:6641 \
  --bootstrap-servers <redpanda-host>:9092 \
  --sasl-username agent \
  --sasl-password agentpass \
  --region-name '<openstack-region>' \
  --publish-kafka
```

### Agent restart behavior

On restart, the agent has no previous counter values. The first poll
establishes baselines for CPU, network, and disk — no samples are emitted.
The second poll computes deltas normally. No data loss beyond one skipped
interval per metric.

## Server stack

```bash
docker compose -f deploy/docker-compose.server.yml up -d
```

Runs Redpanda (SASL/SCRAM-SHA-256), Redpanda Console, and ClickHouse.
Credentials via env vars (see `deploy/.env.example`).

The init container creates an `agent` SASL user with topic/group ACLs and
pre-creates the three topics with 10-min retention and 16MB segments.

## Dashboard

Lightweight React + ApexCharts dashboard querying ClickHouse HTTP API.

```bash
cd dashboard && npm install && npm run dev
```

Three pages:
- **Instance** — per-instance CPU, RAM, Disk I/O, Network charts with 6h/12h/24h/7d range toggle
- **Per Node** — aggregate disk I/O and network traffic for all instances on a host
- **Top Usage** — top 10 instances by CPU, RAM, Network, Disk (avg over 24h from hourly tables)

The Vite dev server proxies `/ch` to ClickHouse `localhost:8123`.

## Synthetic load generator

`agent.py` generates realistic metrics for development. It sends the same
delta-based payload format as `ovn_agent.py`.

```bash
docker compose -f deploy/docker-compose.server.yml up -d
pip install confluent-kafka
python3 agent.py --instances 50000 --sasl-username agent --sasl-password agentpass
```

## Pipeline layout

| Table | Role | TTL |
|-------|------|-----|
| `port_stats_queue` | Kafka source for network deltas | — |
| `port_samples` | raw per-interval RX/TX deltas | 6 hours |
| `port_volume_5min` | 5-min aggregated network volume | 7 days |
| `port_volume_hourly` | hourly aggregated network volume | 3 months |
| `port_usage` | period-to-date traffic view | — |
| `instance_stats_queue` | Kafka source for CPU/RAM | — |
| `instance_samples` | raw cpu_pct and RAM gauges | 6 hours |
| `instance_metrics_5min` | 5-min CPU/RAM aggregates | 7 days |
| `instance_metrics_hourly` | hourly CPU/RAM aggregates | 3 months |
| `disk_stats_queue` | Kafka source for disk deltas | — |
| `disk_samples` | raw disk I/O deltas and size gauges | 6 hours |
| `disk_metrics_5min` | 5-min disk aggregates | 7 days |
| `disk_metrics_hourly` | hourly disk aggregates | 3 months |

## Storage estimate (50k instances, 1-min polling)

| Tier | Size |
|------|------|
| Raw samples (6h) | ~620 MB |
| 5-min aggregates (7d) | ~7.2 GB |
| Hourly aggregates (3mo) | ~8.6 GB |
| **Total** | **~16.4 GB** |

## Notes

- Redpanda has two Kafka listeners: `localhost:9092` (external),
  `redpanda:19092` (internal compose network).
- SASL/SCRAM-SHA-256 is enabled by default. The `admin` superuser is
  bootstrapped via `RP_BOOTSTRAP_USER`. The `agent` user is created by
  the init container.
- `--region-name` is part of the ClickHouse aggregation key, allowing a
  shared deployment to report usage per OpenStack region.
- For SASL-protected libvirt, use `--libvirt-username`/`--libvirt-password`
  or `--libvirt-auth-file` (reads Nova's `credentials-default` section).
  The `LIBVIRT_PASSWORD` env var avoids process-list exposure.
