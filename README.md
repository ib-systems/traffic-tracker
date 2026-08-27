# Compute metrics and per-port traffic accounting PoC

```text
libvirt bulk domain stats -> ovn_agent.py -> Redpanda -> ClickHouse
                               |              |          |
                               +-> OVN SBDB    |          +-> 5-min / hourly aggregates
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
2. If absent, match `tap<port-UUID-prefix>` against SBDB
   `Port_Binding.logical_port` and `external_ids["neutron:device_id"]`.
3. Select only an `up` binding whose primary `chassis` is this compute node.
4. Read `external_ids["neutron:network_name"]` to derive `network_uuid`.

The agent talks directly to OVN Southbound through `ovsdbapp` IDL. It refreshes
one in-memory `Port_Binding` snapshot per poll and does not invoke `virsh`,
`ovs-vsctl`, or `ovn-sbctl`. Only active Nova compute ports
(`neutron:device_owner=compute:nova`) bound to the configured chassis are
collected. During live migration, only the primary `chassis` is accepted, so an
`additional_chassis` does not emit duplicate usage.

### Metric selection

The YAML `metrics` list controls which libvirt stat groups are requested and
which Kafka topics receive samples. Supported values are `network`, `disk`,
`ram`, and `cpu`.

```yaml
# Everything (default)
metrics: [network, disk, ram, cpu]

# Network traffic only
metrics: [network]
```

Values are case-insensitive, whitespace is ignored, and at least one metric is
required. RAM currently shares the `instance-stats` record with CPU, whose
`cpu_pct` field is required, so selecting `ram` also requires `cpu`. CPU can be
selected without RAM; its memory fields are then `null`. OVN SBDB settings are
required only when `network` is enabled.

### Multi-host YAML configuration

Use a YAML config when one long-running agent scrapes multiple remote libvirt
hosts. Every configured host gets one dedicated, long-lived thread. That thread
opens its own read-only libvirt connection on the first scrape, reuses it for
later scrapes, reconnects it in the same thread after a libvirt failure, and
closes it during shutdown. Each host schedules its own polls, so a slow or
failed host does not delay the other hosts.

The main thread refreshes one immutable OVN SBDB snapshot per interval and the
host threads use the latest available snapshot. Counter state remains isolated
per host. Ready metric records go to a shared queue immediately; one publisher
thread owns the Kafka client and sends records to Redpanda without waiting for
the rest of the fleet.

```yaml
interval: 60
metrics: [network, disk, ram, cpu]
region_name: RegionOne
publish_kafka: true

kafka:
  bootstrap_servers: metrics.example.com:9092
  sasl_username: agent
  sasl_password: replace-me

ovn:
  sb_db: tcp:192.0.2.10:6642,tcp:192.0.2.11:6642,tcp:192.0.2.12:6642

libvirt:
  username: nova
  password: replace-me

nodes:
  - host: compute-1
    libvirt_uri: qemu+tcp://192.0.2.101/system
    ovn_chassis: compute-1
  - host: compute-2
    libvirt_uri: qemu+tcp://192.0.2.102/system
    ovn_chassis: compute-2
```

The `compute-N` names and `192.0.2.0/24` addresses are documentation-only
placeholders; replace them with the deployment's actual hosts and addresses.

To scale across multiple agent processes or Kubernetes deployments, give each
agent a different YAML file containing a disjoint subset of `nodes`. Do not put
the same node in two active agents, because both would publish its metrics.

See `deploy/agent.example.yml` for the complete example. Start it with:

```bash
cp deploy/agent.example.yml deploy/agent.yml
# Replace credentials and node addresses in deploy/agent.yml.
python3 ovn_agent.py --config deploy/agent.yml
```

The agent does not read configuration from environment variables. `--config`
is mandatory, and explicit command-line arguments can override YAML for
one-off diagnostics. Because the YAML contains credentials, mount the complete
file from a Kubernetes Secret rather than a ConfigMap.

### Docker image

```bash
docker build -t ovn-traffic-agent:latest .
```

### Run the fleet agent with Docker Compose

```bash
cp deploy/agent.example.yml deploy/agent.yml
# Edit deploy/agent.yml first; it is gitignored because it contains secrets.
docker compose -f deploy/docker-compose.agent.yml up -d
```

Each node's `ovn_chassis` must equal its Open_vSwitch
`external_ids:system-id`, which is also the `Chassis.name` referenced by SBDB
`Port_Binding.chassis`.

Or run directly:

```bash
python3 ovn_agent.py --config deploy/agent.yml
```

### Run the fleet agent in Kubernetes

The Kubernetes Deployment mounts the complete YAML configuration from a
Secret, runs one collector replica, and uses `Recreate` upgrades to prevent
duplicate collection:

```bash
kubectl -n metrics apply -f deploy/kubernetes/deployment.yml
```

See `deploy/kubernetes/README.md` for Secret creation, deployment, configuration
updates, private-registry setup, and fleet splitting.

### Agent restart behavior

On restart, the agent has no previous counter values. The first poll
establishes baselines for CPU, network, and disk — no samples are emitted.
The second poll computes deltas normally. No data loss beyond one skipped
interval per metric.

## Server stack

```bash
cp deploy/.env.example deploy/.env
# Edit deploy/.env and replace the example server credentials first.
docker compose --env-file deploy/.env -f deploy/docker-compose.server.yml up -d
```

Runs Redpanda (SASL/SCRAM-SHA-256), Redpanda Console, and ClickHouse.
Each server service also loads `deploy/.env` as its container environment.
`--env-file` is still required because Redpanda addresses and credentials are
expanded by Docker Compose before containers start. To use another service env
file, set `SERVER_ENV_FILE` to a path relative to `deploy/`.

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
docker compose --env-file deploy/.env -f deploy/docker-compose.server.yml up -d
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
  overrides or set `libvirt.username`/`libvirt.password` in YAML. Alternatively,
  set `libvirt.auth_file` in YAML to read Nova's `credentials-default` section.
