#!/usr/bin/env python3
"""Generate realistic, slowly changing libvirt-like metrics for development.

The simulator publishes the same three JSON record shapes as ``ovn_agent.py``:
one port, one instance, and one disk record per simulated instance per cycle.
Counters only increase while CPU, RAM, and disk-size gauges move gradually.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer


GIB = 1024**3


@dataclass
class InstanceState:
    instance_uuid: str
    port_uuid: str
    network_uuid: str
    host: str
    vcpus: int
    rx_bytes: int
    tx_bytes: int
    cpu_time_ns: int
    cpu_utilization_pct: float
    memory_actual_bytes: int
    memory_rss_bytes: int
    disk_read_bytes: int
    disk_write_bytes: int
    disk_read_requests: int
    disk_write_requests: int
    disk_capacity_bytes: int
    disk_allocation_bytes: int
    disk_physical_bytes: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--sasl-username", default=os.getenv("KAFKA_SASL_USERNAME"))
    parser.add_argument("--sasl-password", default=os.getenv("KAFKA_SASL_PASSWORD"))
    parser.add_argument("--instances", type=int, default=10)
    parser.add_argument("--hosts", type=int, default=3)
    parser.add_argument("--networks", type=int, default=3)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Stop after this many cycles; 0 runs until interrupted",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--region-name", default="poc-region-1")
    args = parser.parse_args()
    if args.instances <= 0 or args.hosts <= 0 or args.networks <= 0:
        parser.error("--instances, --hosts, and --networks must be positive")
    if args.interval <= 0 or args.cycles < 0:
        parser.error("--interval must be positive and --cycles non-negative")
    return args


def build_fleet(instances: int, hosts: int, networks: int, rng: random.Random) -> list[InstanceState]:
    fleet = []
    for index in range(instances):
        actual = rng.choice((2, 4, 8)) * GIB
        rss = int(actual * rng.uniform(0.45, 0.75))
        capacity = rng.choice((40, 80, 120)) * GIB
        allocation = int(capacity * rng.uniform(0.25, 0.65))
        fleet.append(
            InstanceState(
                instance_uuid=f"aaaaaaaa-0000-4000-8000-{index + 1:012x}",
                port_uuid=f"bbbbbbbb-0000-4000-8000-{index + 1:012x}",
                network_uuid=f"cccccccc-0000-4000-8000-{index % networks + 1:012x}",
                host=f"poc-compute-{index % hosts:02d}",
                vcpus=rng.choice((1, 2, 4, 8)),
                rx_bytes=rng.randint(10, 500) * 1024**2,
                tx_bytes=rng.randint(10, 500) * 1024**2,
                cpu_time_ns=rng.randint(60, 3600) * 1_000_000_000,
                cpu_utilization_pct=rng.uniform(5.0, 75.0),
                memory_actual_bytes=actual,
                memory_rss_bytes=rss,
                disk_read_bytes=rng.randint(1, 50) * GIB,
                disk_write_bytes=rng.randint(1, 50) * GIB,
                disk_read_requests=rng.randint(10_000, 500_000),
                disk_write_requests=rng.randint(10_000, 500_000),
                disk_capacity_bytes=capacity,
                disk_allocation_bytes=allocation,
                disk_physical_bytes=allocation,
            )
        )
    return fleet


def advance(state: InstanceState, interval: float, rng: random.Random) -> dict[str, int]:
    rx_delta = rng.randint(64 * 1024, 4 * 1024**2)
    tx_delta = rng.randint(64 * 1024, 4 * 1024**2)
    state.rx_bytes += rx_delta
    state.tx_bytes += tx_delta

    state.cpu_utilization_pct = min(
        95.0, max(1.0, state.cpu_utilization_pct + rng.uniform(-4.0, 4.0))
    )
    state.cpu_time_ns += int(
        interval
        * 1_000_000_000
        * state.vcpus
        * state.cpu_utilization_pct
        / 100.0
    )

    rss_step = rng.randint(-32, 32) * 1024**2
    state.memory_rss_bytes = min(
        int(state.memory_actual_bytes * 0.95),
        max(int(state.memory_actual_bytes * 0.20), state.memory_rss_bytes + rss_step),
    )

    read_ops = rng.randint(5, 250)
    write_ops = rng.randint(5, 250)
    read_delta = read_ops * rng.randint(4, 128) * 1024
    write_delta = write_ops * rng.randint(4, 128) * 1024
    state.disk_read_requests += read_ops
    state.disk_write_requests += write_ops
    state.disk_read_bytes += read_delta
    state.disk_write_bytes += write_delta
    state.disk_allocation_bytes = min(
        state.disk_capacity_bytes,
        max(0, state.disk_allocation_bytes + write_delta - rng.randint(0, write_delta)),
    )
    state.disk_physical_bytes = state.disk_allocation_bytes
    return {
        "rx_delta": rx_delta, "tx_delta": tx_delta,
        "disk_read_delta": read_delta, "disk_write_delta": write_delta,
        "disk_read_ops_delta": read_ops, "disk_write_ops_delta": write_ops,
    }


def records(state: InstanceState, ts: int, region_name: str, deltas: dict[str, int]) -> list[tuple[str, str, dict[str, Any]]]:
    common = {
        "ts": ts,
        "host": state.host,
        "region_name": region_name,
        "instance_uuid": state.instance_uuid,
    }
    memory_usable = max(0, state.memory_actual_bytes - state.memory_rss_bytes)
    return [
        (
            "port-stats",
            state.port_uuid,
            common
            | {
                "port_uuid": state.port_uuid,
                "network_uuid": state.network_uuid,
                "rx_bytes": deltas["rx_delta"],
                "tx_bytes": deltas["tx_delta"],
            },
        ),
        (
            "instance-stats",
            state.instance_uuid,
            common
            | {
                "cpu_pct": round(state.cpu_utilization_pct, 2),
                "memory_actual_bytes": state.memory_actual_bytes,
                "memory_rss_bytes": state.memory_rss_bytes,
                "memory_usable_bytes": memory_usable,
            },
        ),
        (
            "disk-stats",
            f"{state.instance_uuid}/vda",
            common
            | {
                "device": "vda",
                "read_bytes": deltas["disk_read_delta"],
                "write_bytes": deltas["disk_write_delta"],
                "read_requests": deltas["disk_read_ops_delta"],
                "write_requests": deltas["disk_write_ops_delta"],
                "capacity_bytes": state.disk_capacity_bytes,
                "allocation_bytes": state.disk_allocation_bytes,
                "physical_bytes": state.disk_physical_bytes,
            },
        ),
    ]


def produce(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=json.dumps(value).encode())
            return
        except BufferError:
            producer.poll(0.5)


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    fleet = build_fleet(args.instances, args.hosts, args.networks, rng)
    producer_conf = {"bootstrap.servers": args.bootstrap_servers, "linger.ms": 50}
    if args.sasl_username and args.sasl_password:
        producer_conf.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": args.sasl_username,
            "sasl.password": args.sasl_password,
        })
    producer = Producer(producer_conf)
    print(
        f"{len(fleet)} instances / {args.hosts} hosts / {args.networks} networks "
        f"-> port-stats, instance-stats, disk-stats @ {args.bootstrap_servers}, "
        f"every {args.interval:g}s"
    )

    cycle = 0
    while args.cycles == 0 or cycle < args.cycles:
        started = time.time()
        ts = int(started)
        for state in fleet:
            deltas = advance(state, args.interval, rng)
            if cycle == 0:
                continue
            for topic, key, value in records(state, ts, args.region_name, deltas):
                produce(producer, topic, key, value)
        pending = producer.flush(30)
        if pending:
            raise RuntimeError(f"Redpanda timed out with {pending} undelivered messages")
        cycle += 1
        print(
            f"cycle {cycle} @ {ts}: {len(fleet) * 3} records, "
            f"skipped={'yes' if cycle == 1 else 'no'}, took={time.time() - started:.2f}s",
            flush=True,
        )
        if args.cycles == 0 or cycle < args.cycles:
            time.sleep(max(0.0, args.interval - (time.time() - started)))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
