#!/usr/bin/env python3
"""PoC stateless agent: ships cumulative per-port rx/tx counters to Kafka.

Deterministic fleet: UUIDs are derived from indices (1 port per
instance, instances spread over simulated hypervisors), so restarts and
ClickHouse queries always reference the same ports. Counters only grow,
except occasional resets simulating tap recreation (reboot / live
migration).
"""
import json
import random
import time

from confluent_kafka import Producer

BROKER = "localhost:9092"
TOPIC = "port-stats"
INTERVAL = 60            # seconds between polls (prod: 60)
NUM_PORTS = 40_000      # 1 port per instance
NUM_HOSTS = 200         # simulated hypervisors
RESET_CHANCE = 0.0001   # per-port per-cycle probability of counter reset

# port i lives on instance i; UUIDs are pure functions of the index
FLEET = [
    (
        f"aaaaaaaa-0000-4000-8000-{i:012x}",
        f"bbbbbbbb-0000-4000-8000-{i:012x}",
        f"poc-compute-{i % NUM_HOSTS:03d}",
    )
    for i in range(NUM_PORTS)
]


def main():
    producer = Producer({"bootstrap.servers": BROKER, "linger.ms": 50})
    counters = [[0, 0] for _ in range(NUM_PORTS)]
    print(f"{NUM_PORTS} ports / {NUM_PORTS // 2} instances / {NUM_HOSTS} hosts "
          f"-> {TOPIC} @ {BROKER}, every {INTERVAL}s")

    while True:
        start = time.time()
        now = int(start)
        resets = 0
        for i, (instance_uuid, port_uuid, host) in enumerate(FLEET):
            c = counters[i]
            if random.random() < RESET_CHANCE:
                c[0] = c[1] = 0
                resets += 1
            c[0] += random.randint(1000, 5000)
            c[1] += random.randint(1000, 5000)

            while True:
                try:
                    producer.produce(
                        TOPIC,
                        key=port_uuid,
                        value=json.dumps({
                            "ts": now,
                            "host": host,
                            "instance_uuid": instance_uuid,
                            "port_uuid": port_uuid,
                            "rx": c[0],
                            "tx": c[1],
                        }).encode(),
                    )
                    break
                except BufferError:       # local queue full: drain and retry
                    producer.poll(0.5)
            if i % 10_000 == 0:
                producer.poll(0)
        producer.flush(30)
        took = time.time() - start
        print(f"cycle @ {now}: {NUM_PORTS} samples in {took:.1f}s, {resets} resets")
        time.sleep(max(0.0, INTERVAL - took))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
