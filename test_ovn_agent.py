import json
import sys
import types
import unittest
from types import SimpleNamespace
from unittest import mock

import ovn_agent


INSTANCE_UUID = "11111111-1111-4111-8111-111111111111"
PORT_UUID = "22222222-2222-4222-8222-222222222222"
NETWORK_UUID = "33333333-3333-4333-8333-333333333333"


class FakeDomain:
    def UUIDString(self):
        return INSTANCE_UUID

    def XMLDesc(self, flags):
        return f"""
        <domain>
          <devices>
            <interface>
              <target dev="tap22222222-22"/>
              <virtualport><parameters interfaceid="{PORT_UUID}"/></virtualport>
            </interface>
          </devices>
        </domain>
        """


class FakeConnection:
    def __init__(self, stats):
        self.stats = stats
        self.flags = None
        self.closed = False

    def getAllDomainStats(self, flags, domain_flags):
        self.flags = (flags, domain_flags)
        return [(FakeDomain(), self.stats)]

    def close(self):
        self.closed = True


class FakeOVSDB:
    def port_ids_for_taps(self, unresolved):
        return {}


class FakeProducer:
    def __init__(self):
        self.records = []

    def produce(self, topic, key, value):
        self.records.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


class CollectorTest(unittest.TestCase):
    def test_bulk_query_extracts_network_cpu_ram_and_disk(self):
        stats = {
            "cpu.time": 12_000_000_000,
            "vcpu.current": 4,
            "balloon.current": 2_048,
            "balloon.rss": 1_024,
            "balloon.usable": 512,
            "block.count": 1,
            "block.0.name": "vda",
            "block.0.rd.bytes": 100,
            "block.0.wr.bytes": 200,
            "block.0.rd.reqs": 3,
            "block.0.wr.reqs": 4,
            "block.0.capacity": 10_000,
            "block.0.allocation": 5_000,
            "block.0.physical": -1,
            "net.count": 1,
            "net.0.name": "tap22222222-22",
            "net.0.rx.bytes": 300,
            "net.0.tx.bytes": 400,
        }
        connection = FakeConnection(stats)
        fake_libvirt = types.SimpleNamespace(
            VIR_DOMAIN_STATS_CPU_TOTAL=1,
            VIR_DOMAIN_STATS_VCPU=2,
            VIR_DOMAIN_STATS_BALLOON=4,
            VIR_DOMAIN_STATS_BLOCK=8,
            VIR_DOMAIN_STATS_INTERFACE=16,
            VIR_CONNECT_GET_ALL_DOMAINS_STATS_RUNNING=32,
            VIR_CONNECT_RO=64,
            VIR_CRED_AUTHNAME=1,
            VIR_CRED_PASSPHRASE=2,
            libvirtError=RuntimeError,
            openReadOnly=lambda uri: connection,
        )
        args = SimpleNamespace(
            libvirt_username=None,
            libvirt_password=None,
            libvirt_auth_file=None,
            libvirt_uri="qemu:///system",
        )

        with mock.patch.dict(sys.modules, {"libvirt": fake_libvirt}):
            batch = ovn_agent.LibvirtCollector(args, FakeOVSDB()).read()

        self.assertEqual((31, 32), connection.flags)
        self.assertTrue(connection.closed)
        self.assertEqual(
            [ovn_agent.VNICSample(INSTANCE_UUID, "tap22222222-22", PORT_UUID, 300, 400)],
            batch.vnics,
        )
        self.assertEqual(2_048 * 1024, batch.instances[0].memory_actual_bytes)
        self.assertEqual(1_024 * 1024, batch.instances[0].memory_rss_bytes)
        self.assertEqual(512 * 1024, batch.instances[0].memory_usable_bytes)
        self.assertEqual("vda", batch.disks[0].device)
        self.assertEqual(100, batch.disks[0].read_bytes)
        self.assertIsNone(batch.disks[0].physical_bytes)


class CycleTest(unittest.TestCase):
    def test_publishes_each_family_and_marks_only_first_cycle_as_baseline(self):
        batch = ovn_agent.CollectionBatch(
            vnics=[ovn_agent.VNICSample(INSTANCE_UUID, "tap0", PORT_UUID, 10, 20)],
            instances=[
                ovn_agent.InstanceSample(
                    INSTANCE_UUID, 1_000, 2, 4_096, 2_048, None
                )
            ],
            disks=[
                ovn_agent.DiskSample(
                    INSTANCE_UUID, "vda", 100, 200, 3, 4, 1_000, 500, None
                )
            ],
        )
        collector = SimpleNamespace(read=lambda: batch)
        resolver = SimpleNamespace(
            resolve=lambda port_uuid: ovn_agent.PortMetadata(
                PORT_UUID, NETWORK_UUID, INSTANCE_UUID
            )
        )
        args = SimpleNamespace(
            host="compute-1",
            region_name="RegionOne",
            publish_kafka=True,
            topic="port-stats",
            instance_topic="instance-stats",
            disk_topic="disk-stats",
        )
        producer = FakeProducer()
        counters = {}
        seen_instances = set()
        seen_disks = set()

        first = ovn_agent.run_cycle(
            args, collector, resolver, producer, counters, seen_instances, seen_disks
        )
        second = ovn_agent.run_cycle(
            args, collector, resolver, producer, counters, seen_instances, seen_disks
        )

        self.assertEqual((1, 1, 1), (first.baselines, first.instance_baselines, first.disk_baselines))
        self.assertEqual((0, 0, 0), (second.baselines, second.instance_baselines, second.disk_baselines))
        self.assertEqual(
            ["port-stats", "instance-stats", "disk-stats"] * 2,
            [record[0] for record in producer.records],
        )
        self.assertTrue(all(record[2]["is_baseline"] for record in producer.records[:3]))
        self.assertTrue(all(not record[2]["is_baseline"] for record in producer.records[3:]))
        self.assertIsNone(producer.records[1][2]["memory_usable_bytes"])


if __name__ == "__main__":
    unittest.main()
