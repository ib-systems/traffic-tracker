import json
import os
import sys
import threading
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
            metrics=frozenset({"network", "disk", "ram", "cpu"}),
        )

        collector = ovn_agent.LibvirtCollector(args, FakeOVSDB())
        with mock.patch.dict(sys.modules, {"libvirt": fake_libvirt}):
            batch = collector.read()
            collector.read()

        self.assertEqual((31, 32), connection.flags)
        self.assertFalse(connection.closed)
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
        collector.close()
        self.assertTrue(connection.closed)

    def test_network_only_requests_interface_stats(self):
        connection = FakeConnection(
            {
                "net.count": 1,
                "net.0.name": "tap22222222-22",
                "net.0.rx.bytes": 300,
                "net.0.tx.bytes": 400,
            }
        )
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
            metrics=frozenset({"network"}),
        )

        with mock.patch.dict(sys.modules, {"libvirt": fake_libvirt}):
            batch = ovn_agent.LibvirtCollector(args, FakeOVSDB()).read()

        self.assertEqual((16, 32), connection.flags)
        self.assertEqual(1, len(batch.vnics))
        self.assertEqual([], batch.instances)
        self.assertEqual([], batch.disks)

    def test_reconnects_once_after_connection_failure(self):
        class FakeLibvirtError(RuntimeError):
            pass

        class FailingConnection(FakeConnection):
            def getAllDomainStats(self, flags, domain_flags):
                raise FakeLibvirtError("connection reset")

        failed = FailingConnection({})
        recovered = FakeConnection({"net.count": 0})
        connections = iter([failed, recovered])
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
            libvirtError=FakeLibvirtError,
            openReadOnly=lambda uri: next(connections),
        )
        args = SimpleNamespace(
            libvirt_username=None,
            libvirt_password=None,
            libvirt_auth_file=None,
            libvirt_uri="qemu:///system",
            metrics=frozenset({"network"}),
        )
        collector = ovn_agent.LibvirtCollector(args, FakeOVSDB())

        with mock.patch.dict(sys.modules, {"libvirt": fake_libvirt}):
            batch = collector.read()

        self.assertEqual([], batch.vnics)
        self.assertTrue(failed.closed)
        self.assertFalse(recovered.closed)
        self.assertIs(recovered, collector.connection)
        collector.close()
        self.assertTrue(recovered.closed)


class CycleTest(unittest.TestCase):
    def test_skips_first_counter_observation_then_publishes_each_family(self):
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
            refresh=lambda: None,
            resolve=lambda port_uuid: ovn_agent.PortMetadata(
                PORT_UUID, NETWORK_UUID, INSTANCE_UUID
            )
        )
        args = SimpleNamespace(
            host="compute-1",
            region_name="RegionOne",
            metrics=frozenset({"network", "disk", "ram", "cpu"}),
            publish_kafka=True,
            topic="port-stats",
            instance_topic="instance-stats",
            disk_topic="disk-stats",
        )
        producer = FakeProducer()
        counters = {}
        previous_cpu = {}
        previous_disks = {}

        first = ovn_agent.run_cycle(
            args, collector, resolver, producer, counters, previous_cpu, previous_disks
        )
        second = ovn_agent.run_cycle(
            args, collector, resolver, producer, counters, previous_cpu, previous_disks
        )

        self.assertEqual((1, 1, 1), (first.baselines, first.instance_baselines, first.disk_baselines))
        self.assertEqual((0, 0, 0), (second.baselines, second.instance_baselines, second.disk_baselines))
        self.assertEqual(
            ["port-stats", "instance-stats", "disk-stats"],
            [record[0] for record in producer.records],
        )
        self.assertTrue(all("is_baseline" not in record[2] for record in producer.records))
        self.assertIsNone(producer.records[1][2]["memory_usable_bytes"])

    def test_network_only_ignores_instance_and_disk_samples(self):
        batch = ovn_agent.CollectionBatch(
            vnics=[ovn_agent.VNICSample(INSTANCE_UUID, "tap0", PORT_UUID, 10, 20)],
            instances=[ovn_agent.InstanceSample(INSTANCE_UUID, 1_000, 2, None, None, None)],
            disks=[ovn_agent.DiskSample(INSTANCE_UUID, "vda", 1, 2, 3, 4, 5, 6, 7)],
        )
        collector = SimpleNamespace(read=lambda: batch)
        resolver = SimpleNamespace(
            refresh=lambda: None,
            resolve=lambda port_uuid: ovn_agent.PortMetadata(
                PORT_UUID, NETWORK_UUID, INSTANCE_UUID
            ),
        )
        args = SimpleNamespace(
            host="compute-1",
            region_name="RegionOne",
            metrics=frozenset({"network"}),
            publish_kafka=True,
            topic="port-stats",
            instance_topic="instance-stats",
            disk_topic="disk-stats",
        )
        producer = FakeProducer()
        counters = {}

        ovn_agent.run_cycle(args, collector, resolver, producer, counters, {}, {})
        ovn_agent.run_cycle(args, collector, resolver, producer, counters, {}, {})

        self.assertEqual(["port-stats"], [record[0] for record in producer.records])


class MetricSelectionTest(unittest.TestCase):
    def test_parses_case_and_whitespace(self):
        self.assertEqual(
            frozenset({"network", "cpu"}),
            ovn_agent.parse_metrics(" Network, CPU "),
        )

    def test_rejects_unknown_metric(self):
        with self.assertRaisesRegex(ovn_agent.argparse.ArgumentTypeError, "unknown metrics: gpu"):
            ovn_agent.parse_metrics("network,gpu")

    def test_ram_requires_cpu(self):
        with self.assertRaisesRegex(ovn_agent.argparse.ArgumentTypeError, "ram requires cpu"):
            ovn_agent.parse_metrics("ram")


class ExceptionFormattingTest(unittest.TestCase):
    def test_falls_back_when_third_party_exception_str_fails(self):
        class BrokenError(RuntimeError):
            def __str__(self):
                raise AttributeError("missing msg")

        self.assertEqual("BrokenError", ovn_agent.exception_text(BrokenError()))


class FleetConfigTest(unittest.TestCase):
    def test_yaml_creates_fleet_and_environment_is_ignored(self):
        config = {
            "interval": 60,
            "metrics": ["network"],
            "kafka": {
                "sasl_username": "agent",
                "sasl_password": "agentpass",
            },
            "ovn": {"sb_db": "tcp:192.0.2.10:6642"},
            "libvirt": {"username": "nova", "password": "libvirtpass"},
            "nodes": [
                {
                    "host": "compute-1",
                    "libvirt_uri": "qemu+tcp://192.0.2.101/system",
                    "ovn_chassis": "chassis-1",
                },
                {
                    "host": "compute-2",
                    "libvirt_uri": "qemu+tcp://192.0.2.102/system",
                    "ovn_chassis": "chassis-2",
                },
            ],
        }
        fake_yaml = SimpleNamespace(
            safe_load=lambda stream: config,
            YAMLError=ValueError,
        )
        with (
            mock.patch.dict(
                os.environ,
                {"AGENT_WORKERS": "99", "OVN_SB_DB": "tcp:wrong:6642"},
                clear=True,
            ),
            mock.patch.dict(sys.modules, {"yaml": fake_yaml}),
            mock.patch("builtins.open", mock.mock_open(read_data="ignored")),
        ):
            args = ovn_agent.parse_args(["--config", "/config/agent.yml"])

        self.assertEqual(60, args.interval)
        self.assertFalse(hasattr(args, "workers"))
        self.assertEqual("tcp:192.0.2.10:6642", args.ovn_sb_db)
        self.assertEqual(frozenset({"network"}), args.metrics)
        self.assertEqual(["compute-1", "compute-2"], [host.host for host in args.hosts])

    def test_parses_multiple_hosts_with_shared_credentials(self):
        args = SimpleNamespace(
            host="fallback",
            libvirt_uri="qemu:///system",
            ovn_chassis=None,
            region_name="RegionOne",
            libvirt_username="nova",
            libvirt_password="secret",
            libvirt_auth_file=None,
            libvirt_auth_section="credentials-default",
            metrics=frozenset({"network"}),
        )
        hosts = ovn_agent.parse_host_configs(
            {
                "nodes": [
                    {
                        "host": "compute-1",
                        "libvirt_uri": "qemu+tcp://192.0.2.101/system",
                        "ovn_chassis": "chassis-1",
                    },
                    {
                        "host": "compute-2",
                        "libvirt_uri": "qemu+tcp://192.0.2.102/system",
                        "ovn_chassis": "chassis-2",
                    },
                ]
            },
            args,
        )

        self.assertEqual(["compute-1", "compute-2"], [host.host for host in hosts])
        self.assertEqual("nova", hosts[1].libvirt_username)
        self.assertEqual("secret", hosts[1].libvirt_password)

    def test_rejects_duplicate_host_names(self):
        args = SimpleNamespace(
            host="fallback",
            libvirt_uri="qemu:///system",
            ovn_chassis="chassis-1",
            region_name="RegionOne",
            libvirt_username=None,
            libvirt_password=None,
            libvirt_auth_file=None,
            libvirt_auth_section="credentials-default",
            metrics=frozenset({"network"}),
        )
        with self.assertRaisesRegex(ovn_agent.CommandError, "duplicate node host"):
            ovn_agent.parse_host_configs(
                {"nodes": [{"host": "compute-1"}, {"host": "compute-1"}]},
                args,
            )


class OVSDBClientTest(unittest.TestCase):
    def test_port_binding_snapshot_reads_idl_rows(self):
        chassis = SimpleNamespace(name="compute-1")
        row = SimpleNamespace(
            logical_port=PORT_UUID,
            external_ids={"neutron:device_id": INSTANCE_UUID},
            chassis=[chassis],
            up=[True],
            type="",
        )
        client = object.__new__(ovn_agent.OVSDBClients)
        client.sb = SimpleNamespace(
            tables={"Port_Binding": SimpleNamespace(rows={"row-id": row})}
        )

        self.assertEqual(
            [
                ovn_agent.PortBinding(
                    port_uuid=PORT_UUID,
                    external_ids={"neutron:device_id": INSTANCE_UUID},
                    chassis_names=frozenset({"compute-1"}),
                    up=True,
                    port_type="",
                )
            ],
            client.list_port_bindings(),
        )


class ResolverTest(unittest.TestCase):
    def test_refresh_keeps_only_active_local_nova_bindings(self):
        valid = ovn_agent.PortBinding(
            port_uuid=PORT_UUID,
            external_ids={
                "neutron:device_id": INSTANCE_UUID,
                "neutron:device_owner": "compute:nova",
                "neutron:network_name": f"neutron-{NETWORK_UUID}",
            },
            chassis_names=frozenset({"compute-1"}),
            up=True,
            port_type="",
        )
        wrong_chassis = ovn_agent.PortBinding(
            port_uuid="44444444-4444-4444-8444-444444444444",
            external_ids=valid.external_ids,
            chassis_names=frozenset({"compute-2"}),
            up=True,
            port_type="",
        )
        down = ovn_agent.PortBinding(
            port_uuid="55555555-5555-4555-8555-555555555555",
            external_ids=valid.external_ids,
            chassis_names=frozenset({"compute-1"}),
            up=False,
            port_type="",
        )
        ovsdb = SimpleNamespace(list_port_bindings=lambda: [valid, wrong_chassis, down])
        resolver = ovn_agent.OVNResolver(ovsdb, "compute-1")

        resolver.refresh()

        self.assertEqual(
            ovn_agent.PortMetadata(PORT_UUID, NETWORK_UUID, INSTANCE_UUID),
            resolver.resolve(PORT_UUID),
        )
        self.assertIsNone(resolver.resolve(wrong_chassis.port_uuid))
        self.assertIsNone(resolver.resolve(down.port_uuid))

    def test_fleet_snapshot_is_indexed_by_chassis(self):
        binding = ovn_agent.PortBinding(
            port_uuid=PORT_UUID,
            external_ids={
                "neutron:device_id": INSTANCE_UUID,
                "neutron:device_owner": "compute:nova",
                "neutron:network_name": f"neutron-{NETWORK_UUID}",
            },
            chassis_names=frozenset({"compute-1"}),
            up=True,
            port_type="",
        )
        snapshot = ovn_agent.OVNBindingSnapshot([binding])
        local = ovn_agent.OVNResolver(snapshot, "compute-1")
        remote = ovn_agent.OVNResolver(snapshot, "compute-2")

        local.refresh()
        remote.refresh()

        self.assertEqual(
            ovn_agent.PortMetadata(PORT_UUID, NETWORK_UUID, INSTANCE_UUID),
            local.resolve(PORT_UUID),
        )
        self.assertIsNone(remote.resolve(PORT_UUID))


class FleetCycleTest(unittest.TestCase):
    def test_reads_sbdb_once_and_isolates_failed_host(self):
        class EmptyCollector:
            def __init__(self, fail=False):
                self.fail = fail
                self.ovsdb = None
                self.thread_ids = []

            def read(self):
                self.thread_ids.append(threading.get_ident())
                if self.fail:
                    raise ovn_agent.CommandError("unreachable")
                return ovn_agent.CollectionBatch([], [], [])

            def close(self):
                self.thread_ids.append(threading.get_ident())

        class FakeSBDB:
            def __init__(self):
                self.calls = 0

            def list_port_bindings(self):
                self.calls += 1
                return []

        def runtime(host, fail=False):
            args = SimpleNamespace(
                host=host,
                region_name="RegionOne",
                metrics=frozenset({"network"}),
                publish_kafka=False,
                topic="port-stats",
                instance_topic="instance-stats",
                disk_topic="disk-stats",
            )
            return ovn_agent.NodeRuntime(
                args,
                EmptyCollector(fail),
                ovn_agent.OVNResolver(None, host),
            )

        good = runtime("compute-1")
        failed = runtime("compute-2", fail=True)
        sbdb = FakeSBDB()
        snapshots = ovn_agent.SnapshotStore(
            ovn_agent.OVNBindingSnapshot(sbdb.list_port_bindings())
        )
        workers = [
            ovn_agent.HostWorker(good, None, snapshots, interval=60, once=True),
            ovn_agent.HostWorker(failed, None, snapshots, interval=60, once=True),
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        self.assertEqual(1, sbdb.calls)
        self.assertIsInstance(good.collector.ovsdb, ovn_agent.OVNBindingSnapshot)
        self.assertIsInstance(failed.collector.ovsdb, ovn_agent.OVNBindingSnapshot)
        self.assertEqual(1, len(set(good.collector.thread_ids)))
        self.assertNotEqual(threading.get_ident(), good.collector.thread_ids[0])

    def test_host_keeps_the_same_thread_across_cycles_and_shutdown(self):
        class TrackingCollector:
            def __init__(self):
                self.thread_ids = []
                self.ovsdb = None
                self.two_reads = threading.Event()

            def read(self):
                self.thread_ids.append(threading.get_ident())
                if len(self.thread_ids) >= 2:
                    self.two_reads.set()
                return ovn_agent.CollectionBatch([], [], [])

            def close(self):
                self.thread_ids.append(threading.get_ident())

        args = SimpleNamespace(
            host="compute-1",
            region_name="RegionOne",
            metrics=frozenset(),
            publish_kafka=False,
            topic="port-stats",
            instance_topic="instance-stats",
            disk_topic="disk-stats",
        )
        runtime = ovn_agent.NodeRuntime(args, TrackingCollector(), None)
        worker = ovn_agent.HostWorker(
            runtime,
            None,
            ovn_agent.SnapshotStore(None),
            interval=0.001,
        )
        worker.start()
        try:
            self.assertTrue(runtime.collector.two_reads.wait(1))
        finally:
            worker.stop()
            worker.join()

        self.assertGreaterEqual(len(runtime.collector.thread_ids), 3)
        self.assertEqual(1, len(set(runtime.collector.thread_ids)))
        self.assertNotEqual(threading.get_ident(), runtime.collector.thread_ids[0])

    def test_slow_host_does_not_delay_other_host_intervals(self):
        class SlowCollector:
            def __init__(self):
                self.release = threading.Event()
                self.started = threading.Event()
                self.ovsdb = None

            def read(self):
                self.started.set()
                self.release.wait(1)
                return ovn_agent.CollectionBatch([], [], [])

            def close(self):
                pass

        class FastCollector:
            def __init__(self):
                self.reads = 0
                self.two_reads = threading.Event()
                self.ovsdb = None

            def read(self):
                self.reads += 1
                if self.reads >= 2:
                    self.two_reads.set()
                return ovn_agent.CollectionBatch([], [], [])

            def close(self):
                pass

        def runtime(host, collector):
            return ovn_agent.NodeRuntime(
                SimpleNamespace(
                    host=host,
                    region_name="RegionOne",
                    metrics=frozenset(),
                    publish_kafka=False,
                    topic="port-stats",
                    instance_topic="instance-stats",
                    disk_topic="disk-stats",
                ),
                collector,
                None,
            )

        slow = runtime("slow", SlowCollector())
        fast = runtime("fast", FastCollector())
        snapshots = ovn_agent.SnapshotStore(None)
        workers = [
            ovn_agent.HostWorker(slow, None, snapshots, interval=60),
            ovn_agent.HostWorker(fast, None, snapshots, interval=0.001),
        ]
        for worker in workers:
            worker.start()
        try:
            self.assertTrue(slow.collector.started.wait(1))
            self.assertTrue(fast.collector.two_reads.wait(1))
        finally:
            slow.collector.release.set()
            for worker in workers:
                worker.stop()
            for worker in workers:
                worker.join()

        self.assertGreaterEqual(fast.collector.reads, 2)


class QueuedPublisherTest(unittest.TestCase):
    def test_serializes_kafka_calls_in_publisher_thread(self):
        class TrackingProducer(FakeProducer):
            def __init__(self):
                super().__init__()
                self.thread_ids = []

            def produce(self, topic, key, value):
                self.thread_ids.append(threading.get_ident())
                super().produce(topic, key, value)

            def poll(self, timeout):
                self.thread_ids.append(threading.get_ident())
                return 0

            def flush(self, timeout):
                self.thread_ids.append(threading.get_ident())
                return 0

        raw_producer = TrackingProducer()
        publisher = ovn_agent.QueuedPublisher(raw_producer)
        try:
            publisher.produce(
                "port-stats",
                key=PORT_UUID,
                value=json.dumps({"rx_bytes": 1}).encode(),
            )
            self.assertEqual(0, publisher.flush(1))
        finally:
            publisher.close(1)

        self.assertEqual(1, len(raw_producer.records))
        self.assertEqual(1, len(set(raw_producer.thread_ids)))
        self.assertNotEqual(threading.get_ident(), raw_producer.thread_ids[0])


if __name__ == "__main__":
    unittest.main()
