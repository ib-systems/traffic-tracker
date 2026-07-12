import random
import unittest

import agent


class SimulatorTest(unittest.TestCase):
    def test_fleet_is_deterministic_and_has_requested_shape(self):
        first = agent.build_fleet(10, 3, 3, random.Random(42))
        second = agent.build_fleet(10, 3, 3, random.Random(42))

        self.assertEqual(first, second)
        self.assertEqual(10, len(first))
        self.assertEqual(3, len({state.host for state in first}))
        self.assertEqual(3, len({state.network_uuid for state in first}))
        self.assertEqual(10, len({state.instance_uuid for state in first}))

    def test_advance_keeps_counters_monotonic_and_gauges_bounded(self):
        state = agent.build_fleet(1, 1, 1, random.Random(7))[0]
        before = (
            state.rx_bytes,
            state.tx_bytes,
            state.cpu_time_ns,
            state.disk_read_bytes,
            state.disk_write_bytes,
            state.disk_read_requests,
            state.disk_write_requests,
        )

        agent.advance(state, 10.0, random.Random(8))

        after = (
            state.rx_bytes,
            state.tx_bytes,
            state.cpu_time_ns,
            state.disk_read_bytes,
            state.disk_write_bytes,
            state.disk_read_requests,
            state.disk_write_requests,
        )
        self.assertTrue(all(new > old for old, new in zip(before, after)))
        self.assertGreaterEqual(state.memory_rss_bytes, state.memory_actual_bytes * 0.20)
        self.assertLessEqual(state.memory_rss_bytes, state.memory_actual_bytes * 0.95)
        self.assertLessEqual(state.disk_allocation_bytes, state.disk_capacity_bytes)

    def test_records_match_ovn_agent_topics_and_baseline_semantics(self):
        state = agent.build_fleet(1, 1, 1, random.Random(1))[0]
        emitted = agent.records(state, 1234, "RegionOne", True)

        self.assertEqual(
            ["port-stats", "instance-stats", "disk-stats"],
            [topic for topic, _, _ in emitted],
        )
        self.assertTrue(all(value["is_baseline"] for _, _, value in emitted))
        self.assertEqual(state.rx_bytes, emitted[0][2]["rx"])
        self.assertEqual(state.cpu_time_ns, emitted[1][2]["cpu_time_ns"])
        self.assertEqual("vda", emitted[2][2]["device"])


if __name__ == "__main__":
    unittest.main()
