# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from hcu_envcheck.baremetal import BaremetalExecutionConfig
from hcu_envcheck.cluster_checks import (
    ClusterExtraCheckConfig,
    IBStateCheckConfig,
    IBWriteBandwidthConfig,
    build_ib_test_plan,
    evaluate_ib_state_output,
    evaluate_ib_write_bw_output,
    run_cluster_extra_checks,
)


class ClusterExtraChecksTests(unittest.TestCase):


    def test_ib_plan_contains_one_all_direction_round_for_every_hca(self):
        plan = build_ib_test_plan(
            ["node1", "node2", "node3"],
            {
                "node1": ["mlx5_0", "mlx5_1"],
                "node2": ["shca_0", "shca_1"],
                "node3": ["mlx5_0", "mlx5_1"],
            },
        )

        self.assertEqual(len(plan), 12)
        self.assertEqual(
            (plan[0].source, plan[0].destination, plan[0].source_hca, plan[0].destination_hca),
            ("node1", "node2", "mlx5_0", "shca_0"),
        )
        self.assertTrue(all(item.source != item.destination for item in plan))
        self.assertEqual({item.rail_index for item in plan}, {0, 1})

    def test_ib_plan_rejects_hca_count_mismatch(self):
        with self.assertRaisesRegex(ValueError, "HCA count mismatch"):
            build_ib_test_plan(
                ["node1", "node2"],
                {"node1": ["shca_0"], "node2": ["mlx5_0", "mlx5_1"]},
            )

    def test_ib_state_evaluator(self):
        ibstat = """
CA 'shca_0'
    Port 1:
        State: Active
        Physical state: LinkUp
        Rate: 400
        Link layer: InfiniBand
CA 'shca_1'
    Port 1:
        State: Active
        Physical state: LinkUp
        Rate: 400
        Link layer: InfiniBand
"""
        ib = evaluate_ib_state_output(0, ibstat, "", False)
        self.assertEqual(ib["status"], "PASS")
        self.assertEqual(ib["supported_hcas"], ["shca_0", "shca_1"])

        inactive = evaluate_ib_state_output(
            0,
            ibstat.replace("State: Active", "State: Down", 1),
            "",
            False,
        )
        self.assertEqual(inactive["reason_code"], "IB_PORT_NOT_ACTIVE")

    def test_ib_output_parses_bandwidth_and_threshold(self):
        stdout = """
---------------------------------------------------------------------------------------
                    Send BW Test
Device         : mlx5_0
Transport type : IB
Link type      : IB
 #bytes     #iterations    BW peak[Gb/sec]    BW average[Gb/sec]   MsgRate[Mpps]
 1048576     1000           190.00             188.50              0.02
"""
        result = evaluate_ib_write_bw_output(0, stdout, "", False, minimum_gbps=100.0)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["average_gbps"], 188.5)

        slow = evaluate_ib_write_bw_output(0, stdout, "", False, minimum_gbps=200.0)
        self.assertEqual(slow["status"], "FAIL")
        self.assertEqual(slow["reason_code"], "IB_BANDWIDTH_BELOW_THRESHOLD")


    def test_ib_runner_launches_server_and_client_from_controller(self):
        calls = []
        output = """
Device         : mlx5_0
Transport type : IB
Link type      : IB
 #bytes     #iterations    BW peak[Gb/sec]    BW average[Gb/sec]   MsgRate[Mpps]
 1048576     1000           190.00             188.50              0.02
"""

        def runner(argv, **kwargs):
            calls.append(argv)
            sentinel = re.search(
                r"(__HCU_ENVCHECK(?:_IB)?_RC_[0-9a-f]+__)",
                argv[-1],
            ).group(1)
            if "ibstat" in argv[-1]:
                ibstat = """
CA 'mlx5_0'
    Port 1:
        State: Active
        Physical state: LinkUp
        Rate: 400
        Link layer: InfiniBand
"""
                return subprocess.CompletedProcess(argv, 0, f"{ibstat}\n{sentinel}=0\n", "")
            return subprocess.CompletedProcess(argv, 0, f"{output}\n{sentinel}=0\n", "")

        records = [
            {"node": "node1", "status": "READY", "findings": [], "checks": []},
            {"node": "node2", "status": "READY", "findings": [], "checks": []},
        ]
        with tempfile.TemporaryDirectory() as temp:
            execution = BaremetalExecutionConfig(
                output_root=Path(temp) / "evidence",
                transport="ssh",
                concurrency=1,
                ssh_executable="/usr/bin/ssh",
            )
            extra = run_cluster_extra_checks(
                nodes=["node1", "node2"],
                records=records,
                execution_config=execution,
                config=ClusterExtraCheckConfig(
                    ib_state=IBStateCheckConfig(enabled=True),
                    ib=IBWriteBandwidthConfig(
                        enabled=True,
                        minimum_average_gbps=200.0,
                        startup_grace_seconds=0.0,
                    ),
                ),
                output_root=Path(temp) / "evidence",
                runner=runner,
                which=lambda name: "/usr/bin/ssh" if name == "ssh" else None,
            )

        self.assertEqual(extra["ib_write_bw"]["status"], "FAIL")
        destinations = [argv[-2] for argv in calls]
        self.assertIn("node1", destinations)
        self.assertIn("node2", destinations)
        self.assertTrue(all("ssh node" not in argv[-1] for argv in calls))
        self.assertEqual(extra["rounds"], 1)
        self.assertFalse(extra["taint_mutation"])
        self.assertNotIn("acs", extra)
        self.assertEqual(extra["ib_write_bw"]["summary"]["planned_tests"], 2)
        node1_to_node2 = next(
            pair
            for pair in extra["ib_write_bw"]["pairs"]
            if pair["source"] == "node1" and pair["destination"] == "node2"
        )
        self.assertEqual(node1_to_node2["source_hca"], "mlx5_0")
        self.assertEqual(node1_to_node2["destination_hca"], "mlx5_0")
        self.assertEqual(node1_to_node2["rail_index"], 0)
        self.assertIn(
            "node1:mlx5_0 -> node2:mlx5_0 (rail 0): "
            "average bandwidth 188.5 Gbit/s is below 200.0",
            node1_to_node2["message"],
        )
        self.assertTrue(
            any(
                "node1:mlx5_0 -> node2:mlx5_0 (rail 0)" in finding["message"]
                for finding in records[0]["findings"]
            )
        )


if __name__ == "__main__":
    unittest.main()
