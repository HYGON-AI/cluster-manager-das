# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from hcu_envcheck.cli import main


class CliOutputSafetyTests(unittest.TestCase):
    def test_k8s_pod_publish_collision_is_tool_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            with (
                patch("hcu_envcheck.cli.KubernetesPodExecutor", return_value=object()),
                patch("hcu_envcheck.cli.run_k8s_hcu_preflight", return_value=object()),
                patch(
                    "hcu_envcheck.cli.save_result",
                    side_effect=ValueError("simulated concurrent publish collision"),
                ),
                redirect_stdout(io.StringIO()) as stdout,
            ):
                returncode = main(
                    [
                        "k8s-pod",
                        "--namespace",
                        "training",
                        "--pod",
                        "trainer-0",
                        "--container",
                        "trainer",
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(returncode, 3)
            self.assertIn("RESULT        TOOL_ERROR", stdout.getvalue())
            self.assertIn("concurrent publish collision", stdout.getvalue())


class ActiveRdmaCliTests(unittest.TestCase):
    @staticmethod
    def _result(status="PASS", backend="VERBS", transport="IB_VERBS"):
        return SimpleNamespace(
            status=status,
            backend=backend,
            nodes=["e06r1n08", "e06r1n09"],
            data_transport=transport,
            reason_code="TEST_REASON",
            root_cause_candidates=[],
            metrics={},
            evidence_dir="active-evidence",
        )

    def test_verbs_entry_requires_opt_in_and_passes_explicit_nodes(self):
        runner = Mock()
        runner.run_verbs.return_value = self._result()
        with (
            patch("hcu_envcheck.cli.SlurmActiveCheckRunner", return_value=runner),
            redirect_stdout(io.StringIO()) as stdout,
        ):
            returncode = main(
                [
                    "active-rdma-slurm",
                    "--slurm-job-id",
                    "674118",
                    "--node",
                    "e06r1n08",
                    "--node",
                    "e06r1n09",
                    "--backend",
                    "verbs",
                    "--enable-active-checks",
                    "--confirm-allocation-idle",
                    "--verbs-hca",
                    "shca_0",
                    "--verbs-port",
                    "1",
                    "--minimum-verbs-gbps",
                    "200",
                    "--output-dir",
                    "active-output",
                ]
            )
        self.assertEqual(returncode, 0)
        context, config = runner.run_verbs.call_args.args
        self.assertEqual(context.selected_nodes, ("e06r1n08", "e06r1n09"))
        self.assertTrue(context.enabled)
        self.assertTrue(context.confirm_allocation_idle)
        self.assertFalse(context.allow_active_steps)
        self.assertFalse(context.unsafe_allow_overlap)
        self.assertEqual(context.max_selected_nodes, 16)
        self.assertEqual(config.device, "shca_0")
        self.assertEqual(config.minimum_average_gbps, 200.0)
        self.assertIn("SUMMARY", stdout.getvalue())
        self.assertIn("active-summary.md", stdout.getvalue())

    def test_rccl_entry_maps_fail_to_one_and_passes_allowlisted_environment(self):
        runner = Mock()
        runner.run_rccl.return_value = self._result(
            status="FAIL", backend="RCCL", transport="SOCKET"
        )
        with patch("hcu_envcheck.cli.SlurmActiveCheckRunner", return_value=runner):
            returncode = main(
                [
                    "active-rdma-slurm",
                    "--slurm-job-id",
                    "674118",
                    "--node",
                    "e06r1n08",
                    "--node",
                    "e06r1n09",
                    "--backend",
                    "rccl",
                    "--enable-active-checks",
                    "--confirm-allocation-idle",
                    "--rccl-binary",
                    "/opt/rccl-tests/all_reduce_perf",
                    "--container-name",
                    "zytest",
                    "--rccl-env",
                    "NCCL_DMABUF_ENABLE=1",
                    "--rccl-tasks-per-node",
                    "8",
                    "--rccl-devices-per-task",
                    "1",
                    "--rccl-mpi-mode",
                    "pmix_v4",
                    "--minimum-rccl-algbw-gbytes-per-second",
                    "19",
                    "--minimum-rccl-row-busbw-gbytes-per-second",
                    "18",
                    "--require-rccl-gdr",
                    "--rccl-env",
                    "NCCL_NET_PLUGIN=shca",
                    "--rccl-minimum-bytes",
                    "1048576",
                    "--rccl-maximum-bytes",
                    "8388608",
                    "--minimum-rccl-busbw-gbytes-per-second",
                    "20",
                    "--output-dir",
                    "active-output",
                ]
            )
        self.assertEqual(returncode, 1)
        _, config = runner.run_rccl.call_args.args
        self.assertEqual(config.container_name, "zytest")
        self.assertEqual(
            config.environment,
            {"NCCL_NET_PLUGIN": "shca", "NCCL_DMABUF_ENABLE": "1"},
        )
        self.assertEqual(config.minimum_bytes, 1048576)
        self.assertEqual(config.maximum_bytes, 8388608)
        self.assertEqual(config.tasks_per_node, 8)
        self.assertEqual(config.devices_per_task, 1)
        self.assertEqual(config.mpi_mode, "pmix_v4")
        self.assertEqual(config.minimum_algbw_gbytes_per_second, 19.0)
        self.assertEqual(config.minimum_busbw_gbytes_per_second, 18.0)
        self.assertTrue(config.require_gdr)

    def test_active_not_verified_maps_to_two(self):
        runner = Mock()
        runner.run_verbs.return_value = self._result(status="NOT_VERIFIED")
        with (
            patch("hcu_envcheck.cli.SlurmActiveCheckRunner", return_value=runner),
            redirect_stdout(io.StringIO()),
        ):
            returncode = main(
                [
                    "active-rdma-slurm",
                    "--slurm-job-id",
                    "674118",
                    "--node",
                    "e06r1n08",
                    "--node",
                    "e06r1n09",
                    "--backend",
                    "verbs",
                    "--enable-active-checks",
                    "--confirm-allocation-idle",
                    "--unsafe-allow-overlap",
                    "--output-dir",
                    "active-output",
                ]
            )
        self.assertEqual(returncode, 2)
        context, _ = runner.run_verbs.call_args.args
        self.assertTrue(context.unsafe_allow_overlap)

    def test_active_entry_requires_idle_confirmation_flag(self):
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            main(
                [
                    "active-rdma-slurm",
                    "--slurm-job-id",
                    "674118",
                    "--node",
                    "e06r1n08",
                    "--node",
                    "e06r1n09",
                    "--backend",
                    "verbs",
                    "--enable-active-checks",
                    "--output-dir",
                    "active-output",
                ]
            )
        self.assertEqual(raised.exception.code, 3)


if __name__ == "__main__":
    unittest.main()
