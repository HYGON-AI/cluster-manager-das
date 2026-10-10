# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""The shared Docker identity check must not start probes or pull images."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.cli import main
from cluster_run.preflight import container_status_excluded_records, run_container_status
from hcu_envcheck.baremetal import BaremetalExecutionConfig, BaremetalNodeResult
from hcu_envcheck.baremetal_cluster import BaremetalPreflightPolicy, run_baremetal_cluster_preflight


IMAGE = "registry.example/train:1"
IMAGE_ID = "sha256:" + "a" * 64


def inspected(node: str, *, exists: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        success=exists, returncode=0 if exists else 1, error_kind=None,
        stdout=f"/worker|{IMAGE}|{IMAGE_ID}|true|1000\n" if exists else "",
        stderr="" if exists else "Error: No such object: worker",
        result_dir="/temporary/evidence",
    )


class ContainerStatusTests(unittest.TestCase):
    def test_status_only_inspects_containers_and_does_not_require_local_image_tag(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(nodes={
                    "node01": inspected("node01"), "node02": inspected("node02", exists=False),
                })
                report = run_container_status(
                    nodes=["node01", "node02"], container_name="worker",
                    expected_image=None, transport="ssh", concurrency=2,
                    run_dir=Path(temp),
                )
            self.assertEqual(report["ready_nodes"], ["node01"])
            self.assertEqual([issue["code"] for issue in report["issues"]], ["CONTAINER_MISSING"])
            self.assertFalse(report["expected_image_specified"])
            self.assertEqual(executor.return_value.execute.call_count, 1)
            self.assertEqual(executor.return_value.execute.call_args.args[1][:2], ["docker", "inspect"])
            self.assertFalse((Path(temp) / "preflight.json").exists())

    def test_same_image_tag_with_different_ids_blocks_all_present_nodes(self):
        with tempfile.TemporaryDirectory() as temp:
            second = inspected("node02")
            second.stdout = second.stdout.replace(IMAGE_ID, "sha256:" + "b" * 64)
            with patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(nodes={
                    "node01": inspected("node01"), "node02": second,
                })
                report = run_container_status(
                    nodes=["node01", "node02"], container_name="worker",
                    expected_image=IMAGE, transport="ssh", concurrency=2,
                    run_dir=Path(temp),
                )
            self.assertEqual(report["ready_nodes"], [])
            self.assertIn("CONTAINER_IMAGE_INCONSISTENT", {issue["code"] for issue in report["issues"]})

    def test_status_cli_requires_image_but_not_env_script(self):
        with tempfile.TemporaryDirectory() as temp:
            hostfile = Path(temp) / "nodes.txt"
            hostfile.write_text("node01\n", encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["per-node-container", "container-status", "-f", str(hostfile), "--container", "worker"])
            self.assertEqual(rc, 3)
            self.assertIn("requires --container and -i/--image", stderr.getvalue())
            with patch("cluster_run.cli.run_container_status", return_value={
                "status": "PASS", "ready_nodes": ["node01"], "nodes": {"node01": {}},
                "issues": [], "image_reference": IMAGE, "expected_image_specified": True,
            }) as checked:
                stdout = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(io.StringIO()):
                    rc = main(["per-node-container", "container-status", "-f", str(hostfile),
                               "--container", "worker", "-i", IMAGE])
            self.assertEqual(rc, 0)
            self.assertIn("RESULT        PASS", stdout.getvalue())
            checked.assert_called_once()

    def test_all_containers_unready_still_generate_basic_node_records(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(nodes={
                    "node01": inspected("node01", exists=False),
                })
                status = run_container_status(
                    nodes=["node01"], container_name="worker", expected_image=IMAGE,
                    transport="ssh", concurrency=1, run_dir=Path(temp),
                )
            excluded = container_status_excluded_records(status)
            self.assertEqual(excluded["node01"]["status"], "BLOCKED")
            policy = BaremetalPreflightPolicy(env_script="/remote/env.sh", execution_scope="container", container_name="worker")
            report, json_path, markdown_path = run_baremetal_cluster_preflight(
                nodes=["node01"], execution_config=BaremetalExecutionConfig(output_root=Path(temp) / "evidence"),
                policy=policy, output_dir=Path(temp) / "output", excluded_records=excluded,
                run_label="container_platform",
            )
            self.assertEqual(report["summary"]["node_count"], 1)
            self.assertEqual(report["status"], "BLOCKED")
            self.assertIn("CONTAINER_MISSING", json_path.read_text(encoding="utf-8"))
            self.assertTrue(markdown_path.exists())

    def test_basic_probe_fans_out_only_to_container_ready_nodes(self):
        with tempfile.TemporaryDirectory() as temp:
            status = {
                "nodes": {"node01": {"status": "PRESENT", "running": True},
                          "node02": {"status": "UNKNOWN"}},
                "ready_nodes": ["node01"],
                "issues": [{"node": "node02", "code": "CONTAINER_MISSING", "message": "container missing"}],
            }
            failed_probe = BaremetalNodeResult(
                node="node01", transport="ssh", command_name="baremetal-preflight",
                command=["probe"], returncode=1, stdout="", stderr="probe unavailable",
                duration_seconds=0.1, error_kind="REMOTE_COMMAND_FAILED",
            )
            with patch("hcu_envcheck.baremetal_cluster.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(
                    nodes={"node01": failed_probe}, transport="ssh", run_dir="/tmp/fake-evidence",
                )
                report, _, _ = run_baremetal_cluster_preflight(
                    nodes=["node01", "node02"],
                    execution_config=BaremetalExecutionConfig(output_root=Path(temp) / "evidence"),
                    policy=BaremetalPreflightPolicy(env_script="/remote/env.sh", execution_scope="container", container_name="worker"),
                    output_dir=Path(temp) / "output", run_label="container_platform",
                    excluded_records=container_status_excluded_records(status),
                )
            self.assertEqual(executor.call_args.args[0], ["node01"])
            self.assertEqual({item["node"] for item in report["nodes"]}, {"node01", "node02"})
            self.assertEqual(next(item for item in report["nodes"] if item["node"] == "node02")["status"], "BLOCKED")

    def test_container_basic_cli_keeps_failed_node_in_classified_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hostfile = root / "nodes.txt"
            hostfile.write_text("node01\n", encoding="utf-8")
            failed_status = {
                "status": "FAIL", "nodes": {"node01": {"status": "UNKNOWN", "container_name": "worker"}},
                "ready_nodes": [], "image_reference": IMAGE, "expected_image_specified": True,
                "issues": [{"node": "node01", "code": "CONTAINER_MISSING", "message": "container missing"}],
            }
            with patch("cluster_run.cli.run_container_status", return_value=failed_status):
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    rc = main(["per-node-container", "platform", "-f", str(hostfile),
                               "--container", "worker", "-i", IMAGE,
                               "--env-script", "/remote/env.sh", "-o", str(root / "reports")])
            self.assertEqual(rc, 0)  # Basic detection reports findings; it is not a gate.
            report_path = next((root / "reports").glob("container_platform_*/cluster-result.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["cluster"]["container_status"]["status"], "FAIL")
            self.assertEqual(report["node_status"]["nodes"]["node01"]["container_status"]["container_name"], "worker")
            summary = report_path.with_name("cluster-summary.md").read_text(encoding="utf-8")
            self.assertIn("## 容器状态预检", summary)
            self.assertIn("CONTAINER_MISSING", summary)


if __name__ == "__main__":
    unittest.main()
