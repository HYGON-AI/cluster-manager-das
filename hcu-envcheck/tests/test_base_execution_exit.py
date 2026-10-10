# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Basic CLI exit codes describe execution separately from environment health."""

import copy
import io
import json
import signal
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cluster_run.cli import main
from cluster_run.preflight import container_status_excluded_records
from cluster_run.task_control import CancellationReport, NodeCancellation, RemoteTaskSession, TaskInterrupted


def record(node="n01", *, status="INCOMPLETE", returncode=0, **transport):
    return {
        "node": node, "status": status, "reachable": returncode == 0,
        "device_count": None, "devices": [], "environment": {}, "checks": [],
        "findings": [], "metric_summary": {},
        "probe_transport": {"returncode": returncode, **transport},
    }


class BaseExecutionExitTests(unittest.TestCase):
    def run_basic(self, records, *, nodes=("n01",), confirmed=True, interrupted=False,
                  container_status=None, operation="platform"):
        cleanups = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hostfile = root / "nodes.txt"
            hostfile.write_text("\n".join(nodes) + "\n", encoding="utf-8")
            run_dir = root / "report"
            run_dir.mkdir()
            json_path, md_path = run_dir / "cluster-result.json", run_dir / "cluster-summary.md"
            statuses = {item["status"] for item in records}
            health = "BLOCKED" if "BLOCKED" in statuses else "INCOMPLETE" if "INCOMPLETE" in statuses else "READY"
            report = {"status": health, "nodes": copy.deepcopy(records), "transport": "ssh",
                      "summary": {"node_count": len(nodes)}, "evidence_dir": str(run_dir)}

            def cancel(session):
                cleanups.append(tuple(session.nodes))
                session.cancel_event.set()
                session.last_report = CancellationReport(session.run_token, {
                    node: NodeCancellation(node, "CONFIRMED" if confirmed else "UNCONFIRMED",
                                           "test stop evidence", returncode=0 if confirmed else 255)
                    for node in session.nodes
                })
                return session.last_report

            def probe(**kwargs):
                self.assertEqual(kwargs["nodes"], list(nodes))
                if interrupted:
                    raise TaskInterrupted(signal.SIGINT)
                return report, json_path, md_path

            args = ["per-node-container" if container_status is not None else "shared-conda",
                    operation, "-f", str(hostfile), "--env-script", "/__hcu_exit_test__/env.sh",
                    "--output-dir", str(root / "output")]
            if container_status is not None:
                args += ["--container", "worker", "-i", "image:tag"]
            with patch("cluster_run.cli.run_baremetal_cluster_preflight", side_effect=probe), \
                 patch("cluster_run.cli._check_container_status", return_value=container_status), \
                 patch.object(RemoteTaskSession, "cancel_and_wait", cancel), \
                 redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                exit_code = main(args)
            persisted = json.loads(json_path.read_text(encoding="utf-8")) if json_path.exists() else None
            cleanup_path = run_dir / "cleanup.json"
            cleanup = json.loads(cleanup_path.read_text(encoding="utf-8")) if cleanup_path.exists() else None
            cancellations = [json.loads(path.read_text(encoding="utf-8"))
                             for path in (root / "output").glob("cancellation-*.json")]
        return exit_code, cleanups, persisted, cleanup, cancellations

    def test_health_results_with_successful_probe_remain_zero_without_cleanup(self):
        for operation in ("platform", "resource", "platform,resource"):
            for health in ("READY", "BLOCKED", "INCOMPLETE"):
                with self.subTest(operation=operation, health=health):
                    rc, cleanups, report, cleanup, _ = self.run_basic(
                        [record(status=health, returncode=0, success=True)], operation=operation)
                    self.assertEqual(rc, 0)
                    self.assertFalse(cleanups)
                    self.assertIsNone(cleanup)
                    self.assertEqual(report["cluster"]["status"], health)
                    self.assertEqual(report["run"]["execution"]["status"], "PASS")

    def test_confirmed_cleanup_does_not_hide_bootstrap_ssh_probe_failure(self):
        for returncode in (1, 2, 124, 127, 255, -9):
            with self.subTest(returncode=returncode):
                rc, cleanups, report, cleanup, _ = self.run_basic([record(returncode=returncode)])
                self.assertEqual(rc, 2)
                self.assertEqual(cleanups, [("n01",)])
                self.assertEqual(report["cluster"]["status"], "INCOMPLETE")
                self.assertEqual(report["run"]["execution"].get("failed_nodes"), ["n01"])
                self.assertEqual(report["run"]["execution"].get("status"), "FAIL")
                self.assertEqual(cleanup["status"], "FAIL")
                self.assertEqual(cleanup["cleanup"]["nodes"]["n01"]["status"], "CONFIRMED")

    def test_absent_returncode_or_remote_result_is_not_success(self):
        no_code = record()
        del no_code["probe_transport"]["returncode"]
        no_transport = record()
        del no_transport["probe_transport"]
        null_transport = record()
        null_transport["probe_transport"] = None
        for name, records in (("none-code", [record(returncode=None)]), ("missing-code", [no_code]),
                              ("missing-transport", [no_transport]), ("null-transport", [null_transport]),
                              ("missing-record", [])):
            with self.subTest(case=name):
                rc, cleanups, report, cleanup, _ = self.run_basic(records)
                self.assertEqual(rc, 2)
                self.assertEqual(cleanups, [("n01",)])
                self.assertEqual(cleanup["status"], "FAIL")

    def test_zero_returncode_with_transport_failure_metadata_is_not_health(self):
        for metadata in ({"success": False}, {"error_kind": "REMOTE_RESULT_MISSING"}, {"timed_out": True}):
            with self.subTest(metadata=metadata):
                rc, cleanups, _, cleanup, _ = self.run_basic([record(returncode=0, **metadata)])
                self.assertEqual(rc, 2)
                self.assertEqual(cleanups, [("n01",)])
                self.assertEqual(cleanup["status"], "FAIL")

    def test_missing_one_node_result_fails_even_if_other_node_succeeded(self):
        rc, cleanups, report, _, _ = self.run_basic([record(status="READY")], nodes=("n01", "n02"))
        self.assertEqual(rc, 2)
        self.assertEqual(cleanups, [("n01", "n02")])
        self.assertEqual(report["run"]["execution"]["failed_nodes"], ["n02"])

    def test_failed_cleanup_has_exit_three(self):
        rc, _, report, cleanup, _ = self.run_basic([record(returncode=255)], confirmed=False)
        self.assertEqual(rc, 3)
        self.assertEqual(cleanup["status"], "CLEANUP_UNCONFIRMED")
        self.assertEqual(report["run"]["execution"]["status"], "FAIL")

    def test_confirmed_interruption_still_has_exit_130(self):
        rc, cleanups, report, _, cancellations = self.run_basic([], interrupted=True)
        self.assertEqual(rc, 130)
        self.assertEqual(cleanups, [("n01",)])
        self.assertIsNone(report)
        self.assertEqual(cancellations[0]["status"], "CANCELLED")
        self.assertTrue(cancellations[0]["interrupted"])

    def test_unconfirmed_interrupted_cleanup_retains_existing_exit_three(self):
        rc, _, _, _, cancellations = self.run_basic([], interrupted=True, confirmed=False)
        self.assertEqual(rc, 3)
        self.assertEqual(cancellations[0]["status"], "CLEANUP_UNCONFIRMED")

    def test_excluded_bad_containers_are_health_not_probe_execution_failure(self):
        for returncode in (None, 1, 255):
            for include_good_node in (False, True):
                with self.subTest(returncode=returncode, include_good_node=include_good_node):
                    container_status = {
                        "status": "FAIL", "ready_nodes": ["n01"] if include_good_node else [],
                        "image_reference": None, "expected_image_specified": False,
                        "issues": [{"node": "n02", "code": "CONTAINER_MISSING", "message": "missing"}],
                        "nodes": {"n02": {"status": "MISSING", "returncode": returncode}},
                    }
                    if include_good_node:
                        container_status["nodes"]["n01"] = {"status": "PRESENT", "returncode": 0}
                    excluded = list(container_status_excluded_records(container_status).values())
                    records = ([record(status="READY")] if include_good_node else []) + excluded
                    nodes = ("n01", "n02") if include_good_node else ("n02",)
                    rc, cleanups, report, cleanup, _ = self.run_basic(
                        records, nodes=nodes, container_status=container_status)
                    self.assertEqual(rc, 0)
                    self.assertFalse(cleanups)
                    self.assertIsNone(cleanup)
                    self.assertEqual(report["cluster"]["status"], "BLOCKED")
                    self.assertEqual(report["run"]["execution"]["failed_nodes"], [])


if __name__ == "__main__":
    unittest.main()
