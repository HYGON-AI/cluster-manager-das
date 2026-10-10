# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Node-script execution context, cancellation and cleanup evidence contracts."""

import json
import signal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.node_script import run_node_script
from cluster_run.task_control import RemoteTaskSession
from hcu_envcheck.baremetal import BaremetalNodeResult


def result(node, code=0, error_kind=None):
    return BaremetalNodeResult(node, "ssh", "node-script", [], code, "worker output", "worker error" if code else "",
                              0.2, error_kind=error_kind, result_dir="/node/evidence")


class NodeScriptControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.kwargs = dict(nodes=["n01", "n02"], scenario="shared-conda", env_script="/share/env.sh",
                           script="/share/inspect.py", script_args=["--label", "value with spaces"], container_name=None,
                           image=None, transport="ssh", concurrency=2, timeout=11,
                           output_dir=Path(self.temp.name), dry_run=False)
        self.sessions = []
        self.calls = []
        self.stop_calls = []

    def session_factory(self, *args, **kwargs):
        parent = self
        class StopExecutor:
            def __init__(self, nodes, config):
                self.nodes = nodes
                parent.assertIsNone(config.cancel_event)
            def execute(self, name, command):
                parent.stop_calls.append(command)
                token = command[command.index("stop") + 1]
                return SimpleNamespace(nodes={node: BaremetalNodeResult(
                    node, "ssh", name, [], 0, "__HCU_TASK_STOP__=" + json.dumps(
                        dict(token=token, status="STOPPED", remaining=0, tombstone=True)), "", 0.1)
                    for node in self.nodes})
        kwargs["executor_factory"] = StopExecutor
        session = RemoteTaskSession(*args, **kwargs)
        self.sessions.append(session)
        return session

    def executor_factory(self, *, fail=False, interrupted=False, missing=False, error=False):
        parent = self
        class Executor:
            def __init__(self, nodes, config):
                self.nodes = nodes
                parent.calls.append(config)
                parent.assertIs(config.cancel_event, parent.sessions[-1].cancel_event)
            def execute(self, name, command):
                parent.calls.append(command)
                if error:
                    raise OSError("transport construction failed")
                if interrupted:
                    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                return SimpleNamespace(nodes={node: result(node,
                    130 if interrupted else 124 if fail and node == "n02" else 0,
                    "CANCELLED" if interrupted else "COMMAND_TIMEOUT" if fail and node == "n02" else None)
                    for node in self.nodes if not missing or node != "n02"})
        return Executor

    def test_dry_run_container_workdir_shell_python_and_timeout(self):
        self.kwargs.update(dry_run=True, scenario="per-node-container", container_name="worker",
                           container_workdir="/share/project", container_shell="sh", test_python="/conda/bin/python")
        report, directory = run_node_script(**self.kwargs)
        command = report["nodes"]["n01"]["command"]
        self.assertEqual(command[:5], ["docker", "exec", "--workdir", "/share/project", "worker"])
        self.assertIn("timeout", command)
        self.assertIn("11s", command)
        self.assertIn(". /share/env.sh", command[-1])
        self.assertIn("cd", command[-1])
        self.assertLess(command[-1].index("cd"), command[-1].index(". /share/env.sh"))
        self.assertIn("/share/project", command[-1])
        self.assertIn("exec /conda/bin/python /share/inspect.py", command[-1])
        self.assertIn(report["run_token"], command)
        self.assertEqual(report["cleanup"], None)
        self.assertEqual(json.loads((directory / "script-result.json").read_text())["status"], "DRY_RUN")

    def test_success_persists_each_node_without_stop(self):
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory()):
            report, directory = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "PASS")
        self.assertFalse(self.stop_calls)
        self.assertEqual(set(report["nodes"]), {"n01", "n02"})
        for item in report["nodes"].values():
            self.assertEqual(Path(item["stdout"]).read_text(), "worker output")
            self.assertEqual(item["evidence_dir"], "/node/evidence")

    def test_timeout_retains_failure_and_cleans_every_planned_node(self):
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory(fail=True)):
            report, _ = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["nodes"]["n02"]["returncode"], 124)
        self.assertEqual(report["nodes"]["n02"]["error_kind"], "COMMAND_TIMEOUT")
        self.assertTrue(self.stop_calls)
        self.assertEqual(set(report["cleanup"]["nodes"]), {"n01", "n02"})
        self.assertEqual(report["nodes"]["n01"]["cleanup"]["status"], "CONFIRMED")

    def test_signal_returns_cancelled_report_after_node_evidence_and_cleanup(self):
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory(interrupted=True)):
            report, directory = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "CANCELLED")
        self.assertTrue(report["interrupted"])
        self.assertTrue(self.stop_calls)
        self.assertTrue(all(item["status"] == "CANCELLED" for item in report["nodes"].values()))
        self.assertEqual(json.loads((directory / "script-result.json").read_text())["status"], "CANCELLED")

    def test_stop_transport_failure_cannot_report_cancelled_or_successfully_cleaned(self):
        factory = self.session_factory
        def fail_cleanup(*args, **kwargs):
            session = factory(*args, **kwargs)
            session._executor_factory = lambda *args: (_ for _ in ()).throw(OSError("unreachable"))
            return session
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=fail_cleanup), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory(interrupted=True)):
            report, _ = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "CLEANUP_UNCONFIRMED")
        self.assertEqual(report["nodes"]["n01"]["cleanup"]["status"], "UNCONFIRMED")
        self.assertIn("unreachable", report["cleanup"]["nodes"]["n01"]["reason"])

    def test_missing_result_is_failure_with_persisted_reason_and_cleanup(self):
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory(missing=True)):
            report, _ = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["nodes"]["n02"]["error_kind"], "REMOTE_RESULT_MISSING")
        self.assertIn("no remote result", Path(report["nodes"]["n02"]["stderr"]).read_text())
        self.assertTrue(self.stop_calls)

    def test_executor_exception_still_cleans_and_writes_report(self):
        with patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory(error=True)):
            report, directory = run_node_script(**self.kwargs)
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("transport construction failed", report["error"])
        self.assertEqual(len(report["nodes"]), 2)
        self.assertTrue((directory / "script-result.json").exists())
        self.assertTrue(self.stop_calls)

    def test_preflight_excluded_nodes_are_never_launched_or_stopped(self):
        self.kwargs.update(scenario="per-node-container", container_name="worker")
        preflight = {"ready_nodes": ["n01"], "issues": [{"node": "n02", "code": "CONTAINER_MISSING"}]}
        with patch("cluster_run.node_script.run_container_status", return_value=preflight), \
             patch("cluster_run.node_script.RemoteTaskSession", side_effect=self.session_factory), \
             patch("cluster_run.node_script.BaremetalClusterExecutor", self.executor_factory()):
            report, _ = run_node_script(**self.kwargs)
        self.assertEqual(self.sessions[0].nodes, ("n01",))
        self.assertEqual(report["nodes"]["n02"]["status"], "SKIPPED")
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(self.stop_calls)


if __name__ == "__main__":
    unittest.main()
