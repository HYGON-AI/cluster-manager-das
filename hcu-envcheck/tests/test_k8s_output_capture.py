# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hcu_envcheck.k8s import (
    KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
    KubernetesPodExecutor,
    KubernetesPodTarget,
)
from hcu_envcheck.k8s_cluster import _enforce_truncated_output_status
from hcu_envcheck.models import CommandResult
from hcu_envcheck.preflight import (
    _command_finding,
    _output_truncation_finding,
    run_k8s_hcu_preflight,
)


class _TimeoutProcess:
    def __init__(self):
        self.stdout = io.BytesIO(b"partial-stdout")
        self.stderr = io.BytesIO(b"partial-stderr")
        self.stdin = None
        self.returncode = None
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self.killed:
            self.returncode = -9
            return self.returncode
        raise subprocess.TimeoutExpired(["kubectl"], timeout)

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.returncode = -9


class _PodHealthExecutor:
    def _command(self, name: str) -> CommandResult:
        return CommandResult(name, [name], 0, "ok", "", 0.01)

    def pod_identity(self):
        return (
            {
                "phase": "Running",
                "node": "node01",
                "container_state": "running",
                "container_ready": True,
                "device_limit": "8",
                "device_request": "8",
                "image": "training:tag",
            },
            self._command("pod_identity"),
        )

    def node_identity(self, node):
        return (
            {
                "node": node,
                "ready": "True",
                "unschedulable": True,
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "MemoryPressure", "status": "True"},
                    {"type": "NetworkUnavailable", "status": "Unknown"},
                ],
                "taints": [
                    {
                        "key": "node.kubernetes.io/disk-pressure",
                        "effect": "NoSchedule",
                    }
                ],
                "device_capacity": "8",
                "device_allocatable": "8",
            },
            self._command("node_identity"),
        )

    def resolve_tool(self, preferred, fallback=None):
        return f"/usr/bin/{preferred}", self._command(f"resolve_{preferred}")


class K8sBoundedOutputTests(unittest.TestCase):
    def executor(self, *, timeout=5.0, limit=KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES):
        return KubernetesPodExecutor(
            KubernetesPodTarget("ns", "pod", "container"),
            timeout_seconds=timeout,
            output_capture_limit_bytes=limit,
        )

    def test_k8s_pod_health_conditions_taints_and_cordon_block_ready(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = run_k8s_hcu_preflight(
                _PodHealthExecutor(),
                expected_devices=8,
                max_vram_used_percent=5.0,
                max_hcu_util_percent=5.0,
                samples=1,
                busy_sample_quorum=1,
                sample_interval_seconds=0.0,
                evidence_dir=Path(temporary) / "evidence",
                include_environment=False,
            )

        self.assertEqual(result.status, "BLOCKED")
        reason_codes = {item.reason_code for item in result.findings}
        self.assertIn("K8S_NODE_CONDITION_UNHEALTHY", reason_codes)
        self.assertIn("K8S_NODE_HEALTH_TAINT", reason_codes)
        self.assertIn("K8S_NODE_UNSCHEDULABLE", reason_codes)
        self.assertEqual(result.target["node_conditions"][1]["type"], "MemoryPressure")
        self.assertEqual(
            result.target["node_taints"][0]["key"],
            "node.kubernetes.io/disk-pressure",
        )

    def test_ten_megabyte_output_is_streamed_into_bounded_head_tail(self):
        total_bytes = 10 * 1024 * 1024
        script = (
            "import sys; "
            f"sys.stdout.buffer.write(b'HEAD' + b'x' * {total_bytes - 8} + b'TAIL')"
        )
        result = self.executor()._run("large", [sys.executable, "-c", script])

        self.assertEqual(result.returncode, 0)
        self.assertFalse(result.timed_out)
        self.assertEqual(result.stdout_total_bytes, total_bytes)
        self.assertTrue(result.stdout_truncated)
        self.assertTrue(result.stdout.startswith("HEAD"))
        self.assertTrue(result.stdout.endswith("TAIL"))
        self.assertIn("HCU_ENVCHECK omitted", result.stdout)
        self.assertLess(
            len(result.stdout.encode("utf-8")),
            KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES + 256,
        )
        summary = result.summary()
        self.assertEqual(summary["stdout_total_bytes"], total_bytes)
        self.assertTrue(summary["stdout_truncated"])

    def test_concurrent_large_commands_remain_independently_bounded(self):
        workers = 8
        limit = 64 * 1024
        emitted = 512 * 1024
        script = (
            "import sys; "
            f"sys.stdout.buffer.write(b'o' * {emitted}); "
            f"sys.stderr.buffer.write(b'e' * {emitted})"
        )

        def run_one(index: int):
            return self.executor(limit=limit)._run(
                f"large-{index}",
                [sys.executable, "-c", script],
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(run_one, range(workers)))

        self.assertEqual(len(results), workers)
        for result in results:
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout_total_bytes, emitted)
            self.assertEqual(result.stderr_total_bytes, emitted)
            self.assertTrue(result.stdout_truncated)
            self.assertTrue(result.stderr_truncated)
            self.assertLess(len(result.stdout.encode("utf-8")), limit + 256)
            self.assertLess(len(result.stderr.encode("utf-8")), limit + 256)

    def test_timeout_terminates_then_kills_and_drains_both_pipes(self):
        process = _TimeoutProcess()
        with patch("hcu_envcheck.k8s.subprocess.Popen", return_value=process):
            result = self.executor(timeout=0.01)._run("timeout", ["kubectl", "exec"])

        self.assertTrue(result.timed_out)
        self.assertEqual(result.returncode, 124)
        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertGreaterEqual(process.wait_calls, 3)
        self.assertEqual(result.stdout, "partial-stdout")
        self.assertEqual(result.stderr, "partial-stderr")

    def test_truncation_suppresses_ready_but_deterministic_failure_wins(self):
        command = CommandResult(
            name="rocminfo",
            argv=["kubectl", "exec"],
            returncode=1,
            stdout="driver not loaded",
            stderr="",
            duration_seconds=0.1,
            stdout_total_bytes=10 * 1024 * 1024,
            stderr_total_bytes=0,
            stdout_truncated=True,
        )
        deterministic = _command_finding(command)
        truncated = _output_truncation_finding(command)
        self.assertIsNotNone(deterministic)
        self.assertEqual(deterministic.severity, "FAIL")
        self.assertIsNotNone(truncated)
        self.assertEqual(truncated.severity, "UNKNOWN")

        blocked = SimpleNamespace(
            status="BLOCKED",
            findings=[deterministic],
            commands=[command.summary()],
        )
        _enforce_truncated_output_status(blocked)
        self.assertEqual(blocked.status, "BLOCKED")
        self.assertIn(
            "KUBECTL_OUTPUT_TRUNCATED",
            {item.reason_code for item in blocked.findings},
        )

        ready = SimpleNamespace(status="READY", findings=[], commands=[command.summary()])
        _enforce_truncated_output_status(ready)
        self.assertEqual(ready.status, "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
