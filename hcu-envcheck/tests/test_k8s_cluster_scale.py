# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import math
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hcu_envcheck.k8s_cluster import (
    DEFAULT_K8S_CLUSTER_CONCURRENCY,
    DEFAULT_KUBECTL_API_BURST,
    DEFAULT_KUBECTL_API_QPS,
    MAX_K8S_CLUSTER_CONCURRENCY,
    MAX_KUBECTL_API_BURST,
    MAX_KUBECTL_API_QPS,
    KubectlController,
    _RateLimitedKubernetesPodExecutor,
    _TokenBucketRateLimiter,
    _probe_pod_name,
    run_k8s_cluster_preflight,
)
from hcu_envcheck.k8s import KubernetesPodTarget


class FakeExecutor:
    limiters: list[object] = []

    def __init__(self, target, timeout_seconds=30.0, api_limiter=None):
        self.target = target
        self.timeout_seconds = timeout_seconds
        self.api_limiter = api_limiter
        self.__class__.limiters.append(api_limiter)


class FakeController:
    def __init__(
        self,
        *,
        context=None,
        kubeconfig=None,
        timeout_seconds=60.0,
        api_limiter=None,
        fail_get: set[str] | None = None,
        fail_create: set[str] | None = None,
        delay_by_node: dict[str, float] | None = None,
        synchronize_creates: bool = False,
        unschedulable_nodes: set[str] | None = None,
        conditions_by_node: dict[str, list[dict[str, str]]] | None = None,
        taints_by_node: dict[str, list[dict[str, str]]] | None = None,
    ):
        self.api_limiter = api_limiter
        self.fail_get = fail_get or set()
        self.fail_create = fail_create or set()
        self.delay_by_node = delay_by_node or {}
        self.deleted: list[tuple[str, str, str]] = []
        self.create_attempts: list[str] = []
        self.pod_nodes: dict[str, str] = {}
        self._lock = threading.Lock()
        self._active = 0
        self.maximum_active = 0
        self._two_active = threading.Event()
        self.synchronize_creates = synchronize_creates
        self.unschedulable_nodes = unschedulable_nodes or set()
        self.conditions_by_node = conditions_by_node or {}
        self.taints_by_node = taints_by_node or {}

    def get_node(self, node):
        time.sleep(self.delay_by_node.get(node, 0.0))
        if node in self.fail_get:
            raise RuntimeError(f"cannot query {node}")
        return {
            "metadata": {"name": node},
            "spec": {
                "taints": self.taints_by_node.get(node, []),
                "unschedulable": node in self.unschedulable_nodes,
            },
            "status": {
                "conditions": self.conditions_by_node.get(
                    node,
                    [{"type": "Ready", "status": "True"}],
                ),
                "allocatable": {"hygon.com/hcu": "8"},
            },
        }

    def create_probe(self, manifest):
        node = manifest["spec"]["nodeSelector"]["kubernetes.io/hostname"]
        self.create_attempts.append(node)
        self.pod_nodes[manifest["metadata"]["name"]] = node
        with self._lock:
            self._active += 1
            self.maximum_active = max(self.maximum_active, self._active)
            if self._active >= 2:
                self._two_active.set()
        try:
            if self.synchronize_creates:
                self._two_active.wait(timeout=1.0)
            time.sleep(0.01)
            if node in self.fail_create:
                raise subprocess.TimeoutExpired(["kubectl", "create"], 60)
        finally:
            with self._lock:
                self._active -= 1

    def wait_ready(self, namespace, pod, timeout_seconds):
        return None

    def copy_to_pod(self, *args, **kwargs):
        return None

    def delete_probe(self, namespace, pod, run_id):
        self.deleted.append((namespace, pod, run_id))
        return "DELETED", None


def result_for(executor, expected_devices=8):
    return SimpleNamespace(
        status="READY",
        target={
            "node": executor.target.pod.removeprefix("reuse-")
            if executor.target.pod.startswith("reuse-")
            else executor.target.pod.split("-")[-1],
            "image": "training:tag",
            "image_id": "sha256:same",
        },
        devices=[],
        device_count=expected_devices,
        expected_device_count=expected_devices,
        findings=[],
        environment={},
        evidence_dir="evidence",
    )


def run_kwargs(output_dir: Path, nodes: list[str]) -> dict:
    return {
        "nodes": nodes,
        "namespace": "training",
        "image": "training:tag",
        "image_pull_policy": "IfNotPresent",
        "probe_container": "probe",
        "reuse_pods": {
            node: ("training", f"reuse-{node}", "training") for node in nodes
        },
        "context": None,
        "kubeconfig": None,
        "device_resource_name": "hygon.com/hcu",
        "expected_devices": 8,
        "max_vram_used_percent": 5.0,
        "max_hcu_util_percent": 5.0,
        "samples": 1,
        "busy_sample_quorum": 1,
        "sample_interval_seconds": 0.0,
        "command_timeout": 5.0,
        "pod_ready_timeout": 10,
        "output_dir": output_dir,
        "include_environment": False,
    }


class K8sClusterScaleTests(unittest.TestCase):
    def setUp(self):
        FakeExecutor.limiters = []

    def _run(self, controller: FakeController, output_dir: Path, nodes: list[str], **overrides):
        kwargs = run_kwargs(output_dir, nodes)
        kwargs.update(overrides)

        def preflight(executor, **settings):
            return result_for(executor, settings["expected_devices"])

        def controller_factory(**settings):
            controller.api_limiter = settings["api_limiter"]
            return controller
        with (
            patch("hcu_envcheck.k8s_cluster.KubectlController", side_effect=controller_factory),
            patch("hcu_envcheck.k8s_cluster._RateLimitedKubernetesPodExecutor", FakeExecutor),
            patch("hcu_envcheck.k8s_cluster.run_k8s_hcu_preflight", side_effect=preflight),
            patch("hcu_envcheck.k8s_cluster.save_result"),
        ):
            return run_k8s_cluster_preflight(**kwargs)

    def test_probe_pod_names_are_unique_after_slug_normalization(self):
        first = _probe_pod_name("run123", "node.a")
        second = _probe_pod_name("run123", "node-a")
        self.assertNotEqual(first, second)
        self.assertLessEqual(len(first), 63)
        self.assertLessEqual(len(second), 63)
    def test_worker_pool_is_bounded_and_output_order_is_stable(self):
        nodes = ["node03", "node01", "node02", "node04"]
        controller = FakeController(synchronize_creates=True)
        with tempfile.TemporaryDirectory() as temporary:
            # Use temporary Pods so create calls overlap and expose worker count.
            kwargs = run_kwargs(Path(temporary) / "report", nodes)
            kwargs["reuse_pods"] = {}
            kwargs["concurrency"] = 2

            def preflight(executor, **settings):
                node = controller.pod_nodes[executor.target.pod]
                return SimpleNamespace(
                    status="READY",
                    target={"node": node, "image": "training:tag", "image_id": "same"},
                    devices=[],
                    device_count=8,
                    expected_device_count=8,
                    findings=[],
                    environment={},
                    evidence_dir="evidence",
                )

            with (
                patch("hcu_envcheck.k8s_cluster.KubectlController", return_value=controller),
                patch("hcu_envcheck.k8s_cluster._RateLimitedKubernetesPodExecutor", FakeExecutor),
                patch("hcu_envcheck.k8s_cluster.run_k8s_hcu_preflight", side_effect=preflight),
                patch("hcu_envcheck.k8s_cluster.save_result"),
            ):
                report, _, _ = run_k8s_cluster_preflight(**kwargs)

        self.assertEqual([item["node"] for item in report["nodes"]], nodes)
        self.assertEqual(controller.maximum_active, 2)
        self.assertEqual(report["execution"]["requested_concurrency"], 2)
        self.assertEqual(report["execution"]["effective_concurrency"], 2)
        self.assertEqual(report["execution"]["cleanup"]["completed"], len(nodes))

    def test_one_node_failure_does_not_hide_other_results(self):
        nodes = ["node01", "node02", "node03"]
        controller = FakeController(fail_get={"node02"})
        with tempfile.TemporaryDirectory() as temporary:
            report, _, _ = self._run(
                controller,
                Path(temporary) / "report",
                nodes,
                concurrency=3,
            )

        status_by_node = {item["node"]: item["status"] for item in report["nodes"]}
        self.assertEqual(status_by_node["node01"], "READY")
        self.assertEqual(status_by_node["node02"], "INCOMPLETE")
        self.assertEqual(status_by_node["node03"], "READY")
        self.assertEqual(report["status"], "INCOMPLETE")

    def test_create_timeout_still_executes_per_pod_cleanup(self):
        node = "node02"
        controller = FakeController(fail_create={node})
        with tempfile.TemporaryDirectory() as temporary:
            kwargs = run_kwargs(Path(temporary) / "report", [node])
            kwargs["reuse_pods"] = {}
            with patch(
                "hcu_envcheck.k8s_cluster.KubectlController", return_value=controller
            ):
                report, _, _ = run_k8s_cluster_preflight(**kwargs)

        self.assertEqual(controller.create_attempts, [node])
        self.assertEqual(len(controller.deleted), 1)
        self.assertEqual(report["nodes"][0]["cleanup_status"], "DELETED")
        self.assertEqual(report["execution"]["cleanup"]["completed"], 1)

    def test_reuse_pod_on_cordoned_node_is_incomplete_and_records_evidence(self):
        node = "node01"
        controller = FakeController(unschedulable_nodes={node})
        with tempfile.TemporaryDirectory() as temporary:
            report, _, markdown_path = self._run(
                controller,
                Path(temporary) / "report",
                [node],
            )
            markdown = markdown_path.read_text(encoding="utf-8")

        record = report["nodes"][0]
        self.assertEqual(record["status"], "INCOMPLETE")
        self.assertEqual(record["summary"]["reason_codes"], ["K8S_NODE_UNSCHEDULABLE"])
        self.assertTrue(record["node_k8s_evidence"]["unschedulable"])
        self.assertEqual(record["cleanup_status"], "NOT_APPLICABLE")
        self.assertIn('"unschedulable": true', markdown)

    def test_pressure_condition_and_health_taint_block_reuse_pod(self):
        node = "node02"
        controller = FakeController(
            conditions_by_node={
                node: [
                    {"type": "Ready", "status": "True"},
                    {
                        "type": "MemoryPressure",
                        "status": "True",
                        "reason": "KubeletHasInsufficientMemory",
                    },
                ]
            },
            taints_by_node={
                node: [
                    {
                        "key": "node.kubernetes.io/memory-pressure",
                        "effect": "NoSchedule",
                    }
                ]
            },
        )
        with tempfile.TemporaryDirectory() as temporary:
            report, _, _ = self._run(
                controller,
                Path(temporary) / "report",
                [node],
            )

        record = report["nodes"][0]
        self.assertEqual(record["status"], "INCOMPLETE")
        self.assertEqual(
            record["summary"]["reason_codes"],
            ["K8S_NODE_CONDITION_UNHEALTHY"],
        )
        self.assertIn("MemoryPressure=True", record["node_k8s_evidence"]["health_issues"])
        self.assertIn(
            "taint node.kubernetes.io/memory-pressure:NoSchedule",
            record["node_k8s_evidence"]["health_issues"],
        )

    def test_requested_and_effective_concurrency_and_api_limits_are_reported(self):
        nodes = ["node01", "node02"]
        controller = FakeController()
        with tempfile.TemporaryDirectory() as temporary:
            report, _, _ = self._run(
                controller,
                Path(temporary) / "report",
                nodes,
                concurrency=16,
                api_qps=7.5,
                api_burst=9,
            )

        execution = report["execution"]
        self.assertEqual(execution["requested_concurrency"], 16)
        self.assertEqual(execution["effective_concurrency"], 2)
        self.assertEqual(execution["api_rate_limit"]["qps"], 7.5)
        self.assertEqual(execution["api_rate_limit"]["burst"], 9)
        self.assertIsNotNone(controller.api_limiter)
        self.assertTrue(all(item is controller.api_limiter for item in FakeExecutor.limiters))

    def test_invalid_scale_parameters_fail_before_output_or_kubectl(self):
        invalid = (
            {"concurrency": 0},
            {"concurrency": MAX_K8S_CLUSTER_CONCURRENCY + 1},
            {"api_qps": 0},
            {"api_qps": MAX_KUBECTL_API_QPS + 1},
            {"api_qps": math.nan},
            {"api_burst": 0},
            {"api_burst": MAX_KUBECTL_API_BURST + 1},
        )
        with tempfile.TemporaryDirectory() as temporary:
            for index, override in enumerate(invalid):
                with self.subTest(override=override):
                    output = Path(temporary) / f"report-{index}"
                    kwargs = run_kwargs(output, ["node01"])
                    kwargs.update(override)
                    with patch("hcu_envcheck.k8s_cluster.KubectlController") as controller:
                        with self.assertRaises(ValueError):
                            run_k8s_cluster_preflight(**kwargs)
                    controller.assert_not_called()
                    self.assertFalse(output.exists())

    def test_all_real_kubectl_paths_share_one_rate_limiter(self):
        limiter = _TokenBucketRateLimiter(100.0, 10)
        completed = SimpleNamespace(
            returncode=0,
            stdout="{}",
            stderr="",
            duration_seconds=0.0,
            timed_out=False,
            stdout_total_bytes=2,
            stderr_total_bytes=0,
            stdout_truncated=False,
            stderr_truncated=False,
        )
        controller = KubectlController(api_limiter=limiter)
        executor = _RateLimitedKubernetesPodExecutor(
            KubernetesPodTarget("ns", "pod", "container"),
            api_limiter=limiter,
        )
        with patch(
            "hcu_envcheck.k8s_cluster._run_bounded_process",
            return_value=completed,
        ), patch(
            "hcu_envcheck.k8s._run_bounded_process",
            return_value=completed,
        ):
            controller.run(["version"])
            executor.exec("true", ["true"])
        self.assertEqual(limiter.request_count, 2)

    def test_cli_defaults_are_safe_and_scale_flags_are_configurable(self):
        from hcu_envcheck.cli import build_parser

        base = [
            "k8s-cluster",
            "--node",
            "node01",
            "--namespace",
            "training",
            "--image",
            "training:tag",
            "--expected-devices",
            "8",
            "--output-dir",
            "report",
        ]
        args = build_parser().parse_args(base)
        self.assertEqual(args.concurrency, DEFAULT_K8S_CLUSTER_CONCURRENCY)
        self.assertEqual(args.api_qps, DEFAULT_KUBECTL_API_QPS)
        self.assertEqual(args.api_burst, DEFAULT_KUBECTL_API_BURST)

        args = build_parser().parse_args(
            base + ["--concurrency", "64", "--api-qps", "12.5", "--api-burst", "25"]
        )
        self.assertEqual(args.concurrency, 64)
        self.assertEqual(args.api_qps, 12.5)
        self.assertEqual(args.api_burst, 25)


if __name__ == "__main__":
    unittest.main()