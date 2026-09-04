# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import ast
import base64
import builtins
import subprocess
import json
import os
import re
import zlib
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from hcu_envcheck import __version__
from hcu_envcheck.baremetal import BaremetalNodeResult
from hcu_envcheck.cli import build_parser, main
from hcu_envcheck.pod_probe import collect_python
from hcu_envcheck.slurm_cluster import (
    _SOFTWARE_PROBE_SOURCE,
    BaremetalPreflightPolicy,
    apply_conda_collection_plan,
    apply_slurm_state,
    build_baremetal_report,
    build_remote_probe_command,
    collect_slurm_node_states,
    discover_slurm_nodes,
    evaluate_baremetal_environment,
    evaluate_node_result,
    render_baremetal_markdown,
)


def environment_payload(torch_importable=False):
    return {
        "dtk": {
            "version_file": {"path": "/opt/dtk/.dtk_version", "value": "25.04"},
            "tools": {
                "hipcc": {"path": "/opt/dtk/bin/hipcc", "version": {"rc": 0, "stdout": "HIP 6.3"}},
                "hy-smi": {
                    "path": "/usr/bin/hy-smi",
                    "version": {"rc": 0, "stdout": "hy-smi 1"},
                    "driver_version": {"rc": 0, "stdout": "Driver Version: 6.3"},
                },
            },
        },
        "driver": {"modules": ["hycu 1 0 - Live 0x0"], "device_nodes": ["/dev/kfd"]},
        "libraries": {"paths": []},
        "torch": (
            {
                "importable": True,
                "version": "2.7",
                "hip_version": "6.3",
                "hcu_available": True,
                "device_count": 8,
                "distributed_available": True,
                "distributed_nccl_available": True,
            }
            if torch_importable
            else {
                "importable": False,
                "error_type": "ModuleNotFoundError",
                "error": "No module named torch",
            }
        ),
        "python": {"version": "3.10", "packages": {}},
        "system": {
            "os_release": {"PRETTY_NAME": "Linux"},
            "kernel": "6.1",
            "cpu_logical_count": 64,
            "cpu_affinity_count": 64,
            "cpu_models": ["Hygon"],
            "meminfo": {"MemTotal": "1 TB"},
        },
        "network": {"interfaces": [], "rdma_devices": [], "pci_name_source": "numeric-sysfs-only"},
        "runtime_env": {},
    }


def selected_software_inventory(*, healthy=True):
    if healthy:
        return {
            "schema_version": "1.0",
            "evidence_scope": "SELECTED_TRAINING_TARGET",
            "system": {"os_release": {"PRETTY_NAME": "Target Linux"}},
            "dtk": {
                "version_file": {"path": "/opt/dtk/.dtk_version", "value": "DTK-26.04"},
                "tools": {
                    "hipcc": {
                        "path": "/opt/dtk/bin/hipcc",
                        "version": {"rc": 0, "stdout": "HIP 6.4"},
                    },
                    "ucx_info": {
                        "path": "/opt/ucx/bin/ucx_info",
                        "version": {"rc": 0, "stdout": "Library version: 1.16"},
                    },
                },
            },
            "libraries": {"paths": ["/opt/dtk/lib/librccl.so.1"]},
            "torch": {
                "importable": True,
                "version": "2.10-target",
                "hip_version": "6.4",
                "hcu_available": True,
                "device_count": 8,
                "distributed_available": True,
                "distributed_nccl_available": True,
            },
            "python": {"version": "3.12-target", "packages": {"torch": "2.10-target"}},
            "runtime_env": {"HIP_PATH": "/opt/dtk"},
        }
    return {
        "schema_version": "1.0",
        "evidence_scope": "SELECTED_TRAINING_TARGET",
        "dtk": {"version_file": None, "tools": {}},
        "libraries": {"paths": []},
        "torch": {
            "importable": False,
            "error_type": "ImportError",
            "error": "/opt/hyhal/lib/libhcu_smi.so: undefined symbol: hcusmi_init",
        },
        "python": {"version": "3.12-target", "packages": {}},
        "runtime_env": {},
    }

def record(node, status="READY", reachable=True, environment=None):
    return {
        "node": node,
        "status": status,
        "reachable": reachable,
        "device_count": 8 if reachable else None,
        "devices": [],
        "metric_summary": {
            "max_vram_used_percent": 0.0 if reachable else None,
            "max_hcu_util_percent": 0.0 if reachable else None,
        },
        "findings": [],
        "checks": [],
        "environment": environment or {},
        "software_environment": {
            "mode": "NOT_SELECTED",
            "status": "NOT_CHECKED",
            "message": "not selected",
        },
    }


class SlurmDiscoveryTests(unittest.TestCase):
    def test_resolves_job_nodelist_with_controller_only_commands(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append(argv)
            if argv[0] == "squeue":
                return subprocess.CompletedProcess(argv, 0, "e06r1n[01-03]\n", "")
            return subprocess.CompletedProcess(argv, 0, "e06r1n01\ne06r1n02\ne06r1n03\n", "")

        self.assertEqual(
            discover_slurm_nodes(job_id="674118", runner=runner),
            ["e06r1n01", "e06r1n02", "e06r1n03"],
        )
        self.assertEqual(calls[0][:2], ["squeue", "-h"])
        self.assertEqual(calls[1][:3], ["scontrol", "show", "hostnames"])

    def test_rejects_unsafe_job_id(self):
        with self.assertRaises(ValueError):
            discover_slurm_nodes(job_id="1;touch /tmp/x")

    def test_collects_drain_reason(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(
                argv, 0, "e06r1n10|idle|none\ne06r1n11|drain|NHC:ib_tor_ber_high[2]\n", ""
            )

        states = collect_slurm_node_states(["e06r1n10", "e06r1n11"], runner=runner)
        self.assertEqual(states["e06r1n11"]["state"], "drain")
        self.assertEqual(states["e06r1n11"]["reason"], "NHC:ib_tor_ber_high[2]")


class SoftwareSelectionTests(unittest.TestCase):
    def test_software_target_cannot_be_left_unselected(self):
        with self.assertRaises(ValueError):
            BaremetalPreflightPolicy(software_mode="none").validate()

    def test_required_python_package_name_rejects_shell_syntax(self):
        with self.assertRaisesRegex(ValueError, "unsafe required Python package"):
            BaremetalPreflightPolicy(
                required_python_packages=("torch;echo",),
            ).validate()

    def test_host_python_without_package_requirements_skips_torch(self):
        policy = BaremetalPreflightPolicy(software_mode="host-python")
        findings, summary, checks, software = evaluate_baremetal_environment(
            environment_payload(torch_importable=False), policy
        )
        self.assertNotIn("TORCH_IMPORT_FAILED", {item.reason_code for item in findings})
        self.assertNotIn("TORCH_IMPORT", {item["check_id"] for item in checks})
        self.assertEqual(summary["required_python_packages"], [])
        self.assertEqual(software["required_python_packages"], [])
        self.assertEqual(software["status"], "CHECKED")

    def test_explicit_torch_requirement_keeps_torch_failure(self):
        policy = BaremetalPreflightPolicy(
            software_mode="host-python",
            required_python_packages=("torch",),
        )
        findings, summary, checks, software = evaluate_baremetal_environment(
            environment_payload(torch_importable=False), policy
        )
        self.assertIn("TORCH_IMPORT_FAILED", {item.reason_code for item in findings})
        self.assertIn("TORCH_IMPORT", {item["check_id"] for item in checks})
        self.assertEqual(summary["required_python_packages"], ["torch"])
        self.assertEqual(software["required_python_packages"], ["torch"])

    def test_only_explicit_non_torch_python_packages_are_required(self):
        payload = environment_payload(torch_importable=False)
        payload["python"]["packages"] = {"numpy": "2.0"}
        policy = BaremetalPreflightPolicy(
            software_mode="host-python",
            required_python_packages=("numpy", "transformers"),
        )

        findings, _summary, checks, _software = evaluate_baremetal_environment(
            payload,
            policy,
        )

        reasons = {item.reason_code for item in findings}
        by_id = {item["check_id"]: item["status"] for item in checks}
        self.assertNotIn("TORCH_IMPORT_FAILED", reasons)
        self.assertEqual(by_id["PYTHON_PACKAGE_NUMPY"], "PASS")
        self.assertEqual(by_id["PYTHON_PACKAGE_TRANSFORMERS"], "FAIL")
        self.assertIn("PYTHON_PACKAGE_NOT_FOUND", reasons)

    def test_empty_baremetal_package_policy_does_not_import_torch(self):
        real_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "torch":
                raise AssertionError("Torch must not be imported without an explicit requirement")
            return real_import(name, *args, **kwargs)

        with patch.dict(
            os.environ,
            {"HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES": "[]"},
        ):
            with patch("builtins.__import__", side_effect=guarded_import):
                _python_info, torch_info = collect_python()

        self.assertIsNone(torch_info["importable"])
        self.assertEqual(torch_info["check_status"], "NOT_REQUESTED")


    def test_docker_and_conda_use_selected_target_for_dtk_compiler_and_torch(self):
        for mode, options in (
            ("docker", {"docker_image": "repo/train:1"}),
            (
                "conda",
                {"conda_prefix": "/envs/train", "conda_storage": "node-local"},
            ),
        ):
            with self.subTest(mode=mode):
                payload = environment_payload(torch_importable=False)
                # Host software is intentionally old/incomplete.  Driver
                # evidence remains valid and must still come from this scope.
                payload["dtk"]["version_file"] = None
                payload["dtk"]["tools"].pop("hipcc")
                payload["software_target"] = {
                    "status": "SUCCESS",
                    "cleanup_status": "REMOVED",
                    "inventory": selected_software_inventory(),
                }
                policy = BaremetalPreflightPolicy(
                    software_mode=mode,
                    expected_devices=8,
                    required_python_packages=("torch",),
                    require_compiler=True,
                    require_rccl=True,
                    require_ucx=True,
                    **options,
                )
                findings, summary, checks, software = evaluate_baremetal_environment(
                    payload, policy
                )
                reasons = {item.reason_code for item in findings}
                self.assertNotIn("DTK_VERSION_UNAVAILABLE", reasons)
                self.assertNotIn("HIPCC_NOT_AVAILABLE", reasons)
                self.assertNotIn("TORCH_IMPORT_FAILED", reasons)
                self.assertNotIn("RCCL_LIBRARY_NOT_FOUND", reasons)
                self.assertNotIn("UCX_NOT_AVAILABLE", reasons)
                self.assertEqual(summary["dtk_version"], "DTK-26.04")
                self.assertEqual(summary["hipcc_version"], "HIP 6.4")
                self.assertEqual(summary["torch_version"], "2.10-target")
                self.assertEqual(summary["python_version"], "3.12-target")
                self.assertEqual(summary["driver_version"], "6.3")
                self.assertEqual(summary["software_evidence_scope"], "SELECTED_TRAINING_TARGET")
                self.assertEqual(software["target_status"], "SUCCESS")
                by_id = {item["check_id"]: item["status"] for item in checks}
                self.assertEqual(by_id["HIP_COMPILER"], "PASS")
                self.assertEqual(by_id["TORCH_IMPORT"], "PASS")

    def test_unhealthy_docker_target_cannot_fall_back_to_healthy_host(self):
        payload = environment_payload(torch_importable=True)
        payload["software_target"] = {
            "status": "SUCCESS",
            "cleanup_status": "REMOVED_AUTOMATICALLY",
            "inventory": selected_software_inventory(healthy=False),
        }
        findings, summary, checks, _software = evaluate_baremetal_environment(
            payload,
            BaremetalPreflightPolicy(
                software_mode="docker",
                docker_image="repo/train:1",
                required_python_packages=("torch",),
                require_compiler=True,
                require_rccl=True,
            ),
        )
        reasons = {item.reason_code for item in findings}
        self.assertIn("DTK_VERSION_UNAVAILABLE", reasons)
        self.assertIn("HIPCC_NOT_AVAILABLE", reasons)
        self.assertIn("HCUSMI_LIBRARY_ABI_MISMATCH", reasons)
        self.assertIn("RCCL_LIBRARY_NOT_FOUND", reasons)
        self.assertIsNone(summary["dtk_version"])
        self.assertIsNone(summary["hipcc_version"])
        self.assertIsNone(summary["torch_version"])
        self.assertEqual(summary["driver_version"], "6.3")
        self.assertEqual(summary["software_evidence_scope"], "SELECTED_TRAINING_TARGET")
        by_id = {item["check_id"]: item["status"] for item in checks}
        self.assertEqual(by_id["HIP_COMPILER"], "FAIL")
        self.assertEqual(by_id["TORCH_IMPORT"], "FAIL")

    def test_missing_selected_inventory_is_fail_closed(self):
        payload = environment_payload(torch_importable=True)
        payload["software_target"] = {
            "status": "ERROR",
            "cleanup_status": "NOT_CREATED",
            "command": {"stderr": "container did not start"},
        }
        findings, summary, _checks, software = evaluate_baremetal_environment(
            payload,
            BaremetalPreflightPolicy(
                software_mode="docker",
                docker_image="repo/train:1",
                required_python_packages=("torch",),
                require_compiler=True,
            ),
        )
        reasons = {item.reason_code for item in findings}
        self.assertIn("SOFTWARE_TARGET_PROBE_FAILED", reasons)
        self.assertIn("HIPCC_NOT_AVAILABLE", reasons)
        self.assertIn("TORCH_IMPORT_FAILED", reasons)
        self.assertIsNone(summary["dtk_version"])
        self.assertIsNone(summary["torch_version"])
        self.assertEqual(software["target_status"], "ERROR")

    def test_docker_os_is_retained_separately_from_host_hardware_os(self):
        payload = environment_payload(torch_importable=False)
        payload["system"]["os_release"]["PRETTY_NAME"] = "Hygon OS 8.9"
        inventory = selected_software_inventory()
        inventory["system"]["os_release"]["PRETTY_NAME"] = "Ubuntu 22.04"
        payload["software_target"] = {
            "status": "SUCCESS",
            "cleanup_status": "REMOVED_AUTOMATICALLY",
            "inventory": inventory,
        }
        policy = BaremetalPreflightPolicy(
            software_mode="docker",
            docker_image="repo/train:1",
            require_compiler=True,
        )
        _findings, summary, _checks, software = evaluate_baremetal_environment(
            payload, policy
        )
        self.assertEqual(summary["container_os"], "Hygon OS 8.9")
        self.assertEqual(summary["software_target_os"], "Ubuntu 22.04")
        # Selected target DTK/compiler semantics remain unchanged.
        self.assertEqual(summary["dtk_version"], "DTK-26.04")
        self.assertEqual(summary["hipcc_version"], "HIP 6.4")

        item = record("node1", environment=summary)
        item["software_environment"] = software
        report = build_baremetal_report(
            records=[item],
            policy=policy,
            transport="ssh",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
        )
        self.assertEqual(report["hardware_groups"][0]["container_os"], "Hygon OS 8.9")
        self.assertNotIn("Ubuntu 22.04", json.dumps(report["hardware_groups"]))

class ReportTests(unittest.TestCase):
    def test_markdown_names_each_low_bandwidth_hca_path(self):
        extra_checks = {
            "enabled": True,
            "status": "FAIL",
            "rounds": 1,
            "taint_mutation": False,
            "ib_state": {"enabled": False},
            "nhc": {"enabled": False},
            "ib_write_bw": {
                "enabled": True,
                "status": "FAIL",
                "rounds": 1,
                "transport": "ssh",
                "minimum_average_gbps": 100.0,
                "summary": {
                    "planned_tests": 1,
                    "passed_pairs": 0,
                    "failed_pairs": 1,
                    "not_verified_pairs": 0,
                    "minimum_average_gbps_observed": 84.8,
                },
                "pairs": [
                    {
                        "source": "node98",
                        "source_hca": "shca_3",
                        "destination": "node37",
                        "destination_hca": "shca_3",
                        "rail_index": 3,
                        "average_gbps": 84.8,
                        "reason_code": "IB_BANDWIDTH_BELOW_THRESHOLD",
                    }
                ],
            },
        }
        report = build_baremetal_report(
            records=[record("node37")],
            policy=BaremetalPreflightPolicy(expected_devices=8),
            transport="ssh",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
            cluster_extra_checks=extra_checks,
        )

        markdown = render_baremetal_markdown(report)

        self.assertIn(
            "Low bandwidth HCA path: `node98:shca_3 -> node37:shca_3`; "
            "rail=3; average=84.8 Gbit/s; threshold=100.0 Gbit/s",
            markdown,
        )

    def test_same_node_results_are_folded_and_unreachable_is_incomplete(self):
        env = {
            "dtk_version": "25.04",
            "driver_version": "6.3",
            "vbios_versions": [],
            "hsw_firmware_versions": [],
            "nic_hardware_profile": [],
            "rdma_hardware_profile": [],
            "rdma_device_count": 4,
            "rdma_active_device_count": 4,
        }
        records = [
            record("node10", environment=env),
            record("node2", environment=env),
            record("node11", status="INCOMPLETE", reachable=False),
        ]
        records[2]["findings"] = [
            {"severity": "UNKNOWN", "reason_code": "SSH_TRANSPORT_FAILED", "message": "down", "device_id": None}
        ]
        report = build_baremetal_report(
            records=records,
            policy=BaremetalPreflightPolicy(expected_devices=8),
            transport="clush",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
        )
        self.assertEqual(report["status"], "INCOMPLETE")
        self.assertEqual(report["tool_version"], __version__)
        self.assertEqual(report["node_result_groups"][0]["nodes"], ["node2", "node10"])
        self.assertEqual(report["summary"]["unreachable_nodes"], 1)
        markdown = render_baremetal_markdown(report)
        self.assertIn("node2, node10", markdown)
        self.assertIn("explicit host-python training software target", markdown)

    def test_transport_failure_never_becomes_blocked_or_ready(self):
        transport = BaremetalNodeResult(
            node="node1",
            transport="ssh",
            command_name="probe",
            command=["python3"],
            returncode=255,
            stdout="",
            stderr="connection refused",
            duration_seconds=1.0,
            error_kind="SSH_TRANSPORT_FAILED",
        )
        result = evaluate_node_result("node1", transport, BaremetalPreflightPolicy())
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertFalse(result["reachable"])
        self.assertEqual(result["findings"][0]["reason_code"], "SSH_TRANSPORT_FAILED")
        self.assertIsNone(result["device_count"])

    def test_remote_command_failure_is_reachable_but_incomplete(self):
        transport = BaremetalNodeResult(
            node="node1",
            transport="clush",
            command_name="probe",
            command=["python3"],
            returncode=127,
            stdout="",
            stderr="python3: not found",
            duration_seconds=1.0,
            error_kind="REMOTE_COMMAND_FAILED",
        )
        result = evaluate_node_result("node1", transport, BaremetalPreflightPolicy())
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertTrue(result["reachable"])

    def test_probe_parser_tolerates_banner_before_json(self):
        payload = {
            "schema_version": "1.0",
            "environment": environment_payload(),
            "metrics": {
                "hy_smi_path": None,
                "rocminfo_path": None,
                "rocminfo": {"rc": 127},
                "bus": {"rc": 127},
                "memory": [],
                "available": [],
                "memory_percent": [],
                "utilization": [],
            },
        }
        transport = BaremetalNodeResult(
            node="node1",
            transport="ssh",
            command_name="probe",
            command=["python3"],
            returncode=0,
            stdout="site banner\n" + json.dumps(payload) + "\n",
            stderr="",
            duration_seconds=1.0,
        )
        result = evaluate_node_result("node1", transport, BaremetalPreflightPolicy())
        self.assertTrue(result["reachable"])
        self.assertIsNone(result["device_count"])
        self.assertNotIn(
            "NODE_PROBE_OUTPUT_INVALID",
            {item["reason_code"] for item in result["findings"]},
        )

    def test_visible_metric_difference_splits_node_groups(self):
        first = record("node1")
        second = record("node2")
        second["metric_summary"]["max_vram_used_percent"] = 4.9
        report = build_baremetal_report(
            records=[first, second],
            policy=BaremetalPreflightPolicy(expected_devices=8),
            transport="clush",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
        )
        self.assertEqual(len(report["node_result_groups"]), 2)
        self.assertEqual(report["scale_assessment"]["status"], "SAMPLE_READY_FULL_SCALE_UNVERIFIED")

    def test_small_memtotal_variation_does_not_split_hardware_groups(self):
        first = record("node1", environment={"mem_total": "527628140 kB"})
        second = record("node2", environment={"mem_total": "527628196 kB"})
        report = build_baremetal_report(
            records=[first, second],
            policy=BaremetalPreflightPolicy(expected_devices=8),
            transport="clush",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
        )
        self.assertEqual(len(report["hardware_groups"]), 1)
        self.assertEqual(report["hardware_groups"][0]["mem_total"], "503 GiB")
        self.assertEqual(report["hardware_groups"][0]["nodes"], ["node1", "node2"])

    def test_real_memory_capacity_difference_splits_hardware_groups(self):
        first = record("node1", environment={"mem_total": "268435456 kB"})
        second = record("node2", environment={"mem_total": "536870912 kB"})
        report = build_baremetal_report(
            records=[first, second],
            policy=BaremetalPreflightPolicy(expected_devices=8),
            transport="clush",
            evidence_dir="/tmp/evidence",
            started_at="start",
            finished_at="finish",
        )
        self.assertEqual(len(report["hardware_groups"]), 2)

    def test_missing_required_slurm_state_is_unknown(self):
        item = record("node1")
        apply_slurm_state(item, None, required=True)
        self.assertEqual(item["status"], "INCOMPLETE")
        self.assertEqual(item["findings"][0]["reason_code"], "SLURM_STATE_MISSING")

    def test_reachable_drained_node_is_blocked_with_scheduler_reason(self):
        item = record("e06r1n11")
        apply_slurm_state(
            item,
            {"state": "drain", "reason": "NHC:ib_tor_ber_high[2]"},
        )
        self.assertEqual(item["status"], "BLOCKED")
        self.assertIn("NHC:ib_tor_ber_high", item["findings"][0]["message"])


class EntrypointTests(unittest.TestCase):
    def test_cli_exposes_version(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            build_parser(prog="hcu-envcheck").parse_args(["--version"])
        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(output.getvalue().strip(), f"hcu-envcheck {__version__}")

    def test_skip_environment_cannot_silently_bypass_rdma_profile(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            main(
                [
                    "k8s-pod",
                    "--namespace",
                    "train",
                    "--pod",
                    "worker-0",
                    "--container",
                    "trainer",
                    "--skip-environment",
                    "--expected-rdma-protocol",
                    "ib",
                    "--output",
                    "unused.json",
                ]
            )
        self.assertEqual(raised.exception.code, 3)

    def test_parse_errors_use_tool_error_exit_code(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            build_parser().parse_args(["unknown-command"])
        self.assertEqual(raised.exception.code, 3)

    def test_validation_errors_use_tool_error_exit_code(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            main(
                [
                    "baremetal-cluster",
                    "--node",
                    "node1",
                    "--samples",
                    "0",
                    "--software-mode",
                    "host-python",
                    "--output-dir",
                    "/tmp/not-created",
                ]
            )
        self.assertEqual(raised.exception.code, 3)

    def test_cli_exposes_slurm_and_baremetal_node_sources(self):
        args = build_parser().parse_args(
            [
                "baremetal-cluster",
                "--slurm-nodelist",
                "e06r1n[01-15]",
                "--software-mode",
                "host-python",
                "--output-dir",
                "/tmp/report",
            ]
        )
        self.assertEqual(args.command, "baremetal-cluster")
        self.assertEqual(args.transport, "auto")
        self.assertEqual(args.software_mode, "host-python")

    def test_software_probe_hides_hcu_runtime_implementation_path(self):
        module = ast.parse(_SOFTWARE_PROBE_SOURCE, filename="hcu-software-probe")
        helper_names = {"is_hcu_hip_runtime_library", "public_library_inventory"}
        helpers = ast.Module(
            body=[
                node
                for node in module.body
                if isinstance(node, ast.FunctionDef) and node.name in helper_names
            ],
            type_ignores=[],
        )
        namespace = {"os": os}
        exec(compile(helpers, "hcu-software-probe-helpers", "exec"), namespace)

        inventory = namespace["public_library_inventory"](
            [
                "/opt/dtk/lib/libamdhip64.so.6",
                "/opt/dtk/lib/librccl.so.1",
            ]
        )

        self.assertEqual(inventory["paths"], ["/opt/dtk/lib/librccl.so.1"])
        self.assertEqual(
            inventory["hcu_hip_runtime"],
            {"component": "HCU HIP runtime", "detected": True},
        )
        self.assertNotIn("amd", json.dumps(inventory).lower())

    def test_remote_probe_is_compressed_to_a_bounded_command_argument(self):
        command = build_remote_probe_command(
            BaremetalPreflightPolicy(
                samples=1,
                busy_sample_quorum=1,
                required_python_packages=("torch",),
            ),
            "python3",
        )
        self.assertEqual(command[:2], ["python3", "-c"])
        self.assertLess(len(command[2]), 100_000)
        self.assertIn("base64.b64decode", command[2])
        encoded = re.search(r"b64decode\('([^']+)'\)", command[2]).group(1)
        source = zlib.decompress(base64.b64decode(encoded)).decode("utf-8")
        module = ast.parse(source, filename="hcu-node-probe")
        software_assignment = next(
            node
            for node in module.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name)
                and target.id == "_software_probe_source"
                for target in node.targets
            )
        )
        software_source = ast.literal_eval(software_assignment.value)

        compile(source, "hcu-node-probe", "exec")
        compile(software_source, "hcu-software-probe", "exec")
        self.assertIn(
            "_hcu_os.environ['HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES'] "
            '= \'["torch"]\'',
            software_source,
        )


    def test_baremetal_cli_passes_nhc_and_ib_extra_checks(self):
        captured = {}

        def fake_run(**kwargs):
            captured.update(kwargs)
            run_dir = Path("/tmp/report-extra/nodes_check_20260729_123456_123456")
            return (
                {"status": "READY", "node_result_groups": []},
                run_dir / "cluster-result.json",
                run_dir / "cluster-summary.md",
            )

        output = io.StringIO()
        with patch("hcu_envcheck.cli.run_baremetal_cluster_preflight", side_effect=fake_run):
            with redirect_stdout(output):
                rc = main(
                    [
                        "baremetal-cluster",
                        "--node",
                        "node1",
                        "--node",
                        "node2",
                        "--software-mode",
                        "host-python",
                        "--require-python-package",
                        "numpy",
                        "--require-python-package",
                        "torch",
                        "--transport",
                        "ssh",
                        "--enable-node-health-checks",
                        "--confirm-nodes-idle",
                        "--enable-nhc",
                        "--nhc-config",
                        "nhc.json",
                        "--enable-ib-write-bw",
                        "--ib-minimum-average-gbps",
                        "100",
                        "--ib-concurrency",
                        "2",
                        "--ib-max-tests",
                        "64",
                        "--output-dir",
                        "/tmp/report-extra",
                    ]
                )

        self.assertEqual(rc, 0)
        extra_checks = captured["extra_checks"]
        self.assertTrue(extra_checks.ib_state.enabled)
        self.assertTrue(extra_checks.nhc.enabled)
        self.assertEqual(extra_checks.nhc.command, ("run_nhc",))
        self.assertIsNone(extra_checks.nhc.installation_source)
        self.assertEqual(extra_checks.nhc.config, "nhc.json")
        self.assertFalse(hasattr(extra_checks, "acs"))
        self.assertTrue(extra_checks.ib.enabled)
        self.assertEqual(extra_checks.ib.minimum_average_gbps, 100.0)
        self.assertEqual(extra_checks.ib.concurrency, 2)
        self.assertEqual(extra_checks.ib.max_tests, 64)
        self.assertEqual(
            captured["policy"].required_python_packages,
            ("numpy", "torch"),
        )
        self.assertIn(
            "RUN_DIR       /tmp/report-extra/nodes_check_20260729_123456_123456",
            output.getvalue().replace("\\", "/"),
        )

    def test_baremetal_cli_requires_an_explicit_software_target(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            build_parser().parse_args(
                [
                    "baremetal-cluster",
                    "--node",
                    "node1",
                    "--output-dir",
                    "/tmp/report",
                ]
            )
        self.assertEqual(raised.exception.code, 3)

    def test_conda_mode_requires_prefix_and_storage_before_node_access(self):
        output = io.StringIO()
        with redirect_stdout(output):
            rc = main(
                [
                    "baremetal-cluster",
                    "--node",
                    "node1",
                    "--software-mode",
                    "conda",
                    "--output-dir",
                    "/tmp/not-created",
                ]
            )
        self.assertEqual(rc, 3)
        self.assertIn("conda_prefix", output.getvalue())

    def test_docker_probe_is_offline_single_container_and_cleans_its_own_cid(self):
        command = build_remote_probe_command(
            BaremetalPreflightPolicy(
                software_mode="docker",
                docker_image="repo/train:1",
                samples=1,
                busy_sample_quorum=1,
            ),
            "python3",
        )
        encoded = re.search(r"b64decode\('([^']+)'\)", command[2]).group(1)
        source = zlib.decompress(base64.b64decode(encoded)).decode("utf-8")
        compile(source, "remote-docker-probe", "exec")
        self.assertIn('_capture([_docker, "image", "inspect"', source)
        self.assertIn("'repo/train:1'", source)
        self.assertEqual(source.count('"run", "--pull=never"'), 1)
        self.assertIn('[_docker, "rm", "-f", _container_id]', source)
        self.assertIn('"--cidfile", _cidfile', source)
        self.assertIn(
            '"--mount", "type=bind,src=/opt/hyhal,dst=/opt/hyhal,readonly"',
            source,
        )
        self.assertIn('if os.path.isdir("/opt/hyhal"):', source)
        self.assertIn('"--read-only"', source)
        self.assertIn('"--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=256m"', source)
        self.assertIn(
            '"--tmpfs", "/var/log/hylog:rw,nosuid,nodev,size=16m"', source
        )
        self.assertEqual(source.count('"--tmpfs"'), 2)
        self.assertEqual(source.count('"--mount"'), 1)
        self.assertNotIn('"--volume"', source)
        self.assertNotIn('/var/log/hylog:/var/log/hylog', source)
        self.assertNotIn('"--privileged"', source)
        self.assertIn('target["inventory"] = inventory', source)
        self.assertIn('/opt/dtk/.dtk_version', source)
        self.assertLess(source.index("finally:"), source.index('[_docker, "rm", "-f", _container_id]'))
        self.assertNotIn("docker exec", source)
        self.assertNotIn("docker ps", source)
        self.assertLess(source.index("main()"), source.index('elif _software_mode == "docker"'))

    def test_conda_shared_plan_keeps_runtime_coverage_on_every_node(self):
        records = [record("node1"), record("node2")]
        for item in records:
            item["software_target"] = {
                "conda_storage_observation": {
                    "prefix": "/share/env",
                    "prefix_exists": True,
                    "python_executable": True,
                    "realpath": "/share/env",
                    "mount_source": "server:/share",
                    "fs_type": "nfs4",
                    "identity_fingerprint": "dev:inode",
                    "collection_status": "SUCCESS",
                    "shared_backend": True,
                }
            }
        plan = apply_conda_collection_plan(
            records,
            BaremetalPreflightPolicy(
                software_mode="conda",
                conda_prefix="/share/env",
                conda_storage="shared",
            ),
        )
        self.assertEqual(plan["observed_storage_mode"], "shared")
        self.assertEqual(plan["artifact_probe_nodes"], ["node1"])
        self.assertEqual(plan["runtime_probe_nodes"], ["node1", "node2"])

if __name__ == "__main__":
    unittest.main()
