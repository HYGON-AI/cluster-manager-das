# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import tempfile
import unittest
import copy
from pathlib import Path

from hcu_envcheck.k8s_cluster import (
    _build_check_coverage,
    _build_scale_readiness,
    _environment_field_values,
    _group_hardware_environments,
    _rdma_protocol_consistency_findings,
    _group_node_results,
    _group_software_environments,
    _write_cluster_markdown,
)


def _record(node: str, *, hcusmi: str, runtime_env: dict[str, str]) -> dict:
    environment = {
        "kernel": "5.15.0-25-generic",
        "cpu_logical_count": 128,
        "cpu_models": ["Hygon C86 Processor"],
        "mem_total": "528095272 kB",
        "dtk_version": "DTK-26.04-rc4",
        "driver_version": "6.3.32-V1.6.0",
        "hy_smi_version": "1.25.0",
        "smi_library_version": "7.6.0-0.10",
        "hipcc_version": "25.10.0-0",
        "rccl_paths": ["/opt/dtk/lib/librccl.so"],
        "ucx_version": "1.20.1",
        "mpi_version": "5.0.3",
        "physical_nic_count": 6,
        "nic_drivers": ["shca_core", "sxe"],
        "nic_inventory": [
            {
                "vendor": "Dawning Information Industry Co., Ltd.",
                "model": "Device",
                "pci_id": "1cb8:11a2",
                "subsystem_pci_id": "1cb8:1c11",
                "driver": "shca_core",
                "driver_version": "1.0",
                "firmware_version": "2.0",
                "class": "Infiniband controller",
                "local_link": "UP",
                "speed_mbps": "400000",
                "mtu": "4096",
                "count": 4,
            }
        ],
        "nic_link_summary": {"UP": 6, "DOWN": 0, "UNKNOWN": 0},
        "nic_hardware_profile": [
            {
                "pci_id": "1cb8:11a2",
                "subsystem_pci_id": "1cb8:1c11",
                "driver": "shca_core",
                "driver_version": "1.0",
                "firmware_version": "2.0",
                "count": 4,
            }
        ],
        "nic_link_profile": [
            {
                "pci_id": "1cb8:11a2",
                "local_link": "UP",
                "speed_mbps": "400000",
                "mtu": "4096",
                "count": 4,
            }
        ],
        "rdma_nic_inventory": [],
        "rdma_hardware_profile": [],
        "pci_name_source": "lspci-pci.ids",
        "rdma_device_count": 4,
        "rdma_active_port_count": 4,
        "rdma_rates": ["400 Gb/sec (4X NDR)"],
        "vbios_versions": ["6.314.002400Q.998011"],
        "hsw_firmware_versions": ["1.1.0.B114"],
        "container_os": "Ubuntu 22.04.5 LTS",
        "python_version": "3.10.12",
        "python_packages": {"torch": "2.10.0", "hcusmi": hcusmi},
        "core_python_packages": {"torch": "2.10.0", "hcusmi": hcusmi},
        "torch_version": "2.10.0",
        "torch_hip_version": "6.3.26113",
        "torch_device_count": 8,
        "torch_hcu_available": True,
        "torch_distributed_available": True,
        "torch_nccl_backend_available": True,
        "torch_nccl_version": [2, 22, 3],
        "runtime_env": runtime_env,
    }
    return {
        "node": node,
        "status": "READY",
        "probe_source": "temporary-pod",
        "cleanup_status": "DELETED",
        "summary": {
            "device_count": 8,
            "expected_device_count": 8,
            "models": ["BW"],
            "architectures": ["gfx936"],
            "vram_total_mib": [65520.0],
            "max_vram_used_percent": 0.0,
            "max_hcu_util_percent": 0.0,
            "reason_codes": [],
            "image_id": "docker://sha256:same",
            "environment": environment,
        },
    }


class ClusterReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.records = [
            _record("node35", hcusmi="7.6.0", runtime_env={"HIP_PATH": "/opt/dtk"}),
            _record("node36", hcusmi="24.5.3", runtime_env={"HIP_PATH": "/opt/dtk-old"}),
        ]

    def test_software_drift_does_not_split_hardware_group(self):
        groups = _group_hardware_environments(self.records)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["nodes"], ["node35", "node36"])

    def test_software_drift_still_splits_software_group(self):
        groups = _group_software_environments(self.records)
        self.assertEqual(len(groups), 2)

    def test_nic_firmware_difference_splits_hardware_group(self):
        records = copy.deepcopy(self.records)
        records[1]["summary"]["environment"]["nic_hardware_profile"][0][
            "firmware_version"
        ] = "different"
        groups = _group_hardware_environments(records)
        self.assertEqual(len(groups), 2)

    def test_folded_node_names_use_natural_order(self):
        records = [
            _record("node10", hcusmi="1.0.0", runtime_env={"HIP_PATH": "/opt/dtk"}),
            _record("node2", hcusmi="1.0.0", runtime_env={"HIP_PATH": "/opt/dtk"}),
        ]
        groups = _group_hardware_environments(records)
        self.assertEqual(groups[0]["nodes"], ["node2", "node10"])

    def test_identical_visible_node_results_are_folded(self):
        groups = _group_node_results(self.records)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["nodes"], ["node35", "node36"])

    def test_visible_result_difference_splits_group(self):
        self.records[1]["summary"]["max_vram_used_percent"] = 0.06
        groups = _group_node_results(self.records)
        self.assertEqual(len(groups), 2)

    def test_check_boundaries_are_explicit_statuses(self):
        coverage = _build_check_coverage(self.records, include_environment=True)
        status = {item["item"]: item["status"] for item in coverage}
        self.assertEqual(status["HCU枚举、显存与利用率"], "CHECKED")
        self.assertEqual(
            status["交换机端口、PFC/ECN、队列与光模块"],
            "REQUIRES_EXTERNAL_ACCESS",
        )
        self.assertEqual(
            status["RCCL collective与RDMA主动流量"],
            "NOT_EXECUTED_BY_DESIGN",
        )

    def test_failed_probe_is_not_counted_as_checked_hcu_inventory(self):
        failed = {
            "node": "node35",
            "status": "INCOMPLETE",
            "summary": {"device_count": None, "environment": {}},
        }
        coverage = _build_check_coverage([failed], include_environment=True)
        status = {item["item"]: item["status"] for item in coverage}
        self.assertEqual(status["HCU枚举、显存与利用率"], "NOT_EXECUTED")

    def test_partial_probe_coverage_is_reported_as_partial(self):
        failed = {
            "node": "node37",
            "status": "INCOMPLETE",
            "summary": {"device_count": None, "environment": {}},
        }
        coverage = _build_check_coverage([self.records[0], failed], include_environment=True)
        status = {item["item"]: item["status"] for item in coverage}
        self.assertEqual(status["HCU枚举、显存与利用率"], "PARTIAL")
        self.assertEqual(status["宿主硬件、驱动、DTK、NIC/RDMA"], "PARTIAL")

    def test_missing_environment_evidence_is_not_an_inconsistent_value(self):
        failed = {
            "node": "node37",
            "status": "INCOMPLETE",
            "summary": {"device_count": None, "environment": {}},
        }
        values, missing = _environment_field_values(
            [self.records[0], failed],
            "driver_version",
        )
        self.assertEqual(len(values), 1)
        self.assertEqual(missing, ["node37"])

    def test_report_folds_nodes_removes_boundaries_and_old_software_heading(self):
        consistency = [
            {
                "reason_code": "PYTHON_CORE_PACKAGE_INCONSISTENT",
                "field": "core_python_packages",
                "values": {"7.6.0": ["node35"], "24.5.3": ["node36"]},
            }
        ]
        report = {
            "status": "BLOCKED",
            "policy": {"strict_stack_consistency": True},
            "nodes": self.records,
            "node_result_groups": _group_node_results(self.records),
            "groups": [],
            "hardware_environment_groups": _group_hardware_environments(self.records),
            "software_environment_groups": _group_software_environments(self.records),
            "check_coverage": _build_check_coverage(self.records, include_environment=True),
            "scale_readiness": _build_scale_readiness(
                self.records,
                cluster_status="BLOCKED",
                target_devices=10000,
                devices_per_node=8,
                consistency_findings=consistency,
            ),
            "consistency_findings": consistency,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-summary.md"
            _write_cluster_markdown(report, path)
            text = path.read_text(encoding="utf-8")
        level_two_headers = [line for line in text.splitlines() if line.startswith("## ")]
        self.assertEqual(
            level_two_headers[-1],
            "## 多节点一致性判定",
        )
        self.assertNotIn("## 检查覆盖与边界", text)
        self.assertNotIn("## 软件栈一致性、Torch 与 Python 依赖（报告最后）", text)
        self.assertNotIn("## Torch 与 Python 依赖", text)
        self.assertNotIn("## 相同环境结果折叠", text)
        self.assertIn("| node35, node36 |", text)
        self.assertIn("Dawning Information Industry", text)
        self.assertIn("## 万卡规模适用性评估（静态检查，非万卡训练实测）", text)
        hardware_section = text.split("## 硬件、驱动与通信环境（相同结果折叠）", 1)[1].split(
            "## 万卡规模适用性评估", 1
        )[0]
        self.assertNotIn("关键运行环境", hardware_section)
        self.assertIn("### 依赖分组（相同结果折叠）", text)
        self.assertIn("关键运行环境", text.split(level_two_headers[-1], 1)[1])

    def test_report_does_not_claim_consistency_when_strict_check_is_disabled(self):
        report = {
            "status": "READY",
            "policy": {"strict_stack_consistency": False},
            "nodes": self.records,
            "node_result_groups": _group_node_results(self.records),
            "hardware_environment_groups": _group_hardware_environments(self.records),
            "software_environment_groups": _group_software_environments(self.records),
            "scale_readiness": _build_scale_readiness(
                self.records,
                cluster_status="READY",
                target_devices=10000,
                devices_per_node=8,
                consistency_findings=[],
            ),
            "consistency_findings": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-summary.md"
            _write_cluster_markdown(report, path)
            text = path.read_text(encoding="utf-8")
        self.assertIn("RDMA协议/Fabric强制一致性未发现偏差", text)
        self.assertIn("未启用其余软件栈严格一致性比较", text)
        self.assertNotIn("PASS：当前 profile 纳入比较的组件在所有节点一致", text)

    def test_report_marks_rdma_consistency_not_executed_when_environment_skipped(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                report = {
                    "status": "READY",
                    "policy": {
                        "include_environment": False,
                        "strict_stack_consistency": strict,
                    },
                    "nodes": self.records,
                    "node_result_groups": _group_node_results(self.records),
                    "hardware_environment_groups": _group_hardware_environments(self.records),
                    "software_environment_groups": _group_software_environments(self.records),
                    "scale_readiness": _build_scale_readiness(
                        self.records,
                        cluster_status="READY",
                        target_devices=10000,
                        devices_per_node=8,
                        consistency_findings=[],
                    ),
                    "consistency_findings": [],
                }
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "cluster-summary.md"
                    _write_cluster_markdown(report, path)
                    text = path.read_text(encoding="utf-8")
                self.assertIn("NOT_EXECUTED", text)
                self.assertIn("RDMA协议/Fabric一致性未执行", text)
                self.assertNotIn("RDMA协议/Fabric强制一致性未发现偏差", text)
                self.assertNotIn(
                    "PASS：当前 profile 纳入比较的组件在所有节点一致", text
                )

    def test_mandatory_rdma_finding_is_visible_without_strict_stack(self):
        report = {
            "status": "BLOCKED",
            "policy": {"strict_stack_consistency": False},
            "nodes": self.records,
            "node_result_groups": _group_node_results(self.records),
            "hardware_environment_groups": _group_hardware_environments(self.records),
            "software_environment_groups": _group_software_environments(self.records),
            "scale_readiness": _build_scale_readiness(
                self.records,
                cluster_status="BLOCKED",
                target_devices=10000,
                devices_per_node=8,
                consistency_findings=[],
            ),
            "consistency_findings": [
                {
                    "severity": "FAIL",
                    "reason_code": "RDMA_PROTOCOL_CLUSTER_MIXED",
                    "field": "rdma_current_protocol",
                    "values": {"NATIVE_INFINIBAND": ["node35"], "ROCE": ["node36"]},
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-summary.md"
            _write_cluster_markdown(report, path)
            text = path.read_text(encoding="utf-8")
        self.assertIn("RDMA_PROTOCOL_CLUSTER_MIXED", text)

    def test_rdma_fabric_group_records_reach_scale_and_markdown_without_crash(self):
        records = copy.deepcopy(self.records)
        profiles = (
            {
                "protocol": "NATIVE_INFINIBAND",
                "port_profiles": [
                    {"device": "hca0", "port": "1", "subnet_prefix": "fabric-a"}
                ],
            },
            {
                "protocol": "NATIVE_INFINIBAND",
                "port_profiles": [
                    {"device": "hca0", "port": "1", "subnet_prefix": "fabric-b"}
                ],
            },
        )
        for record, profile in zip(records, profiles):
            environment = record["summary"]["environment"]
            environment["rdma_current_protocol"] = "NATIVE_INFINIBAND"
            environment["rdma_fabric_profile"] = profile

        consistency = _rdma_protocol_consistency_findings(records)
        fabric = next(
            item
            for item in consistency
            if item["reason_code"] == "RDMA_FABRIC_PROFILE_INCONSISTENT"
        )
        self.assertIsInstance(fabric["values"], list)
        self.assertEqual(fabric["value_groups"], fabric["values"])
        # Canonical-only findings must remain consumable by scale and Markdown.
        fabric.pop("values")

        scale = _build_scale_readiness(
            records,
            cluster_status="BLOCKED",
            target_devices=10000,
            devices_per_node=8,
            consistency_findings=consistency,
        )
        self.assertEqual(scale["status"], "NOT_READY")
        self.assertEqual(
            scale["consistency_deviation_nodes"], ["node35", "node36"]
        )
        self.assertTrue(scale["consistency_reference_ambiguous"])
        self.assertEqual(
            scale["consistency_ambiguous_nodes"], ["node35", "node36"]
        )

        report = {
            "status": "BLOCKED",
            "policy": {"include_environment": True, "strict_stack_consistency": False},
            "nodes": records,
            "node_result_groups": _group_node_results(records),
            "hardware_environment_groups": _group_hardware_environments(records),
            "software_environment_groups": _group_software_environments(records),
            "scale_readiness": scale,
            "consistency_findings": consistency,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-summary.md"
            _write_cluster_markdown(report, path)
            markdown = path.read_text(encoding="utf-8")
        self.assertIn("RDMA_FABRIC_PROFILE_INCONSISTENT", markdown)
        self.assertIn("一致性参考歧义：是", markdown)

    def test_scale_readiness_accepts_dict_list_and_malformed_value_schemas(self):
        findings = [
            {
                "severity": "FAIL",
                "reason_code": "DICT_SCHEMA",
                "values": {"a": ["node35"], "b": ["node36"]},
            },
            {
                "severity": "FAIL",
                "reason_code": "LIST_SCHEMA",
                "values": [
                    {"value": {"profile": "a"}, "nodes": ["node37"]},
                    {"value": {"profile": "b"}, "nodes": ["node38"]},
                ],
            },
            {
                "severity": "FAIL",
                "reason_code": "FALLBACK_SCHEMA",
                "value_groups": "not-a-group-schema",
                "values": {
                    "baseline": ["node39", "node40"],
                    "minority": ["node41"],
                },
            },
            {
                "severity": "FAIL",
                "reason_code": "MALFORMED_SCHEMA",
                "values": "not-a-group-schema",
            },
        ]
        scale = _build_scale_readiness(
            self.records,
            cluster_status="BLOCKED",
            target_devices=10000,
            devices_per_node=8,
            consistency_findings=findings,
        )
        self.assertEqual(
            scale["consistency_deviation_nodes"],
            ["node35", "node36", "node37", "node38", "node41"],
        )
        self.assertTrue(scale["consistency_reference_ambiguous"])
        self.assertEqual(
            scale["consistency_ambiguous_nodes"],
            ["node35", "node36", "node37", "node38"],
        )
        self.assertEqual(
            scale["consistency_reason_codes"],
            ["DICT_SCHEMA", "FALLBACK_SCHEMA", "LIST_SCHEMA", "MALFORMED_SCHEMA"],
        )
        self.assertEqual(
            scale["consistency_ambiguous_reason_codes"],
            ["DICT_SCHEMA", "LIST_SCHEMA"],
        )

    def test_scale_readiness_attributes_only_unique_minority_group(self):
        scale = _build_scale_readiness(
            self.records,
            cluster_status="BLOCKED",
            target_devices=10000,
            devices_per_node=8,
            consistency_findings=[
                {
                    "severity": "FAIL",
                    "reason_code": "UNIQUE_MAJORITY",
                    "values": {
                        "baseline": ["node35", "node36"],
                        "minority": ["node37"],
                    },
                }
            ],
        )
        self.assertEqual(scale["consistency_deviation_nodes"], ["node37"])
        self.assertFalse(scale["consistency_reference_ambiguous"])
        self.assertEqual(scale["consistency_ambiguous_nodes"], [])

    def test_five_node_ready_sample_never_claims_full_scale_ready(self):
        assessment = _build_scale_readiness(
            self.records,
            cluster_status="READY",
            target_devices=10000,
            devices_per_node=8,
            consistency_findings=[],
        )
        self.assertEqual(assessment["status"], "SAMPLE_READY_FULL_SCALE_UNVERIFIED")
        self.assertFalse(assessment["is_full_scale_test"])
        self.assertNotIn("万卡规模可用", assessment["conclusion"])

    def test_full_static_coverage_does_not_claim_training_ready(self):
        assessment = _build_scale_readiness(
            self.records,
            cluster_status="READY",
            target_devices=16,
            devices_per_node=8,
            consistency_findings=[],
        )
        self.assertEqual(
            assessment["status"],
            "FULL_SCALE_STATIC_PREFLIGHT_PASSED_RUNTIME_UNVERIFIED",
        )
        self.assertTrue(assessment["static_coverage_reached"])
        self.assertFalse(assessment["is_full_scale_test"])

    def test_scale_assessment_lists_nodes_missing_consistency_evidence(self):
        assessment = _build_scale_readiness(
            self.records,
            cluster_status="INCOMPLETE",
            target_devices=10000,
            devices_per_node=8,
            consistency_findings=[
                {
                    "severity": "UNKNOWN",
                    "reason_code": "STACK_COMPONENT_EVIDENCE_MISSING",
                    "missing_nodes": ["node36"],
                }
            ],
        )
        self.assertEqual(assessment["incomplete_nodes"], ["node36"])

    def test_incomplete_environment_can_still_render_report(self):
        failed = {
            "node": "node37",
            "status": "INCOMPLETE",
            "probe_source": "temporary-pod",
            "cleanup_status": "DELETED",
            "summary": {
                "device_count": None,
                "max_vram_used_percent": None,
                "max_hcu_util_percent": None,
                "reason_codes": ["K8S_PROBE_FAILED"],
                "environment": {},
                "image_id": None,
            },
        }
        report = {
            "status": "INCOMPLETE",
            "nodes": [failed],
            "node_result_groups": _group_node_results([failed]),
            "hardware_environment_groups": _group_hardware_environments([failed]),
            "software_environment_groups": _group_software_environments([failed]),
            "scale_readiness": _build_scale_readiness(
                [failed],
                cluster_status="INCOMPLETE",
                target_devices=10000,
                devices_per_node=8,
                consistency_findings=[],
            ),
            "consistency_findings": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster-summary.md"
            _write_cluster_markdown(report, path)
            text = path.read_text(encoding="utf-8")
        self.assertIn("node37", text)
        self.assertIn("未识别；仅原始PCI证据可用", text)
        self.assertNotIn("：None", text)


if __name__ == "__main__":
    unittest.main()
