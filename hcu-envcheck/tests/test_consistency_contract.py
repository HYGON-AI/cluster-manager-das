# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Configuration equality must use the same evidence in JSON and Markdown."""

import copy
import unittest

from cluster_run.consistency import build_consistency_summary, render_consistency_markdown
from hcu_envcheck.baremetal_cluster import (
    BaremetalPreflightPolicy, build_baremetal_report, build_node_first_result,
    render_baremetal_markdown,
)


def node(name):
    return {
        "node": name, "status": "READY", "reachable": True, "device_count": 1,
        "devices": [{"device_id": 0, "model": "HCU", "architecture": "gfx942",
                     "hy_smi_total_mib": 65536, "used_mib": 0, "status": "PASS"}],
        "environment": {
            "container_os": "Linux", "kernel": "6.1", "mem_total": "536870912 kB",
            "driver_version": "6.4", "dtk_version": "26.04", "python_version": "3.12",
            "python_packages": {"torch": "2.10", "triton": "3.4"},
            "torch_nccl_version": "2.27", "rccl_paths": ["/share/dtk/lib/librccl.so.1"],
            "runtime_env": {"ROCM_PATH": "/share/dtk", "UCX_TLS": "rc"},
            "dev_shm": {"total_bytes": 1024, "available_bytes": 900},
        },
        "checks": [], "findings": [], "metric_summary": {},
    }


def report_for(records, scope="platform-and-resource"):
    return {"nodes": records, "status": "READY", "consistency_summary": build_consistency_summary(records, scope=scope)}


class ConsistencyContractTests(unittest.TestCase):
    def test_package_version_differences_reach_json_markdown_and_cluster(self):
        records = [node("n01"), node("n02")]
        records[1]["environment"]["python_packages"]["triton"] = "3.5"
        report = report_for(records)
        classified = build_node_first_result(report)
        self.assertEqual(report["consistency_summary"]["configuration_group_count"], 2)
        self.assertEqual(len(classified["software_components"]["configuration_nodes"]), 2)
        self.assertEqual(len(classified["software_components"]["nodes"]), 2)
        markdown = render_baremetal_markdown(report)
        self.assertIn("python_packages", markdown)
        self.assertIn("3.5", markdown)
        self.assertIn("DIFFERENT", markdown)

    def test_resource_ram_difference_is_configuration_difference(self):
        records = [node("n01"), node("n02")]
        records[1]["environment"]["mem_total"] = "1073741824 kB"
        report = report_for(records, "resource-only")
        classified = build_node_first_result(report)
        summary = report["consistency_summary"]
        self.assertEqual(summary["configuration_group_count"], 2)
        self.assertIn("mem_total", summary["differences_from_reference"][0]["changes"]["resource"])
        self.assertEqual(len(classified["system"]["configuration_nodes"]), 2)
        markdown = render_baremetal_markdown(report)
        self.assertIn("1073741824", markdown)
        self.assertNotIn("一致性分组反映瞬时资源状态", markdown)

    def test_rccl_and_runtime_settings_each_split_configuration(self):
        for field, value in (("torch_nccl_version", "2.28"), ("rccl_paths", ["/share/other/librccl.so.2"]),
                             ("runtime_env", {"UCX_TLS": "tcp"}),
                             ("library_components", {"rccl": {"versions": ["2"]}})):
            with self.subTest(field=field):
                records = [node("n01"), node("n02")]
                records[1]["environment"][field] = value
                report = report_for(records, "platform-only")
                self.assertEqual(report["consistency_summary"]["configuration_group_count"], 2)
                self.assertEqual(len(build_node_first_result(report)["software_components"]["configuration_nodes"]), 2)

    def test_utilization_counters_identity_and_check_outcomes_do_not_split_config(self):
        records = [node("n01"), node("n02")]
        second = records[1]
        second["status"] = "BLOCKED"
        second["devices"][0].update(used_mib=60000, status="FAIL", bdf="0000:33:00.0")
        second["metric_summary"] = {"max_hcu_util_percent": 98}
        second["environment"].update(rdma_active_port_count=0, ib_counter_health={"status": "FAIL"})
        second["environment"]["dev_shm"]["available_bytes"] = 10
        second["checks"] = [{"check_id": "TORCH_HCU_AVAILABLE", "status": "FAIL", "message": "busy"}]
        report = report_for(records)
        classified = build_node_first_result(report)
        self.assertEqual(report["consistency_summary"]["configuration_group_count"], 1)
        for category in ("hardware_devices", "system", "software_components", "network_rdma"):
            self.assertEqual(len(classified[category]["configuration_nodes"]), 1)
        self.assertEqual(len(classified["resource_state"]["nodes"]), 2)
        self.assertIn("已采集的上述类别中未发现跨节点差异", render_baremetal_markdown(report))

    def test_missing_evidence_never_claims_equal_configuration(self):
        records = [{"node": name, "status": "INCOMPLETE", "reachable": False} for name in ("n01", "n02")]
        summary = build_consistency_summary(records)
        self.assertEqual(summary["comparison_status"], "UNVERIFIED")
        self.assertEqual(summary["configuration_unknown_nodes"], ["n01", "n02"])
        self.assertEqual(summary["configuration_groups"][0]["configuration_evidence"], "PARTIAL")
        self.assertIn("不能确认", render_consistency_markdown(summary))
        self.assertIn("UNVERIFIED", render_baremetal_markdown(report_for(records)))

    def test_equal_partial_config_is_not_proof_of_equality(self):
        records = [node("n01"), node("n02")]
        for record in records:
            del record["environment"]["dtk_version"]
        summary = build_consistency_summary(records)
        self.assertEqual(summary["configuration_group_count"], 1)
        self.assertEqual(summary["comparison_status"], "UNVERIFIED")
        self.assertIn("platform.dtk_version", summary["configuration_missing_by_node"]["n01"])

    def test_category_scope_excludes_unrequested_software(self):
        records = [node("n01"), node("n02")]
        records[1]["environment"]["python_packages"]["torch"] = "other"
        summary = build_consistency_summary(records, scope="resource-only")
        self.assertEqual(summary["configuration_group_count"], 1)
        self.assertEqual(summary["comparison_status"], "SAME_OBSERVED_CONFIGURATION")
        records[1]["devices"][0]["hy_smi_total_mib"] = 131072
        self.assertEqual(build_consistency_summary(records, scope="resource-only")["configuration_group_count"], 2)

    def test_configuration_order_and_launcher_ranks_are_not_config_differences(self):
        records = [node("n01"), node("n02")]
        for index, record in enumerate(records):
            record["environment"]["runtime_env"].update(RANK=str(index), OMPI_COMM_WORLD_RANK=str(index))
            record["environment"]["cpu_models"] = ["A", "B"] if index == 0 else ["B", "A"]
        report = report_for(records)
        self.assertEqual(report["consistency_summary"]["configuration_group_count"], 1)
        self.assertEqual(len(build_node_first_result(report)["hardware_devices"]["configuration_nodes"]), 1)
        full_report = build_baremetal_report(records=records,
            policy=BaremetalPreflightPolicy(), transport="ssh", evidence_dir="evidence",
            started_at="start", finished_at="end")
        self.assertFalse(any(item.get("field") in {"runtime_env", "cpu_models"}
                             for item in full_report["consistency_findings"]))

    def test_resource_report_does_not_require_platform_consistency_evidence(self):
        records = [node("n01"), node("n02")]
        for record in records:
            record["environment"] = {"mem_total": "536870912 kB"}
        report = build_baremetal_report(records=records,
            policy=BaremetalPreflightPolicy(check_categories=("resource",)), transport="ssh",
            evidence_dir="evidence", started_at="start", finished_at="end")
        self.assertEqual(report["status"], "READY")
        self.assertFalse(report["consistency_findings"])

    def test_device_status_does_not_hide_memory_capacity_change(self):
        first = node("n01")
        second = copy.deepcopy(first)
        second["node"] = "n02"
        second["devices"][0].update(hy_smi_total_mib=32768, used_mib=0)
        report = report_for([first, second], "resource-only")
        self.assertEqual(report["consistency_summary"]["configuration_group_count"], 2)
        self.assertEqual(len(build_node_first_result(report)["hardware_devices"]["configuration_nodes"]), 2)


if __name__ == "__main__":
    unittest.main()
