# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import unittest

from hcu_envcheck.ib_counters import DEFAULT_IB_COUNTER_RULES
from hcu_envcheck.environment import evaluate_environment
from hcu_envcheck.k8s_cluster import build_probe_manifest, parse_probe_env, parse_reuse_pods
from hcu_envcheck.parsers import parse_hy_smi_samples, parse_rocminfo
from hcu_envcheck.preflight import evaluate_metrics


MEMORY = '{"card0":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"20413"},"card1":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"}}'
LOW_MEMORY = '{"card0":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"},"card1":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"}}'
AVAILABLE = '{"card0":{"Available memory size (MiB)":"45027"},"card1":{"Available memory size (MiB)":"65453"}}'
MEMORY_PERCENT = '{"card0":{"HCU memory use (%)":"31"},"card1":{"HCU memory use (%)":"0"}}'
LOW_MEMORY_PERCENT = '{"card0":{"HCU memory use (%)":"0"},"card1":{"HCU memory use (%)":"0"}}'
UTILIZATION = '{"card0":{"HCU use (%)":"0.0"},"card1":{"HCU use (%)":"0.0"}}'
BUSY_UTILIZATION = '{"card0":{"HCU use (%)":"0.0"},"card1":{"HCU use (%)":"27.5"}}'
BUS = '{"card0":{"PCI Bus":"0000:09:00.0"},"card1":{"PCI Bus":"0000:36:00.0"}}'
ROCMINFO = """
Agent 1
  Name:                    Hygon C86 Processor
  Device Type:             CPU
Agent 9
  Name:                    gfx936
  Uuid:                    GPU-a
  Marketing Name:          BW
  BDFID:                   2304
  Device Type:             HCU
  Pool Info:
    Pool 1
      Segment:                 GLOBAL; FLAGS: COARSE GRAINED
      Size:                    67092480(0x3ffc000) KB
  ISA Info:
Agent 10
  Name:                    gfx936
  Uuid:                    GPU-b
  Marketing Name:          BW
  BDFID:                   13824
  Device Type:             HCU
  Pool Info:
    Pool 1
      Segment:                 GLOBAL; FLAGS: COARSE GRAINED
      Size:                    67092480(0x3ffc000) KB
  ISA Info:
"""


def healthy_ib_counter_window():
    values = {name: "0" for name in DEFAULT_IB_COUNTER_RULES}
    return {
        "status": "COMPLETE",
        "configured_value": "5",
        "configured_interval_seconds": 5,
        "interval_seconds": 5,
        "before": {"counter_status": "COMPLETE", "counters": dict(values)},
        "after": {"counter_status": "COMPLETE", "counters": dict(values)},
    }


class ParserTests(unittest.TestCase):
    def test_parse_reuse_pod(self):
        self.assertEqual(
            parse_reuse_pods(["node36=ai-video/ai-video/ai-video"]),
            {"node36": ("ai-video", "ai-video", "ai-video")},
        )

    def test_probe_manifest_targets_one_node_and_requests_all_devices(self):
        manifest = build_probe_manifest(
            namespace="ai-video",
            pod_name="hcu-envcheck-run-node35",
            container_name="probe",
            node="node35",
            image="registry/image:tag",
            image_pull_policy="IfNotPresent",
            device_resource_name="hygon.com/hcu",
            device_count=8,
            run_id="abc123",
            node_taints=[
                {
                    "key": "node-role.hygon.io/system",
                    "value": "true",
                    "effect": "NoSchedule",
                },
                {
                    "key": "hygon.com/hcu",
                    "value": "training",
                    "effect": "NoSchedule",
                },
                {
                    "key": "node.kubernetes.io/memory-pressure",
                    "effect": "NoSchedule",
                },
            ],
            active_deadline_seconds=600,
            probe_env={"HIP_PATH": "/opt/dtk/hip"},
        )
        spec = manifest["spec"]
        self.assertEqual(spec["nodeSelector"]["kubernetes.io/hostname"], "node35")
        container = spec["containers"][0]
        self.assertEqual(container["resources"]["limits"]["hygon.com/hcu"], "8")
        self.assertEqual(container["resources"]["requests"]["hygon.com/hcu"], "8")
        self.assertEqual(container["resources"]["requests"]["memory"], "1Gi")
        self.assertEqual(container["resources"]["limits"]["memory"], "8Gi")
        self.assertTrue(container["securityContext"]["privileged"])
        self.assertEqual(container["env"], [{"name": "HIP_PATH", "value": "/opt/dtk/hip"}])
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertTrue(spec["hostNetwork"])
        self.assertEqual(
            spec["tolerations"],
            [
                {
                    "key": "hygon.com/hcu",
                    "operator": "Exists",
                    "effect": "NoSchedule",
                }
            ],
        )

    def test_probe_env_allowlist(self):
        self.assertEqual(
            parse_probe_env(["HIP_PATH=/opt/dtk/hip", "ROCM_PATH=/opt/dtk"]),
            {"HIP_PATH": "/opt/dtk/hip", "ROCM_PATH": "/opt/dtk"},
        )
        with self.assertRaises(ValueError):
            parse_probe_env(["PASSWORD=secret"])

    def test_parse_hy_smi_json(self):
        cards = parse_hy_smi_samples(
            [MEMORY], [AVAILABLE], [MEMORY_PERCENT], [UTILIZATION], BUS
        )
        self.assertEqual(cards[0]["total_mib"], 65520)
        self.assertEqual(cards[0]["used_mib"], 20413)
        self.assertEqual(cards[0]["available_mib"], 45027)
        self.assertEqual(cards[0]["bdf"], "0000:09:00.0")

    def test_parse_rocminfo_hcu_agents(self):
        agents = parse_rocminfo(ROCMINFO)
        self.assertEqual(len(agents), 2)
        self.assertEqual(agents[0].bdf, "0000:09:00.0")
        self.assertEqual(agents[0].total_mib, 65520)
        self.assertEqual(agents[0].model, "BW")

    def test_high_vram_blocks_preflight(self):
        cards = parse_hy_smi_samples(
            [MEMORY], [AVAILABLE], [MEMORY_PERCENT], [UTILIZATION], BUS
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=5,
            max_hcu_util_percent=5,
        )
        self.assertEqual(status, "BLOCKED")
        self.assertEqual(devices[0].status, "FAIL")
        self.assertIn("VRAM_IN_USE", [item.reason_code for item in findings])
        self.assertEqual(devices[1].status, "PASS")

    def test_high_hcu_utilization_blocks_preflight(self):
        low_memory = '{"card0":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"},"card1":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"}}'
        cards = parse_hy_smi_samples(
            [low_memory], [AVAILABLE], [LOW_MEMORY_PERCENT], [BUSY_UTILIZATION], BUS
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=50,
            max_hcu_util_percent=5,
        )
        self.assertEqual(status, "BLOCKED")
        self.assertEqual(devices[1].status, "FAIL")
        self.assertIn("HCU_BUSY", [item.reason_code for item in findings])

    def test_single_busy_sample_is_transient_warning(self):
        low_memory = '{"card0":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"},"card1":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"}}'
        cards = parse_hy_smi_samples(
            [low_memory] * 3,
            [AVAILABLE] * 3,
            [LOW_MEMORY_PERCENT] * 3,
            [BUSY_UTILIZATION, UTILIZATION, UTILIZATION],
            BUS,
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=5,
            max_hcu_util_percent=5,
            busy_sample_quorum=2,
        )
        self.assertEqual(status, "READY")
        self.assertEqual(devices[1].status, "WARN")
        self.assertIn("TRANSIENT_HCU_ACTIVITY", [item.reason_code for item in findings])

    def test_missing_card_in_one_sample_is_incomplete(self):
        one_card_memory = '{"card0":{"vram Total Memory (MiB)":"65520","vram Total Used Memory (MiB)":"3"}}'
        one_card_available = '{"card0":{"Available memory size (MiB)":"65453"}}'
        one_card_percent = '{"card0":{"HCU memory use (%)":"0"}}'
        one_card_util = '{"card0":{"HCU use (%)":"0.0"}}'
        cards = parse_hy_smi_samples(
            [LOW_MEMORY, one_card_memory, LOW_MEMORY],
            [AVAILABLE, one_card_available, AVAILABLE],
            [LOW_MEMORY_PERCENT, one_card_percent, LOW_MEMORY_PERCENT],
            [UTILIZATION, one_card_util, UTILIZATION],
            BUS,
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=5,
            max_hcu_util_percent=5,
            busy_sample_quorum=2,
        )
        self.assertEqual(status, "INCOMPLETE")
        self.assertEqual(devices[1].status, "UNKNOWN")
        self.assertIn("HCU_SAMPLE_INCOMPLETE", [item.reason_code for item in findings])

    def test_negative_metric_is_incomplete(self):
        invalid_util = '{"card0":{"HCU use (%)":"-1"},"card1":{"HCU use (%)":"0"}}'
        cards = parse_hy_smi_samples(
            [MEMORY], [AVAILABLE], [MEMORY_PERCENT], [invalid_util], BUS
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=50,
            max_hcu_util_percent=5,
        )
        self.assertEqual(status, "INCOMPLETE")
        self.assertEqual(devices[0].status, "UNKNOWN")
        self.assertIn("HCU_METRIC_OUT_OF_RANGE", [item.reason_code for item in findings])

    def test_explicit_bdf_mismatch_is_not_positionally_paired(self):
        bad_bus = '{"card0":{"PCI Bus":"0000:08:00.0"},"card1":{"PCI Bus":"0000:36:00.0"}}'
        cards = parse_hy_smi_samples(
            [MEMORY], [AVAILABLE], [MEMORY_PERCENT], [UTILIZATION], bad_bus
        )
        devices, findings, status = evaluate_metrics(
            {"node": "node36"},
            cards,
            parse_rocminfo(ROCMINFO),
            expected_devices=2,
            max_vram_used_percent=50,
            max_hcu_util_percent=5,
        )
        self.assertEqual(status, "BLOCKED")
        self.assertIsNone(devices[0].rocminfo_agent)
        self.assertIn("HCU_BDF_MAPPING_MISMATCH", [item.reason_code for item in findings])

    def test_environment_required_training_stack_passes(self):
        payload = {
            "system": {
                "kernel": "5.15.0",
                "os_release": {"PRETTY_NAME": "Ubuntu 22.04"},
                "cpu_logical_count": 128,
                "cpu_models": ["Hygon C86 Processor"],
                "meminfo": {"MemTotal": "528095272 kB"},
            },
            "dtk": {
                "version_file": {"path": "/opt/dtk/.dtk_version", "value": "DTK-26.04-rc4\n"},
                "tools": {
                    "hipcc": {"path": "/opt/dtk/bin/hipcc", "version": {"rc": 0, "stdout": "dcc version: 25.10.0-0"}},
                    "hy-smi": {
                        "path": "/opt/hyhal/bin/hy-smi",
                        "version": {"rc": 0, "stdout": "Version 1.25.0"},
                        "library_version": {"rc": 0, "stdout": "ROCM-SMI-LIB version: 7.6.0-0.10"},
                        "driver_version": {"rc": 0, "stdout": "Driver Version: 6.3.32-V1.6.0"},
                        "vbios": {"rc": 0, "stdout": '{"card0":{"VBIOS version":"v1"}}'},
                        "hsw_firmware": {"rc": 0, "stdout": "HSW[0]: FW Version: fw1"},
                    },
                    "ucx_info": {"path": "/opt/ucx/bin/ucx_info", "version": {"rc": 0, "stdout": "# Library version: 1.20.1"}},
                    "mpirun": {"path": "/opt/mpi/bin/mpirun", "version": {"rc": 0, "stdout": "mpirun (Open MPI) 5.0.3"}},
                },
            },
            "driver": {"modules": ["hycu 123 0 - Live"], "device_nodes": ["/dev/kfd"]},
            "libraries": {"paths": ["/opt/dtk/lib/librccl.so.1.0"]},
            "network": {
                "interfaces": [{"name": "ibs2", "driver": "shca_core"}],
                "rdma_devices": [
                    {
                        "name": f"shca_{index}",
                        "ibv_devinfo": {"rc": 0, "stdout": f"hca_id: shca_{index}"},
                        "ports": [
                            {
                                "port": "1",
                                "state": "4: ACTIVE",
                                "phys_state": "5: LinkUp",
                                "rate": "400 Gb/sec",
                                "link_layer": "InfiniBand",
                                "max_mtu": "4096 (5)",
                                "active_mtu": "4096 (5)",
                                "subnet_prefix": "0xfe80000000000000",
                                "lid": hex(index + 1),
                                "sm_lid": "0x1",
                                "gids": [{
                                    "index": 0,
                                    "gid": f"fe80:0000:0000:0000:0000:0000:0000:{index + 1:04x}",
                                    "type": "IB/RoCE v1",
                                    "netdev": None,
                                }],
                                "pkeys": [{"index": 0, "value": "0xffff"}],
                                "counter_window": healthy_ib_counter_window(),
                            }
                        ],
                    }
                    for index in range(4)
                ],
                "rdma_userspace": {
                    "ibv_devices": {
                        "tool_path": "/usr/bin/ibv_devices",
                        "command": {
                            "rc": 0,
                            "stdout": "\n".join(
                                f"shca_{index} 000000000000000{index}"
                                for index in range(4)
                            ),
                        },
                        "enumerated_devices": [f"shca_{index}" for index in range(4)],
                    },
                    "ibv_devinfo_tool_path": "/usr/bin/ibv_devinfo",
                    "provider_configs": {"files": []},
                    "libraries": {"libraries": []},
                },
            },
            "torch": {
                "importable": True,
                "version": "2.10.0",
                "hip_version": "6.3.26113",
                "hcu_available": True,
                "device_count": 8,
                "distributed_available": True,
                "distributed_nccl_available": True,
                "nccl_version": [2, 22, 3],
            },
            "python": {"version": "3.10.12", "packages": {"torch": "2.10.0"}},
        }
        findings, summary, checks = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=True,
            require_rdma=True,
            minimum_rdma_devices=4,
            require_rccl=True,
            require_ucx=True,
        )
        self.assertFalse([item for item in findings if item.severity == "FAIL"])
        self.assertEqual(summary["rdma_active_port_count"], 4)
        self.assertEqual(summary["rdma_current_protocol"], "NATIVE_INFINIBAND")
        self.assertTrue(all(item["status"] in {"PASS", "NOT_APPLICABLE"} for item in checks))

    def test_missing_required_rccl_fails(self):
        payload = {
            "system": {"os_release": {}, "meminfo": {}},
            "dtk": {
                "version_file": {"value": "DTK-26.04"},
                "tools": {
                    "hipcc": {"path": "/opt/dtk/bin/hipcc", "version": {"rc": 0, "stdout": "dcc version"}},
                    "hy-smi": {"driver_version": {"rc": 0, "stdout": "Driver Version: v1"}},
                },
            },
            "driver": {"modules": ["hycu 1 0 - Live"], "device_nodes": ["/dev/kfd"]},
            "libraries": {"paths": []},
            "network": {"interfaces": [], "rdma_devices": []},
            "torch": {
                "importable": True,
                "version": "2.10",
                "hip_version": "6.3",
                "hcu_available": True,
                "device_count": 8,
                "distributed_nccl_available": False,
            },
            "python": {"packages": {}},
        }
        findings, _, _ = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=False,
            minimum_rdma_devices=0,
            require_rccl=True,
            require_ucx=False,
        )
        self.assertIn("RCCL_LIBRARY_NOT_FOUND", [item.reason_code for item in findings if item.severity == "FAIL"])

    def test_optional_compiler_and_ucx_do_not_block(self):
        payload = {
            "system": {"os_release": {}, "meminfo": {}},
            "dtk": {"version_file": None, "tools": {"hy-smi": {}}},
            "driver": {"modules": [], "device_nodes": ["/dev/kfd"]},
            "libraries": {"paths": ["/opt/dtk/lib/librccl.so.1"]},
            "network": {"interfaces": [], "rdma_devices": []},
            "torch": {
                "importable": True,
                "version": "2.10",
                "hip_version": "6.3",
                "hcu_available": True,
                "device_count": 8,
                "distributed_nccl_available": True,
            },
            "python": {"packages": {}},
        }
        findings, _, _ = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=False,
            minimum_rdma_devices=0,
            require_rccl=False,
            require_ucx=False,
        )
        self.assertFalse([item for item in findings if item.severity == "FAIL"])
        self.assertIn("HIPCC_NOT_AVAILABLE", [item.reason_code for item in findings])
        self.assertIn("UCX_NOT_AVAILABLE", [item.reason_code for item in findings])

    def test_torch_device_count_mismatch_fails(self):
        payload = {
            "system": {"os_release": {}, "meminfo": {}},
            "dtk": {
                "version_file": {"value": "DTK-26.04"},
                "tools": {
                    "hipcc": {"path": "/opt/dtk/bin/hipcc", "version": {"rc": 0, "stdout": "dcc"}},
                    "hy-smi": {},
                },
            },
            "driver": {"modules": [], "device_nodes": ["/dev/kfd"]},
            "libraries": {"paths": ["/opt/dtk/lib/librccl.so.1"]},
            "network": {"interfaces": [], "rdma_devices": []},
            "torch": {
                "importable": True,
                "version": "2.10",
                "hip_version": "6.3",
                "hcu_available": True,
                "device_count": 7,
                "distributed_nccl_available": True,
            },
            "python": {"packages": {}},
        }
        findings, _, _ = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=False,
            minimum_rdma_devices=0,
            require_rccl=False,
            require_ucx=False,
        )
        self.assertIn(
            "TORCH_DEVICE_COUNT_MISMATCH",
            [item.reason_code for item in findings if item.severity == "FAIL"],
        )

    def test_hcusmi_undefined_symbol_is_single_root_cause(self):
        payload = {
            "system": {"os_release": {}, "meminfo": {}},
            "dtk": {
                "version_file": {"value": "DTK-26.04"},
                "tools": {
                    "hipcc": {"path": "/opt/dtk/bin/hipcc", "version": {"rc": 0, "stdout": "dcc"}},
                    "hy-smi": {},
                },
            },
            "driver": {"modules": ["hycu 1 0 - Live"], "device_nodes": ["/dev/kfd"]},
            "libraries": {"paths": ["/opt/dtk/lib/librccl.so.1"]},
            "network": {"interfaces": [], "rdma_devices": []},
            "torch": {
                "importable": False,
                "error_type": "AttributeError",
                "error": "/opt/hyhal/lib/libhcu_smi.so: undefined symbol: hcusmi_init",
            },
            "python": {"packages": {}},
        }
        findings, _, _ = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=False,
            minimum_rdma_devices=0,
            require_rccl=False,
            require_ucx=False,
        )
        torch_reasons = [
            item.reason_code
            for item in findings
            if item.reason_code.startswith("TORCH") or "HCUSMI" in item.reason_code
        ]
        self.assertEqual(torch_reasons, ["HCUSMI_LIBRARY_ABI_MISMATCH"])


if __name__ == "__main__":
    unittest.main()
