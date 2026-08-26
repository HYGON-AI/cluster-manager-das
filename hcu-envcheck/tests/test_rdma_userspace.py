# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import unittest
from unittest import mock

from hcu_envcheck.pod_probe import collect_rdma_userspace_evidence, parse_ibv_devices
from hcu_envcheck.rdma import _evaluate_rdma_userspace, evaluate_rdma_network


class RdmaUserspaceEvaluationTests(unittest.TestCase):
    def _network(
        self,
        *,
        devices_command,
        enumerated_devices=None,
        devinfo_command=None,
        devices_tool_path="/usr/bin/ibv_devices",
        devinfo_tool_path="/usr/bin/ibv_devinfo",
    ):
        if enumerated_devices is None:
            enumerated_devices = []
        return {
            "rdma_devices": [
                {
                    "name": "shca_0",
                    "ibv_devinfo": devinfo_command,
                    "ports": [],
                }
            ],
            "rdma_userspace": {
                "ibv_devices": {
                    "tool_path": devices_tool_path,
                    "command": devices_command,
                    "enumerated_devices": enumerated_devices,
                },
                "ibv_devinfo_tool_path": devinfo_tool_path,
                "provider_configs": {
                    "files": [
                        {
                            "path": "/etc/libibverbs.d/shca.driver",
                            "realpath": "/etc/libibverbs.d/shca.driver",
                            "content": "driver shca",
                            "truncated": False,
                        }
                    ]
                },
                "libraries": {
                    "libraries": [
                        {
                            "kind": "LIBIBVERBS",
                            "path": "/usr/lib64/libibverbs.so.1",
                            "realpath": "/usr/lib64/libibverbs.so.1.14.44.0",
                            "directory_source": "STANDARD",
                        },
                        {
                            "kind": "VERBS_PROVIDER",
                            "path": "/usr/lib64/libshca-rdmav34.so",
                            "realpath": "/usr/lib64/libshca-rdmav34.so",
                            "directory_source": "STANDARD",
                        },
                    ]
                },
            },
        }

    def test_original_container_provider_load_failure_is_fail(self):
        network = self._network(
            devices_command={
                "rc": 1,
                "stdout": "",
                "stderr": "libibverbs: Warning: couldn't load driver 'shca'",
            },
            devinfo_command={"rc": 1, "stderr": "No IB devices found"},
        )
        network["rdma_userspace"]["provider_configs"]["files"] = [
            {
                "path": "/etc/libibverbs.d/mlx5.driver",
                "realpath": "/etc/libibverbs.d/mlx5.driver",
                "content": "driver mlx5",
                "truncated": False,
            }
        ]
        network["rdma_userspace"]["libraries"]["libraries"][1].update(
            {
                "path": "/usr/lib64/libmlx5-rdmav34.so",
                "realpath": "/usr/lib64/libmlx5-rdmav34.so",
            }
        )
        result = _evaluate_rdma_userspace(network)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(
            result["reason_code"], "RDMA_USERSPACE_PROVIDER_UNAVAILABLE"
        )
        self.assertIn("couldn't load driver", result["message"])
        self.assertIn("did not find provider evidence for shca", result["message"])
        self.assertEqual(result["expected_provider_evidence"], [{"provider": "shca", "config_present": False, "library_present": False}])
        findings, checks, summary = evaluate_rdma_network(
            network,
            required=True,
            expected_protocol="ib",
        )
        self.assertIn(
            "RDMA_USERSPACE_PROVIDER_UNAVAILABLE",
            {item.reason_code for item in findings},
        )
        userspace_check = next(
            item for item in checks if item["check_id"] == "RDMA_USERSPACE"
        )
        self.assertEqual(userspace_check["status"], "FAIL")
        self.assertEqual(summary["rdma_userspace"]["status"], "FAIL")

    def test_provider_enumerates_but_device_open_fails(self):
        network = self._network(
            devices_command={
                "rc": 0,
                "stdout": "device node GUID\n------ ----------------\nshca_0 0011223344556677",
            },
            enumerated_devices=["shca_0"],
            devinfo_command={"rc": 1, "stderr": "Failed to open device shca_0"},
        )
        result = _evaluate_rdma_userspace(network)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(
            result["reason_code"], "RDMA_USERSPACE_DEVICE_OPEN_FAILED"
        )
        self.assertIn("Failed to open", result["message"])

    def test_all_sysfs_devices_enumerate_and_open(self):
        network = self._network(
            devices_command={"rc": 0, "stdout": "shca_0 0011223344556677"},
            enumerated_devices=["shca_0"],
            devinfo_command={"rc": 0, "stdout": "hca_id: shca_0"},
        )
        result = _evaluate_rdma_userspace(network)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["reason_code"], "RDMA_USERSPACE_READY")
        self.assertEqual(result["missing_enumerated_devices"], [])
        self.assertEqual(result["device_open_checks"][0]["status"], "PASS")

    def test_tool_missing_is_unknown(self):
        network = self._network(
            devices_command=None,
            devices_tool_path=None,
            devinfo_command=None,
            devinfo_tool_path=None,
        )
        result = _evaluate_rdma_userspace(network)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(
            result["reason_code"], "RDMA_USERSPACE_TOOL_UNAVAILABLE"
        )
        findings, checks, summary = evaluate_rdma_network(
            network,
            required=True,
            expected_protocol="ib",
        )
        self.assertIn(
            "RDMA_USERSPACE_TOOL_UNAVAILABLE",
            {item.reason_code for item in findings},
        )
        self.assertEqual(
            next(item for item in checks if item["check_id"] == "RDMA_USERSPACE")["status"],
            "UNKNOWN",
        )
        self.assertEqual(summary["rdma_userspace"]["status"], "UNKNOWN")

    def test_no_sysfs_hca_is_not_applicable(self):
        result = _evaluate_rdma_userspace({"rdma_devices": []})
        self.assertEqual(result["status"], "NOT_APPLICABLE")

    @mock.patch("hcu_envcheck.pod_probe._collect_rdma_userspace_libraries")
    @mock.patch("hcu_envcheck.pod_probe._collect_rdma_provider_configs")
    @mock.patch("hcu_envcheck.pod_probe.run")
    def test_collector_uses_explicit_tool_and_keeps_bounded_evidence(
        self, run_mock, configs_mock, libraries_mock
    ):
        run_mock.return_value = {"rc": 0, "stdout": "shca_0 0011", "stderr": ""}
        configs_mock.return_value = {"files": [], "limits": {"max_files": 64}}
        libraries_mock.return_value = {
            "libraries": [],
            "limits": {"max_paths": 256},
            "explicit_ld_library_path_directories": ["/custom/lib"],
        }
        result = collect_rdma_userspace_evidence(
            [{"name": "shca_0", "ibv_devinfo": {"rc": 0}}],
            ibv_devices_path="/custom/bin/ibv_devices",
            ibv_devinfo_path="/custom/bin/ibv_devinfo",
        )
        run_mock.assert_called_once_with(
            ["/custom/bin/ibv_devices"], timeout=5, limit=64 * 1024
        )
        self.assertEqual(result["ibv_devices"]["enumerated_devices"], ["shca_0"])
        self.assertEqual(result["provider_configs"]["limits"]["max_files"], 64)
        self.assertEqual(result["libraries"]["limits"]["max_paths"], 256)
    def test_parse_ibv_devices_table(self):
        self.assertEqual(
            parse_ibv_devices(
                "device node GUID\n------ ----------------\nshca_1 0011\nshca_0 0022\n"
            ),
            ["shca_0", "shca_1"],
        )


if __name__ == "__main__":
    unittest.main()
