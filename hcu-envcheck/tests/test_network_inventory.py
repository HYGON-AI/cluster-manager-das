# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from hcu_envcheck.environment import (
    _adapter_hardware_profile,
    _adapter_inventory,
    _adapter_link_profile,
    evaluate_environment,
)
from hcu_envcheck.pod_probe import (
    _collect_ib_counter_windows,
    _local_link_status,
    _rdma_counter_sampling_configuration,
    collect_libraries,
    collect_ip_address_evidence,
    collect_ip_link_evidence,
    parse_ip_address_payload,
    parse_ip_link_payload,
    parse_lspci_device_names,
    parse_lspci_machine_readable,
)


class NetworkInventoryTests(unittest.TestCase):
    @patch("hcu_envcheck.pod_probe.os.path.realpath", side_effect=lambda path: path)
    @patch("hcu_envcheck.pod_probe.glob.glob")
    def test_library_inventory_hides_hcu_runtime_implementation_path(
        self,
        glob_mock,
        _realpath_mock,
    ):
        def fake_glob(pattern):
            if "libamdhip64" in pattern:
                return ["/opt/dtk/lib/libamdhip64.so.6"]
            if "librccl" in pattern:
                return ["/opt/dtk/lib/librccl.so.1"]
            return []

        glob_mock.side_effect = fake_glob
        inventory = collect_libraries()

        self.assertEqual(inventory["paths"], ["/opt/dtk/lib/librccl.so.1"])
        self.assertEqual(
            inventory["hcu_hip_runtime"],
            {"component": "HCU HIP runtime", "detected": True},
        )
        self.assertNotIn("amd", json.dumps(inventory).lower())

    def test_lspci_names_and_numeric_ids_are_both_preserved(self):
        text = (
            '0000:73:00.0 "Infiniband controller [0207]" '
            '"Dawning Information Industry Co., Ltd. [1cb8]" "Device [11a2]" -r02 -p00 '
            '"Dawning Information Industry Co., Ltd. [1cb8]" "Device [1c11]"\n'
        )
        parsed = parse_lspci_machine_readable(text)["0000:73:00.0"]
        self.assertEqual(parsed["pci_vendor_name"], "Dawning Information Industry Co., Ltd.")
        self.assertEqual(parsed["pci_vendor_id"], "1cb8")
        self.assertEqual(parsed["pci_device_id"], "11a2")
        self.assertEqual(parsed["pci_subsystem_device_id"], "1c11")

    def test_numeric_sysfs_ids_remain_usable_without_lspci_names(self):
        inventory = _adapter_inventory(
            [
                {
                    "pci_vendor": "0x1d94",
                    "pci_device": "0x14a8",
                    "driver": "sxe",
                    "local_link_status": "UP",
                }
            ]
        )
        self.assertEqual(inventory[0]["vendor"], "UNKNOWN")
        self.assertEqual(inventory[0]["pci_id"], "1d94:14a8")
        self.assertEqual(inventory[0]["count"], 1)

    def test_firmware_device_name_and_hca_type_override_generic_pci_model(self):
        details = (
            "0000:11:00.4 Ethernet controller [0200]: Vendor Device [1234:5678]\n"
            "\tDeviceName: HYGON XGBE 10G SFP+ NIC\n"
            "\tKernel driver in use: sxe\n"
        )
        self.assertEqual(
            parse_lspci_device_names(details),
            {"0000:11:00.4": "HYGON XGBE 10G SFP+ NIC"},
        )
        inventory = _adapter_inventory(
            [
                {
                    "pci_vendor_name": "Dawning Information Industry Co., Ltd.",
                    "pci_device_name": "Device",
                    "pci_vendor_id": "1cb8",
                    "pci_device_id": "11a2",
                    "hardware_model": "SHCA_400G_P1_4514",
                }
            ]
        )
        self.assertEqual(inventory[0]["model"], "SHCA_400G_P1_4514")

    def test_local_link_missing_carrier_is_unknown(self):
        self.assertEqual(_local_link_status("unknown", None), "UNKNOWN")
        self.assertEqual(_local_link_status("up", "1"), "UP")
        self.assertEqual(_local_link_status("down", "0"), "DOWN")

    def test_rdma_counter_interval_environment_contract(self):
        self.assertEqual(
            _rdma_counter_sampling_configuration({})["interval_seconds"], 5
        )
        self.assertEqual(
            _rdma_counter_sampling_configuration(
                {"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS": "0"}
            )["status"],
            "DISABLED",
        )
        for value in ("1", "60", "2.5"):
            with self.subTest(value=value):
                self.assertEqual(
                    _rdma_counter_sampling_configuration(
                        {"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS": value}
                    )["status"],
                    "ENABLED",
                )
        for value in ("-1", "0.5", "61", "nan", "invalid"):
            with self.subTest(value=value):
                self.assertEqual(
                    _rdma_counter_sampling_configuration(
                        {"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS": value}
                    )["status"],
                    "INVALID_INTERVAL",
                )

    def test_counter_windows_sample_all_ports_before_one_local_sleep(self):
        def snapshot(monotonic_ns, value):
            return {
                "sampled_at_unix_ns": monotonic_ns,
                "monotonic_started_ns": monotonic_ns,
                "monotonic_finished_ns": monotonic_ns,
                "monotonic_ns": monotonic_ns,
                "counter_status": "COMPLETE",
                "counters": {"link_downed": str(value)},
                "hw_counter_status": "COMPLETE",
                "hw_counters": {"phy_symbol_errors": str(value)},
            }

        first = {}
        second = {}
        sleeps = []
        samples = [
            snapshot(1_000_000_000, 1),
            snapshot(2_000_000_000, 2),
            snapshot(6_000_000_000, 3),
            snapshot(7_000_000_000, 4),
        ]
        with patch(
            "hcu_envcheck.pod_probe._collect_ib_counter_snapshot",
            side_effect=samples,
        ):
            summary = _collect_ib_counter_windows(
                [(Path("/rdma/a/1"), first), (Path("/rdma/b/1"), second)],
                environ={"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS": "5"},
                sleep_fn=sleeps.append,
            )

        self.assertEqual(sleeps, [5.0])
        self.assertEqual(summary["status"], "COMPLETE")
        self.assertEqual(first["counter_window"]["before"]["counters"]["link_downed"], "1")
        self.assertEqual(first["counter_window"]["after"]["counters"]["link_downed"], "3")
        self.assertEqual(second["counter_window"]["before"]["counters"]["link_downed"], "2")
        self.assertEqual(second["counter_window"]["after"]["counters"]["link_downed"], "4")
        self.assertEqual(first["hw_counters"], {"phy_symbol_errors": "3"})
        self.assertEqual(first["counter_window"]["interval_seconds"], 5)

    def test_zero_interval_disables_wait_but_preserves_one_snapshot(self):
        payload = {}
        snapshot = {
            "sampled_at_unix_ns": 1,
            "monotonic_started_ns": 1,
            "monotonic_finished_ns": 1,
            "monotonic_ns": 1,
            "counter_status": "COMPLETE",
            "counters": {"link_downed": "7"},
            "hw_counter_status": "UNAVAILABLE",
            "hw_counters": None,
        }
        with patch(
            "hcu_envcheck.pod_probe._collect_ib_counter_snapshot",
            return_value=snapshot,
        ):
            summary = _collect_ib_counter_windows(
                [(Path("/rdma/a/1"), payload)],
                environ={"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS": "0"},
                sleep_fn=lambda _seconds: self.fail("disabled sampling must not sleep"),
            )
        self.assertEqual(summary["status"], "DISABLED")
        self.assertEqual(payload["counter_window"]["status"], "DISABLED")
        self.assertIsNone(payload["counter_window"]["before"])
        self.assertEqual(payload["counters"], {"link_downed": "7"})

    def test_ip_address_evidence_distinguishes_empty_from_collection_failure(self):
        self.assertEqual(parse_ip_address_payload('[{"addr_info": []}]'), [])
        with patch(
            "hcu_envcheck.pod_probe.run",
            return_value={"rc": 0, "stdout": '[{"addr_info": []}]', "stderr": ""},
        ):
            evidence = collect_ip_address_evidence("eth4", "/sbin/ip")
        self.assertEqual(evidence["status"], "COMPLETE")
        self.assertEqual(evidence["addresses"], [])

        with patch(
            "hcu_envcheck.pod_probe.run",
            return_value={"rc": 0, "stdout": "not-json", "stderr": ""},
        ):
            evidence = collect_ip_address_evidence("eth4", "/sbin/ip")
        self.assertEqual(evidence["status"], "PARSE_FAILED")

    def test_ip_link_evidence_preserves_vlan_id_and_protocol(self):
        tagged = parse_ip_link_payload(
            '[{"ifname":"bond0.120","link":"bond0","linkinfo":'
            '{"info_kind":"vlan","info_data":{"id":120,"protocol":"802.1ad"}}}]',
            "bond0.120",
        )
        self.assertEqual(tagged["vlan_id"], 120)
        self.assertEqual(tagged["vlan_protocol"], "802.1ad")
        self.assertEqual(tagged["parent"], "bond0")
        self.assertEqual(
            parse_ip_link_payload('[{"ifname":"eth4"}]', "eth4")["vlan_id"],
            0,
        )
        with patch(
            "hcu_envcheck.pod_probe.run",
            return_value={"rc": 2, "stdout": "", "stderr": "missing"},
        ):
            evidence = collect_ip_link_evidence("eth4", "/sbin/ip")
        self.assertEqual(evidence["status"], "COMMAND_FAILED")

    def test_static_and_link_profiles_reaggregate_after_projection(self):
        inventory = [
            {
                "pci_id": "1cb8:11a2",
                "subsystem_pci_id": "1cb8:1c11",
                "driver": "shca_core",
                "driver_version": "2.500.4.74",
                "firmware_version": "2.500.4.74",
                "local_link": "UP",
                "speed_mbps": "400000",
                "mtu": "2044",
                "count": 2,
            },
            {
                "pci_id": "1cb8:11a2",
                "subsystem_pci_id": "1cb8:1c11",
                "driver": "shca_core",
                "driver_version": "2.500.4.74",
                "firmware_version": "2.500.4.74",
                "local_link": "DOWN",
                "speed_mbps": "400000",
                "mtu": "2044",
                "count": 2,
            },
        ]
        static_profile = _adapter_hardware_profile(inventory)
        link_profile = _adapter_link_profile(inventory)
        self.assertEqual(static_profile[0]["count"], 4)
        self.assertEqual(len(static_profile), 1)
        self.assertEqual(len(link_profile), 2)

    def test_inventory_order_is_stable_when_firmware_input_is_reversed(self):
        items = [
            {
                "pci_vendor_id": "1cb8",
                "pci_device_id": "11a2",
                "driver": "shca_core",
                "firmware_version": firmware,
                "local_link_status": "UP",
            }
            for firmware in ("2.500.4.54", "2.500.4.74")
        ]
        self.assertEqual(_adapter_inventory(items), _adapter_inventory(list(reversed(items))))

    def test_unverified_pod_scope_does_not_report_zero_host_nics(self):
        payload = {
            "dtk": {"tools": {}},
            "driver": {"modules": [], "device_nodes": []},
            "libraries": {"paths": []},
            "network": {
                "interfaces": [{"pci_vendor": "0x1cb8", "pci_device": "0x11a2"}],
                "rdma_devices": [{"name": "hca0", "ports": []}],
            },
            "torch": {"importable": False, "error_type": "ImportError", "error": "missing"},
            "python": {"packages": {}},
            "system": {},
        }
        findings, summary, _checks = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=True,
            minimum_rdma_devices=4,
            require_rccl=False,
            require_ucx=False,
            network_host_scope_verified=False,
        )
        self.assertEqual(summary["network_scope"], "UNVERIFIED_POD_SCOPE")
        self.assertIsNone(summary["physical_nic_count"])
        self.assertIsNone(summary["rdma_device_count"])
        self.assertIsNone(summary["nic_hardware_profile"])
        self.assertIsNone(summary["nic_link_profile"])
        self.assertIsNone(summary["rdma_hardware_profile"])
        self.assertIsNone(summary["rdma_rates"])
        self.assertIn("NETWORK_HOST_SCOPE_UNVERIFIED", {item.reason_code for item in findings})

    def test_active_rdma_count_is_per_device_not_per_port(self):
        payload = {
            "dtk": {"tools": {}},
            "driver": {"modules": [], "device_nodes": []},
            "libraries": {"paths": []},
            "network": {
                "interfaces": [],
                "rdma_devices": [
                    {
                        "name": "hca0",
                        "ports": [
                            {"port": "1", "state": "4: ACTIVE", "phys_state": "5: LINKUP"},
                            {"port": "2", "state": "4: ACTIVE", "phys_state": "5: LINKUP"},
                        ],
                    },
                    {
                        "name": "hca1",
                        "ports": [
                            {"port": "1", "state": "1: DOWN", "phys_state": "3: DISABLED"},
                        ],
                    },
                ],
            },
            "torch": {"importable": False, "error_type": "ImportError", "error": "missing"},
            "python": {"packages": {}},
            "system": {},
        }
        findings, summary, _checks = evaluate_environment(
            payload,
            expected_device_count=8,
            require_compiler=False,
            require_rdma=True,
            minimum_rdma_devices=2,
            require_rccl=False,
            require_ucx=False,
        )
        self.assertEqual(summary["rdma_device_count"], 2)
        self.assertEqual(summary["rdma_active_device_count"], 1)
        self.assertEqual(summary["rdma_active_port_count"], 2)
        self.assertIn("RDMA_DEVICE_NOT_ACTIVE", {item.reason_code for item in findings})


if __name__ == "__main__":
    unittest.main()
