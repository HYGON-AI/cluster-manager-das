# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import copy
import unittest

from hcu_envcheck.ib_counters import DEFAULT_IB_COUNTER_RULES
from hcu_envcheck.k8s_cluster import _rdma_protocol_consistency_findings
from hcu_envcheck.pod_probe import parse_ibv_devinfo_ports
from hcu_envcheck.preflight import validate_environment_profile
from hcu_envcheck.rdma import classify_rdma_port, evaluate_rdma_network
from hcu_envcheck.slurm_cluster import _consistency_findings


def ib_port(**overrides):
    payload = {
        "port": "1",
        "state": "4: ACTIVE",
        "phys_state": "5: LinkUp",
        "rate": "400 Gb/sec (4X NDR)",
        "link_layer": "InfiniBand",
        "max_mtu": "4096 (5)",
        "active_mtu": "4096 (5)",
        "lid": "0x10",
        "sm_lid": "0x1",
        "subnet_prefix": "0xfe80000000000000",
        "gids": [
            {
                "index": 0,
                "gid": "fe80:0000:0000:0000:0001:0002:0003:0004",
                "type": "IB/RoCE v1",
                "netdev": None,
            }
        ],
        "pkeys": [{"index": 0, "value": "0xffff"}],
    }
    payload.update(overrides)
    return payload


def roce_port(*, versions=("v2",), **overrides):
    gids = []
    for index, version in enumerate(versions):
        gids.append(
            {
                "index": index,
                "gid": f"fe80:0000:0000:0000:0001:0002:0003:{index + 1:04x}",
                "type": "RoCE v2" if version == "v2" else "IB/RoCE v1",
                "netdev": "eth4",
            }
        )
    payload = {
        "port": "1",
        "state": "4: ACTIVE",
        "phys_state": "5: LinkUp",
        "rate": "400 Gb/sec",
        "link_layer": "Ethernet",
        "gids": gids,
        "pkeys": [],
    }
    payload.update(overrides)
    return payload


def network_with_ports(*ports, dcb_rc=0, dcb_mode="configured"):
    dcb_output = {
        "pfc": "prio-pfc 0:off 1:off 2:off 3:on 4:off 5:off 6:off 7:off",
        "ets": "prio-tc 0:0 1:0 2:0 3:3 4:0 5:0 6:0 7:0\ntc-tsa 0:strict 3:ets",
        "app": "dscp-prio 24:3",
        "buffer": "prio-buffer 0:0 1:0 2:0 3:3 4:0 5:0 6:0 7:0",
        "dcbx": "host",
    }
    if dcb_mode == "empty":
        dcb_output = {name: "" for name in dcb_output}
    elif dcb_mode == "pfc_off":
        dcb_output["pfc"] = "prio-pfc 0:off 1:off 2:off 3:off 4:off 5:off 6:off 7:off"
    return {
        "rdma_devices": [{"name": "hca0", "ports": list(ports)}],
        "interfaces": [
            {
                "name": "eth4",
                "local_link_status": "UP",
                "mtu": "9000",
                "roce_configuration": {
                    "dcb_commands": {
                        name: {"rc": dcb_rc, "stdout": dcb_output[name]}
                        for name in ("pfc", "ets", "app", "buffer", "dcbx")
                    },
                    "pause": {
                        "rc": 0,
                        "stdout": (
                            "Pause parameters for eth4:\n"
                            "Autonegotiate: off\nRX: off\nTX: off"
                        ),
                    },
                    "fec": {
                        "rc": 0,
                        "stdout": (
                            "FEC parameters for eth4:\n"
                            "Configured FEC encodings: rs\n"
                            "Active FEC encoding: rs"
                        ),
                    },
                },
            }
        ],
    }


def with_counter_window(port, *, changes=None, status="COMPLETE", interval=5):
    before = {name: "0" for name in DEFAULT_IB_COUNTER_RULES}
    after = dict(before)
    after.update({name: str(value) for name, value in (changes or {}).items()})
    port["counter_window"] = {
        "status": status,
        "configured_value": str(interval),
        "configured_interval_seconds": interval,
        "interval_seconds": interval,
        "before": {
            "counter_status": "COMPLETE",
            "counters": before,
            "hw_counter_status": "COMPLETE",
            "hw_counters": {"phy_errors": "0"},
        },
        "after": {
            "counter_status": "COMPLETE",
            "counters": after,
            "hw_counter_status": "COMPLETE",
            "hw_counters": {"phy_errors": "0"},
        },
    }
    return port


class RdmaProtocolTests(unittest.TestCase):
    def test_ambiguous_gid_type_on_ib_port_is_not_roce(self):
        classified = classify_rdma_port(ib_port())
        self.assertEqual(classified["current_protocol"], "NATIVE_INFINIBAND")
        self.assertEqual(classified["roce_versions"], [])

    def test_roce_v1_and_v2_on_one_port_is_not_mixed_protocol(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(roce_port(versions=("v1", "v2"))),
            expected_protocol="roce",
            required=True,
        )
        self.assertEqual(summary["rdma_current_protocol"], "ROCE")
        self.assertEqual(summary["roce_endpoint"]["versions"], ["v1", "v2"])
        self.assertEqual(summary["roce_endpoint"]["status"], "PASS")
        self.assertNotIn(
            "RDMA_PROTOCOL_MIXED_ON_NODE", {item.reason_code for item in findings}
        )

    def test_native_ib_endpoint_passes_and_roce_is_inactive_in_current_mode(self):
        findings, checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port()),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["rdma_current_protocol"], "NATIVE_INFINIBAND")
        self.assertEqual(summary["ib_endpoint"]["status"], "PASS")
        self.assertEqual(
            summary["roce_endpoint"]["status"],
            "NOT_ACTIVE_IN_CURRENT_PORT_MODE",
        )
        self.assertFalse(summary["rdma_runtime_transport_verified"])
        self.assertFalse([item for item in findings if item.severity == "FAIL"])
        self.assertIn(
            ("ROCE_ENDPOINT", "NOT_APPLICABLE"),
            {(item["check_id"], item["status"]) for item in checks},
        )

    def test_stable_ib_counter_window_is_an_independent_pass(self):
        findings, checks, summary = evaluate_rdma_network(
            network_with_ports(with_counter_window(ib_port())),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["ib_endpoint"]["status"], "PASS")
        self.assertEqual(summary["ib_counter_health"]["status"], "PASS")
        self.assertEqual(summary["ib_counter_health"]["observed_status"], "PASS")
        self.assertIn(
            ("IB_COUNTER_HEALTH", "PASS"),
            {(item["check_id"], item["status"]) for item in checks},
        )
        self.assertNotIn(
            "IB_COUNTER_ERROR_GROWTH", {item.reason_code for item in findings}
        )

    def test_ib_counter_error_growth_fails_required_check_not_endpoint(self):
        findings, checks, summary = evaluate_rdma_network(
            network_with_ports(
                with_counter_window(ib_port(), changes={"port_rcv_errors": 1})
            ),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["ib_endpoint"]["status"], "PASS")
        self.assertEqual(summary["ib_counter_health"]["status"], "FAIL")
        self.assertEqual(summary["ib_counter_health"]["observed_status"], "FAIL")
        self.assertIn(
            ("IB_COUNTER_HEALTH", "FAIL"),
            {(item["check_id"], item["status"]) for item in checks},
        )
        counter_findings = [
            item for item in findings if item.reason_code == "IB_COUNTER_ERROR_GROWTH"
        ]
        self.assertEqual([item.severity for item in counter_findings], ["FAIL"])

    def test_ib_counter_error_growth_warns_when_rdma_is_optional(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(
                with_counter_window(ib_port(), changes={"link_downed": 1})
            ),
            expected_protocol="auto",
            required=False,
        )
        self.assertEqual(summary["ib_counter_health"]["status"], "WARN")
        self.assertEqual(summary["ib_counter_health"]["observed_status"], "FAIL")
        counter_findings = [
            item for item in findings if item.reason_code == "IB_COUNTER_ERROR_GROWTH"
        ]
        self.assertEqual([item.severity for item in counter_findings], ["WARN"])

    def test_missing_ib_counter_evidence_is_unknown_when_required(self):
        port = with_counter_window(ib_port())
        port["counter_window"]["after"]["counters"].pop("symbol_error")
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="ib", required=True
        )
        self.assertEqual(summary["ib_endpoint"]["status"], "PASS")
        self.assertEqual(summary["ib_counter_health"]["status"], "UNKNOWN")
        self.assertIn(
            "IB_COUNTER_HEALTH_EVIDENCE_MISSING",
            {item.reason_code for item in findings},
        )

    def test_ib_xmit_wait_growth_is_warning_even_when_required(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(
                with_counter_window(ib_port(), changes={"port_xmit_wait": 100})
            ),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["ib_counter_health"]["status"], "WARN")
        self.assertIn(
            "IB_CONGESTION_SIGNAL_GROWTH", {item.reason_code for item in findings}
        )

    def test_explicitly_disabled_ib_counter_sampling_is_not_pass(self):
        port = ib_port()
        port["counter_window"] = {
            "status": "DISABLED",
            "configured_value": "0",
            "configured_interval_seconds": 0,
            "interval_seconds": None,
            "before": None,
            "after": None,
        }
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="ib", required=True
        )
        self.assertEqual(summary["ib_counter_health"]["status"], "UNKNOWN")
        self.assertIn(
            "IB_COUNTER_SAMPLING_DISABLED", {item.reason_code for item in findings}
        )

    def test_ethernet_without_mapped_roce_gid_is_unconfirmed(self):
        port = roce_port(gids=[])
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["rdma_current_protocol"], "ETHERNET_RDMA_UNCONFIRMED")
        self.assertIn("ROCE_GID_NOT_CONFIGURED", {item.reason_code for item in findings})
        self.assertIn("RDMA_PROTOCOL_MISMATCH", {item.reason_code for item in findings})

    def test_expected_protocol_mismatch_is_a_failure(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port()), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["rdma_protocol_status"], "FAIL")
        self.assertIn("RDMA_PROTOCOL_MISMATCH", {item.reason_code for item in findings})

    def test_native_ib_and_roce_ports_on_one_node_is_mixed(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port(), roce_port()), expected_protocol="auto"
        )
        self.assertEqual(summary["rdma_current_protocol"], "MIXED")
        self.assertEqual(summary["rdma_protocol_status"], "FAIL")
        self.assertIn(
            "RDMA_PROTOCOL_MIXED_ON_NODE", {item.reason_code for item in findings}
        )

    def test_roce_dcb_evidence_is_independently_reported(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(roce_port(), dcb_rc=1),
            expected_protocol="roce",
            required=True,
        )
        self.assertEqual(summary["roce_endpoint"]["status"], "PASS")
        self.assertEqual(
            summary["roce_endpoint"]["dcb_status"], "INCOMPLETE_COMMAND_FAILED"
        )
        self.assertIn(
            "ROCE_DCB_CONFIGURATION_INCOMPLETE",
            {item.reason_code for item in findings},
        )

    def test_dcb_rc_zero_with_empty_output_is_not_pass(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(roce_port(), dcb_mode="empty"),
            expected_protocol="roce",
            required=True,
        )
        self.assertEqual(
            summary["roce_endpoint"]["dcb_status"],
            "COLLECTED_POLICY_INCOMPLETE",
        )
        self.assertIn(
            "ROCE_DCB_CONFIGURATION_INCOMPLETE",
            {item.reason_code for item in findings},
        )

    def test_pfc_all_off_is_not_reported_as_dcb_pass(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(roce_port(), dcb_mode="pfc_off"),
            expected_protocol="roce",
            required=True,
        )
        self.assertEqual(
            summary["roce_endpoint"]["dcb_status"],
            "COLLECTED_POLICY_INCOMPLETE",
        )
        self.assertIn(
            "ROCE_DCB_CONFIGURATION_INCOMPLETE",
            {item.reason_code for item in findings},
        )

    def test_roce_bond_mapping_uses_physical_dcb_target(self):
        port = roce_port()
        port["gids"][0]["netdev"] = "bond0"
        network = network_with_ports(port)
        interface = network["interfaces"][0]
        commands = interface["roce_configuration"].pop("dcb_commands")
        interface["name"] = "bond0"
        interface["roce_configuration"]["dcb_targets"] = {"eth4": commands}
        _findings, _checks, summary = evaluate_rdma_network(
            network, expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_endpoint"]["status"], "PASS")
        targets = summary["roce_endpoint"]["dcb_profile"]["interfaces"]["bond0"]["targets"]
        self.assertIn("eth4", targets)

    def test_partial_gid_read_is_unknown_not_invalid_configuration(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port(gid_collection_status="PARTIAL_READ_ERROR")),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["ib_endpoint"]["status"], "UNKNOWN")
        self.assertIn(
            "IB_ENDPOINT_EVIDENCE_MISSING", {item.reason_code for item in findings}
        )

    def test_ib_plus_unconfirmed_ethernet_never_passes_expected_ib(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port(), roce_port(gids=[])),
            expected_protocol="ib",
            required=True,
        )
        self.assertEqual(summary["rdma_current_protocol"], "MIXED_OR_INCOMPLETE")
        self.assertEqual(summary["rdma_protocol_status"], "FAIL")
        self.assertIn("RDMA_PROTOCOL_MISMATCH", {item.reason_code for item in findings})

    def test_missing_ib_mtu_is_unknown_not_pass(self):
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(ib_port(active_mtu=None)), required=True
        )
        self.assertEqual(summary["ib_endpoint"]["status"], "UNKNOWN")

    def test_ibv_devinfo_fills_driver_omitted_mtu_fields(self):
        parsed = parse_ibv_devinfo_ports(
            """
hca_id: shca_0
    port: 1
        state: PORT_ACTIVE (4)
        max_mtu: 4096 (5)
        active_mtu: 4096 (5)
        sm_lid: 1
        port_lid: 100348
        port_lmc: 0x00
        link_layer: InfiniBand
"""
        )
        self.assertEqual(parsed["1"]["state"], "4: ACTIVE")
        self.assertEqual(parsed["1"]["active_mtu"], "4096 (5)")
        self.assertEqual(parsed["1"]["lid"], "100348")

    def test_roce_unreadable_gid_value_type_or_ndev_is_unknown(self):
        cases = (
            {
                "gids": None,
                "gid_collection_status": "UNAVAILABLE",
                "gid_type_collection_status": "NOT_COLLECTED",
                "gid_ndev_collection_status": "NOT_COLLECTED",
            },
            {"gid_type_collection_status": "PARTIAL_READ_ERROR"},
            {"gid_ndev_collection_status": "PARTIAL_READ_ERROR"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                port = roce_port(**overrides)
                findings, _checks, summary = evaluate_rdma_network(
                    network_with_ports(port), expected_protocol="roce", required=True
                )
                reasons = {item.reason_code for item in findings}
                self.assertEqual(
                    summary["rdma_current_protocol"],
                    "ETHERNET_RDMA_EVIDENCE_INCOMPLETE",
                )
                self.assertEqual(summary["rdma_protocol_status"], "UNKNOWN")
                self.assertEqual(summary["roce_endpoint"]["status"], "UNKNOWN")
                self.assertIsNone(summary["rdma_fabric_profile"])
                self.assertIn("RDMA_EXPECTED_PROTOCOL_EVIDENCE_MISSING", reasons)
                self.assertIn("ROCE_GID_EVIDENCE_MISSING", reasons)
                self.assertNotIn("RDMA_PROTOCOL_MISMATCH", reasons)
                self.assertNotIn("ROCE_GID_NOT_CONFIGURED", reasons)

    def test_complete_empty_gid_table_is_not_a_read_error(self):
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(
                roce_port(
                    gids=[],
                    gid_collection_status="COMPLETE",
                    gid_type_collection_status="COMPLETE",
                    gid_ndev_collection_status="COMPLETE",
                )
            ),
            expected_protocol="roce",
            required=True,
        )
        reasons = {item.reason_code for item in findings}
        self.assertEqual(
            summary["rdma_current_protocol"], "ETHERNET_RDMA_UNCONFIRMED"
        )
        self.assertEqual(summary["roce_endpoint"]["status"], "FAIL")
        self.assertIn("ROCE_GID_NOT_CONFIGURED", reasons)
        self.assertIn("RDMA_PROTOCOL_MISMATCH", reasons)
        self.assertNotIn("ROCE_GID_EVIDENCE_MISSING", reasons)

    def test_ib_fabric_profile_preserves_per_port_association(self):
        network_a = network_with_ports(
            ib_port(port="1", subnet_prefix="0xfe80000000000001", pkeys=[{"index": 0, "value": "0xffff"}]),
            ib_port(port="2", subnet_prefix="0xfe80000000000002", pkeys=[{"index": 0, "value": "0x7fff"}]),
        )
        network_b = network_with_ports(
            ib_port(port="1", subnet_prefix="0xfe80000000000001", pkeys=[{"index": 0, "value": "0x7fff"}]),
            ib_port(port="2", subnet_prefix="0xfe80000000000002", pkeys=[{"index": 0, "value": "0xffff"}]),
        )
        _, _, summary_a = evaluate_rdma_network(network_a, required=True)
        _, _, summary_b = evaluate_rdma_network(network_b, required=True)
        self.assertNotEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )
        records = [
            {"node": "node01", "summary": {"environment": summary_a}},
            {"node": "node02", "summary": {"environment": summary_b}},
        ]
        findings = _rdma_protocol_consistency_findings(records)
        self.assertIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in findings},
        )

    def test_ib_fabric_profile_ignores_port_enumeration_order(self):
        first = ib_port(port="1", subnet_prefix="0xfe80000000000001")
        second = ib_port(port="2", subnet_prefix="0xfe80000000000002")
        _, _, summary_a = evaluate_rdma_network(
            network_with_ports(first, second), required=True
        )
        _, _, summary_b = evaluate_rdma_network(
            network_with_ports(second, first), required=True
        )
        self.assertEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )

    def test_ib_fabric_profile_preserves_hca_rail_identity_but_not_device_order(self):
        rail_a = ib_port(
            port="1",
            subnet_prefix="0xfe80000000000001",
            pkeys=[{"index": 0, "value": "0xffff"}],
        )
        rail_b = ib_port(
            port="1",
            subnet_prefix="0xfe80000000000002",
            pkeys=[{"index": 0, "value": "0x7fff"}],
        )

        def network(first, second, *, reverse_inventory=False):
            payload = network_with_ports()
            devices = [
                {"name": "hca0", "ports": [copy.deepcopy(first)]},
                {"name": "hca1", "ports": [copy.deepcopy(second)]},
            ]
            payload["rdma_devices"] = (
                list(reversed(devices)) if reverse_inventory else devices
            )
            return payload

        _, _, baseline = evaluate_rdma_network(
            network(rail_a, rail_b), required=True
        )
        _, _, reordered = evaluate_rdma_network(
            network(rail_a, rail_b, reverse_inventory=True), required=True
        )
        _, _, swapped = evaluate_rdma_network(
            network(rail_b, rail_a), required=True
        )
        self.assertEqual(
            baseline["rdma_fabric_profile"], reordered["rdma_fabric_profile"]
        )
        self.assertNotEqual(
            baseline["rdma_fabric_profile"], swapped["rdma_fabric_profile"]
        )
        self.assertEqual(
            {
                (item["device"], item["port"])
                for item in baseline["rdma_fabric_profile"]["port_profiles"]
            },
            {("hca0", "1"), ("hca1", "1")},
        )
        findings = _rdma_protocol_consistency_findings(
            [
                {"node": "node01", "summary": {"environment": baseline}},
                {"node": "node02", "summary": {"environment": swapped}},
            ]
        )
        self.assertIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in findings},
        )
    def test_partial_pfc_or_ets_table_blocks_roce_fabric_profile(self):
        cases = (
            ("pfc", "prio-pfc 3:on"),
            ("ets", "prio-tc 3:3"),
        )
        for section, output in cases:
            with self.subTest(section=section):
                network = network_with_ports(roce_port())
                commands = network["interfaces"][0]["roce_configuration"][
                    "dcb_commands"
                ]
                commands[section]["stdout"] = output
                _findings, _checks, summary = evaluate_rdma_network(
                    network, expected_protocol="roce", required=True
                )
                self.assertEqual(
                    summary["roce_endpoint"]["dcb_status"],
                    "COLLECTED_POLICY_INCOMPLETE",
                )
                self.assertIsNone(summary["rdma_fabric_profile"])

    def test_truncated_dcb_command_blocks_roce_fabric_profile(self):
        network = network_with_ports(roce_port())
        commands = network["interfaces"][0]["roce_configuration"]["dcb_commands"]
        commands["pfc"]["stdout_truncated"] = True
        _findings, _checks, summary = evaluate_rdma_network(
            network, expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_endpoint"]["dcb_status"], "PARTIAL")
        self.assertIsNone(summary["rdma_fabric_profile"])
    def test_roce_fabric_compares_normalized_dcb_policy(self):
        network_a = network_with_ports(roce_port())
        network_b = network_with_ports(roce_port())
        commands = network_b["interfaces"][0]["roce_configuration"]["dcb_commands"]
        commands["pfc"]["stdout"] = (
            "prio-pfc 0:off 1:off 2:off 3:off 4:on 5:off 6:off 7:off"
        )
        commands["app"]["stdout"] = "dscp-prio 24:4"
        _, _, summary_a = evaluate_rdma_network(
            network_a, expected_protocol="roce", required=True
        )
        _, _, summary_b = evaluate_rdma_network(
            network_b, expected_protocol="roce", required=True
        )
        self.assertNotEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )
        findings = _rdma_protocol_consistency_findings(
            [
                {"node": "node01", "summary": {"environment": summary_a}},
                {"node": "node02", "summary": {"environment": summary_b}},
            ]
        )
        self.assertIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in findings},
        )

    def test_equivalent_dcb_whitespace_normalizes_equal(self):
        network_a = network_with_ports(roce_port())
        network_b = network_with_ports(roce_port())
        commands = network_b["interfaces"][0]["roce_configuration"]["dcb_commands"]
        for command in commands.values():
            command["stdout"] = "\n  " + command["stdout"].replace(" ", "   ") + "  \r\n"
        _, _, summary_a = evaluate_rdma_network(network_a, required=True)
        _, _, summary_b = evaluate_rdma_network(network_b, required=True)
        self.assertEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )

    def test_incomplete_fabric_profile_is_unknown_not_inconsistent(self):
        _, _, complete = evaluate_rdma_network(
            network_with_ports(ib_port()), required=True
        )
        _, _, incomplete = evaluate_rdma_network(
            network_with_ports(
                ib_port(pkey_collection_status="UNAVAILABLE")
            ),
            required=True,
        )
        self.assertIsNotNone(complete["rdma_fabric_profile"])
        self.assertIsNone(incomplete["rdma_fabric_profile"])
        k8s_records = [
            {"node": "node01", "summary": {"environment": complete}},
            {"node": "node02", "summary": {"environment": incomplete}},
        ]
        k8s_findings = _rdma_protocol_consistency_findings(k8s_records)
        self.assertIn(
            "RDMA_FABRIC_PROFILE_EVIDENCE_MISSING",
            {item["reason_code"] for item in k8s_findings},
        )
        self.assertNotIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in k8s_findings},
        )
        slurm_records = [
            {"node": "node01", "reachable": True, "environment": complete},
            {"node": "node02", "reachable": True, "environment": incomplete},
        ]
        slurm_findings = _consistency_findings(slurm_records, strict=False)
        fabric_findings = [
            item for item in slurm_findings if item["field"] == "rdma_fabric_profile"
        ]
        self.assertTrue(fabric_findings)
        self.assertTrue(all(item["severity"] == "UNKNOWN" for item in fabric_findings))
        self.assertNotIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in fabric_findings},
        )

    def test_unknown_protocol_is_missing_evidence_not_cluster_mixed(self):
        known_profile = {"protocol": "NATIVE_INFINIBAND", "port_profiles": []}
        k8s_records = [
            {"node": "node01", "summary": {"environment": {"rdma_current_protocol": "NATIVE_INFINIBAND", "rdma_fabric_profile": known_profile}}},
            {"node": "node02", "summary": {"environment": {"rdma_current_protocol": "UNKNOWN", "rdma_fabric_profile": None}}},
        ]
        findings = _rdma_protocol_consistency_findings(k8s_records)
        reasons = {item["reason_code"] for item in findings}
        self.assertIn("RDMA_PROTOCOL_EVIDENCE_MISSING", reasons)
        self.assertNotIn("RDMA_PROTOCOL_CLUSTER_MIXED", reasons)
        slurm_records = [
            {"node": "node01", "reachable": True, "environment": k8s_records[0]["summary"]["environment"]},
            {"node": "node02", "reachable": True, "environment": k8s_records[1]["summary"]["environment"]},
        ]
        slurm_findings = _consistency_findings(slurm_records, strict=False)
        protocol_findings = [
            item for item in slurm_findings if item["field"] == "rdma_current_protocol"
        ]
        self.assertTrue(protocol_findings)
        self.assertTrue(all(item["severity"] == "UNKNOWN" for item in protocol_findings))
        self.assertNotIn(
            "RDMA_PROTOCOL_CLUSTER_MIXED",
            {item["reason_code"] for item in protocol_findings},
        )

    def test_api_profile_validation_rejects_skipped_explicit_checks(self):
        cases = (
            {"require_compiler": True},
            {"require_rdma": True},
            {"minimum_rdma_devices": 1},
            {"expected_rdma_protocol": "ib"},
            {"expected_rdma_protocol": "roce"},
            {"require_rccl": True},
            {"require_ucx": True},
        )
        defaults = {
            "require_compiler": False,
            "require_rdma": False,
            "minimum_rdma_devices": 0,
            "expected_rdma_protocol": "auto",
            "require_rccl": False,
            "require_ucx": False,
        }
        for override in cases:
            with self.subTest(override=override):
                with self.assertRaises(ValueError):
                    validate_environment_profile(
                        include_environment=False,
                        **{**defaults, **override},
                    )
        validate_environment_profile(include_environment=False, **defaults)

    def test_expected_roce_with_confirmed_and_incomplete_roce_ports_is_unknown(self):
        incomplete = roce_port(
            port="2",
            gid_type_collection_status="PARTIAL_READ_ERROR",
        )
        findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(roce_port(port="1"), incomplete),
            expected_protocol="roce",
            required=True,
        )
        reasons = {item.reason_code for item in findings}
        self.assertEqual(summary["rdma_current_protocol"], "MIXED_OR_INCOMPLETE")
        self.assertEqual(summary["rdma_protocol_status"], "UNKNOWN")
        self.assertIn("RDMA_EXPECTED_PROTOCOL_EVIDENCE_MISSING", reasons)
        self.assertNotIn("RDMA_PROTOCOL_MISMATCH", reasons)
        self.assertIsNone(summary["rdma_fabric_profile"])

    def test_ib_fabric_profile_normalizes_equivalent_mtu_pkey_and_rate(self):
        _, _, summary_a = evaluate_rdma_network(
            network_with_ports(
                ib_port(
                    active_mtu="4096 (5)", max_mtu="4096 (5)",
                    pkeys=[{"index": 0, "value": "0xffff"}],
                    rate="400 Gb/sec (4X NDR)",
                )
            ),
            required=True,
        )
        _, _, summary_b = evaluate_rdma_network(
            network_with_ports(
                ib_port(
                    active_mtu="4096", max_mtu="4096",
                    pkeys=[{"index": 0, "value": "65535"}],
                    rate="400 Gbps",
                )
            ),
            required=True,
        )
        self.assertEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )
        self.assertEqual(
            summary_a["rdma_fabric_profile"],
            {
                "protocol": "NATIVE_INFINIBAND",
                "ports": 1,
                "port_profiles": [{
                    "device": "hca0",
                    "port": "1",
                    "subnet_prefix": "0xfe80000000000000",
                    "pkeys": ["0xffff"],
                    "active_mtu": 4096,
                    "max_mtu": 4096,
                    "rate_mbps": 400000,
                }],
            },
        )


    def test_main_rdma_evaluator_exposes_roce_policy_chain(self):
        from tests.test_roce_health import full_policy, healthy_network

        findings, checks, summary = evaluate_rdma_network(
            healthy_network(),
            expected_protocol="roce",
            required=True,
            rdma_policy=full_policy(),
        )
        self.assertEqual(summary["roce_configuration_health"]["status"], "PASS")
        self.assertEqual(summary["roce_endpoint"]["configuration_status"], "PASS")
        self.assertIn(
            ("ROCE_CONFIGURATION_CHAIN", "PASS"),
            {(item["check_id"], item["status"]) for item in checks},
        )
        self.assertTrue(
            [item for item in checks if item.get("component") == "ROCE_CONFIGURATION_CHAIN"]
        )
        self.assertNotIn(
            "ROCE_CONFIGURATION_POLICY_MISMATCH",
            {item.reason_code for item in findings},
        )

    def test_main_rdma_evaluator_rejects_invalid_roce_policy(self):
        with self.assertRaises(ValueError):
            evaluate_rdma_network(
                network_with_ports(roce_port()),
                rdma_policy={"protcol": "roce-v2"},
            )

    def test_roce_counter_error_growth_fails_even_with_other_missing_counter(self):
        port = with_counter_window(
            roce_port(), changes={"port_xmit_discards": 1}
        )
        port["counter_window"]["after"]["counters"].pop("symbol_error")
        findings, checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_endpoint"]["status"], "PASS")
        self.assertEqual(summary["roce_counter_health"]["status"], "FAIL")
        self.assertEqual(
            summary["roce_counter_health"]["observed_status"], "FAIL"
        )
        self.assertIn(
            ("ROCE_COUNTER_HEALTH", "FAIL"),
            {(item["check_id"], item["status"]) for item in checks},
        )
        self.assertIn(
            "ROCE_COUNTER_ERROR_OR_DROP_GROWTH",
            {item.reason_code for item in findings},
        )

    def test_roce_pfc_or_cnp_growth_is_visible_warning(self):
        port = with_counter_window(roce_port())
        port["counter_window"]["before"]["roce_counters"] = {
            "rx_pfc_pause": "5",
            "cnp_sent": "7",
        }
        port["counter_window"]["after"]["roce_counters"] = {
            "rx_pfc_pause": "5",
            "cnp_sent": "8",
        }
        _findings, checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_counter_health"]["status"], "WARN")
        self.assertIn(
            "ROCE_CNP_COUNTER_GROWTH",
            summary["roce_counter_health"]["reason_codes"],
        )
        self.assertIn(
            ("ROCE_COUNTER_HEALTH", "WARN"),
            {(item["check_id"], item["status"]) for item in checks},
        )

    def test_roce_counter_missing_peer_sample_is_not_pass(self):
        port = with_counter_window(roce_port())
        port["counter_window"]["after"]["counters"].pop("port_xmit_discards")
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_counter_health"]["status"], "UNKNOWN")
        self.assertNotEqual(
            summary["roce_counter_health"]["observed_status"], "PASS"
        )

    def test_roce_partial_window_or_omitted_required_counter_is_not_pass(self):
        partial = with_counter_window(roce_port(), status="PARTIAL")
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(partial), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_counter_health"]["status"], "UNKNOWN")

        omitted = with_counter_window(roce_port())
        for snapshot in ("before", "after"):
            omitted["counter_window"][snapshot]["counters"].pop("port_xmit_discards")
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(omitted), expected_protocol="roce", required=True
        )
        self.assertEqual(summary["roce_counter_health"]["status"], "UNKNOWN")
        self.assertIn(
            "port_xmit_discards",
            summary["roce_counter_health"]["port_results"][0]["coverage"][
                "missing_required_standard_counters"
            ],
        )
    def test_roce_pma_stable_width_maxima_are_possible_saturation(self):
        for bits in (4, 8, 16, 32):
            with self.subTest(bits=bits):
                maximum = (1 << bits) - 1
                port = with_counter_window(roce_port())
                for snapshot in ("before", "after"):
                    port["counter_window"][snapshot]["counters"][
                        "port_rcv_errors"
                    ] = str(maximum)
                _findings, _checks, summary = evaluate_rdma_network(
                    network_with_ports(port),
                    expected_protocol="roce",
                    required=True,
                )
                port_result = summary["roce_counter_health"]["port_results"][0]
                metric = port_result["metrics"]["counters:port_rcv_errors"]
                self.assertEqual(port_result["status"], "UNKNOWN")
                self.assertEqual(metric["status"], "UNKNOWN")
                self.assertEqual(
                    metric["reason_code"], "POSSIBLE_COUNTER_SATURATION"
                )
                self.assertEqual(metric["possible_saturation_bits"], bits)
                self.assertIn(
                    "POSSIBLE_COUNTER_SATURATION",
                    port_result["reason_codes"],
                )
                self.assertTrue(
                    port_result["coverage"]["possible_counter_saturation"]
                )

    def test_roce_known_error_delta_to_width_maximum_still_fails(self):
        port = with_counter_window(roce_port())
        port["counter_window"]["before"]["counters"]["port_rcv_errors"] = "14"
        port["counter_window"]["after"]["counters"]["port_rcv_errors"] = "15"
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        port_result = summary["roce_counter_health"]["port_results"][0]
        metric = port_result["metrics"]["counters:port_rcv_errors"]
        self.assertEqual(port_result["status"], "FAIL")
        self.assertEqual(metric["status"], "FAIL")
        self.assertEqual(
            metric["reason_code"], "ROCE_COUNTER_ERROR_OR_DROP_GROWTH"
        )
        self.assertIsNone(metric["possible_saturation_bits"])

    def test_roce_truncated_counter_evidence_is_never_pass(self):
        cases = (
            ("window", "output_truncated"),
            ("before", "stdout_truncated"),
            ("after", "counter_output_truncated"),
        )
        for location, flag in cases:
            with self.subTest(location=location, flag=flag):
                port = with_counter_window(roce_port())
                target = (
                    port["counter_window"]
                    if location == "window"
                    else port["counter_window"][location]
                )
                target[flag] = True
                _findings, _checks, summary = evaluate_rdma_network(
                    network_with_ports(port),
                    expected_protocol="roce",
                    required=True,
                )
                port_result = summary["roce_counter_health"]["port_results"][0]
                self.assertEqual(port_result["status"], "UNKNOWN")
                self.assertIn(
                    "ROCE_COUNTER_EVIDENCE_TRUNCATED",
                    port_result["reason_codes"],
                )
                self.assertTrue(port_result["coverage"]["output_truncated"])
                self.assertFalse(port_result["coverage"]["complete"])

    def test_non_pma_counter_at_pma_maximum_is_not_false_saturation(self):
        port = with_counter_window(roce_port())
        for snapshot in ("before", "after"):
            port["counter_window"][snapshot]["hw_counters"][
                "phy_errors"
            ] = "65535"
        _findings, _checks, summary = evaluate_rdma_network(
            network_with_ports(port), expected_protocol="roce", required=True
        )
        port_result = summary["roce_counter_health"]["port_results"][0]
        metric = port_result["metrics"]["hw_counters:phy_errors"]
        self.assertEqual(metric["status"], "PASS")
        self.assertNotIn(
            "POSSIBLE_COUNTER_SATURATION", port_result["reason_codes"]
        )
    def test_roce_fabric_profile_preserves_gid_ip_vlan_rail_mapping(self):
        from tests.test_roce_health import full_policy, healthy_network

        _findings, _checks, summary = evaluate_rdma_network(
            healthy_network(),
            expected_protocol="roce",
            required=True,
            rdma_policy=full_policy(),
        )
        profile = summary["rdma_fabric_profile"]
        self.assertIsNotNone(profile)
        port_profile = profile["port_profiles"][0]
        self.assertEqual(port_profile["device"], "hca0")
        self.assertEqual(port_profile["port"], "1")
        rail = port_profile["gid_layout"][0]
        self.assertEqual(rail["netdev"], "bond0.100")
        self.assertEqual(rail["ip_networks"], ["10.20.30.0/24"])
        self.assertEqual(rail["gid_ip_network"], "10.20.30.0/24")
        self.assertTrue(rail["gid_matches_netdev_ip"])
        self.assertEqual(rail["vlan_id"], 100)

    def test_roce_rail_swap_is_cross_node_fabric_inconsistency(self):
        def network(swapped=False):
            first = roce_port(port="1")
            second = roce_port(port="2")
            first["gids"][0]["netdev"] = "eth5" if swapped else "eth4"
            second["gids"][0]["netdev"] = "eth4" if swapped else "eth5"
            result = network_with_ports(first, second)
            eth5 = copy.deepcopy(result["interfaces"][0])
            eth5["name"] = "eth5"
            result["interfaces"].append(eth5)
            return result

        _, _, summary_a = evaluate_rdma_network(
            network(False), expected_protocol="roce", required=True
        )
        _, _, summary_b = evaluate_rdma_network(
            network(True), expected_protocol="roce", required=True
        )
        self.assertNotEqual(
            summary_a["rdma_fabric_profile"], summary_b["rdma_fabric_profile"]
        )
        findings = _rdma_protocol_consistency_findings(
            [
                {"node": "node01", "summary": {"environment": summary_a}},
                {"node": "node02", "summary": {"environment": summary_b}},
            ]
        )
        self.assertIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in findings},
        )

    def test_roce_dcb_policy_is_bound_to_leaf_rail_and_order_independent(self):
        def apply_priority(interface, priority):
            commands = interface["roce_configuration"]["dcb_commands"]
            commands["pfc"]["stdout"] = "prio-pfc " + " ".join(
                f"{item}:{'on' if item == priority else 'off'}"
                for item in range(8)
            )
            commands["ets"]["stdout"] = "prio-tc " + " ".join(
                f"{item}:{priority if item == priority else 0}"
                for item in range(8)
            )
            commands["app"]["stdout"] = f"dscp-prio 24:{priority}"
            commands["buffer"]["stdout"] = "prio-buffer " + " ".join(
                f"{item}:{priority if item == priority else 0}"
                for item in range(8)
            )

        def network(*, swapped=False, reordered=False):
            first = roce_port(port="1")
            second = roce_port(port="2")
            first["gids"][0]["netdev"] = "eth4"
            second["gids"][0]["netdev"] = "eth5"
            result = network_with_ports(first, second)
            eth4 = result["interfaces"][0]
            eth5 = copy.deepcopy(eth4)
            eth5["name"] = "eth5"
            apply_priority(eth4, 4 if swapped else 3)
            apply_priority(eth5, 3 if swapped else 4)
            result["interfaces"] = [eth4, eth5]
            if reordered:
                result["interfaces"].reverse()
                result["rdma_devices"][0]["ports"].reverse()
            return result

        _, _, baseline = evaluate_rdma_network(
            network(), expected_protocol="roce", required=True
        )
        _, _, reordered = evaluate_rdma_network(
            network(reordered=True), expected_protocol="roce", required=True
        )
        _, _, swapped = evaluate_rdma_network(
            network(swapped=True), expected_protocol="roce", required=True
        )
        self.assertEqual(
            baseline["rdma_fabric_profile"], reordered["rdma_fabric_profile"]
        )
        self.assertNotEqual(
            baseline["rdma_fabric_profile"], swapped["rdma_fabric_profile"]
        )
        self.assertEqual(
            {
                (item["source_netdev"], item["target_netdev"])
                for item in baseline["rdma_fabric_profile"]["dcb_policy_profiles"]
            },
            {("eth4", "eth4"), ("eth5", "eth5")},
        )
        findings = _rdma_protocol_consistency_findings(
            [
                {"node": "node01", "summary": {"environment": baseline}},
                {"node": "node02", "summary": {"environment": swapped}},
            ]
        )
        self.assertIn(
            "RDMA_FABRIC_PROFILE_INCONSISTENT",
            {item["reason_code"] for item in findings},
        )

    def test_complete_global_pause_and_fec_are_compared_without_policy(self):
        from tests.test_roce_health import healthy_network

        def summary(network):
            return evaluate_rdma_network(
                network, expected_protocol="roce", required=True
            )[2]

        baseline = summary(healthy_network())
        profile = baseline["rdma_fabric_profile"]
        self.assertIsNotNone(profile)
        global_rail = next(
            item
            for item in profile["configuration_rail_profiles"]
            if item["source_netdev"] == "bond0.100"
            and item["target_netdev"] == "bond0.100"
        )
        self.assertEqual(global_rail["pause"]["settings"], {"rx": False, "tx": False})
        self.assertEqual(global_rail["fec"]["settings"]["active"], "rs")

        pause_changed = healthy_network()
        pause_changed["interfaces"][0]["roce_configuration"]["pause"]["stdout"] = (
            "Pause parameters for bond0.100:\n"
            "Autonegotiate: off\nRX: on\nTX: off"
        )
        fec_changed = healthy_network()
        fec_changed["interfaces"][0]["roce_configuration"]["fec"]["stdout"] = (
            "FEC parameters for bond0.100:\n"
            "Configured FEC encodings: baser\n"
            "Active FEC encoding: baser"
        )
        for candidate in (summary(pause_changed), summary(fec_changed)):
            self.assertIsNotNone(candidate["rdma_fabric_profile"])
            self.assertNotEqual(profile, candidate["rdma_fabric_profile"])
            findings = _rdma_protocol_consistency_findings(
                [
                    {"node": "node01", "summary": {"environment": baseline}},
                    {"node": "node02", "summary": {"environment": candidate}},
                ]
            )
            self.assertIn(
                "RDMA_FABRIC_PROFILE_INCONSISTENT",
                {item["reason_code"] for item in findings},
            )

    def test_targeted_pause_fec_bind_to_leaf_rail_and_ignore_collection_order(self):
        from tests.test_roce_health import command, healthy_network

        pause_off = command("RX: off\nTX: off")
        pause_on = command("RX: on\nTX: off")
        fec_rs = command(
            "Configured FEC encodings: rs\nActive FEC encoding: rs"
        )
        fec_baser = command(
            "Configured FEC encodings: baser\nActive FEC encoding: baser"
        )

        def network(*, reverse_order=False, pause_change=False, fec_change=False):
            result = healthy_network()
            configuration = result["interfaces"][0]["roce_configuration"]
            configuration.pop("pause")
            configuration.pop("fec")
            configuration["pause_targets"] = {
                "eth4": copy.deepcopy(pause_off),
                "eth5": copy.deepcopy(pause_on if pause_change else pause_off),
            }
            configuration["fec_targets"] = {
                "eth4": copy.deepcopy(fec_rs),
                "eth5": copy.deepcopy(fec_baser if fec_change else fec_rs),
            }
            if reverse_order:
                configuration["dcb_targets"] = dict(
                    reversed(list(configuration["dcb_targets"].items()))
                )
                configuration["pause_targets"] = dict(
                    reversed(list(configuration["pause_targets"].items()))
                )
                configuration["fec_targets"] = dict(
                    reversed(list(configuration["fec_targets"].items()))
                )
                result["interfaces"].reverse()
            return result

        def summary(network):
            return evaluate_rdma_network(
                network, expected_protocol="roce", required=True
            )[2]

        baseline = summary(network())
        reordered = summary(network(reverse_order=True))
        pause_changed = summary(network(pause_change=True))
        fec_changed = summary(network(fec_change=True))
        self.assertEqual(
            baseline["rdma_fabric_profile"], reordered["rdma_fabric_profile"]
        )
        for candidate in (pause_changed, fec_changed):
            self.assertNotEqual(
                baseline["rdma_fabric_profile"], candidate["rdma_fabric_profile"]
            )
        eth5_rail = next(
            item
            for item in baseline["rdma_fabric_profile"]["configuration_rail_profiles"]
            if item["source_netdev"] == "bond0.100"
            and item["target_netdev"] == "eth5"
        )
        self.assertEqual(eth5_rail["pause"]["settings"]["rx"], False)
        self.assertEqual(eth5_rail["fec"]["settings"]["active"], "rs")

    def test_missing_pause_or_fec_makes_roce_fabric_evidence_incomplete(self):
        complete_network = network_with_ports(roce_port())
        incomplete_network = copy.deepcopy(complete_network)
        incomplete_network["interfaces"][0]["roce_configuration"].pop("pause")
        _, _, complete = evaluate_rdma_network(
            complete_network, expected_protocol="roce", required=True
        )
        _, _, incomplete = evaluate_rdma_network(
            incomplete_network, expected_protocol="roce", required=True
        )
        self.assertIsNotNone(complete["rdma_fabric_profile"])
        self.assertIsNone(incomplete["rdma_fabric_profile"])
        findings = _rdma_protocol_consistency_findings(
            [
                {"node": "node01", "summary": {"environment": complete}},
                {"node": "node02", "summary": {"environment": incomplete}},
            ]
        )
        reasons = {item["reason_code"] for item in findings}
        self.assertIn("RDMA_FABRIC_PROFILE_EVIDENCE_MISSING", reasons)
        self.assertNotIn("RDMA_FABRIC_PROFILE_INCONSISTENT", reasons)

    def test_k8s_cluster_mixed_protocol_is_a_failure_without_strict_stack(self):
        records = [
            {
                "node": "node01",
                "summary": {"environment": {"rdma_current_protocol": "NATIVE_INFINIBAND"}},
            },
            {
                "node": "node02",
                "summary": {"environment": {"rdma_current_protocol": "ROCE"}},
            },
        ]
        findings = _rdma_protocol_consistency_findings(records)
        self.assertIn(
            "RDMA_PROTOCOL_CLUSTER_MIXED",
            {item["reason_code"] for item in findings if item["severity"] == "FAIL"},
        )

    def test_k8s_fabric_profile_difference_is_a_failure(self):
        records = [
            {
                "node": "node01",
                "summary": {"environment": {"rdma_current_protocol": "NATIVE_INFINIBAND", "rdma_fabric_profile": {"protocol": "NATIVE_INFINIBAND", "subnet_prefixes": ["a"], "pkeys": ["0xffff"], "active_mtus": ["4096"]}}},
            },
            {
                "node": "node02",
                "summary": {"environment": {"rdma_current_protocol": "NATIVE_INFINIBAND", "rdma_fabric_profile": {"protocol": "NATIVE_INFINIBAND", "subnet_prefixes": ["b"], "pkeys": ["0xffff"], "active_mtus": ["4096"]}}},
            },
        ]
        findings = _rdma_protocol_consistency_findings(records)
        self.assertIn("RDMA_FABRIC_PROFILE_INCONSISTENT", {item["reason_code"] for item in findings})

    def test_slurm_fabric_profile_difference_is_a_failure_without_strict(self):
        records = [
            {"node": "node01", "reachable": True, "environment": {"rdma_current_protocol": "NATIVE_INFINIBAND", "rdma_fabric_profile": {"protocol": "NATIVE_INFINIBAND", "pkeys": ["0xffff"]}}},
            {"node": "node02", "reachable": True, "environment": {"rdma_current_protocol": "NATIVE_INFINIBAND", "rdma_fabric_profile": {"protocol": "NATIVE_INFINIBAND", "pkeys": ["0x7fff"]}}},
        ]
        findings = _consistency_findings(records, strict=False)
        fabric = [item for item in findings if item["field"] == "rdma_fabric_profile"]
        self.assertEqual(fabric[0]["severity"], "FAIL")
        self.assertEqual(fabric[0]["reason_code"], "RDMA_FABRIC_PROFILE_INCONSISTENT")

    def test_slurm_cluster_mixed_protocol_is_a_failure_without_strict_hardware(self):
        records = [
            {
                "node": "node01",
                "reachable": True,
                "environment": {"rdma_current_protocol": "NATIVE_INFINIBAND"},
            },
            {
                "node": "node02",
                "reachable": True,
                "environment": {"rdma_current_protocol": "ROCE"},
            },
        ]
        findings = _consistency_findings(records, strict=False)
        protocol_findings = [item for item in findings if item["field"] == "rdma_current_protocol"]
        self.assertEqual(protocol_findings[0]["severity"], "FAIL")
        self.assertEqual(protocol_findings[0]["reason_code"], "RDMA_PROTOCOL_CLUSTER_MIXED")


if __name__ == "__main__":
    unittest.main()
