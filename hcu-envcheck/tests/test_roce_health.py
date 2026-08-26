# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import copy
import unittest

from hcu_envcheck.roce_health import (
    evaluate_roce_configuration,
    evaluate_roce_health,
    normalize_roce_policy,
)


def command(stdout="", rc=0):
    return {"rc": rc, "stdout": stdout, "stderr": ""}


def dcb_commands(*, pfc_priority=3, app_priority=3, tc=3, rc=0):
    pfc = " ".join(
        f"{priority}:{'on' if priority == pfc_priority else 'off'}"
        for priority in range(8)
    )
    return {
        "pfc": command(f"pfc-cap 8\nprio-pfc {pfc}", rc),
        "ets": command(f"prio-tc 0:0 1:0 2:0 3:{tc} 4:0 5:0 6:0 7:0", rc),
        "app": command(f"dscp-prio 24:{app_priority}", rc),
        "buffer": command("prio-buffer 0:0 1:0 2:0 3:3 4:0 5:0 6:0 7:0", rc),
        "dcbx": command("mode host", rc),
    }


def full_policy(**overrides):
    result = {
        "protocol": "roce-v2",
        "versions": ["v2"],
        "address_families": ["ipv4"],
        "allowed_prefixes": ["10.20.30.0/24"],
        "vlan_ids": [100],
        "minimum_mtu": 9000,
        "minimum_rate_mbps": 100000,
        "lossless_priorities": [3],
        "dscp_to_priority": {"24": 3},
        "priority_to_tc": {"3": 3},
        "dcbx_mode": "host",
        "global_pause": "off",
        "fec_mode": "rs",
    }
    result.update(overrides)
    return result


def healthy_network():
    return {
        "rdma_devices": [
            {
                "name": "hca0",
                "ports": [
                    {
                        "port": "1",
                        "state": "4: ACTIVE",
                        "phys_state": "5: LinkUp",
                        "rate": "400 Gb/sec",
                        "link_layer": "Ethernet",
                        "gids": [
                            {
                                "index": 3,
                                "gid": "::ffff:10.20.30.11",
                                "type": "RoCE v2",
                                "netdev": "bond0.100",
                            }
                        ],
                        "gid_collection_status": "COMPLETE",
                        "gid_type_collection_status": "COMPLETE",
                        "gid_ndev_collection_status": "COMPLETE",
                    }
                ],
            }
        ],
        "interfaces": [
            {
                "name": "bond0.100",
                "local_link_status": "UP",
                "mtu": "9000",
                "vlan_id": 100,
                "lower_interfaces": ["bond0"],
                "bond_slaves": [],
                "roce_configuration": {
                    "ip_address_collection_status": "COMPLETE",
                    "ip_addresses": [
                        {
                            "family": "inet",
                            "local": "10.20.30.11",
                            "prefixlen": 24,
                            "scope": "global",
                        }
                    ],
                    "dcb_targets": {
                        "eth4": dcb_commands(),
                        "eth5": dcb_commands(),
                    },
                    "pause": command(
                        "Pause parameters for bond0.100:\n"
                        "Autonegotiate: off\nRX: off\nTX: off"
                    ),
                    "fec": command(
                        "FEC parameters for bond0.100:\n"
                        "Configured FEC encodings: rs\n"
                        "Active FEC encoding: rs"
                    ),
                },
            },
            {
                "name": "bond0",
                "local_link_status": "UP",
                "bond_slaves": ["eth4", "eth5"],
                "lower_interfaces": [],
            },
            {"name": "eth4", "local_link_status": "UP", "bond_slaves": [], "lower_interfaces": []},
            {"name": "eth5", "local_link_status": "UP", "bond_slaves": [], "lower_interfaces": []},
        ],
    }


def check(result, check_id):
    return next(item for item in result["checks"] if item["check_id"] == check_id)


class RoceHealthTests(unittest.TestCase):
    def test_complete_configuration_and_policy_pass(self):
        result = evaluate_roce_health(healthy_network(), full_policy())
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["summary"]["roce_bindings"], 1)
        self.assertEqual(result["bindings"][0]["gid_address_family"], "ipv4")
        self.assertEqual(
            result["interfaces"]["bond0.100"]["topology"]["paths"],
            [["bond0.100", "bond0", "eth4"], ["bond0.100", "bond0", "eth5"]],
        )
        self.assertEqual(result["interfaces"]["bond0.100"]["vlan"]["vlan_id"], 100)
        self.assertEqual(result["dcb_targets"]["eth4"]["pfc_enabled_priorities"], [3])
        self.assertEqual(result["pause"]["bond0.100"]["settings"], {"rx": False, "tx": False})
        self.assertEqual(result["fec"]["bond0.100"]["settings"]["active"], "rs")

    def test_alias_has_identical_semantics(self):
        network = healthy_network()
        policy = full_policy()
        self.assertEqual(
            evaluate_roce_configuration(network, policy),
            evaluate_roce_health(network, policy),
        )

    def test_complete_evidence_without_policy_is_unvalidated(self):
        result = evaluate_roce_health(healthy_network())
        self.assertEqual(result["status"], "UNVALIDATED")
        self.assertTrue(result["summary"]["unvalidated_checks"])
        self.assertFalse(result["summary"]["failed_checks"])
        self.assertFalse(result["summary"]["unknown_checks"])

    def test_incomplete_gid_evidence_is_unknown_not_failure(self):
        network = healthy_network()
        port = network["rdma_devices"][0]["ports"][0]
        port["gids"] = None
        port["gid_collection_status"] = "UNAVAILABLE"
        port["gid_type_collection_status"] = "NOT_COLLECTED"
        port["gid_ndev_collection_status"] = "NOT_COLLECTED"
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(check(result, "ROCE_PROTOCOL")["reason_code"], "ROCE_GID_EVIDENCE_MISSING")
        self.assertFalse(result["summary"]["failed_checks"])

    def test_complete_empty_gid_table_is_explicit_failure(self):
        network = healthy_network()
        network["rdma_devices"][0]["ports"][0]["gids"] = []
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_PROTOCOL")["reason_code"], "ROCE_GID_NOT_CONFIGURED")

    def test_address_collection_unknown_is_not_no_address_failure(self):
        network = healthy_network()
        configuration = network["interfaces"][0]["roce_configuration"]
        configuration["ip_addresses"] = []
        configuration["ip_address_collection_status"] = "COMMAND_FAILED"
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(check(result, "ROCE_ADDRESSES")["reason_code"], "ROCE_ADDRESS_EVIDENCE_MISSING")

    def test_complete_empty_address_table_is_failure(self):
        network = healthy_network()
        configuration = network["interfaces"][0]["roce_configuration"]
        configuration["ip_addresses"] = []
        configuration["ip_address_collection_status"] = "COMPLETE"
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_ADDRESSES")["reason_code"], "ROCE_ADDRESS_NOT_CONFIGURED")

    def test_wrong_prefix_and_address_family_fail(self):
        network = healthy_network()
        result = evaluate_roce_health(
            network,
            full_policy(address_families=["ipv6"], allowed_prefixes=["192.0.2.0/24"]),
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_ADDRESS_FAMILIES")["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_ADDRESS_PREFIXES")["status"], "FAIL")

    def test_roce_v2_gid_must_match_a_netdev_address(self):
        network = healthy_network()
        network["interfaces"][0]["roce_configuration"]["ip_addresses"][0]["local"] = "10.20.30.12"
        result = evaluate_roce_health(network, full_policy())
        binding_check = check(result, "ROCE_GID_ADDRESS_BINDING:hca0:1:3")
        self.assertEqual(binding_check["status"], "FAIL")
        self.assertEqual(binding_check["reason_code"], "ROCE_GID_ADDRESS_BINDING_MISMATCH")

    def test_missing_vlan_evidence_is_unknown_but_known_mismatch_fails(self):
        missing = healthy_network()
        missing["interfaces"][0].pop("vlan_id")
        result = evaluate_roce_health(missing, full_policy())
        self.assertEqual(check(result, "ROCE_VLAN")["status"], "UNKNOWN")
        self.assertEqual(result["status"], "UNKNOWN")

        mismatch = healthy_network()
        mismatch["interfaces"][0]["vlan_id"] = 200
        result = evaluate_roce_health(mismatch, full_policy())
        self.assertEqual(check(result, "ROCE_VLAN")["status"], "FAIL")
        self.assertEqual(result["status"], "FAIL")

    def test_bond_leaf_down_fails_and_missing_leaf_is_unknown(self):
        down = healthy_network()
        down["interfaces"][-1]["local_link_status"] = "DOWN"
        result = evaluate_roce_health(down, full_policy())
        self.assertEqual(check(result, "ROCE_LEAVES:bond0.100")["status"], "FAIL")

        missing = healthy_network()
        missing["interfaces"].pop()
        result = evaluate_roce_health(missing, full_policy())
        self.assertEqual(check(result, "ROCE_LEAVES:bond0.100")["status"], "UNKNOWN")

    def test_interface_topology_cycle_is_failure(self):
        network = healthy_network()
        network["interfaces"][1]["lower_interfaces"] = ["bond0.100"]
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(check(result, "ROCE_LEAVES:bond0.100")["status"], "FAIL")
        self.assertEqual(
            check(result, "ROCE_LEAVES:bond0.100")["reason_code"],
            "ROCE_INTERFACE_TOPOLOGY_CYCLE",
        )

    def test_dcb_must_cover_every_resolved_leaf(self):
        network = healthy_network()
        network["interfaces"][0]["roce_configuration"]["dcb_targets"].pop("eth5")
        result = evaluate_roce_health(network, full_policy())
        coverage = check(result, "ROCE_DCB_TARGETS:bond0.100")
        self.assertEqual(coverage["status"], "UNKNOWN")
        self.assertEqual(coverage["reason_code"], "ROCE_DCB_TARGET_EVIDENCE_MISSING")

        wrong = healthy_network()
        wrong["interfaces"][0]["roce_configuration"]["dcb_targets"]["eth9"] = dcb_commands()
        result = evaluate_roce_health(wrong, full_policy())
        self.assertEqual(check(result, "ROCE_DCB_TARGETS:bond0.100")["status"], "FAIL")

    def test_app_priority_without_pfc_is_internal_failure(self):
        network = healthy_network()
        for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands.update(dcb_commands(pfc_priority=4, app_priority=3))
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_DCB_INTERNAL:eth4")["reason_code"], "ROCE_APP_PRIORITY_NOT_LOSSLESS")

    def test_app_priority_with_incomplete_ets_table_is_unknown(self):
        network = healthy_network()
        for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["ets"] = command("prio-tc 0:0 1:0 2:0 4:4 5:0 6:0 7:0")
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(check(result, "ROCE_DCB_INTERNAL:eth4")["status"], "UNKNOWN")
        self.assertEqual(
            check(result, "ROCE_DCB_INTERNAL:eth4")["reason_code"],
            "ROCE_ETS_EVIDENCE_INCOMPLETE",
        )
        self.assertEqual(result["status"], "UNKNOWN")

    def test_dcb_command_failure_is_unknown_not_bad_configuration(self):
        network = healthy_network()
        network["interfaces"][0]["roce_configuration"]["dcb_targets"]["eth4"]["pfc"] = command(rc=127)
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(check(result, "ROCE_DCB_EVIDENCE:eth4")["status"], "UNKNOWN")
        self.assertNotIn("ROCE_DCB_EVIDENCE:eth4", result["summary"]["failed_checks"])

    def test_pfc_requires_complete_unique_zero_to_seven_table(self):
        complete = (
            "prio-pfc 0:off 1:off 2:off 3:on "
            "4:off 5:off 6:off 7:off"
        )
        cases = {
            "single_policy_item": "prio-pfc 3:on",
            "missing_priority": (
                "prio-pfc 0:off 1:off 2:off 3:on "
                "4:off 5:off 6:off"
            ),
            "duplicate_same": complete + " 3:on",
            "duplicate_conflict": complete + " 3:off",
        }
        for case, output in cases.items():
            with self.subTest(case=case):
                network = healthy_network()
                for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
                    commands["pfc"] = command(output)
                result = evaluate_roce_health(network, full_policy())
                finding = check(result, "ROCE_LOSSLESS_PRIORITIES:eth4")
                self.assertEqual(finding["status"], "UNKNOWN")
                self.assertEqual(finding["reason_code"], "ROCE_PFC_PARSE_INCOMPLETE")
                self.assertEqual(check(result, "ROCE_DCB_EVIDENCE:eth4")["status"], "UNKNOWN")
                self.assertFalse(result["dcb_targets"]["eth4"]["pfc_table_complete"])
                self.assertNotEqual(result["status"], "PASS")

    def test_ets_requires_complete_unique_zero_to_seven_table(self):
        complete = "prio-tc 0:0 1:0 2:0 3:3 4:0 5:0 6:0 7:0"
        cases = {
            "single_policy_item": "prio-tc 3:3",
            "missing_priority": "prio-tc 0:0 1:0 2:0 3:3 4:0 5:0 6:0",
            "duplicate_same": complete + " 3:3",
            "duplicate_conflict": complete + " 3:4",
        }
        for case, output in cases.items():
            with self.subTest(case=case):
                network = healthy_network()
                for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
                    commands["ets"] = command(output)
                result = evaluate_roce_health(network, full_policy())
                finding = check(result, "ROCE_PRIORITY_TO_TC:eth4")
                self.assertEqual(finding["status"], "UNKNOWN")
                self.assertEqual(finding["reason_code"], "ROCE_ETS_PARSE_INCOMPLETE")
                self.assertEqual(check(result, "ROCE_DCB_EVIDENCE:eth4")["status"], "UNKNOWN")
                self.assertFalse(result["dcb_targets"]["eth4"]["ets_table_complete"])
                self.assertNotEqual(result["status"], "PASS")

    def test_truncated_dcb_command_evidence_never_passes(self):
        cases = (
            (
                "pfc",
                "stdout_truncated",
                "ROCE_LOSSLESS_PRIORITIES:eth4",
                "ROCE_PFC_EVIDENCE_TRUNCATED",
            ),
            (
                "ets",
                "output_truncated",
                "ROCE_PRIORITY_TO_TC:eth4",
                "ROCE_ETS_EVIDENCE_TRUNCATED",
            ),
            (
                "app",
                "stdout_truncated",
                "ROCE_DSCP_TO_PRIORITY:eth4",
                "ROCE_APP_EVIDENCE_TRUNCATED",
            ),
            (
                "dcbx",
                "output_truncated",
                "ROCE_DCBX:eth4",
                "ROCE_DCBX_EVIDENCE_TRUNCATED",
            ),
            ("buffer", "stdout_truncated", None, None),
        )
        for section, flag, check_id, reason_code in cases:
            with self.subTest(section=section, flag=flag):
                network = healthy_network()
                for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
                    commands[section][flag] = True
                result = evaluate_roce_health(network, full_policy())
                generic = check(result, "ROCE_DCB_EVIDENCE:eth4")
                self.assertEqual(generic["status"], "UNKNOWN")
                self.assertEqual(
                    generic["reason_code"], "ROCE_DCB_EVIDENCE_TRUNCATED"
                )
                if check_id is not None:
                    finding = check(result, check_id)
                    self.assertEqual(finding["status"], "UNKNOWN")
                    self.assertEqual(finding["reason_code"], reason_code)
                self.assertEqual(result["status"], "UNKNOWN")

    def test_truncated_pause_and_fec_evidence_never_passes(self):
        cases = (
            (
                "pause",
                "stdout_truncated",
                "ROCE_GLOBAL_PAUSE:bond0.100",
                "ROCE_GLOBAL_PAUSE_EVIDENCE_TRUNCATED",
            ),
            (
                "fec",
                "output_truncated",
                "ROCE_FEC:bond0.100",
                "ROCE_FEC_EVIDENCE_TRUNCATED",
            ),
        )
        for section, flag, check_id, reason_code in cases:
            with self.subTest(section=section, flag=flag):
                network = healthy_network()
                network["interfaces"][0]["roce_configuration"][section][flag] = True
                result = evaluate_roce_health(network, full_policy())
                finding = check(result, check_id)
                self.assertEqual(finding["status"], "UNKNOWN")
                self.assertEqual(finding["reason_code"], reason_code)
                self.assertEqual(result["status"], "UNKNOWN")
    def test_each_dcb_policy_mismatch_is_failure(self):
        cases = (
            ("lossless_priorities", [4], "ROCE_LOSSLESS_PRIORITIES:eth4"),
            ("dscp_to_priority", {"24": 4}, "ROCE_DSCP_TO_PRIORITY:eth4"),
            ("priority_to_tc", {"3": 4}, "ROCE_PRIORITY_TO_TC:eth4"),
            ("dcbx_mode", "ieee", "ROCE_DCBX:eth4"),
        )
        for field, value, check_id in cases:
            with self.subTest(field=field):
                result = evaluate_roce_health(healthy_network(), full_policy(**{field: value}))
                self.assertEqual(check(result, check_id)["status"], "FAIL")
                self.assertEqual(result["status"], "FAIL")

    def test_pause_and_fec_missing_are_unknown_mismatch_is_failure(self):
        missing = healthy_network()
        missing["interfaces"][0]["roce_configuration"]["pause"] = None
        missing["interfaces"][0]["roce_configuration"]["fec"] = command(rc=95)
        result = evaluate_roce_health(missing, full_policy())
        self.assertEqual(check(result, "ROCE_GLOBAL_PAUSE:bond0.100")["status"], "UNKNOWN")
        self.assertEqual(check(result, "ROCE_FEC:bond0.100")["status"], "UNKNOWN")

        result = evaluate_roce_health(
            healthy_network(),
            full_policy(global_pause="on", fec_mode="baser"),
        )
        self.assertEqual(check(result, "ROCE_GLOBAL_PAUSE:bond0.100")["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_FEC:bond0.100")["status"], "FAIL")

        no_active = healthy_network()
        no_active["interfaces"][0]["roce_configuration"]["fec"] = command(
            "Configured FEC encodings: rs"
        )
        result = evaluate_roce_health(no_active, full_policy())
        self.assertEqual(check(result, "ROCE_FEC:bond0.100")["status"], "UNKNOWN")

    def test_version_mtu_and_rate_policy_mismatches_fail(self):
        result = evaluate_roce_health(
            healthy_network(),
            full_policy(versions=["v1"], minimum_mtu=9500, minimum_rate_mbps=800000),
        )
        self.assertEqual(check(result, "ROCE_VERSIONS")["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_MTU:bond0.100")["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_RATE:bond0.100")["status"], "FAIL")

    def test_collected_leaf_mtu_pause_and_fec_targets_are_authoritative(self):
        network = healthy_network()
        configuration = network["interfaces"][0]["roce_configuration"]
        configuration["topology"] = {
            "status": "COMPLETE",
            "paths": [
                ["bond0.100", "bond0", "eth4"],
                ["bond0.100", "bond0", "eth5"],
            ],
            "leaf_evidence": {
                "eth4": {"local_link_status": "UP", "mtu": "9000"},
                "eth5": {"local_link_status": "UP", "mtu": "9000"},
            },
        }
        configuration["pause_targets"] = {
            name: command("RX: off\nTX: off") for name in ("eth4", "eth5")
        }
        configuration["fec_targets"] = {
            name: command("Configured FEC encodings: rs\nActive FEC encoding: rs")
            for name in ("eth4", "eth5")
        }
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(check(result, "ROCE_LEAF_MTU:bond0.100")["status"], "PASS")
        self.assertEqual(check(result, "ROCE_PAUSE_TARGETS:bond0.100")["status"], "PASS")
        self.assertEqual(check(result, "ROCE_FEC_TARGETS:bond0.100")["status"], "PASS")
        self.assertEqual(set(result["pause"]), {"eth4", "eth5"})
        self.assertEqual(set(result["fec"]), {"eth4", "eth5"})

    def test_collected_leaf_mtu_below_policy_fails(self):
        network = healthy_network()
        configuration = network["interfaces"][0]["roce_configuration"]
        configuration["topology"] = {
            "status": "COMPLETE",
            "leaf_evidence": {
                "eth4": {"local_link_status": "UP", "mtu": "1500"},
                "eth5": {"local_link_status": "UP", "mtu": "9000"},
            },
        }
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(check(result, "ROCE_LEAF_MTU:bond0.100")["status"], "FAIL")
        self.assertEqual(
            check(result, "ROCE_LEAF_MTU:bond0.100")["reason_code"],
            "ROCE_LEAF_MTU_BELOW_POLICY",
        )

    def test_any_out_of_policy_address_fails_prefix_check(self):
        network = healthy_network()
        network["interfaces"][0]["roce_configuration"]["ip_addresses"].append(
            {
                "family": "inet",
                "local": "192.0.2.11",
                "prefixlen": 24,
                "scope": "global",
            }
        )
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_ADDRESS_PREFIXES")["status"], "FAIL")
        self.assertIn("192.0.2.11", check(result, "ROCE_ADDRESS_PREFIXES")["message"])

    def test_extra_pfc_app_and_ets_policy_entries_are_not_false_passes(self):
        network = healthy_network()
        commands = {
            "pfc": command(
                "pfc-cap 8\nprio-pfc 0:off 1:off 2:off 3:on 4:on 5:off 6:off 7:off"
            ),
            "ets": command("prio-tc 0:0 1:0 2:0 3:3 4:4 5:0 6:0 7:0"),
            "app": command("dscp-prio 24:3 25:4"),
            "buffer": command("prio-buffer 0:0 1:0 2:0 3:3 4:4 5:0 6:0 7:0"),
            "dcbx": command("mode host"),
        }
        for target in network["interfaces"][0]["roce_configuration"]["dcb_targets"]:
            network["interfaces"][0]["roce_configuration"]["dcb_targets"][target] = commands
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        for check_id in (
            "ROCE_LOSSLESS_PRIORITIES:eth4",
            "ROCE_DSCP_TO_PRIORITY:eth4",
        ):
            self.assertEqual(check(result, check_id)["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_PRIORITY_TO_TC:eth4")["status"], "PASS")
    def test_known_pfc_mismatch_is_not_hidden_by_missing_other_dcb_sections(self):
        network = healthy_network()
        for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["pfc"] = dcb_commands(pfc_priority=4)["pfc"]
            commands["ets"] = None
            commands["app"] = command(rc=127)
            commands["dcbx"] = None
        result = evaluate_roce_health(network, full_policy())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(check(result, "ROCE_DCB_EVIDENCE:eth4")["status"], "UNKNOWN")
        self.assertEqual(check(result, "ROCE_LOSSLESS_PRIORITIES:eth4")["status"], "FAIL")
        self.assertEqual(
            check(result, "ROCE_LOSSLESS_PRIORITIES:eth4")["reason_code"],
            "ROCE_PFC_POLICY_MISMATCH",
        )

    def test_each_available_dcb_section_is_evaluated_when_peers_are_missing(self):
        cases = (
            (
                "app",
                lambda commands: commands.update(
                    {"pfc": None, "app": command("dscp-prio 24:4"), "ets": None, "dcbx": None}
                ),
                "ROCE_DSCP_TO_PRIORITY:eth4",
                "FAIL",
            ),
            (
                "ets",
                lambda commands: commands.update(
                    {"pfc": None, "app": None, "ets": command("prio-tc 3:4"), "dcbx": None}
                ),
                "ROCE_PRIORITY_TO_TC:eth4",
                "UNKNOWN",
            ),
            (
                "dcbx",
                lambda commands: commands.update(
                    {"pfc": None, "app": None, "ets": None, "dcbx": command("mode ieee")}
                ),
                "ROCE_DCBX:eth4",
                "FAIL",
            ),
        )
        for section, mutate, check_id, expected_status in cases:
            with self.subTest(section=section):
                network = healthy_network()
                for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
                    mutate(commands)
                result = evaluate_roce_health(network, full_policy())
                self.assertEqual(check(result, check_id)["status"], expected_status)
                self.assertEqual(result["status"], expected_status)
    def test_exact_app_policy_supports_all_standard_selector_families(self):
        network = healthy_network()
        app = (
            "dscp-prio 24:3 pcp-prio 3:3 port-prio 4791:3 "
            "stream-port-prio 443:3 dgram-port-prio 4791:3 "
            "ethtype-prio 8915:3"
        )
        for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["app"] = command(app)
        policy = full_policy()
        policy.pop("dscp_to_priority")
        policy["app_mappings"] = {
            "dscp-prio": {"24": 3},
            "pcp-prio": {"3": 3},
            "port-prio": {"4791": 3},
            "stream-port-prio": {"443": 3},
            "dgram-port-prio": {"4791": 3},
            "ethertype": {"0x8915": 3},
        }
        result = evaluate_roce_health(network, policy)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(check(result, "ROCE_APP_MAPPINGS:eth4")["status"], "PASS")
        self.assertEqual(
            result["normalized_policy"]["app_mappings"]["ethtype-prio"],
            {0x8915: 3},
        )

    def test_legacy_dscp_policy_does_not_pass_extra_non_dscp_mapping(self):
        network = healthy_network()
        for commands in network["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["app"] = command("dscp-prio 24:3 pcp-prio 3:3")
        result = evaluate_roce_health(network, full_policy())
        legacy = check(result, "ROCE_DSCP_TO_PRIORITY:eth4")
        self.assertEqual(legacy["status"], "UNKNOWN")
        self.assertEqual(legacy["reason_code"], "ROCE_APP_POLICY_SCOPE_INCOMPLETE")
        self.assertNotEqual(result["status"], "PASS")

    def test_exact_app_policy_fails_extra_selector_and_unknown_selector_is_not_pass(self):
        policy = full_policy()
        policy.pop("dscp_to_priority")
        policy["app_mappings"] = {"dscp-prio": {"24": 3}}

        extra = healthy_network()
        for commands in extra["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["app"] = command("dscp-prio 24:3 pcp-prio 3:3")
        result = evaluate_roce_health(extra, policy)
        self.assertEqual(check(result, "ROCE_APP_MAPPINGS:eth4")["status"], "FAIL")

        unknown = healthy_network()
        for commands in unknown["interfaces"][0]["roce_configuration"]["dcb_targets"].values():
            commands["app"] = command("dscp-prio 24:3 vendor-prio 9:3")
        result = evaluate_roce_health(unknown, policy)
        exact = check(result, "ROCE_APP_MAPPINGS:eth4")
        self.assertEqual(exact["status"], "UNKNOWN")
        self.assertEqual(exact["reason_code"], "ROCE_APP_SELECTOR_UNPARSED")
        self.assertNotEqual(result["status"], "PASS")
    def test_policy_validation_catches_typo_and_invalid_ranges(self):
        invalid = (
            {"protcol": "roce"},
            {"versions": ["v3"]},
            {"address_families": ["ipx"]},
            {"allowed_prefixes": ["not-a-network"]},
            {"vlan_ids": [4095]},
            {"lossless_priorities": [8]},
            {"dscp_to_priority": {"64": 3}},
            {"global_pause": {"rx": "off", "tx": False}},
        )
        for policy in invalid:
            with self.subTest(policy=policy):
                with self.assertRaises(ValueError):
                    normalize_roce_policy(policy)

    def test_input_is_not_mutated(self):
        network = healthy_network()
        before = copy.deepcopy(network)
        evaluate_roce_health(network, full_policy())
        self.assertEqual(network, before)

    def test_native_ib_without_roce_policy_is_not_a_roce_failure(self):
        network = {
            "rdma_devices": [
                {
                    "name": "hca0",
                    "ports": [{"port": "1", "link_layer": "InfiniBand"}],
                }
            ],
            "interfaces": [],
        }
        result = evaluate_roce_health(network)
        self.assertEqual(result["status"], "UNVALIDATED")
        self.assertFalse(result["summary"]["failed_checks"])

if __name__ == "__main__":
    unittest.main()
