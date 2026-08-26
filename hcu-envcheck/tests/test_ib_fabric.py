# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from hcu_envcheck.active_rdma import (
    ActiveCheckSafetyError,
    SlurmActiveContext,
    SlurmAllocation,
)
from hcu_envcheck.ib_fabric import (
    IBFabricCheckConfig,
    IBFabricCheckResult,
    REQUIRED_STANDARD_COUNTERS,
    SlurmIBFabricRunner,
    evaluate_fabric_counter_delta,
    parse_iblinkinfo,
    parse_extended_xmit_wait,
    parse_perfquery_counters,
    parse_smpquery_fabric_link,
    write_ib_fabric_reports,
)


from hcu_envcheck.cli import _run_ib_fabric_slurm, build_parser, main


class StreamingPipe:
    """Produce large deterministic output without retaining it in the test process."""

    def __init__(self, total_bytes, head=b"H", tail=b"T"):
        self.total_bytes = total_bytes
        self.head = head
        self.tail = tail
        self.position = 0
        self.closed = False

    def read(self, size=-1):
        if self.closed or self.position >= self.total_bytes:
            return b""
        if size is None or size < 0:
            size = self.total_bytes - self.position
        count = min(size, self.total_bytes - self.position)
        marker = self.head if self.position < self.total_bytes // 2 else self.tail
        self.position += count
        return marker * count

    def close(self):
        self.closed = True

def context(**overrides):
    values = {
        "job_id": "674118",
        "selected_nodes": ("e06r1n08", "e06r1n09"),
        "enabled": True,
        "confirm_allocation_idle": True,
        "current_user": "qianyj1",
        "controller_hostname": "zz-login01",
    }
    values.update(overrides)
    return SlurmActiveContext(**values)


def fabric_config(**overrides):
    values = {
        "hcas": ("shca_0",),
        "expected_link_width": "4X",
        "minimum_link_speed_gbps": 100.0,
    }
    values.update(overrides)
    return IBFabricCheckConfig(**values)

def allocation():
    return SlurmAllocation(
        job_id="674118",
        owner="qianyj1",
        state="RUNNING",
        nodes=("e06r1n08", "e06r1n09"),
        active_steps=(),
        exclusive_mode="NODE",
        foreign_active_job_ids=(),
        node_exclusivity_proven=True,
    )


def link_line(
    node="e06r1n08",
    source_guid="0xe508e8611a658f52",
    source_lid=34812,
    switch_guid="0xe508e8611a4b9f7e",
    switch_lid=7237,
    switch_port=69,
    switch_name="SW400-P05F1IB21-e06r1u",
    state="Active",
    physical="LinkUp",
    width="4X",
    speed=106.25,
):
    return (
        f'{source_guid} "               {node} shca_0"  {source_lid}    1[  ] '
        f'==( {width}        {speed:g} Gbps {state}/  {physical})==>  '
        f'{switch_guid}   {switch_lid}   {switch_port}[  ] "{switch_name}" ( )\n'
    )


def local_portinfo_output(lid=4989, width="4X", speed=106.25, state="Active", physical="LinkUp"):
    return (
        "# Port info: DR path slid 0; dlid 0; 0,1\n"
        f"Lid:.............................{lid}\n"
        f"LinkWidthActive:.................{width}\n"
        f"LinkState:.......................{state}\n"
        f"PhysLinkState:...................{physical}\n"
        "LinkSpeedActive:.................Extended speed\n"
        f"LinkSpeedExtActive:..............{speed:g} Gbps\n"
    )


def leaf_nodeinfo_output(guid="0xe508e8611a4ba3ca", local_port=69, node_type="Switch"):
    return (
        "# Node info: DR path slid 0; dlid 0; 0,1\n"
        f"NodeType:........................{node_type}\n"
        f"SystemGuid:......................{guid}\n"
        f"Guid:............................{guid}\n"
        f"LocalPort:.......................{local_port}\n"
    )


def leaf_nodedesc_output(name="SW400-P05F1IB32-e08r4u"):
    return f"Node Description:..........{name}\n"


def leaf_portinfo_output(lid=7397, local_port=69):
    return (
        "# Port info: DR path slid 0; dlid 0; 0,1\n"
        f"Lid:.............................{lid}\n"
        f"LocalPort:.......................{local_port}\n"
    )


def directed_smp_output(argv, *, shared_leaf=False):
    node = next(
        value.split("=", 1)[1] for value in argv if value.startswith("--nodelist=")
    )
    tool_index = next(
        index for index, value in enumerate(argv) if Path(value).name == "smpquery"
    )
    tool = argv[tool_index:]
    is_first = node == "e06r1n08" or shared_leaf
    local_lid = 34812 if is_first else 34816
    switch_port = 69 if is_first else 62
    hca = tool[tool.index("-C") + 1]
    if hca != "shca_0":
        local_lid += 1
        switch_port += 1
    if "-x" in tool:
        return local_portinfo_output(lid=local_lid)
    attribute = tool[tool.index("-D") + 1]
    if attribute == "nodeinfo":
        return leaf_nodeinfo_output(local_port=switch_port)
    if attribute == "nodedesc":
        return leaf_nodedesc_output(name="SW400-P05F1IB21-e06r1u")
    if attribute == "portinfo":
        return leaf_portinfo_output(lid=7237, local_port=switch_port)
    raise AssertionError(f"unexpected directed SMP: {tool}")

def route():
    parsed = parse_iblinkinfo(
        link_line(), node="e06r1n08", hca="shca_0", ib_port=1
    )
    assert parsed is not None
    return parsed


def standard_counters(*, relay=9, symbol=0, link_down=0, discards=0, wait=12):
    return (
        "# Port counters: Lid 7237 port 69\n"
        f"SymbolErrorCounter:..............{symbol}\n"
        "LinkErrorRecoveryCounter:........0\n"
        f"LinkDownedCounter:...............{link_down}\n"
        "PortRcvErrors:...................0\n"
        "PortRcvRemotePhysicalErrors:......0\n"
        f"PortRcvSwitchRelayErrors:........{relay}\n"
        f"PortXmitDiscards:................{discards}\n"
        "PortXmitConstraintErrors:.........0\n"
        "PortRcvConstraintErrors:..........0\n"
        "LocalLinkIntegrityErrors:.........0\n"
        "ExcessiveBufferOverrunErrors:.....0\n"
        "VL15Dropped:......................0\n"
        f"PortXmitWait:....................{wait}\n"
    )


def congestion_counters(value=100):
    return (
        "# SwPortVLCongestion counters: Lid 7237 port 69\n"
        f"SWPortVLCongestion0:.............{value}\n"
        "SWPortVLCongestion1:.............0\n"
    )


def extended_counters(wait=123456789012):
    return (
        "# Port extended counters: Lid 7237 port 69 "
        "(CapMask: 0x0200 CapMask2: 0x0000020)\n"
        "PortXmitData:....................123\n"
        f"PortXmitWait:....................{wait}\n"
    )


class ParserTests(unittest.TestCase):

    def test_parse_real_one_hop_link(self):
        item = parse_iblinkinfo(
            link_line(), node="e06r1n08",
            hca="shca_0",
            ib_port=1,
            expected_link_width="4X",
            minimum_link_speed_gbps=400.0
        )
        self.assertIsNotNone(item)
        self.assertEqual(item.status, "PASS")
        self.assertEqual(item.switch_name, "SW400-P05F1IB21-e06r1u")
        self.assertEqual(item.switch_guid, "0xe508e8611a4b9f7e")
        self.assertEqual(item.switch_lid, 7237)
        self.assertEqual(item.switch_port, 69)
        self.assertEqual(item.rate, "4X 106.25 Gbps")
        self.assertEqual(item.lane_count, 4)
        self.assertEqual(item.lane_speed_gbps, 106.25)
        self.assertEqual(item.aggregate_speed_gbps, 425.0)
        self.assertEqual(item.state, "Active")
        self.assertEqual(item.physical_state, "LinkUp")

    def test_parse_real_shca_directed_smp_bundle(self):
        link, reason, message = parse_smpquery_fabric_link(
            local_portinfo=local_portinfo_output(),
            leaf_nodeinfo=leaf_nodeinfo_output(),
            leaf_nodedesc=leaf_nodedesc_output(),
            leaf_portinfo=leaf_portinfo_output(),
            node="e08r4n08",
            hca="shca_0",
            ib_port=1,
            expected_link_width="4X",
            minimum_link_speed_gbps=400.0,
        )

        self.assertIsNone(reason, message)
        self.assertIsNotNone(link)
        self.assertEqual(link.source_lid, 4989)
        self.assertEqual(link.switch_guid, "0xe508e8611a4ba3ca")
        self.assertEqual(link.switch_name, "SW400-P05F1IB32-e08r4u")
        self.assertEqual(link.switch_lid, 7397)
        self.assertEqual(link.switch_port, 69)
        self.assertEqual(link.width, "4X")
        self.assertEqual(link.aggregate_speed_gbps, 425.0)
        self.assertEqual(link.state_status, "PASS")
        self.assertEqual(link.status, "PASS")

    def test_directed_neighbor_must_be_a_switch(self):
        link, reason, _ = parse_smpquery_fabric_link(
            local_portinfo=local_portinfo_output(),
            leaf_nodeinfo=leaf_nodeinfo_output(node_type="Channel Adapter"),
            leaf_nodedesc=leaf_nodedesc_output(),
            leaf_portinfo=leaf_portinfo_output(),
            node="e08r4n08",
            hca="shca_0",
            ib_port=1,
        )
        self.assertIsNone(link)
        self.assertEqual(reason, "SMPQUERY_ONE_HOP_NEIGHBOR_NOT_SWITCH")

    def test_missing_critical_directed_field_is_not_verified(self):
        link, reason, message = parse_smpquery_fabric_link(
            local_portinfo=local_portinfo_output(),
            leaf_nodeinfo=leaf_nodeinfo_output(),
            leaf_nodedesc=leaf_nodedesc_output(),
            leaf_portinfo="LocalPort:.......................69\n",
            node="e08r4n08",
            hca="shca_0",
            ib_port=1,
        )
        self.assertIsNone(link)
        self.assertEqual(reason, "SMPQUERY_ONE_HOP_EVIDENCE_MISSING")
        self.assertIn("leaf.PortInfo.Lid", message)
    def test_active_link_without_rate_policy_is_not_verified(self):
        item = parse_iblinkinfo(
            link_line(), node="e06r1n08", hca="shca_0", ib_port=1
        )
        self.assertIsNotNone(item)
        self.assertEqual(item.state_status, "PASS")
        self.assertEqual(item.performance_status, "NOT_VERIFIED")
        self.assertEqual(item.status, "NOT_VERIFIED")

    def test_link_downshift_fails_explicit_rate_policy(self):
        item = parse_iblinkinfo(
            link_line(),
            node="e06r1n08",
            hca="shca_0",
            ib_port=1,
            expected_link_width="8X",
            minimum_link_speed_gbps=200.0,
        )
        self.assertIsNotNone(item)
        self.assertEqual(item.state_status, "PASS")
        self.assertEqual(item.performance_status, "FAIL")
        self.assertEqual(item.status, "FAIL")

    def test_aggregate_speed_below_policy_fails(self):
        item = parse_iblinkinfo(
            link_line(speed=50),
            node="e06r1n08",
            hca="shca_0",
            ib_port=1,
            expected_link_width="4X",
            minimum_link_speed_gbps=400.0,
        )
        self.assertIsNotNone(item)
        self.assertEqual(item.aggregate_speed_gbps, 200.0)
        self.assertEqual(item.performance_status, "FAIL")
        self.assertEqual(item.status, "FAIL")

    def test_non_active_link_is_fail(self):
        item = parse_iblinkinfo(
            link_line(state="Down", physical="Polling"),
            node="e06r1n08",
            hca="shca_0",
            ib_port=1,
        )
        self.assertIsNotNone(item)
        self.assertEqual(item.status, "FAIL")

    def test_counter_parser_accepts_decimal_and_hex(self):
        parsed = parse_perfquery_counters(
            "PortXmitDiscards:........0\nCounterSelect:........0x000f\n"
        )
        self.assertEqual(parsed["PortXmitDiscards"], 0)
        self.assertEqual(parsed["CounterSelect"], 15)

    def test_extended_xmit_wait_parser_requires_attribute_header_and_exact_field(self):
        self.assertEqual(
            parse_extended_xmit_wait(extended_counters(wait=4294967295)),
            {"PortXmitWait": 4294967295},
        )
        self.assertEqual(
            parse_extended_xmit_wait("PortXmitWait:........7\n"),
            {},
        )
        self.assertEqual(
            parse_extended_xmit_wait(
                "# Port extended counters: Lid 7237 port 69\n"
                "PortXmitData:........7\n"
            ),
            {},
        )
        mixed_attributes = (
            "# Port counters: Lid 7237 port 69\n"
            "PortXmitWait:................42\n"
            "# Port extended counters: Lid 7237 port 69\n"
            "PortXmitData:................1\n"
        )
        self.assertEqual(parse_extended_xmit_wait(mixed_attributes), {})


class CounterDeltaTests(unittest.TestCase):

    def evaluate(self, before, after):
        normalized_before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        normalized_after = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        normalized_before.update(before)
        normalized_after.update(after)
        return evaluate_fabric_counter_delta(
            normalized_before,
            normalized_after,
            route=route(),
            sample_interval_seconds=5,
        )

    def test_historical_nonzero_stable_does_not_fail(self):
        before = {
            "PortRcvSwitchRelayErrors": 9,
            "PortXmitWait": 12,
            "SWPortVLCongestion0": 100,
        }
        result = self.evaluate(before, dict(before))
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.historical_nonzero_stable["PortRcvSwitchRelayErrors"], 9)

    def test_error_drop_or_link_delta_is_fail(self):
        before = {"LinkDownedCounter": 0, "PortXmitWait": 0}
        after = {"LinkDownedCounter": 1, "PortXmitWait": 0}
        result = self.evaluate(before, after)
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.failure_deltas, {"LinkDownedCounter": 1})

    def test_per_vl_congestion_delta_is_warn(self):
        before = {"PortXmitDiscards": 0, "SWPortVLCongestion0": 100}
        after = {"PortXmitDiscards": 0, "SWPortVLCongestion0": 105}
        result = self.evaluate(before, after)
        self.assertEqual(result.status, "WARN")
        self.assertEqual(result.congestion_deltas, {"SWPortVLCongestion0": 5})

    def test_saturated_counter_is_unknown(self):
        before = {"PortXmitDiscards": 0, "PortXmitWait": 4294967295}
        after = {"PortXmitDiscards": 0, "PortXmitWait": 4294967295}
        result = self.evaluate(before, after)
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.reason_code, "LEAF_PORT_COUNTER_SATURATED")
        self.assertEqual(result.saturated_counters, ["PortXmitWait"])

    def test_each_standard_counter_uses_its_real_pma_width(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["SymbolErrorCounter"] = 0xFFFF
        after["SymbolErrorCounter"] = 0xFFFF
        result = evaluate_fabric_counter_delta(
            before, after, route=route(), sample_interval_seconds=5
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.reason_code, "LEAF_PORT_COUNTER_SATURATED")
        self.assertEqual(result.saturated_counters, ["SymbolErrorCounter"])

    def test_counter_reset_or_wrap_is_unknown(self):
        before = {"PortRcvErrors": 7, "PortXmitWait": 3}
        after = {"PortRcvErrors": 1, "PortXmitWait": 3}
        result = self.evaluate(before, after)
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.reset_or_wrapped_counters, ["PortRcvErrors"])

    def test_missing_second_sample_is_unknown(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        after.pop("PortXmitWait")
        result = evaluate_fabric_counter_delta(
            before, after, route=route(), sample_interval_seconds=5
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.missing_counters, ["PortXmitWait"])

    def test_missing_any_core_error_counter_is_unknown(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before.pop("PortRcvRemotePhysicalErrors")
        after.pop("PortRcvRemotePhysicalErrors")
        result = evaluate_fabric_counter_delta(
            before, after, route=route(), sample_interval_seconds=5
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("PortRcvRemotePhysicalErrors", result.missing_counters)

    def test_proven_failure_wins_over_saturated_congestion(self):
        before = {"PortXmitDiscards": 0, "PortXmitWait": 4294967295}
        after = {"PortXmitDiscards": 1, "PortXmitWait": 4294967295}
        result = self.evaluate(before, after)
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.error_drop_status, "FAIL")

    def test_saturated_standard_xmit_wait_uses_valid_extended_fallback(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["PortXmitWait"] = 0xFFFFFFFF
        after["PortXmitWait"] = 0xFFFFFFFF
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={"PortXmitWait": 9_000_000_000},
            extended_after={"PortXmitWait": 9_000_000_000},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.error_drop_status, "PASS")
        self.assertEqual(result.congestion_status, "PASS")
        self.assertEqual(result.effective_xmit_wait_source, "PortCountersExtended")
        self.assertEqual(
            result.standard_counter_evidence.width_bits["PortXmitWait"], 32
        )
        self.assertEqual(
            result.extended_xmit_wait_evidence.width_bits["PortXmitWait"], 64
        )
        self.assertTrue(
            result.extended_xmit_wait_evidence.used_for_congestion_verdict
        )

    def test_extended_value_equal_to_32_bit_max_is_not_guessed_as_saturated(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["PortXmitWait"] = 0xFFFFFFFF
        after["PortXmitWait"] = 0xFFFFFFFF
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={"PortXmitWait": 0xFFFFFFFF},
            extended_after={"PortXmitWait": 0x100000000},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "WARN")
        self.assertEqual(result.congestion_deltas, {"PortXmitWait": 1})
        self.assertNotIn(
            "PortXmitWait[PortCountersExtended]", result.saturated_counters
        )

    def test_extended_64_bit_saturation_remains_unknown(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["PortXmitWait"] = 0xFFFFFFFF
        after["PortXmitWait"] = 0xFFFFFFFF
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={"PortXmitWait": 0xFFFFFFFFFFFFFFFF},
            extended_after={"PortXmitWait": 0xFFFFFFFFFFFFFFFF},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(
            result.congestion_reason_code, "EXTENDED_XMIT_WAIT_SATURATED"
        )
        self.assertIn(
            "PortXmitWait[PortCountersExtended]", result.saturated_counters
        )

    def test_extended_value_outside_attribute_width_is_unknown(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["PortXmitWait"] = 0xFFFFFFFF
        after["PortXmitWait"] = 0xFFFFFFFF
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={"PortXmitWait": 0x10000000000000000},
            extended_after={"PortXmitWait": 0x10000000000000000},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(
            result.congestion_reason_code,
            "EXTENDED_XMIT_WAIT_OUT_OF_RANGE",
        )

    def test_missing_extended_field_cannot_be_replaced_by_stable_per_vl_counter(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before.update({"PortXmitWait": 0xFFFFFFFF, "SWPortVLCongestion0": 7})
        after.update({"PortXmitWait": 0xFFFFFFFF, "SWPortVLCongestion0": 7})
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={},
            extended_after={},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(
            result.congestion_reason_code, "EXTENDED_XMIT_WAIT_NOT_EXPOSED"
        )
        self.assertEqual(result.effective_xmit_wait_source, "NONE")

    def test_per_vl_growth_is_preserved_but_cannot_hide_unknown_fallback(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before.update({"PortXmitWait": 0xFFFFFFFF, "SWPortVLCongestion0": 7})
        after.update({"PortXmitWait": 0xFFFFFFFF, "SWPortVLCongestion0": 8})
        result = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={},
            extended_after={},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.congestion_deltas, {"SWPortVLCongestion0": 1})

    def test_extended_decrease_or_query_failure_remains_unknown(self):
        before = {name: 0 for name in REQUIRED_STANDARD_COUNTERS}
        after = dict(before)
        before["PortXmitWait"] = 0xFFFFFFFF
        after["PortXmitWait"] = 0xFFFFFFFF
        decreased = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={"PortXmitWait": 100},
            extended_after={"PortXmitWait": 99},
            extended_before_query_status="COMPLETE",
            extended_after_query_status="COMPLETE",
        )
        failed = evaluate_fabric_counter_delta(
            before,
            after,
            route=route(),
            sample_interval_seconds=5,
            extended_before={},
            extended_after={},
            extended_before_query_status="QUERY_FAILED",
            extended_after_query_status="QUERY_FAILED",
        )
        self.assertEqual(decreased.status, "UNKNOWN")
        self.assertEqual(
            decreased.congestion_reason_code,
            "EXTENDED_XMIT_WAIT_RESET_OR_WRAPPED",
        )
        self.assertEqual(failed.status, "UNKNOWN")
        self.assertEqual(
            failed.congestion_reason_code, "EXTENDED_XMIT_WAIT_QUERY_FAILED"
        )

    def test_valid_standard_xmit_wait_does_not_depend_on_optional_fallback(self):
        result = self.evaluate({"PortXmitWait": 12}, {"PortXmitWait": 12})
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.effective_xmit_wait_source, "PortCounters")
        self.assertEqual(
            result.extended_xmit_wait_evidence.reason_code,
            "EXTENDED_XMIT_WAIT_NOT_EXECUTED",
        )


class FabricRunnerTests(unittest.TestCase):

    def make_runner(self, workload, sleeps=None):
        return SlurmIBFabricRunner(
            runner=workload,
            sleeper=(sleeps.append if sleeps is not None else (lambda _: None)),
            allocation_inspector=lambda _: allocation(),
        )

    @staticmethod

    def stable_workload(shared_leaf=False):
        calls = []
        counts = {}

        def runner(argv, **kwargs):
            calls.append(list(argv))
            if "smpquery" in {Path(value).name for value in argv}:
                return subprocess.CompletedProcess(
                    argv, 0, directed_smp_output(argv, shared_leaf=shared_leaf), ""
                )
            if "perfquery" in argv:
                key = (
                    argv[-2],
                    argv[-1],
                    "--swportvlcong" in argv,
                    "-x" in argv,
                )
                counts[key] = counts.get(key, 0) + 1
                output = (
                    congestion_counters()
                    if key[2]
                    else extended_counters()
                    if key[3]
                    else standard_counters()
                )
                return subprocess.CompletedProcess(argv, 0, output, "")
            raise AssertionError(f"unexpected command: {argv}")

        return runner, calls

    def test_default_srun_step_is_exclusive_and_unsafe_is_explicit(self):
        workload, _ = self.stable_workload()
        runner = self.make_runner(workload)
        safe = runner.build_link_command(
            context(), fabric_config(), node="e06r1n08", hca="shca_0"
        )
        self.assertIn("--exclusive", safe)
        self.assertIn("--exact", safe)
        self.assertIn("--immediate=1", safe)
        self.assertNotIn("--overlap", safe)

        unsafe = runner.build_link_command(
            context(unsafe_allow_overlap=True),
            fabric_config(),
            node="e06r1n08",
            hca="shca_0",
        )
        self.assertIn("--overlap", unsafe)
        self.assertNotIn("--exclusive", unsafe)

    def test_unsafe_overlap_can_never_receive_formal_pass(self):
        workload, calls = self.stable_workload()
        result = self.make_runner(workload).run(
            context(unsafe_allow_overlap=True),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "OVERLAP_NOT_PROVEN_IDLE")
        self.assertEqual(result.safety_boundary, "OVERLAP_NOT_PROVEN_IDLE")
        self.assertTrue(all("--overlap" in argv for argv in calls))

    def test_global_deadline_stops_queued_queries_without_running_workload(self):
        ticks = iter((0.0, 2.0, 2.0))
        calls = []

        def forbidden(argv, **kwargs):
            calls.append(argv)
            raise AssertionError("deadline-expired workload must not start")

        runner = SlurmIBFabricRunner(
            runner=forbidden,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks),
            allocation_inspector=lambda _: allocation(),
        )
        result = runner.run(
            context(),
            fabric_config(
                hcas=("shca_0",),
                max_workers=1,
                overall_timeout_seconds=1,
            ),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(calls, [])
        self.assertEqual(
            {issue.reason_code for issue in result.issues},
            {"FABRIC_GLOBAL_DEADLINE_EXCEEDED"},
        )
        self.assertTrue(all(command.deadline_exceeded for command in result.commands))

    def test_all_queries_use_srun_and_only_direct_one_hop(self):
        workload, calls = self.stable_workload()
        result = self.make_runner(workload).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(len(result.adjacency_links), 2)
        self.assertEqual(len(result.counter_health), 2)
        self.assertEqual(len(calls), 20)  # 2 endpoints * 4 SMPs + 2 ports * 6 counter reads
        self.assertTrue(all(Path(argv[0]).name == "srun" for argv in calls))
        self.assertTrue(all("ibnetdiscover" not in argv for argv in calls))
        smp_commands = [
            argv for argv in calls
            if "smpquery" in {Path(value).name for value in argv}
        ]
        self.assertEqual(len(smp_commands), 8)
        expected_suffixes = {
            ("smpquery", "-x", "-C", "shca_0", "-P", "1", "-D", "portinfo", "0", "1"),
            ("smpquery", "-C", "shca_0", "-P", "1", "-D", "nodeinfo", "0,1"),
            ("smpquery", "-C", "shca_0", "-P", "1", "-D", "nodedesc", "0,1"),
            ("smpquery", "-C", "shca_0", "-P", "1", "-D", "portinfo", "0,1", "0"),
        }
        observed_suffixes = set()
        for argv in smp_commands:
            index = next(
                position for position, value in enumerate(argv)
                if Path(value).name == "smpquery"
            )
            observed_suffixes.add(tuple([Path(argv[index]).name, *argv[index + 1:]]))
            self.assertNotIn("ibnetdiscover", argv)
            self.assertNotIn("iblinkinfo", argv)
        self.assertEqual(observed_suffixes, expected_suffixes)

    def test_extended_counter_reads_are_targeted_and_reset_is_rejected(self):
        workload, calls = self.stable_workload()
        runner = self.make_runner(workload)
        result = runner.run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "PASS")
        extended_calls = [
            argv for argv in calls if "perfquery" in argv and "-x" in argv
        ]
        self.assertEqual(len(extended_calls), 4)
        for argv in extended_calls:
            self.assertNotIn("-r", argv)
            self.assertNotIn("-R", argv)
            self.assertTrue(argv[-2].isdigit())
            self.assertTrue(argv[-1].isdigit())

        call_count = len(calls)
        for reset_option in (
            "-r",
            "-R",
            "--reset_after_read",
            "--reset-after-read",
            "--reset_only",
            "--reset-only",
        ):
            with self.subTest(reset_option=reset_option):
                reset_argv = runner.build_counter_command(
                    context(),
                    fabric_config(),
                    route=route(),
                    per_vl_congestion=False,
                )
                reset_argv.insert(-2, reset_option)
                with self.assertRaises(ActiveCheckSafetyError) as caught:
                    runner._execute(
                        stage="leaf-counters-before",
                        node="e06r1n08",
                        hca="shca_0",
                        argv=reset_argv,
                        config=fabric_config(),
                    )
                self.assertEqual(
                    caught.exception.reason_code, "FABRIC_COUNTER_RESET_REJECTED"
                )
                self.assertEqual(len(calls), call_count)

        positional_mask = runner.build_counter_command(
            context(),
            fabric_config(),
            route=route(),
            per_vl_congestion=False,
        )
        positional_mask.append("0xffff")
        with self.assertRaises(ActiveCheckSafetyError) as caught:
            runner._execute(
                stage="leaf-counters-before",
                node="e06r1n08",
                hca="shca_0",
                argv=positional_mask,
                config=fabric_config(),
            )
        self.assertEqual(
            caught.exception.reason_code, "UNBOUNDED_FABRIC_SCAN_REJECTED"
        )
        self.assertEqual(len(calls), call_count)

    def test_runner_uses_extended_fallback_for_saturated_standard_xmit_wait(self):
        def runner(argv, **kwargs):
            if "smpquery" in {Path(value).name for value in argv}:
                return subprocess.CompletedProcess(
                    argv, 0, directed_smp_output(argv), ""
                )
            if "--swportvlcong" in argv:
                return subprocess.CompletedProcess(argv, 0, congestion_counters(), "")
            if "-x" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, extended_counters(wait=8_000_000_000), ""
                )
            if "perfquery" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, standard_counters(wait=0xFFFFFFFF), ""
                )
            raise AssertionError(f"unexpected command: {argv}")

        result = self.make_runner(runner).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "PASS")
        self.assertTrue(result.counter_health)
        self.assertTrue(
            all(
                item.effective_xmit_wait_source == "PortCountersExtended"
                for item in result.counter_health
            )
        )
        self.assertFalse(
            any(
                issue.reason_code.startswith("EXTENDED_XMIT_WAIT")
                for issue in result.issues
            )
        )

    def test_runner_keeps_unknown_when_extended_field_is_missing(self):
        def runner(argv, **kwargs):
            if "smpquery" in {Path(value).name for value in argv}:
                return subprocess.CompletedProcess(
                    argv, 0, directed_smp_output(argv), ""
                )
            if "--swportvlcong" in argv:
                return subprocess.CompletedProcess(argv, 0, congestion_counters(), "")
            if "-x" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "# Port extended counters: Lid 7237 port 69\n"
                    "PortXmitData:....................1\n",
                    "",
                )
            if "perfquery" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, standard_counters(wait=0xFFFFFFFF), ""
                )
            raise AssertionError(f"unexpected command: {argv}")

        result = self.make_runner(runner).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertTrue(
            all(item.congestion_status == "UNKNOWN" for item in result.counter_health)
        )
        self.assertIn(
            "EXTENDED_XMIT_WAIT_NOT_EXPOSED",
            {issue.reason_code for issue in result.issues},
        )

    def test_runner_keeps_unknown_when_extended_query_fails(self):
        def runner(argv, **kwargs):
            if "smpquery" in {Path(value).name for value in argv}:
                return subprocess.CompletedProcess(
                    argv, 0, directed_smp_output(argv), ""
                )
            if "--swportvlcong" in argv:
                return subprocess.CompletedProcess(argv, 0, congestion_counters(), "")
            if "-x" in argv:
                return subprocess.CompletedProcess(
                    argv, 1, "", "extended attribute unsupported"
                )
            if "perfquery" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, standard_counters(wait=0xFFFFFFFF), ""
                )
            raise AssertionError(f"unexpected command: {argv}")

        result = self.make_runner(runner).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertIn(
            "EXTENDED_XMIT_WAIT_QUERY_FAILED",
            {issue.reason_code for issue in result.issues},
        )

    def test_exclusive_srun_is_serial_per_node_but_overlaps_across_nodes(self):
        base_workload, _ = self.stable_workload()
        state_lock = threading.Lock()
        first_nodes: set[str] = set()
        first_node_barrier = threading.Barrier(2)
        active_by_node: dict[str, int] = {}
        peak_by_node: dict[str, int] = {}
        counter_peak_by_node: dict[str, int] = {}
        global_active = 0
        global_peak = 0

        def workload(argv, **kwargs):
            nonlocal global_active, global_peak
            node = next(
                value.split("=", 1)[1]
                for value in argv
                if value.startswith("--nodelist=")
            )
            is_counter = "perfquery" in argv
            with state_lock:
                active_by_node[node] = active_by_node.get(node, 0) + 1
                peak_by_node[node] = max(
                    peak_by_node.get(node, 0), active_by_node[node]
                )
                if is_counter:
                    counter_peak_by_node[node] = max(
                        counter_peak_by_node.get(node, 0), active_by_node[node]
                    )
                global_active += 1
                global_peak = max(global_peak, global_active)
                first_for_node = node not in first_nodes
                first_nodes.add(node)
            try:
                if first_for_node:
                    first_node_barrier.wait(timeout=5)
                time.sleep(0.002)
                return base_workload(argv, **kwargs)
            finally:
                with state_lock:
                    active_by_node[node] -= 1
                    global_active -= 1

        result = self.make_runner(workload).run(
            context(),
            fabric_config(
                hcas=("shca_0", "shca_1"),
                max_workers=8,
                query_qps=20,
                sample_interval_seconds=0,
            ),
        )

        self.assertEqual(result.status, "PASS")
        self.assertEqual(set(peak_by_node), {"e06r1n08", "e06r1n09"})
        self.assertTrue(all(value <= 1 for value in peak_by_node.values()))
        self.assertTrue(counter_peak_by_node)
        self.assertTrue(all(value <= 1 for value in counter_peak_by_node.values()))
        self.assertGreaterEqual(global_peak, 2)

    def test_node_slot_wait_stops_at_global_deadline(self):
        calls = []

        def forbidden(argv, **kwargs):
            calls.append(argv)
            raise AssertionError("held node slot must prevent workload execution")

        runner = SlurmIBFabricRunner(
            runner=forbidden,
            sleeper=lambda _: None,
            monotonic=time.monotonic,
            allocation_inspector=lambda _: allocation(),
        )
        config = fabric_config(command_timeout_seconds=1)
        argv = runner.build_link_command(
            context(), config, node="e06r1n08", hca="shca_0"
        )
        node_lock = runner._node_execution_lock("e06r1n08")
        node_lock.acquire()
        runner._deadline_monotonic = time.monotonic() + 0.05
        started = time.monotonic()
        try:
            evidence = runner._execute(
                stage="local-port-info",
                node="e06r1n08",
                hca="shca_0",
                argv=argv,
                config=config,
            )
        finally:
            node_lock.release()
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.5)
        self.assertTrue(evidence.deadline_exceeded)
        self.assertEqual(evidence.returncode, 124)
        self.assertEqual(calls, [])
    def test_unlisted_smpquery_target_is_rejected_before_execution(self):
        workload, calls = self.stable_workload()
        runner = self.make_runner(workload)
        with self.assertRaises(ActiveCheckSafetyError) as caught:
            runner._execute(
                stage="one-hop-adjacency",
                node="e06r1n08",
                hca="shca_0",
                argv=[
                    "srun", "smpquery", "-C", "shca_0", "-P", "1",
                    "nodeinfo", "1",
                ],
                config=fabric_config(),
            )
        self.assertEqual(caught.exception.reason_code, "UNBOUNDED_FABRIC_SCAN_REJECTED")
        self.assertEqual(calls, [])
    def test_duplicate_leaf_port_is_queried_once(self):
        workload, calls = self.stable_workload(shared_leaf=True)
        result = self.make_runner(workload).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(len(result.adjacency_links), 2)
        self.assertEqual(len(result.counter_health), 1)
        self.assertEqual(len([argv for argv in calls if "perfquery" in argv]), 6)

    def test_explicit_container_uses_only_docker_exec(self):
        workload, calls = self.stable_workload()
        result = self.make_runner(workload).run(
            context(),
            fabric_config(
                hcas=("shca_0",),
                container_name="zytest",
                sample_interval_seconds=0,
            ),
        )
        self.assertEqual(result.status, "PASS")
        for argv in calls:
            self.assertIn("docker", argv)
            docker_index = argv.index("docker")
            self.assertEqual(argv[docker_index : docker_index + 3], ["docker", "exec", "zytest"])
            self.assertNotIn("create", argv)
            self.assertNotIn("start", argv)

    def test_optional_congestion_failure_cannot_hide_standard_counter_failure(self):
        standard_calls = 0

        def runner(argv, **kwargs):
            nonlocal standard_calls
            if "smpquery" in {Path(value).name for value in argv}:
                return subprocess.CompletedProcess(argv, 0, directed_smp_output(argv), "")
            if "--swportvlcong" in argv:
                return subprocess.CompletedProcess(argv, 1, "", "unsupported optional counter")
            if "-x" in argv:
                return subprocess.CompletedProcess(argv, 0, extended_counters(), "")
            if "perfquery" in argv:
                standard_calls += 1
                output = standard_counters(link_down=0 if standard_calls == 1 else 1)
                return subprocess.CompletedProcess(argv, 0, output, "")
            raise AssertionError(f"unexpected command: {argv}")

        result = self.make_runner(runner).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "IB_ONE_HOP_FABRIC_FAILURE")
        self.assertEqual(result.counter_health[0].failure_deltas, {"LinkDownedCounter": 1})
        self.assertTrue(any(issue.stage == "leaf-congestion-before" for issue in result.issues))

    def test_umad_permission_failure_is_explicit(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(
                argv,
                1,
                "",
                "mad_rpc_open_port: can't open UMAD port (shca_0:1): Permission denied",
            )

        result = self.make_runner(runner).run(
            context(), fabric_config(hcas=("shca_0",))
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual({issue.reason_code for issue in result.issues}, {"UMAD_PERMISSION_DENIED"})
        self.assertEqual(result.counter_health, [])

    def test_missing_tool_is_explicit(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(
                argv, 127, "", "execve(): smpquery: No such file or directory"
            )

        result = self.make_runner(runner).run(
            context(), fabric_config(hcas=("shca_0",))
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual({issue.reason_code for issue in result.issues}, {"FABRIC_TOOL_MISSING"})

    def test_docker_daemon_permission_is_not_mislabeled_as_umad(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(
                argv,
                1,
                "",
                "permission denied while trying to connect to the Docker daemon",
            )

        result = self.make_runner(runner).run(
            context(),
            fabric_config(hcas=("shca_0",), container_name="zytest"),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            {issue.reason_code for issue in result.issues},
            {"FABRIC_CONTAINER_UNAVAILABLE"},
        )

    def test_disabled_starts_no_slurm_or_workload_query(self):
        calls = []

        def forbidden(*args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError("disabled check must perform no query")

        result = SlurmIBFabricRunner(
            runner=forbidden,
            allocation_inspector=forbidden,
        ).run(context(enabled=False), fabric_config(hcas=("shca_0",)))
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "FABRIC_CHECK_DISABLED")
        self.assertEqual(calls, [])

    def test_allocation_safety_error_blocks_every_srun(self):
        calls = []

        def workload(argv, **kwargs):
            calls.append(argv)
            raise AssertionError("workload must not start")

        def inspector(_):
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_HAS_ACTIVE_STEPS", "training is still running"
            )

        result = SlurmIBFabricRunner(
            runner=workload, allocation_inspector=inspector
        ).run(context(), fabric_config(hcas=("shca_0",)))
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_ALLOCATION_HAS_ACTIVE_STEPS")
        self.assertEqual(calls, [])

    def test_hca_limit_blocks_before_query(self):
        workload, calls = self.stable_workload()
        result = self.make_runner(workload).run(
            context(),
            fabric_config(
                hcas=("shca_0", "shca_1"), max_hcas_per_node=1
            ),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "INVALID_FABRIC_CHECK_CONFIGURATION")
        self.assertEqual(calls, [])

    def test_qps_and_sample_sleeps_are_applied(self):
        workload, _ = self.stable_workload()
        sleeps = []
        result = self.make_runner(workload, sleeps=sleeps).run(
            context(),
            fabric_config(
                hcas=("shca_0",), sample_interval_seconds=5, query_qps=2
            ),
        )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(sleeps.count(5), len(result.counter_health))
        query_delays = [delay for delay in sleeps if 0 < delay <= 0.5]
        self.assertEqual(len(query_delays), len(result.commands) - 1)

    def test_default_popen_streams_and_bounds_ten_megabytes_per_pipe(self):
        total_bytes = 10 * 1024 * 1024

        class FakeProcess:
            def __init__(self):
                self.stdout = StreamingPipe(total_bytes, b"H", b"T")
                self.stderr = StreamingPipe(total_bytes, b"E", b"Z")
                self.returncode = None

            def wait(self, timeout=None):
                self.returncode = 0
                return 0

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        runner = SlurmIBFabricRunner(
            popen=lambda argv, **kwargs: FakeProcess(),
            sleeper=lambda _: None,
            allocation_inspector=lambda _: allocation(),
        )
        evidence = runner._execute(
            stage="one-hop-adjacency",
            node="e06r1n08",
            hca="shca_0",
            argv=[
                "srun", "smpquery", "-x", "-C", "shca_0", "-P", "1",
                "-D", "portinfo", "0", "1",
            ],
            config=fabric_config(command_timeout_seconds=1),
        )

        self.assertEqual(evidence.returncode, 0)
        self.assertFalse(evidence.timed_out)
        self.assertEqual(evidence.stdout_total_bytes, total_bytes)
        self.assertEqual(evidence.stderr_total_bytes, total_bytes)
        self.assertTrue(evidence.stdout_truncated)
        self.assertTrue(evidence.stderr_truncated)
        self.assertLess(len(evidence.stdout.encode("utf-8")), 70000)
        self.assertLess(len(evidence.stderr.encode("utf-8")), 70000)
        self.assertTrue(evidence.stdout.startswith("H"))
        self.assertTrue(evidence.stdout.endswith("T"))
        self.assertTrue(evidence.stderr.startswith("E"))
        self.assertTrue(evidence.stderr.endswith("Z"))
        self.assertIn("omitted", evidence.stdout)

    def test_max_workers_stream_large_outputs_without_retaining_full_pipes(self):
        nodes = tuple(f"node{index:02d}" for index in range(64))
        per_pipe_bytes = 256 * 1024
        gate = threading.Event()
        created_lock = threading.Lock()
        created = 0

        class FakeProcess:
            def __init__(self):
                self.stdout = StreamingPipe(per_pipe_bytes, b"X", b"Y")
                self.stderr = StreamingPipe(per_pipe_bytes, b"E", b"Z")
                self.returncode = None

            def wait(self, timeout=None):
                if not gate.wait(10):
                    raise RuntimeError("64-worker process gate was not reached")
                self.returncode = 0
                return 0

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        def popen(argv, **kwargs):
            nonlocal created
            process = FakeProcess()
            with created_lock:
                created += 1
                if created == len(nodes):
                    gate.set()
            return process

        runner = SlurmIBFabricRunner(
            popen=popen,
            sleeper=lambda _: None,
            monotonic=lambda: 0.0,
            allocation_inspector=lambda _: SlurmAllocation(
                job_id="674118",
                owner="qianyj1",
                state="RUNNING",
                nodes=nodes,
                active_steps=(),
                exclusive_mode="NODE",
                foreign_active_job_ids=(),
                node_exclusivity_proven=True,
            ),
        )
        result = runner.run(
            context(selected_nodes=nodes, max_selected_nodes=64),
            fabric_config(
                hcas=("shca_0",),
                max_nodes=64,
                max_workers=64,
                query_qps=20,
                sample_interval_seconds=0,
            ),
        )

        self.assertEqual(created, 256)
        self.assertEqual(len(result.commands), 256)
        self.assertTrue(all(item.stdout_truncated for item in result.commands))
        self.assertTrue(all(item.stderr_truncated for item in result.commands))
        self.assertTrue(
            all(item.stdout_total_bytes == per_pipe_bytes for item in result.commands)
        )
        self.assertTrue(
            all(len(item.stdout.encode("utf-8")) < 70000 for item in result.commands)
        )
        self.assertNotEqual(result.status, "PASS")

    def test_default_popen_timeout_terminates_then_kills(self):
        class TimeoutProcess:
            def __init__(self):
                self.stdout = StreamingPipe(1024)
                self.stderr = StreamingPipe(1024, b"E", b"Z")
                self.returncode = None
                self.wait_calls = 0
                self.terminated = False
                self.killed = False

            def wait(self, timeout=None):
                self.wait_calls += 1
                if self.wait_calls <= 2:
                    raise subprocess.TimeoutExpired(["srun"], timeout)
                self.returncode = -9
                return -9

            def terminate(self):
                self.terminated = True

            def kill(self):
                self.killed = True

        process = TimeoutProcess()
        runner = SlurmIBFabricRunner(
            popen=lambda argv, **kwargs: process,
            sleeper=lambda _: None,
            allocation_inspector=lambda _: allocation(),
        )
        evidence = runner._execute(
            stage="one-hop-adjacency",
            node="e06r1n08",
            hca="shca_0",
            argv=[
                "srun", "smpquery", "-x", "-C", "shca_0", "-P", "1",
                "-D", "portinfo", "0", "1",
            ],
            config=fabric_config(command_timeout_seconds=1),
        )

        self.assertTrue(evidence.timed_out)
        self.assertEqual(evidence.returncode, 124)
        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertEqual(process.wait_calls, 3)
    def test_truncated_command_output_suppresses_pass(self):
        stable_workload, _ = self.stable_workload()

        def oversized_workload(argv, **kwargs):
            completed = stable_workload(argv, **kwargs)
            return subprocess.CompletedProcess(
                argv,
                completed.returncode,
                f"{completed.stdout}{'X' * 70000}",
                completed.stderr,
            )

        result = self.make_runner(oversized_workload).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "FABRIC_COMMAND_OUTPUT_TRUNCATED")
        self.assertTrue(result.commands)
        self.assertTrue(all(command.stdout_truncated for command in result.commands))
        self.assertTrue(
            all(command.stdout_total_bytes > 65536 for command in result.commands)
        )

    def test_json_and_markdown_keep_switch_policy_not_verified(self):
        workload, _ = self.stable_workload(shared_leaf=True)
        result = self.make_runner(workload).run(
            context(),
            fabric_config(hcas=("shca_0",), sample_interval_seconds=0),
        )
        published = {}

        def capture(path, content):
            published[path.name] = content

        output = Path("fabric-output")
        with patch("hcu_envcheck.ib_fabric.claim_output_directory") as claim, patch(
            "hcu_envcheck.ib_fabric.atomic_write_text_exclusive",
            side_effect=capture,
        ):
            json_path, markdown_path = write_ib_fabric_reports(result, output)
        claim.assert_called_once_with(output)
        self.assertEqual(json_path, output / "ib-fabric-result.json")
        self.assertEqual(markdown_path, output / "ib-fabric-summary.md")
        payload = json.loads(published["ib-fabric-result.json"])
        summary = published["ib-fabric-summary.md"]
        self.assertEqual(payload["status"], "PASS")
        self.assertEqual(
            payload["switch_configuration_policy"]["status"], "NOT_VERIFIED"
        )
        self.assertIn("## One-hop adjacency", summary)
        self.assertIn("## Leaf-port counter health", summary)
        self.assertIn("## Counter source evidence", summary)
        self.assertIn("7237/69", summary)
        counter = payload["counter_health"][0]
        self.assertEqual(
            counter["standard_counter_evidence"]["attribute"], "PortCounters"
        )
        self.assertEqual(
            counter["extended_xmit_wait_evidence"]["attribute"],
            "PortCountersExtended",
        )
        self.assertEqual(
            counter["extended_xmit_wait_evidence"]["width_bits"]["PortXmitWait"],
            64,
        )
        self.assertIn("SWITCH_MANAGEMENT_EVIDENCE_NOT_PROVIDED", summary)


class FabricCliTests(unittest.TestCase):
    @staticmethod

    def args(**overrides):
        values = {
            "node": ["e06r1n08", "e06r1n09"],
            "nodes_file": None,
            "max_nodes": 64,
            "slurm_job_id": "674118",
            "hca": ["shca_0"],
            "ib_port": 1,
            "container_name": "zytest",
            "enable_fabric_check": True,
            "confirm_allocation_idle": True,
            "unsafe_allow_overlap": False,
            "control_timeout": 20.0,
            "counter_interval": 5.0,
            "command_timeout": 15.0,
            "query_qps": 2.0,
            "max_workers": 16,
            "overall_timeout": 900.0,
            "max_hcas_per_node": 16,
            "max_unique_leaf_ports": 512,
            "expected_link_width": "4X",
            "minimum_link_speed_gbps": 100.0,
            "output_dir": Path("fabric-cli-output"),
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    @staticmethod

    def result(status):
        return IBFabricCheckResult(
            status=status,
            reason_code=f"FABRIC_{status}",
            message="test result",
            job_id="674118",
            nodes=["e06r1n08", "e06r1n09"],
            started_at="2026-07-20T00:00:00+00:00",
            finished_at="2026-07-20T00:00:01+00:00",
            sample_interval_seconds=5,
            safety_boundary="EXCLUSIVE_SLURM_ALLOCATION_AND_STEP",
        )

    def test_cli_claims_output_before_first_fabric_query(self):
        order = []
        fake_runner = Mock()
        fake_runner.run.side_effect = lambda *_: (order.append("run") or self.result("PASS"))
        with patch(
            "hcu_envcheck.cli.claim_output_directory",
            side_effect=lambda *_: order.append("claim"),
        ), patch(
            "hcu_envcheck.cli.SlurmIBFabricRunner", return_value=fake_runner
        ), patch(
            "hcu_envcheck.cli.write_ib_fabric_reports",
            side_effect=lambda *args, **kwargs: (
                order.append("write") or (Path("result.json"), Path("summary.md"))
            ),
        ) as writer:
            returncode = _run_ib_fabric_slurm(self.args(), build_parser())
        self.assertEqual(returncode, 0)
        self.assertEqual(order, ["claim", "run", "write"])
        active_context, _ = fake_runner.run.call_args.args
        self.assertEqual(active_context.max_selected_nodes, 64)
        self.assertTrue(writer.call_args.kwargs["output_dir_claimed"])

    def test_cli_status_exit_codes_are_strict(self):
        expected = {"PASS": 0, "WARN": 1, "FAIL": 1, "NOT_VERIFIED": 2}
        for status, returncode in expected.items():
            with self.subTest(status=status), patch(
                "hcu_envcheck.cli.claim_output_directory"
            ), patch("hcu_envcheck.cli.SlurmIBFabricRunner") as runner_type, patch(
                "hcu_envcheck.cli.write_ib_fabric_reports",
                return_value=(Path("result.json"), Path("summary.md")),
            ):
                runner_type.return_value.run.return_value = self.result(status)
                self.assertEqual(
                    _run_ib_fabric_slurm(self.args(), build_parser()), returncode
                )

    def test_invalid_cli_interval_blocks_before_output_or_srun(self):
        with patch("hcu_envcheck.cli.claim_output_directory") as claim, patch(
            "hcu_envcheck.cli.SlurmIBFabricRunner"
        ) as runner_type:
            returncode = _run_ib_fabric_slurm(
                self.args(counter_interval=0), build_parser()
            )
        self.assertEqual(returncode, 3)
        claim.assert_not_called()
        runner_type.return_value.run.assert_not_called()

    def test_output_conflict_blocks_before_srun(self):
        with patch(
            "hcu_envcheck.cli.claim_output_directory",
            side_effect=ValueError("output directory already exists"),
        ), patch("hcu_envcheck.cli.SlurmIBFabricRunner") as runner_type:
            returncode = _run_ib_fabric_slurm(self.args(), build_parser())
        self.assertEqual(returncode, 3)
        runner_type.return_value.run.assert_not_called()

    def test_main_dispatches_without_static_only_arguments(self):
        argv = [
            "ib-fabric-slurm",
            "--slurm-job-id",
            "674118",
            "--node",
            "e06r1n08",
            "--node",
            "e06r1n09",
            "--hca",
            "shca_0",
            "--enable-fabric-check",
            "--confirm-allocation-idle",
            "--output-dir",
            "fabric-output",
        ]
        with patch("hcu_envcheck.cli._run_ib_fabric_slurm", return_value=2) as run:
            self.assertEqual(main(argv), 2)
        run.assert_called_once()

if __name__ == "__main__":
    unittest.main()
