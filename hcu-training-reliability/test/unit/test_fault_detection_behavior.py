# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from collections import deque
from unittest.mock import MagicMock

import pytest

from cluster_manager.launcher.fault_detection import FaultDetection


@pytest.mark.parametrize(
    ("log", "expected"),
    [
        ("hcu_xid_76[HCU5]", (2, "76", "5")),
        ("hcu_lose[HCU2]", (2, None, "2")),
        ("ib_pcs_link_down[HCA0]", (1, None, None)),
        ("storage_not_mount[/data]", (3, None, None)),
        ("node01: Connection closed by remote host", (3, None, None)),
        ("node02: ssh: connect to host x No route to host", (3, None, None)),
    ],
)
def test_parse_fault_log_recognizes_fault_families(log, expected):
    result = FaultDetection().parse_fault_log(log)
    assert (result["fault_type"], result["error_code"], result["gpu_id"]) == expected


def test_parse_fault_log_prefers_higher_fault_type_and_handles_empty():
    detector = FaultDetection()
    assert detector.parse_fault_log("")["fault_type"] is None
    result = detector.parse_fault_log(
        "ib_pcs_link_down[HCA0] and node01: Connection closed by remote host"
    )
    assert result["fault_type"] == 3


def clush_block(node, result, counter=None):
    counter_line = f"\nCounter: {counter}" if counter is not None else ""
    return (
        "---------------\n"
        f"{node}\n"
        "---------------\n"
        f"[CHECK RESULT]: {result}{counter_line}\n"
    )


def test_parse_clush_output_accepts_normal_and_degraded_nodes():
    output = (
        clush_block("node[01-02]", "PASSED")
        + clush_block("node03", "ib_pcs_link_down[HCA0]", 19)
        + clush_block("node04", "ib_pcs_link_down[HCA0]", 20)
    )
    assert FaultDetection().parse_clush_output(output) == [
        "node01",
        "node02",
        "node03",
    ]


def test_run_nhc_returns_failed_nodes_and_command_failure(monkeypatch):
    detector = FaultDetection()
    monkeypatch.setattr(
        "cluster_manager.launcher.fault_detection.read_hostfile",
        lambda _: ["node01", "node02"],
    )
    execute = MagicMock(
        side_effect=[(0, clush_block("node01", "PASSED")), (7, "ssh failed")]
    )
    monkeypatch.setattr(
        "cluster_manager.launcher.fault_detection.CmdExecutor.execute_command",
        execute,
    )
    detector.write_run_nhc_error = MagicMock()

    assert detector.run_nhc("hosts") == (["node01"], ["node02"], True)
    assert detector.run_nhc("hosts") == ([], [], False)
    assert detector.write_run_nhc_error.call_count == 2


def test_parse_memory_and_node_outputs():
    detector = FaultDetection()
    memory = (
        "---------------\nnode01\n---------------\n"
        "Mem: 100 20 30 1 40 80\nSwap: 10 2 8\n"
        "---------------\nnode02\n---------------\nMem: malformed\n"
    )
    assert detector._parse_mem_info(memory) == [
        {
            "node": "node01",
            "Mem": {
                "total": 100,
                "used": 20,
                "free": 30,
                "shared": 1,
                "buff/cache": 40,
                "available": 80,
            },
            "Swap": {"total": 10, "used": 2, "free": 8},
        }
    ]
    nodes = "NodeName=node01 State=IDLE\nNodeName=node02 Reason=hardware fault"
    parsed = detector._parse_nodes_output(nodes)
    assert parsed[0]["node"] == "node01"
    assert parsed[1] == {"node": "node02", "reason": "hardware fault"}
    assert detector._check_nodes_output(nodes) == ["node01"]


def test_parse_hcu_output_skips_invalid_cards():
    output = (
        'node01: {"card0": {"HCU memory use (%)": "40", '
        '"Average Graphics Package Power (W)": "210", '
        '"HCU use (%)": "80", "Available memory size (MiB)": "100"}}\n'
        'node02: not-json\n'
    )
    parsed = FaultDetection()._parse_hcu_output(output)
    assert parsed["node01"]["card0"]["vram"] == 40.0
    assert parsed["node01"]["card0"]["power"] == 210.0
    assert "node02" not in parsed


@pytest.mark.parametrize(
    ("output", "threshold", "expected"),
    [
        ("", 1, False),
        ("JOBID NODES ST\n1 4 R", 4, True),
        ("JOBID NODES ST\n1 3 R", 4, False),
        ("JOBID NODES ST\n1 2 R\n2 2 PD", 4, True),
        ("JOBID STATE\n1 R", 1, False),
    ],
)
def test_check_squeue_nodes(output, threshold, expected):
    assert FaultDetection().check_squeue_nodes(output, threshold) is expected


def test_slope_stability_and_card_anomalies():
    detector = FaultDetection()
    assert detector._calc_slope([1]) == 0.0
    assert detector._calc_slope([1, 2, 3]) == pytest.approx(1.0)
    assert detector._is_stable([0, 0])
    assert detector._is_stable([10, 10.1, 9.9])
    assert not detector._is_stable([1])

    anomalies = []
    detector.analyze_node_cards(
        "node01",
        {
            "card0": {"vram": 99, "available": 100},
            "card1": {"vram": 10, "available": 100},
        },
        anomalies,
    )
    kinds = {item["type"] for item in anomalies}
    assert "Single_Card_OOM_RISK" in kinds
    assert "IMBALANCE" in kinds


def test_update_state_detects_stable_idle_card():
    detector = FaultDetection()
    detector._state["node01"]["card0"] = {
        "history": deque([10.0] * 29, maxlen=30),
        "use_history": deque([1.0] * 29, maxlen=30),
        "power_history": deque([5.0] * 29, maxlen=30),
    }
    state, unhealthy, anomalies = detector._update_state_and_detect(
        {
            "node01": {
                "card0": {
                    "vram": 10.0,
                    "power": 5.0,
                    "use": 1.0,
                    "available": 100.0,
                }
            }
        },
        0,
    )
    assert "card0" in state["node01"]
    assert unhealthy == {"node01": ["card0"]}
    assert any(item["type"] == "HANG" for item in anomalies)
