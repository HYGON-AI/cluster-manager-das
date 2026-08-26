# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import pytest

from cluster_manager.monitor.log_parser import create_log_parser


def test_base_selects_megatron_parser():
    parser = create_log_parser("base")

    assert parser.line_parser.__module__ == "cluster_manager.monitor.mega.analyze_mega_log"


def test_special_selects_special_parser():
    parser = create_log_parser("special")

    assert parser.line_parser.__module__ == (
        "cluster_manager.monitor.special.analyze_special_log"
    )
    result = parser("epoch 1: 2 / 10 loss=1.25 num_updates=2")
    assert result["type"] == "log"
    assert result["current_iter"] == 2
    assert result["total_iter"] == 10


def test_unknown_log_parser_type_is_rejected():
    with pytest.raises(ValueError, match="expected 'base' or 'special'"):
        create_log_parser("generic")


def test_base_parses_prte_process_exit_with_signal():
    parser = create_log_parser("base")

    result = parser(
        "   Process name: [prterun-m09r2n09-80749@1,2] Exit code:    137"
    )

    assert result["type"] == "exit"
    assert result["data"] == {
        "type": "proc",
        "fault_pid": "[prterun-m09r2n09-80749@1,2]",
        "exit_code": 137,
        "runtime": "prte",
        "signal": 9,
    }


def test_base_keeps_legacy_openmpi_process_name_support():
    parser = create_log_parser("base")

    result = parser("  Process name: [[4871,1],683]")

    assert result["type"] == "exit"
    assert result["data"]["type"] == "rank"
    assert result["data"]["fault_pid"] == "[[4871,1],683]"
