# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import pytest

from cluster_manager.runtime.run_state_manager import RunState
from cluster_manager.runtime.runtime_context import RetryInfo, RuntimeContext


def test_retry_info_tracks_updates_and_reset():
    retry = RetryInfo()
    retry.increment("t1")
    retry.increment("t2")
    assert retry.to_dict() == {"retry_count": 2, "last_retry_time": "t2"}

    retry.update(count=7, timestamp="t3")
    assert retry.retry_count == 7
    assert retry.last_retry_time == "t3"
    retry.reset()
    assert retry.to_dict() == {"retry_count": 0, "last_retry_time": None}


def test_runtime_context_builds_topology_and_persistent_config(monkeypatch):
    validator = MagicMock()
    validator.get_world_size.return_value = 16
    validator.calc_dp.return_value = 4
    validator.calc_edp.return_value = 2
    monkeypatch.setattr(
        "cluster_manager.runtime.runtime_context.ParallelTopologyValidator", validator
    )
    monkeypatch.setattr(
        "cluster_manager.runtime.runtime_context.get_train_config",
        lambda: {
            "--tensor-model-parallel-size": 2,
            "--pipeline-model-parallel-size": 2,
            "--context-parallel-size": 1,
            "--expert-model-parallel-size": 2,
            "--expert-tensor-parallel-size": 1,
        },
    )
    monkeypatch.setattr(
        "cluster_manager.runtime.runtime_context.global_config.PERSISTENT_KEYS",
        ["--world-size", "--tensor-model-parallel-size"],
    )
    state_manager = MagicMock()
    state_manager.get_state.return_value = RunState.PENDING
    monkeypatch.setattr(
        "cluster_manager.runtime.runtime_context.RunStateManager",
        MagicMock(return_value=state_manager),
    )

    ctx = RuntimeContext(
        {"job_name": "job", "required_nodes_num": 2, "slots_per_node": 8},
        MagicMock(),
    )

    assert ctx.run_state is RunState.PENDING
    assert ctx.train_cfg_cache["runtime_config"] == {
        "job_name": "job",
        "required_nodes_num": 2,
        "slots_per_node": 8,
    }
    assert ctx.train_cfg_cache["megatron_config"] == {
        "--world-size": 16,
        "--tensor-model-parallel-size": 2,
        "--dp": 4,
        "--edp": 2,
    }
    validator.validate.assert_called_once()


@pytest.mark.parametrize(
    ("text", "rank"),
    [
        ("[prterun-a05r3n07-802797@1,76]", 76),
        ("Process name: [[24917,1],18]", 18),
    ],
)
def test_parse_rank_supports_mpi_formats(text, rank):
    ctx = object.__new__(RuntimeContext)
    assert ctx._parse_rank_from_fault(text) == rank


@pytest.mark.parametrize("text", ["missing comma", "@broken", "name,12"])
def test_parse_rank_rejects_malformed_values(text):
    ctx = object.__new__(RuntimeContext)
    with pytest.raises((ValueError, IndexError)):
        ctx._parse_rank_from_fault(text)


def make_fault_context(slots=8):
    ctx = object.__new__(RuntimeContext)
    ctx.runtime_args = {"slots_per_node": slots}
    ctx.node_pool_proxy = MagicMock()
    return ctx


def test_global_rank_fault_maps_to_node_number():
    ctx = make_fault_context()
    reason = {"type": "exit"}
    assert ctx.handle_runtime_fault("17", "global_rank", reason)
    ctx.node_pool_proxy.add_fault_nodes_no.assert_called_once_with(
        2, fault_reason=reason
    )


def test_mpi_rank_fault_maps_to_node_number():
    ctx = make_fault_context(slots=4)
    assert ctx.handle_runtime_fault("Process name: [[1,1],9]", "rank")
    ctx.node_pool_proxy.add_fault_nodes_no.assert_called_once_with(
        2, fault_reason=None
    )


def test_node_and_process_fault_paths():
    ctx = make_fault_context()
    reason = {"source": "nhc"}
    assert ctx.handle_runtime_fault("node07", "node", reason)
    ctx.node_pool_proxy.add_fault_nodes.assert_called_once_with(["node07"], reason)
    assert ctx.handle_runtime_fault("pid", "proc")


@pytest.mark.parametrize(
    ("fault_info", "fault_type"),
    [("-1", "global_rank"), ("bad", "global_rank"), ("x", "unknown")],
)
def test_invalid_runtime_fault_returns_false(fault_info, fault_type):
    assert make_fault_context().handle_runtime_fault(fault_info, fault_type) is False


def test_runtime_fault_proxy_exception_is_contained():
    ctx = make_fault_context()
    ctx.node_pool_proxy.add_fault_nodes.side_effect = RuntimeError("storage failed")
    assert ctx.handle_runtime_fault("node01", "node") is False
