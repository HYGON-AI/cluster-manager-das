# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cluster_manager.event.event_bus import Event, EventType
from cluster_manager.runtime.job_state_machine import JobCommand, JobStateMachine
from cluster_manager.runtime.run_state_manager import RunState, RunStateManager


class FakeContext:
    def __init__(self, state=RunState.INIT):
        self.job_name = "unit-job"
        self.run_state = state
        self.recovery_phase = None
        self.retry_info = MagicMock()
        self.node_pool_proxy = MagicMock()
        self.node_pool_proxy.get_current_snapshot.return_value = SimpleNamespace(
            first_iter=None
        )
        self.handle_runtime_fault = MagicMock(return_value=True)

    def set_state(self, state):
        self.run_state = state


@pytest.fixture
def machine(monkeypatch):
    notify = MagicMock()
    monkeypatch.setattr(
        "cluster_manager.runtime.job_state_machine.Notify",
        MagicMock(return_value=notify),
    )
    ctx = FakeContext()
    state_machine = JobStateMachine(ctx)
    return state_machine, ctx, notify


def test_init_start_and_running_lifecycle(machine):
    state_machine, ctx, _ = machine

    state_machine.init_state()
    assert ctx.run_state is RunState.STARTING
    assert state_machine.next_action() is JobCommand.START_TRAINING
    ctx.node_pool_proxy.release_runing_nodes.assert_called_once_with()

    assert state_machine.on_train_success("start", ["node01"]) is JobCommand.NONE
    assert ctx.run_state is RunState.PENDING

    command = state_machine.on_event(
        Event(EventType.LOG_MONITOR, {"type": "normal"})
    )
    assert command is JobCommand.NONE
    assert ctx.run_state is RunState.RUNNING
    ctx.retry_info.reset.assert_called_once_with()


def test_pending_timeout_runs_stop_start_recovery_cycle(machine):
    state_machine, ctx, _ = machine
    ctx.run_state = RunState.PENDING

    command = state_machine.on_event(
        Event(EventType.LOG_MONITOR, {"type": "timeout"})
    )
    assert command is JobCommand.STOP_TRAINING
    assert ctx.run_state is RunState.RECOVERING
    assert ctx.recovery_phase == "stopping"
    assert state_machine.next_action() is JobCommand.STOP_TRAINING

    assert state_machine.on_train_success("stop") is JobCommand.START_TRAINING
    assert ctx.recovery_phase == "starting"
    assert state_machine.next_action() is JobCommand.START_TRAINING

    assert state_machine.on_train_success("start") is JobCommand.NONE
    assert ctx.run_state is RunState.PENDING
    ctx.retry_info.reset.assert_called_once_with()


def test_hang_and_node_fault_do_not_restart_recovery_twice(machine):
    state_machine, ctx, notify = machine
    ctx.run_state = RunState.RUNNING

    assert state_machine.on_event(
        Event(EventType.LOG_MONITOR, {"type": "hang"})
    ) is JobCommand.STOP_TRAINING
    assert ctx.run_state is RunState.RECOVERING
    notify.send_feishu_alert.assert_called_once()

    assert state_machine.on_event(
        Event(EventType.NHC_MONITOR, {"abnormal_nodes": ["node02"]})
    ) is JobCommand.NONE
    assert ctx.run_state is RunState.RECOVERING


def test_root_cause_exit_blacklists_host_before_recovery(machine):
    state_machine, ctx, _ = machine
    ctx.run_state = RunState.RUNNING
    payload = {
        "type": "exit",
        "data": {
            "type": "root_cause",
            "host": "node03",
            "rank": 17,
            "exit_code": 1,
        },
    }

    command = state_machine.on_event(Event(EventType.LOG_MONITOR, payload))

    assert command is JobCommand.STOP_TRAINING
    assert ctx.run_state is RunState.RECOVERING
    ctx.handle_runtime_fault.assert_called_once_with(
        "node03", "node", fault_reason=payload
    )


def test_unlocatable_exit_requires_manual_intervention(machine):
    state_machine, ctx, notify = machine
    ctx.run_state = RunState.RUNNING

    command = state_machine.on_event(
        Event(
            EventType.LOG_MONITOR,
            {"type": "exit", "data": {"type": "root_cause", "host": ""}},
        )
    )

    assert command is JobCommand.NONE
    assert ctx.run_state is RunState.RUNNING
    notify.send_feishu_alert.assert_called_once()
    assert "Manual intervention required" in state_machine._flow_trace[-1]["extra"][-1]


def test_illegal_transition_is_ignored_and_traced(machine):
    state_machine, ctx, _ = machine
    ctx.run_state = RunState.STARTING

    command = state_machine.on_event(
        Event(EventType.LOG_MONITOR, {"type": "normal"})
    )

    assert command is JobCommand.NONE
    assert ctx.run_state is RunState.STARTING
    trace = state_machine._flow_trace[-1]
    assert trace["handler"] is None
    assert "No handler for STARTING/LOG_RUNNING" in trace["extra"]


@pytest.mark.parametrize(
    ("saved_state", "expected_state"),
    [
        (RunState.RUNNING, RunState.PENDING),
        (RunState.RECOVERING, RunState.RECOVERING),
        (RunState.COMPLETE, RunState.COMPLETE),
    ],
)
def test_run_state_manager_restores_persisted_state(
    tmp_path, saved_state, expected_state
):
    manager = RunStateManager(MagicMock())
    manager.train_config_path = str(tmp_path / "running_state.json")
    runtime_args = {"megatron_config": {"--world-size": 16}}

    manager.set_state(saved_state, runtime_args)

    assert manager.get_state({"megatron_config": {"--world-size": 16}}) is expected_state


def test_run_state_manager_restarts_from_init_when_config_changes(tmp_path):
    manager = RunStateManager(MagicMock())
    manager.train_config_path = str(tmp_path / "running_state.json")
    manager.set_state(
        RunState.RUNNING,
        {"megatron_config": {"--world-size": 16, "--tp": 2}},
    )

    restored = manager.get_state(
        {"megatron_config": {"--world-size": 32, "--tp": 2}}
    )

    assert restored is RunState.INIT
