# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cluster_manager.controller.distributed_job_manager import DistributedJobManager
from cluster_manager.event.event_bus import Event
from cluster_manager.runtime.job_state_machine import JobCommand
from cluster_manager.runtime.run_state_manager import RunState


def make_manager(schedule="NONE"):
    manager = object.__new__(DistributedJobManager)
    manager.runtime_args = {
        "job_name": "job",
        "required_nodes_num": 2,
        "slots_per_node": 8,
        "exec_path": "/work/train.sh",
    }
    manager.workspace_dir = "/work"
    manager.job_name = "job"
    manager.hostfile = "/work/hostfile"
    manager.cluster_schedule = schedule
    manager.event_bus = MagicMock()
    manager.launcher = MagicMock()
    manager.notify = MagicMock()
    manager.ctx = SimpleNamespace(node_pool_proxy=MagicMock(), run_state=RunState.INIT)
    manager.ctx.node_pool_proxy.normal_nodes_file.return_value = "/work/normal"
    manager.state_machine = MagicMock()
    manager.log_monitor = MagicMock()
    manager.slurm_mgr = MagicMock() if schedule == "SLURM" else None
    manager.running = True
    return manager


def test_ensure_slurm_job_retries_query_then_updates_hostfile(monkeypatch):
    manager = make_manager("SLURM")
    manager.slurm_mgr.get_job_nodes.side_effect = [
        (False, []),
        (True, ["node01", "node02"]),
    ]
    manager.slurm_mgr.update_hostfile.return_value = True
    sleep = MagicMock()
    monkeypatch.setattr(
        "cluster_manager.controller.distributed_job_manager.time.sleep", sleep
    )

    manager._ensure_slurm_job()

    sleep.assert_called_once_with(60)
    manager.slurm_mgr.update_hostfile.assert_called_once_with(["node01", "node02"])


def test_ensure_slurm_job_submits_when_queue_is_empty():
    manager = make_manager("SLURM")
    manager.slurm_mgr.get_job_nodes.return_value = (True, [])
    manager.slurm_mgr.submit_and_get_nodes.return_value = True
    manager._ensure_slurm_job()
    manager.slurm_mgr.submit_and_get_nodes.assert_called_once_with()


@pytest.mark.parametrize("failure", ["update", "submit"])
def test_ensure_slurm_job_exits_on_irrecoverable_failure(failure):
    manager = make_manager("SLURM")
    if failure == "update":
        manager.slurm_mgr.get_job_nodes.return_value = (True, ["node01"])
        manager.slurm_mgr.update_hostfile.return_value = False
    else:
        manager.slurm_mgr.get_job_nodes.return_value = (True, [])
        manager.slurm_mgr.submit_and_get_nodes.return_value = False
    with pytest.raises(SystemExit):
        manager._ensure_slurm_job()


@pytest.mark.parametrize("state", [RunState.PENDING, RunState.RUNNING])
def test_restore_state_starts_monitor_for_active_states(state):
    manager = make_manager()
    manager.ctx.run_state = state
    manager._restore_state()
    manager.state_machine.init_state.assert_called_once_with()
    manager.log_monitor.start.assert_called_once_with()


def test_restore_state_does_not_monitor_starting_job():
    manager = make_manager()
    manager.ctx.run_state = RunState.STARTING
    manager._restore_state()
    manager.log_monitor.start.assert_not_called()


def test_next_command_prioritizes_event_over_state_action():
    manager = make_manager()
    event = Event("test", {"x": 1})
    manager.event_bus.get_event.return_value = event
    manager.state_machine.on_event.return_value = JobCommand.STOP_TRAINING
    assert manager._next_command() is JobCommand.STOP_TRAINING
    manager.state_machine.next_action.assert_not_called()

    manager.event_bus.get_event.return_value = None
    manager.state_machine.next_action.return_value = JobCommand.START_TRAINING
    assert manager._next_command() is JobCommand.START_TRAINING


def test_execute_dispatches_commands():
    manager = make_manager()
    manager._start_training = MagicMock(return_value=JobCommand.NONE)
    manager._stop_training = MagicMock(return_value=JobCommand.START_TRAINING)
    assert manager._execute(JobCommand.START_TRAINING) is JobCommand.NONE
    assert manager._execute(JobCommand.STOP_TRAINING) is JobCommand.START_TRAINING
    assert manager._execute(JobCommand.START_LOG_MONITOR) is JobCommand.NONE
    manager.log_monitor.start.assert_called_once_with()


def test_start_training_success_starts_monitor_and_updates_state():
    manager = make_manager()
    manager.ctx.node_pool_proxy.apply_node_num_resources.return_value = (
        "node01",
        "/work/slots",
    )
    manager.launcher.start.return_value = (0, ["node01", "node02"])
    manager.state_machine.on_train_success.return_value = JobCommand.NONE

    assert manager._start_training() is JobCommand.NONE
    manager.launcher.start.assert_called_once_with("/work/train.sh", "/work/slots")
    manager.log_monitor.start.assert_called_once_with()
    manager.state_machine.on_train_success.assert_called_once_with(
        "start", ["node01", "node02"], ""
    )


def test_start_training_releases_nodes_and_retries_after_launcher_failure(monkeypatch):
    manager = make_manager()
    manager.ctx.node_pool_proxy.apply_node_num_resources.return_value = (
        "node01",
        "/work/slots",
    )
    manager.launcher.start.side_effect = [(9, []), (0, ["node01", "node02"])]
    manager.state_machine.on_train_success.return_value = JobCommand.NONE
    monkeypatch.setattr(
        "cluster_manager.controller.distributed_job_manager.time.sleep", MagicMock()
    )

    assert manager._start_training() is JobCommand.NONE
    manager.ctx.node_pool_proxy.release_runing_nodes.assert_called_once_with()
    assert manager.launcher.start.call_count == 2


def test_start_training_bare_metal_shortage_is_fatal():
    manager = make_manager("NONE")
    manager.ctx.node_pool_proxy.apply_node_num_resources.return_value = (None, None)
    with pytest.raises(RuntimeError, match="healthy nodes are fewer"):
        manager._start_training()


def test_stop_training_success_reports_releases_and_updates_state():
    manager = make_manager()
    manager.launcher.stop.return_value = (0, ["node01"])
    manager.report_fault = MagicMock()
    manager.state_machine.on_train_success.return_value = JobCommand.START_TRAINING

    assert manager._stop_training() is JobCommand.START_TRAINING
    manager.report_fault.assert_called_once_with()
    manager.ctx.node_pool_proxy.release_runing_nodes.assert_called_once_with()
    manager.log_monitor.stop.assert_called_once_with()
    manager.state_machine.on_train_success.assert_called_once_with(
        "stop", ["node01"], ""
    )


def test_stop_training_forces_restart_after_three_failures(monkeypatch):
    manager = make_manager()
    manager.launcher.stop.return_value = (8, [])
    manager.report_fault = MagicMock()
    monkeypatch.setattr(
        "cluster_manager.controller.distributed_job_manager.time.sleep", MagicMock()
    )

    assert manager._stop_training() is JobCommand.START_TRAINING
    assert manager.launcher.stop.call_count == 3
    manager.state_machine._set_state.assert_called_once_with(RunState.STARTING)
    manager.ctx.node_pool_proxy.release_runing_nodes.assert_called_once_with()


def test_stop_marks_manager_and_monitor_stopped():
    manager = make_manager()
    manager.stop()
    assert manager.running is False
    manager.log_monitor.stop.assert_called_once_with()
