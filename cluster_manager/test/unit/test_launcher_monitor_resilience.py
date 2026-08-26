# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cluster_manager.event.event_bus import EventType
from cluster_manager.launcher.mpirun_launcher import MPIRunLauncher
from cluster_manager.monitor.log_fault_handler import FaultHandler
from cluster_manager.monitor.log_monitor import LogMonitor
from cluster_manager.monitor.log_tracker import LogFileTracker


def test_mpirun_start_returns_nonzero_process_result(monkeypatch):
    result = SimpleNamespace(returncode=7, stdout="launch failed")
    execute = MagicMock(return_value=result)
    monkeypatch.setattr(
        "cluster_manager.launcher.mpirun_launcher.CmdExecutor.exec_mpirun_cmd",
        execute,
    )

    returned = MPIRunLauncher().start("/work/train.sh", "/work/slots.txt")

    assert returned is result
    command, work_dir, capture_output, timeout = execute.call_args.args
    assert command == f"cd {Path('/work')} ; bash /work/train.sh /work/slots.txt 2>&1 &"
    assert work_dir == Path("/work")
    assert capture_output is False
    assert timeout > 0


def test_mpirun_start_propagates_timeout(monkeypatch):
    monkeypatch.setattr(
        "cluster_manager.launcher.mpirun_launcher.CmdExecutor.exec_mpirun_cmd",
        MagicMock(side_effect=subprocess.TimeoutExpired("mpirun", 10)),
    )

    with pytest.raises(subprocess.TimeoutExpired):
        MPIRunLauncher().start("/work/train.sh", "/work/slots.txt")


def test_mpirun_stop_targets_hostfile_and_propagates_failure(monkeypatch):
    result = SimpleNamespace(returncode=1, stderr="pkill failed")
    execute = MagicMock(return_value=result)
    monkeypatch.setattr(
        "cluster_manager.launcher.mpirun_launcher.CmdExecutor.exec_mpirun_cmd",
        execute,
    )

    assert MPIRunLauncher().stop("/work/hosts") is result
    assert execute.call_args.args[0] == "clush --hostfile /work/hosts -b pkill -9 -f python"
    assert execute.call_args.kwargs["capture_output"] is False


def test_fixed_log_truncation_requires_reopen(tmp_path):
    log_file = tmp_path / "train.log"
    log_file.write_bytes(b"first line\nsecond line\n")
    tracker = LogFileTracker(log_file=log_file, persist_offset=False)
    handle, offset = tracker.open_log_file(log_file, 0)
    assert offset == 0
    handle.seek(0, os.SEEK_END)
    old_offset = handle.buffer.tell()

    log_file.write_bytes(b"new\n")
    try:
        assert tracker.fixed_file_needs_reopen(handle, old_offset)
    finally:
        handle.close()


def test_fixed_log_replacement_requires_reopen(tmp_path):
    log_file = tmp_path / "train.log"
    log_file.write_text("old\n", encoding="utf-8")
    tracker = LogFileTracker(log_file=log_file, persist_offset=False)
    handle, _ = tracker.open_log_file(log_file, 0)
    replacement = tmp_path / "replacement.log"
    replacement.write_text("new\n", encoding="utf-8")
    os.replace(replacement, log_file)
    try:
        assert tracker.fixed_file_needs_reopen(handle, 0)
    finally:
        handle.close()


def test_fault_handler_prefers_root_cause_and_publishes_context(monkeypatch):
    published = []
    handler = FaultHandler(
        event_publisher=published.append,
        slots_per_node=8,
        get_cur_iter=lambda: 42,
        get_cur_iter_timestamp=lambda: "2026-08-13 10:00:00",
    )
    monkeypatch.setattr(handler, "_reset_timer", lambda: None)
    handler.add_fault({"type": "exit", "data": {"type": "proc", "fault_pid": "p1"}})
    handler.add_fault(
        {"type": "exit", "data": {"type": "root_cause", "host": "node01"}}
    )

    handler._send_fault_message(0)

    assert len(published) == 1
    event = published[0]
    assert event.type == EventType.LOG_MONITOR
    assert event.payload["type"] == "exit"
    assert event.payload["data"]["type"] == "root_cause"
    assert event.payload["cur_iter"] == 42
    assert event.payload["timestamp"] == "2026-08-13 10:00:00"


def test_cleared_fault_generation_cannot_publish_stale_event(monkeypatch):
    published = []
    handler = FaultHandler(event_publisher=published.append)
    monkeypatch.setattr(handler, "_reset_timer", lambda: None)
    handler.add_fault(
        {"type": "exit", "data": {"type": "root_cause", "host": "node01"}}
    )
    stale_generation = handler._generation

    handler.clear("new log file")
    handler._send_fault_message(stale_generation)

    assert published == []


def test_fault_handler_publishes_prte_nonzero_process_exit(monkeypatch):
    published = []
    handler = FaultHandler(
        event_publisher=published.append,
        get_cur_iter=lambda: 12,
        get_cur_iter_timestamp=lambda: "2026-08-14 17:41:48",
    )
    monkeypatch.setattr(handler, "_reset_timer", lambda: None)
    handler.add_fault(
        {
            "type": "exit",
            "data": {
                "type": "proc",
                "fault_pid": "[prterun-m09r2n09-80749@1,2]",
                "exit_code": 137,
                "runtime": "prte",
                "signal": 9,
            },
        }
    )

    handler._send_fault_message(0)

    assert len(published) == 1
    payload = published[0].payload
    assert payload["type"] == "exit"
    assert payload["data"]["fault_info"] == "[prterun-m09r2n09-80749@1,2]"
    assert payload["data"]["exit_code"] == 137
    assert payload["data"]["signal"] == 9
    assert payload["cur_iter"] == 12


def test_monitor_root_cause_bypasses_delayed_fault_cache():
    monitor = object.__new__(LogMonitor)
    monitor.fault_handler = MagicMock()
    monitor.cur_iter = 9
    monitor.cur_iter_timestamp = "now"
    monitor._send_message = MagicMock()

    monitor._handle_parsed_event(
        {"type": "exit", "data": {"type": "root_cause", "host": "node02"}}
    )

    monitor.fault_handler.clear.assert_called_once_with(
        reason="root_cause_preferred"
    )
    monitor.fault_handler.add_fault.assert_not_called()
    event = monitor._send_message.call_args.args[0]
    assert event.payload["data"]["host"] == "node02"
    assert event.payload["cur_iter"] == 9


def test_monitor_hang_alert_propagates_iteration_context():
    monitor = object.__new__(LogMonitor)
    monitor.cur_iter = 77
    monitor.cur_iter_timestamp = "timestamp"
    monitor._send_message = MagicMock()

    monitor.trigger_alert("hang", "no log update")

    event = monitor._send_message.call_args.args[0]
    assert event.type == EventType.LOG_MONITOR
    assert event.payload == {
        "type": "hang",
        "cur_iter": 77,
        "timestamp": "timestamp",
    }
