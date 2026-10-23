# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
import threading

import pytest

from cluster_manager.event.event_bus import EventType
from cluster_manager.monitor.log_monitor import LogMonitor


def make_monitor_shell():
    monitor = object.__new__(LogMonitor)
    monitor.last_update_time = 0
    monitor.fault_handler = MagicMock()
    monitor.cur_iter = 10
    monitor.cur_iter_timestamp = "time"
    monitor._send_message = MagicMock()
    monitor.process_status = "other"
    return monitor


@pytest.mark.parametrize("event_type", ["exit", "hang"])
def test_exit_and_hang_events_are_delayed_in_fault_cache(event_type):
    monitor = make_monitor_shell()
    data = {"type": event_type, "data": {"type": "proc"}}
    monitor._handle_parsed_event(data)
    monitor.fault_handler.add_fault.assert_called_once_with(data)
    monitor._send_message.assert_not_called()


@pytest.mark.parametrize("event_type", ["loss", "inf"])
def test_loss_and_inf_events_publish_immediately(event_type):
    monitor = make_monitor_shell()
    monitor._handle_parsed_event(
        {"type": event_type, "data": {"rank": 3, "type": event_type}}
    )
    event = monitor._send_message.call_args.args[0]
    assert event.type == EventType.LOG_MONITOR
    assert event.payload["type"] == event_type
    assert event.payload["cur_iter"] == 10


def test_checkpoint_events_update_status_and_emit_iteration(monkeypatch):
    monitor = make_monitor_shell()
    monitor.data_recorder = MagicMock()
    monitor.data_recorder.get_last_valid_iter.return_value = 8
    monitor.handle_checkpoint_time = MagicMock()
    monitor.trigger_alert = MagicMock()
    monitor.cur_iter = 9
    monitor.cur_iter_timestamp = datetime(2026, 8, 13, 10, 0, 0)
    monkeypatch.setattr(
        "cluster_manager.monitor.log_monitor.pytz.timezone",
        lambda _: timezone.utc,
    )

    monitor._handle_parsed_event({"type": "ckpt_start"})
    assert monitor.process_status == "ckpt"
    monitor._handle_parsed_event({"type": "ckpt_time", "max_time": 1500})

    assert monitor.process_status == "other"
    monitor.handle_checkpoint_time.assert_called_once_with(8, 1500)
    alert_type, _, extra = monitor.trigger_alert.call_args.args
    assert alert_type == "iter"
    assert extra["is_ckpt"] is True
    assert extra["cur_iter"] == 9


def test_checkpoint_and_evaluate_statistics_update_notifier():
    monitor = make_monitor_shell()
    monitor.data_recorder = MagicMock()
    monitor.data_recorder.get_all_ckpt_times.return_value = [100.0, 300.0]
    monitor.data_recorder.get_all_eval_times.return_value = [200.0, 400.0]
    monitor.regular_notify_enabled = True
    monitor.notify = MagicMock()

    monitor.handle_checkpoint_time(4, 300.0)
    monitor.handle_evaluate_time(5, 400.0)

    monitor.data_recorder.add_iter_extra_time.assert_any_call(4, "ckpt", 300.0)
    monitor.data_recorder.add_iter_extra_time.assert_any_call(5, "eval", 400.0)
    monitor.notify.update_ckpt_avg_time.assert_called_once_with(200.0)
    monitor.notify.update_eval_avg_time.assert_called_once_with(300.0)


def test_iteration_anomaly_is_published():
    monitor = make_monitor_shell()
    monitor.anomaly_detector = MagicMock()
    monitor.anomaly_detector.check_iteration_time.return_value = {
        "warn_msg": "slow",
        "payload": {"type": "hang", "reason": "slow iteration"},
    }
    monitor.check_iteration_time(7, 9000.0)
    event = monitor._send_message.call_args.args[0]
    assert event.payload["reason"] == "slow iteration"


def test_log_event_records_first_and_followup_iterations(monkeypatch):
    monitor = make_monitor_shell()
    monitor.iter_dumper_enabled = False
    monitor.regular_notify_enabled = False
    monitor.data_recorder = MagicMock()
    monitor.data_recorder.record_iter_metrics.return_value = (1.0, 2.0)
    monitor.data_recorder.training_data = {"loss": [1.0]}
    monitor.PRINT_LOSS_INTERVAL = 50
    monitor.total_iter = None
    monitor.cur_iter = None
    monitor.eval_interval = 0
    monitor.eval_start_time = None
    monitor.trigger_alert = MagicMock()
    monitor.check_iteration_time = MagicMock()
    monkeypatch.setattr(
        "cluster_manager.monitor.log_monitor.global_config.ENABLE_LOSS_GRAD_CHECK",
        False,
    )

    first = {
        "type": "log",
        "current_iter": 1,
        "total_iter": 100,
        "iter_time": "12.5",
        "timestamp": "2026-08-13 10:00:00",
    }
    monitor._handle_log_event(first)
    assert monitor.cur_iter == 1
    assert monitor.total_iter == 100
    assert monitor.trigger_alert.call_args.args[2]["is_first"] is True
    monitor.check_iteration_time.assert_called_with(1, 12.5)

    second = dict(first, current_iter=2, timestamp="2026-08-13 10:00:01")
    monitor._handle_log_event(second)
    assert monitor.cur_iter == 2
    assert "is_first" not in monitor.trigger_alert.call_args.args[2]


def test_log_event_without_iteration_is_ignored():
    monitor = make_monitor_shell()
    monitor.iter_dumper_enabled = False
    monitor.data_recorder = MagicMock()
    monitor._handle_log_event({"type": "log", "timestamp": "now"})
    monitor.data_recorder.record_iter_metrics.assert_not_called()


def test_send_message_prefers_sink_then_bus():
    monitor = make_monitor_shell()
    event = SimpleNamespace(type="x")
    monitor.event_sink = MagicMock()
    monitor.event_bus = MagicMock()
    LogMonitor._send_message(monitor, event)
    monitor.event_sink.publish.assert_called_once_with(event)
    monitor.event_bus.publish.assert_not_called()

    monitor.event_sink = None
    LogMonitor._send_message(monitor, event)
    monitor.event_bus.publish.assert_called_once_with(event)


def test_constructor_rejects_two_log_sources(tmp_path):
    with pytest.raises(ValueError, match="cannot be configured"):
        LogMonitor(log_dir=str(tmp_path), log_file=str(tmp_path / "train.log"))


def test_start_and_stop_manage_threads_and_statistics(tmp_path, monkeypatch):
    log_file = tmp_path / "train.log"
    log_file.write_text("", encoding="utf-8")

    class FakeThread:
        def __init__(self, target, daemon, name):
            self.target = target
            self.name = name
            self.started = False

        def start(self):
            self.started = True

        def is_alive(self):
            return self.started

        def join(self, timeout=None):
            self.started = False

    monkeypatch.setattr("cluster_manager.monitor.log_monitor.threading.Thread", FakeThread)
    monitor = LogMonitor(
        log_file=str(log_file),
        mode="standalone",
        enable_no_update=True,
    )
    monitor.notify = MagicMock()
    monitor.fault_handler = MagicMock()
    monitor.data_recorder = MagicMock()
    monitor.data_recorder.get_all_iter_times.return_value = [1, 2]
    monitor.data_recorder.get_all_ckpt_times.return_value = [3]
    monitor.data_recorder.get_all_eval_times.return_value = []
    monitor.data_recorder.calculate_avg_pure_iter.return_value = 12.34
    monkeypatch.setattr(
        "cluster_manager.monitor.log_monitor.TrainingDataRecorder", MagicMock
    )

    monitor.start()
    assert monitor.tail_thread.started
    assert monitor.no_update_thread.started
    monitor.notify.start.assert_called_once_with()

    stats = monitor.stop()
    assert stats == {
        "total_iterations": 2,
        "ckpt_count": 1,
        "eval_count": 0,
        "avg_pure_iter_ms": 12.3,
    }
    monitor.notify.stop.assert_called_once_with()
    monitor.fault_handler.clear.assert_called_once_with(reason="log_monitor_stop")
