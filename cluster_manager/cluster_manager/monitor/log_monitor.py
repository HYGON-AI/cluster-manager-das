# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
训练日志监控器 - 薄编排层

组装各子模块，协调日志追踪、解析、异常检测、故障处理、迭代转存的完整流程。

子模块：
- LogFileTracker：日志文件发现与偏移量管理
- LogParser (工厂)：日志行解析
- AnomalyDetector：异常检测（loss/grad/performance/trajectory）
- FaultHandler：故障缓存与延迟发送
- IterDumper：迭代行转存（iters.log + iters.csv）
- TrainingDataRecorder：训练数据记录
- Notify：飞书即时告警
"""
import os
import sys
import time
import threading
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import pytz

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import cluster_manager.config.global_config as global_config
from cluster_manager.config.global_config import logger
from cluster_manager.event.event_bus import Event, EventType
from cluster_manager.platform.notify import Notify
from cluster_manager.monitor.mega.training_data_recorder import TrainingDataRecorder
from cluster_manager.monitor.log_tracker import LogFileTracker, k8s_offset_settings
from cluster_manager.monitor.log_parser import create_log_parser
from cluster_manager.monitor.log_anomaly_detector import AnomalyDetector
from cluster_manager.monitor.log_fault_handler import FaultHandler
from cluster_manager.monitor.log_iter_dumper import IterDumper
from cluster_manager.monitor.log_event_sink import create_log_event_sink

iter_file_name = "iters.log"
iter_csv_name = "iters.csv"
workspace = f"{global_config.WORK_DIR}/workspace"

def _get_int_config(key: str, default: int = 0) -> int:
    value = global_config.MEGATRON_CONFIG.get(key, default)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"[LogMonitor] Invalid integer config {key}={value!r}, fallback to {default}.")
        return default


def _default_monitor_mode(event_bus=None) -> str:
    if event_bus is not None:
        return "fault_tolerance"
    configured_mode = os.getenv("LOG_MONITOR_MODE")
    if configured_mode:
        return configured_mode
    if os.getenv("FT_POD_NAME") and (
        os.getenv("FT_FAULT_MARKER_FILE") or os.getenv("FT_FAULT_REPORT_URL")
    ):
        return "k8s"
    return "standalone"


def _env_truthy(value: Any) -> bool:
    return str(value).lower() in ("true", "1", "yes", "on")


def _k8s_last_node_no_update_enabled(environ=None) -> bool:
    env = os.environ if environ is None else environ
    if not _env_truthy(env.get("LOG_MONITOR_ENABLE_NO_UPDATE", "true")):
        return False
    if not _env_truthy(env.get("LOG_MONITOR_LAST_NODE_ONLY", "false")):
        return True

    try:
        replica_count = int(env.get("FT_REPLICA_COUNT", "1"))
    except (TypeError, ValueError):
        logger.warning("[LogMonitor] Invalid FT_REPLICA_COUNT; disable hang detection for this Pod.")
        return False

    if replica_count <= 1:
        return True

    pod_name = env.get("FT_POD_NAME") or env.get("POD_NAME") or ""
    try:
        replica_index = int(pod_name.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        logger.warning(
            f"[LogMonitor] Cannot derive replica index from Pod name {pod_name!r}; "
            "disable hang detection for this Pod."
        )
        return False
    return replica_index == replica_count - 1


class LogMonitor:
    def __init__(
        self,
        event_bus=None,
        job_name=None,
        mode: str = None,
        log_dir: str = None,
        log_file: str = None,
        alert_threshold_ms: int = None,
        no_update_threshold: int = None,
        enable_no_update: bool = None,
        slots_per_node: int = 1,
        event_sink=None,
    ):
        # ---- 配置 ----
        if log_dir and log_file:
            raise ValueError("log_dir and log_file cannot be configured at the same time")
        self.log_file = Path(log_file) if log_file else None
        self.log_dir = Path(log_dir) if log_dir else (
            self.log_file.parent if self.log_file else Path(global_config.LOG_DIR)
        )
        self.alert_threshold_ms = alert_threshold_ms if alert_threshold_ms is not None else global_config.TRAIN_ALERT_THRESHOLD
        self.no_update_threshold = no_update_threshold if no_update_threshold is not None else global_config.TRAIN_NO_UPDATE_THRESHOLD
        requested_no_update = enable_no_update if enable_no_update is not None else (
            _env_truthy(os.getenv("LOG_MONITOR_ENABLE_NO_UPDATE", "true"))
        )
        self.PRINT_LOSS_INTERVAL = 50
        self.normal_alert_interval = 60
        self.running = True
        self.job_name = job_name
        self.mode = mode or _default_monitor_mode(event_bus)
        self.enable_no_update = (
            requested_no_update
            and (self.mode != "k8s" or _k8s_last_node_no_update_enabled())
        )

        # ---- 子模块 ----
        self.data_recorder = TrainingDataRecorder()
        self.notify = Notify(job_name=job_name)
        self.event_bus = event_bus
        self.event_sink = event_sink or create_log_event_sink(self.mode, notify=self.notify, event_bus=event_bus)
        self.regular_notify_enabled = global_config.ENABLE_REGULAR_NOTIFY
        self.iter_dumper_enabled = global_config.ENABLE_ITER_DUMPER

        tracker_options = {}
        if self.mode == "k8s":
            offset_dir, persist_offset = k8s_offset_settings(self.log_dir)
            tracker_options.update(offset_dir=offset_dir, persist_offset=persist_offset)
        self.tracker = LogFileTracker(
            log_dir=self.log_dir,
            log_file=self.log_file,
            **tracker_options,
        )
        self.parse_log = create_log_parser(
            log_parser_type=global_config.LOG_PARSER_TYPE,
            data_recorder=self.data_recorder,
        )
        self.anomaly_detector = AnomalyDetector(
            data_recorder=self.data_recorder,
            job_name=job_name,
            alert_callback=self.notify.send_feishu_alert,
        )
        self.fault_handler = FaultHandler(
            event_bus=event_bus,
            event_publisher=self._send_message,
            slots_per_node=slots_per_node,
            get_cur_iter=lambda: self.cur_iter,
            get_cur_iter_timestamp=lambda: self.cur_iter_timestamp,
        )
        self.iter_dumper = IterDumper(
            dump_file=Path(workspace) / iter_file_name,
            csv_file=Path(workspace) / iter_csv_name,
        ) if self.iter_dumper_enabled else None

        # ---- 运行时状态 ----
        self.last_update_time = time.time()
        self.tail_thread: Optional[threading.Thread] = None
        self.no_update_thread: Optional[threading.Thread] = None
        self.has_received_first_log = threading.Event()
        self.last_normal_alert_time = 0

        # 训练配置（从 MEGATRON_CONFIG 读取）
        self.save_interval = _get_int_config("--save-interval", 0)
        self.eval_interval = _get_int_config("--eval-interval", 0)
        self.seq_len = _get_int_config("--seq-length", 0)
        self.train_samples = _get_int_config("--train-samples", 0)

        # CKPT/EVAL 阶段追踪
        self.process_status = "other"
        self.eval_start_time = None

        # 当前迭代状态
        self.cur_iter = None
        self.cur_iter_timestamp = None
        self.total_iter = None

        # 告警标记
        self.alerts = {
            'no_update': False,
        }
        monitor_target = self.log_file or self.log_dir
        logger.info(f"[LogMonitor] initialized, mode={self.mode}, target={monitor_target}")

    def start(self):
        """启动监控"""
        logger.info(f"Start monitoring log target: {self.log_file or self.log_dir}")
        logger.info(f"Alarm threshold: {self.alert_threshold_ms}ms | No update alarm threshold: {self.no_update_threshold} seconds")

        if self.log_file is None and not self.log_dir.is_dir():
            logger.info(f"Log dir does not exist: {self.log_dir}")
            return
        if self.log_file is not None and not self.log_file.exists():
            logger.info(f"Log file does not exist yet, waiting for creation: {self.log_file}")

        self.running = True
        self.has_received_first_log.clear()
        self.last_normal_alert_time = time.time()

        if self.enable_no_update:
            self.no_update_thread = threading.Thread(
                target=self._monitor_no_update, daemon=True, name="NoUpdateMonitorThread"
            )
            self.no_update_thread.start()
        else:
            logger.info("No-update detection is disabled for this LogMonitor instance.")

        self.tail_thread = threading.Thread(
            target=self._tail_log_file, daemon=True, name="LogTailThread"
        )
        self.tail_thread.start()

        self.notify.start()
        logger.info("All monitoring threads have been started.")

    def stop(self):
        """停止监控"""
        if not self.running:
            return {}

        self.running = False
        self.has_received_first_log.set()
        logger.info("LogMonitor stop(), closing the log monitoring process")

        self.notify.stop()

        for thread in [self.tail_thread, self.no_update_thread]:
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=5)
                except TimeoutError:
                    logger.warning(f"Thread {thread.name} exit timed out")

        self.fault_handler.clear(reason="log_monitor_stop")

        stats = {
            "total_iterations": len(self.data_recorder.get_all_iter_times()),
            "ckpt_count": len(self.data_recorder.get_all_ckpt_times()),
            "eval_count": len(self.data_recorder.get_all_eval_times()),
            "avg_pure_iter_ms": round(self.data_recorder.calculate_avg_pure_iter(), 1),
        }
        logger.info(f"\n=== Monitoring stats ===\nIter: {stats['total_iterations']}, CKPT: {stats['ckpt_count']}, EVAL: {stats['eval_count']}, Avg: {stats['avg_pure_iter_ms']}ms")

        self.data_recorder = TrainingDataRecorder()
        self.cur_iter = None
        self.cur_iter_timestamp = None
        self.current_log_file = None

        self.alerts = {
            'no_update': False,
        }

        return stats

    def trigger_alert(self, alert_type: str, message: str, extra_dict: dict = None):
        """触发告警事件"""
        if message:
            logger.info(f"alert message sent, type = {alert_type}, message = {message}.")

        payload = {"type": alert_type}
        if extra_dict is not None and isinstance(extra_dict, dict):
            payload.update(extra_dict)

        if alert_type == "hang":
            payload["cur_iter"] = self.cur_iter
            payload["timestamp"] = self.cur_iter_timestamp

        event = Event(type=EventType.LOG_MONITOR, payload=payload)
        self._send_message(event)

    def _send_message(self, event):
        if self.event_sink:
            self.event_sink.publish(event)
            return
        if self.event_bus:
            self.event_bus.publish(event)
            return
        logger.warning("[LogMonitor] no event sink configured, drop event: %s", event)

    def handle_checkpoint_time(self, ckpt_iter, ckpt_max_time_ms):
        """处理 CKPT 耗时"""
        if ckpt_iter is None:
            logger.info(f"[Warning] CKPT Time {ckpt_max_time_ms:.1f}ms cannot be linked to iter, skipped")
            return

        self.data_recorder.add_iter_extra_time(ckpt_iter, 'ckpt', ckpt_max_time_ms)
        self.data_recorder.add_ckpt_time(ckpt_max_time_ms)
        logger.info(f"[Extra time] CKPT Time {ckpt_max_time_ms:.1f}ms")

        all_ckpt = self.data_recorder.get_all_ckpt_times()
        avg_ckpt_ms = sum(all_ckpt) / len(all_ckpt) if all_ckpt else 0.0
        if self.regular_notify_enabled:
            self.notify.update_ckpt_avg_time(avg_ckpt_ms)

    def handle_evaluate_time(self, eval_iter, eval_max_time_ms):
        """处理 EVAL 耗时"""
        if eval_iter is None:
            logger.info(f"[Warning] EVAL elapsed time {eval_max_time_ms:.1f}ms cannot be attributed, skipped")
            return

        self.data_recorder.add_iter_extra_time(eval_iter, 'eval', eval_max_time_ms)
        self.data_recorder.add_eval_time(eval_max_time_ms)
        logger.info(f"[Extra time] EVAL elapsed time {eval_max_time_ms:.1f}ms | Attributed to [iter:{eval_iter}]")

        all_eval = self.data_recorder.get_all_eval_times()
        avg_eval_ms = sum(all_eval) / len(all_eval) if all_eval else 0.0
        if self.regular_notify_enabled:
            self.notify.update_eval_avg_time(avg_eval_ms)

    def check_iteration_time(self, iter_num: int, iter_time_ms: float):
        """检查迭代耗时"""
        result = self.anomaly_detector.check_iteration_time(iter_num, iter_time_ms)
        if result:
            logger.warning(result["warn_msg"])
            event = Event(type=EventType.LOG_MONITOR, payload=result["payload"])
            self._send_message(event)

    def _handle_parsed_event(self, data_dict: Dict[str, Any]):
        """根据解析后的事件类型进行分发处理"""
        self.last_update_time = time.time()

        if not data_dict or 'type' not in data_dict:
            return

        event_type = data_dict['type']

        # exit/hang → 故障缓存
        if event_type == 'exit' and data_dict.get("data", {}).get("type") == "root_cause":
            self.fault_handler.clear(reason="root_cause_preferred")
            event = Event(
                type=EventType.LOG_MONITOR,
                payload={
                    "type": event_type,
                    "data": data_dict["data"],
                    "cur_iter": self.cur_iter,
                    "timestamp": self.cur_iter_timestamp,
                },
            )
            logger.info("Torchrun Root Cause found in the detection log")
            self._send_message(event)
        elif event_type in ('exit', 'hang'):
            self.fault_handler.add_fault(data_dict)

        # loss/inf → 即时事件
        elif event_type in ('loss', 'inf'):
            event = Event(
                type=EventType.LOG_MONITOR,
                payload={
                    "type": data_dict["type"],
                    "data": data_dict["data"],
                    "cur_iter": self.cur_iter,
                    "timestamp": self.cur_iter_timestamp,
                }
            )
            logger.info(f"Process {event_type} found in the detection log")
            self._send_message(event)

        elif event_type == "log":
            self._handle_log_event(data_dict)

        elif event_type == 'ckpt_start':
            self.process_status = "ckpt"

        elif event_type == 'ckpt_time':
            self.process_status = "other"
            target_iter = self.data_recorder.get_last_valid_iter()
            value = data_dict['max_time']
            self.handle_checkpoint_time(target_iter, value)
            ckpt_timestamp = datetime.now(pytz.timezone('Asia/Shanghai'))
            if self.cur_iter_timestamp is not None:
                ckpt_timestamp = self.cur_iter_timestamp + timedelta(milliseconds=value)
            extra_dict = {"cur_iter": self.cur_iter, "timestamp": ckpt_timestamp, "is_ckpt": True}
            self.trigger_alert("iter", f"ckpt saved : {target_iter}", extra_dict)

    def _handle_log_event(self, data_dict: Dict[str, Any]):
        """处理 base（Megatron）/ special 解析器返回的 'log' 类型事件"""
        if self.iter_dumper_enabled and 'all' in data_dict:
            self.iter_dumper.add_line(data_dict['all'])

        if "current_iter" not in data_dict:
            logger.warning("event_type equals 'log', but the current_iter field is not present.")
            return

        iter_num = data_dict['current_iter']

        iter_time = data_dict.get('iter_time')
        if iter_time is None:
            # Special 日志场景：train_time 来自 train_wall（单位：秒），转换为 ms
            train_time_sec = float(data_dict.get('train_time', 0.0))
            iter_time = train_time_sec * 1000.0
        else:
            iter_time = float(iter_time)

        time_str = data_dict['timestamp']
        current_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

        # 首次获取 total_iter（部分框架日志无此字段，缺失时跳过）
        if not self.total_iter and 'total_iter' in data_dict:
            self.total_iter = int(data_dict['total_iter'])
            if self.regular_notify_enabled:
                self.notify.update_train_total_config(
                    total_iters=self.total_iter,
                    save_interval=self.eval_interval,
                    eval_interval=self.eval_interval,
                    train_samples=self.train_samples,
                )

        # 更新 cur_iter
        if self.cur_iter is None:
            self.cur_iter = iter_num
            self.cur_iter_timestamp = current_dt
            self.trigger_alert("iter", f"First Iter : {iter_num}",
                               {"cur_iter": self.cur_iter, "is_first": True, "timestamp": self.cur_iter_timestamp})
        else:
            self.cur_iter = iter_num
            self.cur_iter_timestamp = current_dt
            self.trigger_alert("iter", f"Iter updating : {iter_num}",
                               {"cur_iter": self.cur_iter, "timestamp": self.cur_iter_timestamp})

        self.last_normal_alert_time = time.time()

        # EVAL 阶段追踪
        self.process_status = "other"
        if self.eval_interval and iter_num % self.eval_interval == 0:
            self.eval_start_time = current_dt
            self.process_status = "eval"
        elif self.eval_start_time is not None and iter_num % self.eval_interval == 1:
            eval_duration = (current_dt - self.eval_start_time).total_seconds()
            self.handle_evaluate_time(iter_num - 1, eval_duration * 1000)
            self.eval_start_time = None

        # 迭代耗时检测
        self.check_iteration_time(iter_num, iter_time)

        # 记录训练数据（区分 base/Megatron 与 special 日志）
        loss, grad_norm = self.data_recorder.record_iter_metrics(
            iter_num, data_dict, log_parser_type=global_config.LOG_PARSER_TYPE
        )

        # 更新 notify 数据
        if self.regular_notify_enabled:
            consumed_samples = float(data_dict.get('consumed_samples', 0.0))
            consumed_tokens = consumed_samples * self.seq_len / 1e9
            remaining_tokens = 0.0
            if self.train_samples > 0:
                total_tokens = self.train_samples * self.seq_len / 1e9
                remaining_tokens = max(total_tokens - consumed_tokens, 0.0)
            self.notify.update_training_data(data_dict, consumed_tokens, remaining_tokens)

        # Loss/梯度异常检测（受全局开关控制）
        if global_config.ENABLE_LOSS_GRAD_CHECK:
            anomaly = self.anomaly_detector.check_loss_and_grad(iter_num, loss, grad_norm)
            if anomaly:
                event = Event(type=EventType.LOG_MONITOR, payload=anomaly["payload"])
                self._send_message(event)
                if anomaly.get("feishu_message"):
                    self.notify.send_feishu_alert(anomaly["feishu_message"])

        # 定期打印 loss
        if len(self.data_recorder.training_data['loss']) % self.PRINT_LOSS_INTERVAL == 0:
            self.data_recorder.print_loss(iter_num, self.PRINT_LOSS_INTERVAL)

    def _monitor_no_update(self):
        """监控长时间无新内容"""
        while self.running:
            current_time = time.time()
            time_since_last_update = current_time - self.last_update_time

            if self.cur_iter is None:
                timeout_threshold = global_config.STARTUP_NO_LOG_TIMEOUT_SEC
            else:
                timeout_threshold = self.no_update_threshold

            if time_since_last_update > timeout_threshold and not self.alerts['no_update']:
                trigger_hang = False

                if self.cur_iter is None:
                    trigger_hang = True
                elif self.process_status == "other":
                    trigger_hang = True
                else:
                    avg_time = 0.0
                    if self.process_status == "ckpt":
                        all_ckpt = self.data_recorder.get_all_ckpt_times()
                        if all_ckpt:
                            avg_time = sum(all_ckpt) / len(all_ckpt) / 1000
                    elif self.process_status == "eval":
                        all_eval = self.data_recorder.get_all_eval_times()
                        if all_eval:
                            avg_time = sum(all_eval) / len(all_eval) / 1000

                    if avg_time > 0 and time_since_last_update > avg_time * 2:
                        trigger_hang = True
                    elif avg_time <= 0:
                        logger.info(f"No historical {self.process_status} data, skip hang check")

                if trigger_hang:
                    self.trigger_alert("hang", f"No new log content detected! Exceeded {timeout_threshold} seconds")
                    self.alerts['no_update'] = True

            if not self.has_received_first_log.is_set():
                if not self.alerts['no_update'] and current_time - self.last_normal_alert_time >= global_config.STARTUP_NO_LOG_TIMEOUT_SEC:
                    self.trigger_alert("hang", f"1st log not updated for long! Exceeded {global_config.STARTUP_NO_LOG_TIMEOUT_SEC}s.")
                    self.has_received_first_log.set()

            time.sleep(5)

    def _tail_log_file(self):
        """模拟 tail -f 功能，实时读取日志文件"""
        file_handle = None
        # Scan immediately on startup; subsequent scans follow CHECK_NEW_LOG_INTERVAL.
        last_check_time = 0.0
        last_log_file: Optional[Path] = None

        while self.running:
            latest_result = None
            if self.tracker.log_file and last_log_file is None:
                # Wait for this exact path once; never scan its directory.
                latest_result = self.tracker.get_latest_log_file()
            elif not self.tracker.log_file and self.tracker.should_check_new_log(last_check_time):
                last_check_time = time.time()
                latest_result = self.tracker.get_latest_log_file()

                # 日志文件变化
            if latest_result:
                latest_file, latest_offset_file, latest_read_offset = latest_result
                self.tracker.current_log_file = latest_file

                if self.tracker.current_log_file != last_log_file and self.tracker.current_log_file:
                    if self.tracker.log_file:
                        logger.info(f"Bind explicit log file: {self.tracker.current_log_file}")
                    else:
                        logger.info(f"A new log file is detected. Switch to: {self.tracker.current_log_file}")
                    self.fault_handler.clear(reason="log_file_changed")

                    if file_handle:
                        try:
                            file_handle.close()
                        except Exception:
                            pass
                        file_handle = None

                    last_log_file = self.tracker.current_log_file
                    self.tracker.current_offset_file = latest_offset_file
                    self.tracker.current_offset = latest_read_offset
                    self.tracker.line_read_counter = 0

            if not self.tracker.current_log_file or not self.tracker.current_log_file.exists():
                time.sleep(self.tracker.tail_interval)
                continue

            if not file_handle:
                file_handle, self.tracker.current_offset = self.tracker.open_log_file(
                    self.tracker.current_log_file, self.tracker.current_offset
                )
                if not file_handle:
                    time.sleep(1)
                    continue

            try:
                line = file_handle.readline()
                if line:
                    if not self.has_received_first_log.is_set():
                        self.has_received_first_log.set()
                        self.last_normal_alert_time = time.time()
                        self.trigger_alert("normal", "")
                        logger.info("The first log has been read.")
                    else:
                        current_time = time.time()
                        if (current_time - self.last_normal_alert_time) >= self.normal_alert_interval:
                            self.trigger_alert("normal", "Log is updating normally.")
                            self.last_normal_alert_time = current_time

                    # 解析并处理
                    data_dict = self.parse_log(line)
                    self._handle_parsed_event(data_dict)

                    # 更新偏移量
                    self.tracker.current_offset = file_handle.tell()
                    self.tracker.line_read_counter += 1

                    # 定期保存偏移量
                    if self.tracker.should_save_offset():
                        if self.iter_dumper_enabled:
                            self.iter_dumper.flush()
                        self.tracker.write_file_offset(
                            self.tracker.current_offset_file,
                            self.tracker.current_offset
                        )
                        self.tracker.mark_offset_saved()

                else:
                    if self.tracker.fixed_file_needs_reopen(
                        file_handle, self.tracker.current_offset
                    ):
                        logger.info(
                            "Fixed log file was truncated or replaced, reopen from beginning: %s",
                            self.tracker.current_log_file,
                        )
                        file_handle.close()
                        file_handle = None
                        self.tracker.current_offset = 0
                        self.tracker.line_read_counter = 0
                        continue

                    now = time.time()
                    if self.tracker.line_read_counter and now - self.tracker.last_offset_save_time >= self.tracker.TIME_UPDATE_INTERVAL:
                        if self.iter_dumper_enabled:
                            self.iter_dumper.flush()
                        self.tracker.write_file_offset(
                            self.tracker.current_offset_file,
                            self.tracker.current_offset
                        )
                        self.tracker.mark_offset_saved()

                    time.sleep(self.tracker.tail_interval)

            except Exception as e:
                logger.error(f"Failed to read log line: {e}")
                if file_handle:
                    try:
                        file_handle.close()
                    except Exception:
                        pass
                file_handle = None
                time.sleep(1)

        # 退出时保存最终偏移量
        if file_handle:
            try:
                self.tracker.current_offset = file_handle.tell()
                file_handle.close()
            except Exception:
                pass
        if self.tracker.current_offset_file and self.tracker.current_offset >= 0:
            if self.iter_dumper_enabled:
                self.iter_dumper.flush()
            self.tracker.write_file_offset(
                self.tracker.current_offset_file,
                self.tracker.current_offset,
                stopped=True
            )
        logger.info(f"Log monitor has stopped. Last offset: {self.tracker.current_offset}")


def main():
    parser = argparse.ArgumentParser(description='训练日志实时监控工具')
    log_source = parser.add_mutually_exclusive_group(required=True)
    log_source.add_argument('--log-dir', '-f', help='监控目录中最新的 log*-YYYY-MM-DD-HHMM*.log')
    log_source.add_argument('--log-file', help='精确监控指定日志文件，不限制文件名')
    parser.add_argument('--alert-threshold', '-t', type=int, default=1000, help='告警阈值(ms)')
    parser.add_argument('--no-update-threshold', '-nu', type=int, default=600, help='无更新告警阈值(秒)')
    parser.add_argument(
        '--mode',
        choices=['standalone', 'fault_tolerance', 'k8s'],
        default=_default_monitor_mode(),
        help='输出模式：standalone=故障发飞书；fault_tolerance=容错内通过event_bus；k8s=写故障marker并保留JSONL审计记录'
    )
    parser.add_argument('--job-name', default=None, help='任务名称，仅用于飞书展示')
    args = parser.parse_args()
    if args.mode == "fault_tolerance":
        logger.warning("fault_tolerance mode requires an external EventBus; CLI will fallback to standalone mode.")
        args.mode = "standalone"

    monitor = LogMonitor(
        job_name=args.job_name,
        mode=args.mode,
        log_dir=args.log_dir,
        log_file=args.log_file,
        alert_threshold_ms=args.alert_threshold,
        no_update_threshold=args.no_update_threshold,
    )
    monitor.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Log monitor process stopped by user.")
    finally:
        monitor.stop()


if __name__ == "__main__":
    main()
