# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# cluster_manager/runtime/job_state_machine.py
from enum import Enum, auto
from datetime import datetime
from typing import Union
from cluster_manager.event.event_bus import Event, EventType
from cluster_manager.runtime.run_state_manager import RunState
from cluster_manager.platform.notify import Notify
from cluster_manager.config.global_config import logger, TRAIN_NO_UPDATE_THRESHOLD
import math


class JobCommand(Enum):
    START_TRAINING = auto()
    STOP_TRAINING = auto()
    SBATCH_QUEUE = auto()
    START_LOG_MONITOR = auto()
    NONE = auto()


class Signal(Enum):
    START_SUCCESS = auto()
    STOP_SUCCESS = auto()
    LOG_RUNNING = auto()
    LOG_HANG = auto()
    LOG_LOSS = auto()
    LOG_INF = auto()
    LOG_EXIT = auto()
    LOG_TIMEOUT = auto()
    LOG_ITER = auto()
    NODE_ABNORMAL = auto()
    JOB_RELEASE = auto()


class JobStateMachine:
    """
    训练任务状态机

    设计原则：状态和命令分离
    - 状态驱动（next_action 主动推进）：STARTING / RECOVERING
    - 事件驱动（等待外部事件）：PENDING / RUNNING
    - 事件即时处理（不改状态）：HANG → 重启 / LOSS_DIVERGENCE → 告警
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.notify = Notify()
        self._flow_trace = []
        self._current_trace = None
        self.start_time = None
        self.first_iter_time = None
        self.first_iter = None
        self.last_iter_time = None
        self.last_iter = None
        self.last_ckpt_iter_time = None
        self.last_ckpt_iter = None
        self.stop_time = None

        # 通用信号 handler：NODE_ABNORMAL 在所有状态下行为一致
        self._common_handlers = {
            Signal.NODE_ABNORMAL: self._handle_node_abnormal,
            Signal.JOB_RELEASE: self._handle_job_release,
        }
        # 状态特定 handler：只定义与通用不同的信号
        self._state_handlers = {
            RunState.STARTING: {
                Signal.START_SUCCESS: self._to_pending,
            },
            RunState.PENDING: {
                Signal.LOG_RUNNING: self._to_running,
                Signal.LOG_TIMEOUT: self._pending_timeout,
                Signal.LOG_HANG: self._to_hang,
            },
            RunState.RUNNING: {
                Signal.LOG_HANG: self._to_hang,
                Signal.LOG_LOSS: self._to_loss,
                Signal.LOG_INF: self._to_inf,
                Signal.LOG_EXIT: self._handle_log_exit,
                Signal.LOG_ITER: self._to_iter,
            },
            RunState.RECOVERING: {
                Signal.STOP_SUCCESS: self._recovery_stop_ok,
                Signal.START_SUCCESS: self._recovery_start_ok,
            },
            RunState.FAULT_RECOVER: {
                Signal.LOG_HANG: self._to_hang,
                Signal.LOG_LOSS: self._to_loss,
                Signal.LOG_INF: self._to_inf,
                Signal.LOG_EXIT: self._handle_log_exit,
                Signal.LOG_ITER: self._to_iter,
            },
        }

    # ==================== 核心接口 ====================

    def next_action(self):
        """状态驱动推进：根据当前状态返回下一步命令"""
        state = self.ctx.run_state
        if state == RunState.STARTING:
            return JobCommand.START_TRAINING
        elif state == RunState.RECOVERING:
            if self.ctx.recovery_phase == 'stopping':
                return JobCommand.STOP_TRAINING
            elif self.ctx.recovery_phase == 'starting':
                return JobCommand.START_TRAINING
        return JobCommand.NONE

    def init_state(self):
        """初始化状态机：根据磁盘状态设置初始状态，由 Manager 初始化时调用"""
        state = self.ctx.run_state
        logger.info(f"[StateMachine][{self.ctx.job_name}] Init state: {state}")

        if state == RunState.INIT:
            self.ctx.node_pool_proxy.release_runing_nodes()
            self._set_state(RunState.STARTING)
        elif state == RunState.RECOVERING:
            # 重启时恢复 RECOVERING 状态，需要重新初始化 recovery_phase
            logger.info(f"[StateMachine][{self.ctx.job_name}] Resume RECOVERING, set phase to 'stopping'")
            self.ctx.recovery_phase = 'stopping'
        elif state not in (RunState.PENDING, RunState.RUNNING):
            self._to_recovering(f"init recovery from {state.name}")

    def on_event(self, event: Event):
        """跨线程事件入口：处理 LOG_MONITOR 和 NHC_MONITOR 事件"""
        logger.info(f"[on_event] type={event.type}, payload={event.payload}")
        signal = self._event_to_signal(event)
        if signal is None:
            logger.warning(f"[{self.ctx.job_name}] Unhandled event type: {event.type}")
            return JobCommand.NONE
        return self._dispatch_with_trace(event.type, signal, event.payload)

    def on_train_success(self, action, node_list=None, error_info=""):
        """同线程调用：训练操作成功结果反馈（Manager → StateMachine）"""
        signal = Signal.START_SUCCESS if action == "start" else Signal.STOP_SUCCESS
        payload = {"node_list": node_list, "error_info": error_info}
        now = datetime.now()
        if signal == Signal.START_SUCCESS:
            self.start_time = now
            self.ctx.node_pool_proxy.add_current_snapshot_param(start_time=now)
        elif signal == Signal.STOP_SUCCESS:
            self.stop_time = now
            self.ctx.node_pool_proxy.add_current_snapshot_param(stop_time=now)
        return self._dispatch_with_trace(f"train_{action}", signal, payload)

    # ==================== 信号分发 ====================

    def _event_to_signal(self, event: Event):
        """EventBus 事件 → 内部 Signal（仅处理 LOG_MONITOR 和 NHC_MONITOR）"""
        if event.type == EventType.LOG_MONITOR:
            return {
                'hang': Signal.LOG_HANG, 'exit': Signal.LOG_EXIT,
                'loss': Signal.LOG_LOSS, 'normal': Signal.LOG_RUNNING,
                'timeout': Signal.LOG_TIMEOUT, 'iter': Signal.LOG_ITER,
                'inf': Signal.LOG_INF,
            }.get(event.payload.get('type'))
        elif event.type == EventType.NHC_MONITOR:
            return {
                'job_release': Signal.JOB_RELEASE,
            }.get(event.payload.get('type'), Signal.NODE_ABNORMAL)
        return None

    def _dispatch_with_trace(self, source, signal, payload=None):
        """信号分发 + trace 记录（on_event 和 on_train_success 共用）"""
        self._current_trace = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'before_state': self.ctx.run_state.name,
            'current_state': self.ctx.run_state.name,
            'event': source, 'signal': signal.name, 'handler': None,
            'next_command': None, 'extra': [], 'error': False
        }
        try:
            command = self._dispatch(signal, payload)
            self._current_trace['current_state'] = self.ctx.run_state.name
            return command
        except Exception as e:
            self._current_trace['error'] = True
            self._current_trace['extra'].append(f"Exception: {e}")
            logger.exception(f'--------报错{e}------------')
            return self._require_manual_intervention(f"exception {e}")
        finally:
            if self._current_trace and not self._current_trace.get('_added'):
                self._add_current_trace()
            self._current_trace = None

    def _dispatch(self, signal, payload=None):
        """查表分发：状态特定优先，通用兜底"""
        handler = self._state_handlers.get(self.ctx.run_state, {}).get(signal)
        if not handler:
            handler = self._common_handlers.get(signal)
        if handler:
            self._current_trace['handler'] = handler.__name__
            command = handler(payload)
            self._current_trace['next_command'] = command.name
            return command
        self._current_trace['extra'].append(
            f"No handler for {self.ctx.run_state.name}/{signal.name}")
        return JobCommand.NONE

    # ==================== 状态转换 ====================
    def _handle_job_release(self, payload):
        return self._to_recovering('job released from queue')

    def _set_state(self, new_state: RunState):
        old_state = self.ctx.run_state
        logger.info(f"[JobStateMachine] {old_state.name} -> {new_state.name}")
        self.ctx.set_state(new_state)

    def _to_pending(self, payload=None):
        self._set_state(RunState.PENDING)
        return JobCommand.NONE

    def _pending_timeout(self, payload=None):
        return self._to_recovering('pending timeout')

    def _to_running(self, payload=None):
        try:
            self.calculate_training_metrics()
        except Exception as e:
            logger.exception(f'_to_running metrics error: {e}')
        self.ctx.retry_info.reset()
        self._set_state(RunState.RUNNING)
        return JobCommand.NONE

    def _to_hang(self, payload=None):
        """HANG 故障：预留检测定位 → 触发重启恢复"""
        self._diagnose_hang(payload)
        return self._to_recovering("hang detected, trigger recovery")

    def _to_loss(self, payload=None):
        """LOSS_DIVERGENCE 故障：预留检测定位 → 告警 + 触发恢复"""
        self._diagnose_loss(payload)
        data = (payload or {}).get("data", {})
        if "type" in data:
            # 传递完整的 payload，包含 data、cur_iter、timestamp 等信息
            self.ctx.handle_runtime_fault(data.get("rank", ""), "global_rank", fault_reason=payload)
            return self._to_recovering("loss divergence detected, trigger recovery")
        return JobCommand.NONE

    def _to_inf(self, payload=None):
        """INF_DIVERGENCE 故障：预留检测定位 → 告警 + 触发恢复"""
        self._diagnose_inf(payload)
        data = (payload or {}).get("data", {})
        if "type" in data:
            # 传递完整的 payload，包含 data、cur_iter、timestamp 等信息
            self.ctx.handle_runtime_fault(data.get("rank", ""), "global_rank", fault_reason=payload)
        return self._to_recovering("inf divergence detected, trigger recovery")

    def _diagnose_hang(self, payload):
        """HANG 检测定位（预留）"""
        message = f"[{self.ctx.job_name}] HANG detected, diagnose placeholder"
        logger.warning(message)
        self.notify.send_feishu_alert(message)

    def _diagnose_loss(self, payload):
        """LOSS 检测定位 + 告警（预留）"""
        message = f"[{self.ctx.job_name}] loss divergence detected"
        logger.warning(message)
        self.notify.send_feishu_alert(message)

    def _diagnose_inf(self, payload):
        """INF 检测定位 + 告警（预留）"""
        message = f"[{self.ctx.job_name}] inf divergence detected"
        logger.warning(message)
        self.notify.send_feishu_alert(message)

    def _to_iter(self, payload):
        first_iter_flag = payload.get("is_first", False)
        ckpt_iter_flag = payload.get("is_ckpt", False)

        snapshot = self.ctx.node_pool_proxy.get_current_snapshot()
        snapshot_params = {}

        if snapshot and snapshot.first_iter is None:
            self.first_iter_time = payload.get("timestamp")
            self.first_iter = payload.get("cur_iter")
            logger.info("[ToIter] First iteration detected, record first_iter and first_iter_time")
            snapshot_params["first_iter"] = self.first_iter
            snapshot_params["first_iter_time"] = self.first_iter_time


        if ckpt_iter_flag:
            self.last_ckpt_iter_time = payload.get("timestamp")
            self.last_ckpt_iter = payload.get("cur_iter")
            snapshot_params["last_ckpt_iter"] = self.last_ckpt_iter
            snapshot_params["last_ckpt_iter_time"] = self.last_ckpt_iter_time

        # 最后迭代信息赋值保持不变
        self.last_iter_time = payload.get("timestamp")
        self.last_iter = payload.get("cur_iter")
        snapshot_params["last_iter"] = self.last_iter
        snapshot_params["last_iter_time"] = self.last_iter_time

        if snapshot_params:  # 只有存在需要更新的参数时，才执行落盘
            self.ctx.node_pool_proxy.add_current_snapshot_param(** snapshot_params)

        return JobCommand.NONE

    def _to_recovering(self, reason):
        if self.ctx.run_state == RunState.RECOVERING:
            logger.info(f"[JobStateMachine] Already RECOVERING, skip: {reason}")
            return JobCommand.NONE
        
        """进入 RECOVERING 状态，记录快照，返回 STOP_TRAINING"""
        now = datetime.now()
        self.stop_time = now
        self.ctx.node_pool_proxy.add_current_snapshot_param(stop_time=now)
        self._set_state(RunState.RECOVERING)
        self.ctx.recovery_phase = 'stopping'
        logger.info(f"[JobStateMachine] Recovering: {reason}")
        return JobCommand.STOP_TRAINING

    # ==================== 恢复流程 ====================

    def _recovery_stop_ok(self, payload):
        self.ctx.recovery_phase = 'starting'
        return JobCommand.START_TRAINING

    def _recovery_start_ok(self, payload):
        self.ctx.retry_info.reset()
        self._set_state(RunState.PENDING)
        return JobCommand.NONE

    # ==================== 故障处理 ====================

    def _handle_node_abnormal(self, payload):
        nodes = payload.get('abnormal_nodes')
        if nodes:
            return self._to_recovering('node abnormal')
        return JobCommand.NONE

    def _handle_log_exit(self, payload):
        data = payload.get("data", {})
        data_type = data.get("type", "")
        fault_info = data.get("fault_info", "")
        exit_code = data.get("exit_code", "")
        logger.info(f"[{self.ctx.job_name}] Log exit: type={data_type}, fault={fault_info}, code={exit_code}")
        if data_type == "root_cause":
            host = str(data.get("host", "")).strip()
            rank = data.get("rank")
            if host:
                handled = self.ctx.handle_runtime_fault(
                    host, "node", fault_reason=payload
                )
                fault_location = f"host={host}, rank={rank}"
            else:
                return self._require_manual_intervention(
                    "root_cause has no host"
                )

            if not handled:
                return self._require_manual_intervention(
                    f"failed to blacklist root_cause location: {fault_location}"
                )
            return self._to_recovering(
                f"torchrun root_cause at {fault_location}, exit_code={exit_code}"
            )
        if data_type in ("global_rank", "rank", "node"):
            self.ctx.handle_runtime_fault(
                fault_info, data_type, fault_reason=payload
            )
            return self._to_recovering(f"exit with {fault_info} fault")
        return self._require_manual_intervention(f'exit with unknown type: {data_type}')

    def _require_manual_intervention(self, reason):
        message = f"Manual intervention required: {reason}"
        self._add_trace_extra(message)
        message = f"训练中遇到软件故障{reason}，请及时处理，{TRAIN_NO_UPDATE_THRESHOLD} 后将会重启"
        self.notify.send_feishu_alert(message)
        return JobCommand.NONE

    # ==================== 追踪 ====================

    def _add_current_trace(self):
        if self._current_trace and not self._current_trace.get('_added'):
            self._flow_trace.append(self._current_trace.copy())
            logger.info(f"[JobStateMachine] Trace {self._current_trace}")
            self._current_trace['_added'] = True
            if len(self._flow_trace) > 30:
                self._flow_trace.pop(0)

    def _add_trace_extra(self, info):
        if self._current_trace:
            self._current_trace.setdefault('extra', []).append(info)

    # ==================== 训练指标 ====================

    def safe_timestamp_to_datetime(self, ts: Union[str, int, float, None]) -> datetime:
        """将时间字符串/数字时间戳/None 安全转换为 datetime"""
        if ts is None:
            return datetime.now()
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts)
        if isinstance(ts, str):
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(ts, fmt)
                except ValueError:
                    continue
            try:
                return datetime.fromtimestamp(float(ts))
            except ValueError:
                pass
        raise ValueError(f"无法解析时间: {ts}")

    def fmt_duration(self, seconds):
        if seconds is None or math.isnan(seconds):
            return "0秒"
        sec = int(seconds)
        h, remainder = divmod(sec, 3600)
        m, s = divmod(remainder, 60)
        if h > 0:
            return f"{h}小时{m}分钟{s}秒"
        elif m > 0:
            return f"{m}分钟{s}秒"
        return f"{s}秒"

    def _calculate_history_metrics(self) -> str:
        """计算历史故障汇总信息"""
        from cluster_manager.config.global_config import SNAPSHOT_START_OFFSET
        snapshots = self.ctx.node_pool_proxy.get_snapshots()
        if not snapshots:
            return "━━━ 历史汇总 ━━━\n无历史故障数据"

        history_fault_seconds = 0.0
        history_invalid_steps = 0
        history_invalid_seconds = 0.0
        prev_snap = None
        snapshot_details = []
        effective_times = []
        snap_intervals = []
        sorted_items = sorted(snapshots.items(), key=lambda x: x[0])

        offset = SNAPSHOT_START_OFFSET
        if offset > 0:
            if offset >= len(sorted_items):
                return f"━━━ 历史汇总 ━━━\n偏移量 {offset} 大于等于快照总数 {len(sorted_items)}，无数据"
            sorted_items = sorted_items[offset:]

        first_start_dt = None
        now_dt = datetime.now()

        for idx, (snap_key, snap) in enumerate(sorted_items):
            if not first_start_dt and snap.start_time:
                first_start_dt = self.safe_timestamp_to_datetime(snap.start_time)
            if prev_snap and prev_snap.stop_time and snap.start_time:
                gap = max(
                    (self.safe_timestamp_to_datetime(snap.start_time) -
                     self.safe_timestamp_to_datetime(prev_snap.stop_time)).total_seconds(), 0)
                history_fault_seconds += gap

            start_iter = snap.first_iter if snap.first_iter is not None else 0
            last_iter = snap.last_iter if snap.last_iter is not None else 0
            last_ckpt_iter = snap.last_ckpt_iter if snap.last_ckpt_iter is not None else start_iter
            effective_steps = max(last_ckpt_iter - start_iter, 0)
            lost_steps = max(last_iter - last_ckpt_iter, 0)

            start_dt = self.safe_timestamp_to_datetime(snap.start_time) if snap.start_time else None
            last_iter_dt = self.safe_timestamp_to_datetime(snap.last_iter_time) if snap.last_iter_time else None
            last_ckpt_dt = self.safe_timestamp_to_datetime(snap.last_ckpt_iter_time) if snap.last_ckpt_iter_time else None

            effective_sec = 0.0
            if start_dt and last_ckpt_dt and last_ckpt_dt > start_dt:
                effective_sec = (last_ckpt_dt - start_dt).total_seconds()
            lost_sec = 0.0
            if last_ckpt_dt and last_iter_dt and last_iter_dt > last_ckpt_dt:
                lost_sec = (last_iter_dt - last_ckpt_dt).total_seconds()

            history_invalid_steps += lost_steps
            history_invalid_seconds += lost_sec
            effective_times.append(effective_sec)
            snap_intervals.append((snap.start_time, snap.stop_time))

            train_start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else "无"
            train_stop_str = self.safe_timestamp_to_datetime(snap.stop_time).strftime('%Y-%m-%d %H:%M:%S') if snap.stop_time else "无"
            snapshot_details.append(
                f"第{idx+1}次：开始iter={start_iter}，结束iter={last_iter}，checkpoint iter={last_ckpt_iter}\n"
                f"           有效步数 {effective_steps} 步（{self.fmt_duration(effective_sec)}），"
                f"无效步数 {lost_steps} 步（{self.fmt_duration(lost_sec)}）\n"
                f"           训练开始：{train_start_str} | 训练结束：{train_stop_str}\n")
            prev_snap = snap

        if effective_times:
            n = len(effective_times)
            total_effective = sum(effective_times)
            first_start_ts, last_stop_ts = snap_intervals[0][0], snap_intervals[-1][1]
            if first_start_ts and last_stop_ts:
                total_elapsed = max(
                    (self.safe_timestamp_to_datetime(last_stop_ts) -
                     self.safe_timestamp_to_datetime(first_start_ts)).total_seconds(), 0)
            else:
                total_elapsed = 0
            if total_elapsed > 0:
                efficiency_ratio = total_effective / total_elapsed
                import cluster_manager.config.global_config as gconf
                gconf.EFFICIENCY_FACTOR = efficiency_ratio
                logger.info(f"[HistoryMetrics] 最近{n}次快照：有效={total_effective:.2f}s，"
                            f"总计={total_elapsed:.2f}s，效率={efficiency_ratio:.4f}")

        history_run_seconds = max((now_dt - first_start_dt).total_seconds(), 0) if first_start_dt else 0
        history_effective_seconds = sum(effective_times)
        effective_percent = (history_effective_seconds / history_run_seconds * 100) if history_run_seconds > 0 else 0
        return (
            f"━━━ 历史汇总 ━━━\n"
            f"累计故障：{len(sorted_items)} 次    故障总时长：{self.fmt_duration(history_fault_seconds)}\n"
            f"各轮次步数明细：\n{''.join(snapshot_details)}\n"
            f"总无效步数：{history_invalid_steps} 步（{self.fmt_duration(history_invalid_seconds)}）\n"
            f"总运行时长：{self.fmt_duration(history_run_seconds)}    "
            f"有效训练：{self.fmt_duration(history_effective_seconds)}（{effective_percent:.1f}%）")

    def calculate_training_metrics(self):
        """计算训练有效时长、无效步数及故障时间等指标，并汇总历史故障"""
        last_snapshot = self.ctx.node_pool_proxy.get_last_snapshot()
        if not last_snapshot:
            logger.info("首次启动，无历史快照，跳过训练指标统计")
            return
        current_start_ts = self.start_time
        if not current_start_ts:
            logger.warning("本次启动时间为空，跳过统计")
            return

        start_ts = last_snapshot.start_time
        stop_ts = last_snapshot.stop_time
        first_iter = last_snapshot.first_iter
        last_iter = last_snapshot.last_iter
        last_ckpt_iter = last_snapshot.last_ckpt_iter

        if not (start_ts and stop_ts):
            logger.warning("Missing data for calculation.")
            return

        start_dt = self.safe_timestamp_to_datetime(start_ts)
        stop_dt = self.safe_timestamp_to_datetime(stop_ts)
        current_start_dt = self.safe_timestamp_to_datetime(current_start_ts)
        first_iter_dt = self.safe_timestamp_to_datetime(last_snapshot.first_iter_time) if last_snapshot.first_iter_time else None
        last_iter_dt = self.safe_timestamp_to_datetime(last_snapshot.last_iter_time) if last_snapshot.last_iter_time else None
        last_ckpt_iter_dt = self.safe_timestamp_to_datetime(last_snapshot.last_ckpt_iter_time) if last_snapshot.last_ckpt_iter_time else None

        fault_seconds = max((current_start_dt - stop_dt).total_seconds(), 0.0)
        run_seconds = max((stop_dt - start_dt).total_seconds(), 0.0)
        iter_seconds = 0.0
        if first_iter_dt and last_iter_dt:
            iter_seconds = max((last_iter_dt - first_iter_dt).total_seconds(), 0)

        total_steps = max(last_iter - first_iter, 0) if (first_iter and last_iter) else 0

        if last_ckpt_iter is not None and first_iter is not None and last_ckpt_iter >= first_iter:
            lost_steps = max(last_iter - last_ckpt_iter, 0)
            effective_steps = max(last_ckpt_iter - first_iter, 0)
            lost_seconds = max((last_iter_dt - last_ckpt_iter_dt).total_seconds(), 0) if last_ckpt_iter_dt else 0
        else:
            lost_steps = total_steps
            effective_steps = 0
            lost_seconds = iter_seconds

        effective_seconds = 0.0
        if last_ckpt_iter_dt and last_ckpt_iter_dt > start_dt:
            effective_seconds = max((last_ckpt_iter_dt - start_dt).total_seconds(), 0)

        total_cost_seconds = run_seconds + fault_seconds
        effective_percent = (effective_seconds / total_cost_seconds * 100) if total_cost_seconds > 0 else 0

        if first_iter is None or last_iter is None:
            step_info = "步数概览：整体0步，有效0步"
        else:
            step_info = (f"步数概览：整体 {first_iter}→{last_iter} 步，有效 {effective_steps} 步"
                         if effective_steps > 0 else
                         f"步数概览：整体 {first_iter}→{last_iter} 步，有效 0 步")

        total_lost_seconds = fault_seconds + lost_seconds
        current_fault_msg = (
            f"━━━━━━━━━━ 训练重启成功 ━━━━━━━━━━\n"
            f"本次启动时间：{current_start_dt.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"上次故障详情\n"
            f"启动：{start_dt.strftime('%Y-%m-%d %H:%M:%S')} ｜ 结束：{stop_dt.strftime('%Y-%m-%d %H:%M:%S')} ｜ "
            f"运行时长：{self.fmt_duration(run_seconds)} ｜ 故障间隔：{self.fmt_duration(fault_seconds)}\n\n"
            f"{step_info}\n"
            f"无效统计：无效步数 {lost_steps} 步（{self.fmt_duration(lost_seconds)}）｜ "
            f"总无效时长：{self.fmt_duration(total_lost_seconds)}\n"
            f"有效训练：{self.fmt_duration(effective_seconds)}（占比 {effective_percent:.1f}%）")

        summary_log = f"任务ID：{self.ctx.job_name}\n{current_fault_msg}\n{self._calculate_history_metrics()}"
        logger.info(f"[TrainingMetrics] Summary:\n{summary_log}")
        self.notify.send_feishu_alert(summary_log)
