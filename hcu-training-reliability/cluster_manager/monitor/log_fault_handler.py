# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
故障缓存处理器 - 负责故障日志的缓存、归纳、延迟发送

职责：
- 缓存 exit/hang 类故障
- 归纳故障（优先 node/rank/loss/inf，其次 proc+Traceback）
- 延迟发送（FAULT_DELAY 秒后批量发送）
- 日志文件切换或 LogMonitor 停止时清空缓存并作废未触发的定时发送
"""
import threading
from copy import deepcopy
from typing import List, Dict, Any, Optional, Callable

from cluster_manager.config.global_config import logger
from cluster_manager.event.event_bus import Event, EventType


class FaultHandler:
    """
    故障缓存处理器：缓存、归纳、延迟发送故障事件
    """

    def __init__(
        self,
        event_bus=None,
        event_publisher: Optional[Callable] = None,
        fault_delay: int = 20,
        slots_per_node: int = 1,
        get_cur_iter: Optional[Callable] = None,
        get_cur_iter_timestamp: Optional[Callable] = None,
    ):
        """
        Args:
            event_bus: 事件总线
            fault_delay: 延迟发送时间（秒）
            get_cur_iter: 获取当前 iter 的回调
            get_cur_iter_timestamp: 获取当前 iter 时间戳的回调
        """
        self.event_bus = event_bus
        self.event_publisher = event_publisher
        self.FAULT_DELAY = fault_delay
        self.slots_per_node = max(int(slots_per_node), 1)
        self.get_cur_iter = get_cur_iter or (lambda: None)
        self.get_cur_iter_timestamp = get_cur_iter_timestamp or (lambda: None)

        self._lock = threading.Lock()
        self._cache: List[Dict] = []
        self._timer: Optional[threading.Timer] = None
        self._generation = 0

    def add_fault(self, data_dict: Dict):
        """将故障日志添加到缓存，并重置延迟发送定时器"""
        with self._lock:
            self._cache.append(data_dict)
            self._reset_timer()

    def clear(self, reason: str = "fault cache reset"):
        """清空缓存并取消定时器；作废此前已安排的延迟发送（含 cancel 后仍可能触发的 Timer 回调）"""
        timer_to_join: Optional[threading.Timer] = None
        with self._lock:
            self._generation += 1
            if self._timer:
                self._timer.cancel()
                timer_to_join = self._timer
            self._timer = None
            self._cache.clear()
        if timer_to_join is not None:
            timer_to_join.join(timeout=float(self.FAULT_DELAY) + 2.0)
        logger.info("Cleared fault cache (%s).", reason)

    def _reset_timer(self):
        """重置延迟发送定时器（须在持有 self._lock 时调用）"""
        if self._timer:
            self._timer.cancel()
        scheduled_gen = self._generation
        self._timer = threading.Timer(
            self.FAULT_DELAY,
            self._send_fault_message,
            args=(scheduled_gen,),
        )
        self._timer.daemon = True
        self._timer.start()

    def _induce_fault_cache(self) -> List[Dict]:
        """归纳故障缓存：优先 node/rank/loss/inf，其次 proc+Traceback"""
        ranked_tracebacks = []
        for index, item in enumerate(self._cache):
            data = item.get("data", {}) or {}
            if str(data.get("type") or "").lower() != "proc":
                continue
            raw_rank = data.get("traceback_rank")
            if raw_rank is None:
                continue
            try:
                rank = int(raw_rank)
            except (TypeError, ValueError):
                continue
            if rank >= 0:
                ranked_tracebacks.append((index, rank, item))

        if ranked_tracebacks:
            traceback_ranks = sorted({rank for _, rank, _ in ranked_tracebacks})
            traceback_nodes = {
                rank // self.slots_per_node for rank in traceback_ranks
            }
            if len(traceback_nodes) == 1:
                _, first_rank, first_item = min(
                    ranked_tracebacks, key=lambda value: value[0]
                )
                selected = deepcopy(first_item)
                data = selected.setdefault("data", {})
                data.update(
                    {
                        "type": "global_rank",
                        "fault_pid": str(first_rank),
                        "rank": first_rank,
                        "traceback_ranks": traceback_ranks,
                        "traceback_node_no": next(iter(traceback_nodes)),
                        "message": "single-node traceback aggregation",
                    }
                )
                logger.info(
                    "Single-node traceback aggregation: ranks=%s, node_no=%s",
                    traceback_ranks,
                    data["traceback_node_no"],
                )
                return [selected]

            logger.info(
                "Tracebacks span multiple nodes; keep legacy proc handling: "
                "ranks=%s, node_nos=%s",
                traceback_ranks,
                sorted(traceback_nodes),
            )

        def priority(item: Dict) -> int:
            data = item.get("data", {}) or {}
            detail_type = str(data.get("type") or "").lower()
            has_explicit_node = bool(
                data.get("node")
                or data.get("fault_node")
                or data.get("target_node")
                or data.get("node_name")
                or detail_type == "node"
            )
            if has_explicit_node:
                return 300
            if detail_type == "root_cause":
                return 200
            if detail_type in {"rank", "loss", "inf", "nan"}:
                return 150
            if detail_type == "communication":
                return 100

            fault_pid = data.get("fault_pid") or data.get("fault_info")
            if detail_type == "proc" and isinstance(fault_pid, str):
                try:
                    exit_code = int(data.get("exit_code"))
                except (TypeError, ValueError):
                    exit_code = 0
                if exit_code != 0:
                    return 125
                if fault_pid.startswith("Traceback (most recent call last):"):
                    return 0
            return -1

        candidates = []
        for index, item in enumerate(self._cache):
            item_priority = priority(item)
            if item_priority >= 0:
                candidates.append((item_priority, -index, item))
        if not candidates:
            return []

        return [max(candidates, key=lambda candidate: (candidate[0], candidate[1]))[2]]

    def _send_fault_message(self, scheduled_gen: int):
        """定时器到期后，归纳并发送故障事件"""
        with self._lock:
            if scheduled_gen != self._generation:
                return
            induced_faults = self._induce_fault_cache()
            if not induced_faults:
                logger.info("The fault cache is empty, no need to send.")
                return

            for fault_msg in induced_faults:
                data = fault_msg.get("data", {})
                if "fault_pid" in data:
                    fault_pid_value = data.pop("fault_pid")
                    data["fault_info"] = fault_pid_value

                event = Event(
                    type=EventType.LOG_MONITOR,
                    payload={
                        "type": fault_msg.get("type"),
                        "data": data,
                        "cur_iter": self.get_cur_iter(),
                        "timestamp": self.get_cur_iter_timestamp(),
                    }
                )
                if self.event_publisher:
                    self.event_publisher(event)
                elif self.event_bus:
                    self.event_bus.publish(event)
                else:
                    logger.warning("No fault event publisher configured, drop event: %s", event)
                logger.info(f"fault message sent: type={fault_msg.get('type')}, fault_type={data.get('type')}")

            self._cache.clear()
            self._timer = None
