# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# cluster_manager/event/event_bus.py

import threading
from queue import Queue, Empty
from typing import Callable, List
from cluster_manager.config.global_config import logger

class EventType:
    """
    系统统一事件类型
    """

    TRAIN = "TRAIN"
    LOG_MONITOR = "LOG_MONITOR"
    NHC_MONITOR = "NHC_MONITOR"
    PLATFORM = "PLATFORM"
# {
#     "type": "TRAIN",
#     "payload": {
#         "action": "start | stop,
#         "success": bool,
#         "node_list": list[str],
#         "error_info": str
#     }
# }

# {
#     "type": "LOG_MONITOR",
#     "payload": {
#         "type": hang/normal/loss/exit/,        # 是否启动成功
#         "xxx": xxx, 
#         "...": ...      
#     }
# }

class Event:
    """
    EventBus 传递的事件对象
    """

    def __init__(self, type: str, payload: dict | None = None):
        self.type = type
        self.payload = payload or {}

    def __repr__(self):
        return f"Event(type={self.type}, payload={self.payload})"


class EventBus:
    """
    多线程事件总线（线程安全队列）

    生产者：各线程通过 publish() 投递事件
    消费者：通过 get_event() / get_event_nowait() 取出事件
    """

    def __init__(self):
        self._queue = Queue()

    def publish(self, event: Event):
        """发布事件（线程安全）"""
        self._queue.put(event)

    def get_event(self, timeout=2):
        """阻塞获取事件，超时返回 None"""
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None

    def get_event_nowait(self):
        """非阻塞获取事件，无事件时返回 None"""
        try:
            return self._queue.get_nowait()
        except Empty:
            return None