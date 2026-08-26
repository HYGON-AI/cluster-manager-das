# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import threading
from typing import Callable, Optional

class ThreadWrapper:
    """线程包装工具（原线程创建逻辑封装）"""
    @staticmethod
    def create_daemon_thread(target: Callable, args: tuple = (), name: Optional[str] = None) -> threading.Thread:
        """创建守护线程（原逻辑保留）"""
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        return thread