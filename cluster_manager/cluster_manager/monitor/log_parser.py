# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
日志行解析器 - 负责将日志行解析为结构化数据

支持两种解析模式：
1. Megatron：默认训练日志解析器
2. Special：特定训练框架日志兼容解析器
"""
import re
import time
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from cluster_manager.config.global_config import logger
from cluster_manager.monitor.log_patterns import COMPILED_PATTERNS, ERROR_PATTERNS


class TorchrunRootCauseParser:
    """Add stateful parsing for torchrun's multi-line Root Cause block."""

    _START_PATTERN = re.compile(r"Root Cause \(first observed failure\):")
    _TIME_PATTERN = re.compile(r"^\s*time\s*:\s*(.+?)\s*$")
    _HOST_PATTERN = re.compile(r"^\s*host\s*:\s*(\S+)\s*$")
    _RANK_PATTERN = re.compile(
        r"^\s*rank\s*:\s*(-?\d+)\s*\(local_rank:\s*(-?\d+)\)\s*$"
    )
    _EXIT_CODE_PATTERN = re.compile(r"^\s*exitcode\s*:\s*(-?\d+)(?:\s*\(.*\))?\s*$")

    def __init__(self, line_parser: Callable[[str], Optional[Dict[str, Any]]]):
        self.line_parser = line_parser
        self._active = False
        self._fields: Dict[str, Any] = {}
        self._lines_seen = 0

    def _reset(self) -> None:
        self._active = False
        self._fields = {}
        self._lines_seen = 0

    def __call__(self, line: str) -> Optional[Dict[str, Any]]:
        if self._START_PATTERN.search(line):
            self._active = True
            self._fields = {}
            self._lines_seen = 0
            return None

        if not self._active:
            return self.line_parser(line)

        self._lines_seen += 1
        if self._lines_seen > 100:
            self._reset()
            return self.line_parser(line)

        if match := self._TIME_PATTERN.search(line):
            self._fields["failure_time"] = match.group(1)
            return None
        if match := self._HOST_PATTERN.search(line):
            self._fields["host"] = match.group(1)
            return None
        if match := self._RANK_PATTERN.search(line):
            self._fields["rank"] = int(match.group(1))
            self._fields["local_rank"] = int(match.group(2))
            return None
        if match := self._EXIT_CODE_PATTERN.search(line):
            self._fields["exit_code"] = int(match.group(1))
            required = {"host", "rank", "local_rank", "exit_code"}
            if required.issubset(self._fields):
                data = dict(self._fields)
                data.update({"type": "root_cause", "first_observed": True})
                self._reset()
                return {"type": "exit", "data": data}
            return None

        return None


def create_log_parser(log_parser_type: str = "base", data_recorder=None) -> Callable[[str], Optional[Dict[str, Any]]]:
    """
    工厂方法：根据训练框架类型创建对应的日志解析函数

    Args:
        log_parser_type: 日志解析器类型，"base" 表示 Megatron 日志，"special" 表示其他特定训练框架日志
        data_recorder: 保留参数，用于兼容现有调用

    Returns:
        解析函数 (line) -> Optional[Dict]
    """
    if log_parser_type == "base":
        from cluster_manager.monitor.mega.analyze_mega_log import create_analyze_mega_log
        line_parser = create_analyze_mega_log()
    elif log_parser_type == "special":
        from cluster_manager.monitor.special.analyze_special_log import (
            create_analyze_special_log,
        )
        line_parser = create_analyze_special_log()
    else:
        raise ValueError(
            f"Unsupported LOG_PARSER_TYPE: {log_parser_type!r}; expected 'base' or 'special'"
        )

    return TorchrunRootCauseParser(line_parser)
