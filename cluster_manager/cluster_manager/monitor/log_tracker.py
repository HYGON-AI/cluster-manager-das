# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
日志文件追踪器 - 负责日志文件发现、偏移量管理、tail -f 读取

"""
import os
import re
import io
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

import cluster_manager.config.global_config as global_config
from cluster_manager.config.global_config import logger

log_prefix = "log"
workspace = f"{global_config.WORK_DIR}/workspace"


def k8s_offset_settings(log_dir: Path, environ=None):
    env = environ if environ is not None else os.environ
    pod_uid = str(env.get("FT_POD_UID", "")).strip()
    offset_root = Path(env.get("FT_LOG_DIR") or log_dir) / ".offset"
    if not pod_uid:
        logger.warning(
            "[LogMonitor] FT_POD_UID is empty; disable persistent offset to avoid "
            "reusing an offset from a previous Pod generation."
        )
        return offset_root, False
    return offset_root / pod_uid, True


class LogFileTracker:
    """
    日志文件追踪器：发现最新日志文件、管理字节偏移量、逐行读取
    
    职责：
    - 在 log_dir 中发现最新的 log-*.log 文件
    - 读取/写入 .offset 文件实现断点续读
    - 维护文件句柄，提供 readline 接口
    - 检测日志文件切换

    """

    def __init__(
        self,
        log_dir: Optional[Path] = None,
        log_file: Optional[Path] = None,
        offset_dir: Optional[Path] = None,
        persist_offset: bool = True,
        line_update_interval: int = 500,
        time_update_interval: int = 30,
        check_new_log_interval: int = 10,
    ):
        self.log_file = Path(log_file) if log_file else None
        self.log_dir = Path(log_dir) if log_dir else (
            self.log_file.parent if self.log_file else Path(global_config.LOG_DIR)
        )
        self.offset_dir = Path(offset_dir) if offset_dir else Path(workspace)
        self.persist_offset = persist_offset
        self.LINE_UPDATE_INTERVAL = line_update_interval
        self.TIME_UPDATE_INTERVAL = time_update_interval
        self.CHECK_NEW_LOG_INTERVAL = check_new_log_interval

        # 当前追踪状态
        self.current_log_file: Optional[Path] = None
        self.current_offset_file: Optional[Path] = None
        self.current_offset: int = 0
        self.line_read_counter: int = 0
        self.last_offset_save_time: float = time.time()

        self.tail_interval = 0.1

    def get_latest_log_file(self) -> Optional[Tuple[Path, Path, int]]:
        """获取 log_dir 下最新的日志文件（按文件名中的时间戳排序）"""
        if self.log_file:
            if not self.log_file.exists() or not self.log_file.is_file():
                return None

            offset_file = self.get_offset_file(self.log_file)
            read_offset = self.read_file_offset(offset_file, resume_stopped=True)
            try:
                if read_offset > self.log_file.stat().st_size:
                    logger.info(
                        "Saved offset %s exceeds fixed log file size, reset to beginning: %s",
                        read_offset,
                        self.log_file,
                    )
                    read_offset = 0
            except OSError as e:
                logger.warning("Failed to stat fixed log file %s: %s", self.log_file, e)
                return None
            return self.log_file, offset_file, read_offset

        if not self.log_dir.exists():
            logger.info(f"Log dir does not exist: {self.log_dir}")
            return None

        log_pattern = re.compile(rf"^{re.escape(log_prefix)}.*\.log$")
        timestamp_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{4})")

        log_files = []
        for file in self.log_dir.iterdir():
            if not file.is_file():
                continue
            if not log_pattern.match(file.name):
                continue
            ts_match = timestamp_pattern.search(file.name)
            if ts_match:
                try:
                    file_datetime = datetime.strptime(ts_match.group(1), "%Y-%m-%d-%H%M")
                    log_files.append((file_datetime, file))
                except ValueError:
                    continue

        if not log_files:
            return None

        log_files.sort(reverse=True, key=lambda x: x[0])
        latest_file = log_files[0][1]
        # 检查最新日志文件的 offset 文件是否有 stopped 标志，同时读取偏移量
        latest_offset_file = self.get_offset_file(latest_file)
        read_offset = self.read_file_offset(latest_offset_file)
        if read_offset == -1:
            logger.info(f"Log file {latest_file.name} is marked as stopped, waiting for a new log file.")
            return None

        #logger.info(f"找到最新日志文件: {latest_file.name}（时间戳: {log_files[0][0]}）")
        return (latest_file, latest_offset_file, read_offset)

    def get_offset_file(self, log_file: Path) -> Path:
        """获取偏移量文件路径"""
        offset_filename = log_file.with_suffix('.offset').name
        return self.offset_dir / offset_filename

    def read_file_offset(self, offset_file: Path, resume_stopped: bool = False) -> int:
        """读取偏移量文件中的字节偏移量
        Returns:
            偏移量（>=0），若文件包含 stopped 标志则返回 -1
        """
        if not self.persist_offset:
            return 0

        try:
            if offset_file.exists():
                with open(offset_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:
                        return 0
                    if content.endswith(',stopped') and not resume_stopped:
                        logger.info(f"Offset file {offset_file.name} is marked as stopped.")
                        return -1

                    parts = content.split(',')
                    return int(parts[0]) if parts[0].isdigit() else 0
        except Exception as e:
            logger.error(f"Failed to read the offset file {offset_file}: {e}")
        return 0

    def fixed_file_needs_reopen(self, file_handle, offset: int) -> bool:
        """Detect truncation or replacement of an explicitly monitored file."""
        if not self.log_file or not self.log_file.exists():
            return False
        try:
            opened_stat = os.fstat(file_handle.fileno())
            path_stat = self.log_file.stat()
            return path_stat.st_size < offset or not os.path.samestat(opened_stat, path_stat)
        except (OSError, ValueError):
            return False

    def write_file_offset(self, offset_file: Path, offset: int, stopped: bool = False) -> None:
        """写入字节偏移量到文件（替代原_write_line_number）
            Args:
                stopped: 若为 True，追加 stopped 标志，表示该日志文件已停止监控，不再处理
        """
        if not self.persist_offset:
            return

        try:
            save_content = f"{offset}"
            if stopped:
                save_content += ",stopped"
            offset_file.parent.mkdir(parents=True, exist_ok=True)
            temporary_file = offset_file.with_suffix(offset_file.suffix + ".tmp")
            with open(temporary_file, 'w', encoding='utf-8') as f:
                f.write(save_content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary_file, offset_file)
        except Exception as e:
            logger.error(f"Failed to write to the offset file {offset_file}: {e}")

    def open_log_file(self, log_file: Path, offset: int = 0):
        """打开日志文件并定位到指定偏移量，返回文本文件句柄"""
        try:
            file_handle = open(log_file, 'rb')
            file_size = os.fstat(file_handle.fileno()).st_size
            if offset > file_size:
                logger.info(f"Offset {offset} exceeds file size {file_size}, resetting to end.")
                offset = file_size
            file_handle.seek(offset)
            text_handle = io.TextIOWrapper(file_handle, encoding='utf-8', errors='ignore')
            return text_handle, offset
        except Exception as e:
            logger.error(f"Failed to open log file {log_file}: {e}")
            return None, offset

    def should_save_offset(self) -> bool:
        """判断是否需要保存偏移量"""
        now = time.time()
        if self.line_read_counter >= self.LINE_UPDATE_INTERVAL:
            return True
        if now - self.last_offset_save_time >= self.TIME_UPDATE_INTERVAL:
            return True
        return False

    def mark_offset_saved(self):
        """标记偏移量已保存"""
        self.line_read_counter = 0
        self.last_offset_save_time = time.time()

    def should_check_new_log(self, last_check_time: float) -> bool:
        """判断是否需要检查新日志文件"""
        if self.log_file:
            return False
        return time.time() - last_check_time >= self.CHECK_NEW_LOG_INTERVAL
