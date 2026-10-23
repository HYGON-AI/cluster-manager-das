# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
迭代行转存器 - 负责将迭代行写入 iters.log 和 iters.csv

职责：
- 缓存迭代行
- 批量写入 iters.log（原始行）
- 批量写入 iters.csv（结构化数据）
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional

from cluster_manager.monitor.log_patterns import COMPILED_PATTERNS
from cluster_manager.config.global_config import logger


class IterDumper:
    """
    迭代行转存器：将迭代行写入 iters.log + iters.csv
    """

    CSV_FIELDNAMES = [
        'iter_num',
        'timestamp',
        'elapsed time per iteration (ms)',
        'lm loss',
        'throughput per GPU',
        'grad norm',
        'learning rate',
        'consumed samples'
    ]

    def __init__(self, dump_file: Path, csv_file: Path):
        """
        Args:
            dump_file: iters.log 路径
            csv_file: iters.csv 路径
        """
        self.dump_file = dump_file
        self.csv_file = csv_file
        self._cache: List[str] = []

        # 正则模式（用于 CSV 数据提取）
        self.patterns = {
            'iteration': COMPILED_PATTERNS['COMMON']['iteration'],
            'loss_pattern': COMPILED_PATTERNS['ITERATION']['lm_loss'],
            'tflops_pattern': COMPILED_PATTERNS['ITERATION']['throughput_tflops_per_gpu'],
            'grad_norm_pattern': COMPILED_PATTERNS['ITERATION']['gradient_norm'],
            'learning_rate_pattern': COMPILED_PATTERNS['ITERATION']['learning_rate'],
            'consumed_samples': COMPILED_PATTERNS['ITERATION']['consumed_samples'],
        }

    def add_line(self, line: str):
        """将一行迭代日志加入缓存"""
        self._cache.append(line.rstrip('\n') + '\n')

    def flush(self):
        """将缓存中的迭代行批量写入文件"""
        if not self._cache:
            return

        try:
            self.dump_file.parent.mkdir(parents=True, exist_ok=True)

            # 写 iters.log
            with open(self.dump_file, 'a', encoding='utf-8') as f:
                f.writelines(self._cache)

            # 写 iters.csv
            rows = []
            for line in self._cache:
                row = self._extract_csv_data(line)
                if row:
                    rows.append(row)

            if rows:
                file_exists = self.csv_file.exists()
                with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDNAMES)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(rows)

            dumped_count = len(self._cache)
            self._cache.clear()
            logger.info(f"Successfully dumped {dumped_count} iteration logs to: {self.dump_file}")

        except Exception as e:
            logger.error(f"Failed to dump iteration logs to {self.dump_file}: {e}")

    def _extract_csv_data(self, line: str) -> dict:
        """从迭代行提取 CSV 数据"""
        data = {}
        iter_match = self.patterns['iteration'].match(line.strip())
        if not iter_match:
            return data

        data['timestamp'] = iter_match.group('timestamp')
        data['iter_num'] = int(iter_match.group('iter_num'))
        data['elapsed time per iteration (ms)'] = float(iter_match.group('iter_time'))

        loss_match = self.patterns['loss_pattern'].search(line)
        if loss_match:
            data['lm loss'] = float(loss_match.group(1))

        tflops_match = self.patterns['tflops_pattern'].search(line)
        if tflops_match:
            data['throughput per GPU'] = float(tflops_match.group(1))

        grad_norm_match = self.patterns['grad_norm_pattern'].search(line)
        if grad_norm_match:
            data['grad norm'] = float(grad_norm_match.group(1))

        learning_rate_match = self.patterns['learning_rate_pattern'].search(line)
        if learning_rate_match:
            data['learning rate'] = float(learning_rate_match.group(1))

        consumed_samples_match = self.patterns['consumed_samples'].search(line)
        if consumed_samples_match:
            data['consumed samples'] = float(consumed_samples_match.group(1))

        return data
