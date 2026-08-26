# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
import time
import datetime
import threading
import argparse
import os
from pathlib import Path
from datetime import datetime
import requests
import json
from typing import Dict, List, Optional, Set, Union

# 飞书机器人 Webhook 必须通过环境变量注入，禁止写入源码。
URL = os.environ.get("FEISHU_WEBHOOK_URL", "")

class TrainingDataRecorder:
    """
    训练数据记录器 - 仅管理TrainingLogMonitor中已有的数据，保持原有计算逻辑
    """
    def __init__(self):
        # 累计平均值相关（与原监控类完全一致）
        self.all_iter_times: List[float] = []          # 所有历史迭代总耗时
        self.all_ckpt_times: List[float] = []          # 所有历史CKPT耗时
        self.all_eval_times: List[float] = []          # 所有历史EVAL耗时
        self.all_pure_iter_times: List[float] = []     # 所有历史累计迭代耗时
        
        # 迭代核心管理（与原监控类完全一致）
        self.iter_log_timestamps: Dict[int, datetime] = {}     # {iter_num: 日志中的时间戳（datetime对象）}
        self.iter_extra_time_map: Dict[int, Dict[str, float]] = {}     # {iter_num: {'ckpt': xx, 'eval': xx}}
        self.processed_iter_lines: Set[str] = set() # 去重：已处理的迭代行唯一标识
        self.last_valid_iter: Optional[int] = None       # 上一个有效迭代号

    def get_line_unique_id(self, iter_num: int, iter_time: float) -> str:
        """生成迭代行唯一标识（用于去重）- 与原逻辑一致"""
        return f"{iter_num}_{iter_time}"

    # ========== 数据添加方法 ==========
    def add_iter_time(self, iter_time_ms: float, pure_iter_time: float) -> None:
        """添加迭代耗时数据（保持原去重逻辑）"""
        if len(self.all_iter_times) == 0 or self.all_iter_times[-1] != iter_time_ms or self.all_pure_iter_times[-1] != pure_iter_time:
            self.all_iter_times.append(iter_time_ms)
            self.all_pure_iter_times.append(pure_iter_time)

    def add_ckpt_time(self, ckpt_max_time_ms: float) -> None:
        """添加CKPT耗时数据（保持原去重逻辑）"""
        if len(self.all_ckpt_times) == 0 or self.all_ckpt_times[-1] != ckpt_max_time_ms:
            self.all_ckpt_times.append(ckpt_max_time_ms)

    def add_eval_time(self, eval_max_time_ms: float) -> None:
        """添加EVAL耗时数据（保持原去重逻辑）"""
        if len(self.all_eval_times) == 0 or self.all_eval_times[-1] != eval_max_time_ms:
            self.all_eval_times.append(eval_max_time_ms)

    def add_iter_timestamp(self, iter_num: int, timestamp: datetime) -> None:
        """记录迭代时间戳"""
        self.iter_log_timestamps[iter_num] = timestamp

    def add_iter_extra_time(self, iter_num: int, extra_type: str, value: float) -> None:
        """记录迭代的额外耗时（ckpt/eval）"""
        if iter_num not in self.iter_extra_time_map:
            self.iter_extra_time_map[iter_num] = {}
        self.iter_extra_time_map[iter_num][extra_type] = value

    def mark_iter_line_processed(self, unique_id: str) -> None:
        """标记迭代行已处理（去重）"""
        self.processed_iter_lines.add(unique_id)

    def set_last_valid_iter(self, iter_num: int) -> None:
        """设置上一个有效迭代号"""
        self.last_valid_iter = iter_num

    # ========== 数据获取方法 ==========
    def get_processed_iter_lines(self) -> Set[str]:
        """获取已处理的迭代行ID集合"""
        return self.processed_iter_lines

    def get_last_valid_iter(self) -> Optional[int]:
        """获取上一个有效迭代号"""
        return self.last_valid_iter

    def get_iter_timestamp(self, iter_num: int) -> Optional[datetime]:
        """获取指定迭代的时间戳"""
        return self.iter_log_timestamps.get(iter_num, None)

    def get_iter_extra_time(self, iter_num: int) -> Dict[str, float]:
        """获取指定迭代的额外耗时"""
        return self.iter_extra_time_map.get(iter_num, {})

    def get_all_pure_iter_times(self) -> List[float]:
        """获取所有纯迭代耗时列表"""
        return self.all_pure_iter_times.copy()

    def get_all_ckpt_times(self) -> List[float]:
        """获取所有CKPT耗时列表"""
        return self.all_ckpt_times.copy()

    def get_all_eval_times(self) -> List[float]:
        """获取所有EVAL耗时列表"""
        return self.all_eval_times.copy()

    def get_all_iter_times(self) -> List[float]:
        """获取所有迭代总耗时列表"""
        return self.all_iter_times.copy()

    # ========== 快捷计算方法（复用原逻辑） ==========
    def calculate_avg_pure_iter(self, exclude_last: bool = False) -> float:
        """计算纯迭代平均耗时（排除最后一个/不排除）"""
        pure_times = self.all_pure_iter_times
        if exclude_last and len(pure_times) > 1:
            pure_times = pure_times[:-1]
        return sum(pure_times) / len(pure_times) if pure_times else 0.0

    def calculate_avg_ckpt(self, exclude_last: bool = False) -> float:
        """计算CKPT平均耗时（排除最后一个/不排除）"""
        ckpt_times = self.all_ckpt_times
        if exclude_last and len(ckpt_times) > 1:
            ckpt_times = ckpt_times[:-1]
        return sum(ckpt_times) / len(ckpt_times) if ckpt_times else 0.0

class TrainingLogMonitor:
    def __init__(self, log_file_path, alert_threshold_ms=20000, no_update_threshold=60):
        """
        初始化监控器（集成数据记录器，保持原有计算逻辑）
        
        Args:
            log_file_path: 日志文件路径
            alert_threshold_ms: 时间变慢告警阈值（毫秒）
            no_update_threshold: 无新内容告警阈值（秒）
        """
        self.log_file = log_file_path
        self.alert_threshold_ms = alert_threshold_ms
        self.no_update_threshold = no_update_threshold
        self.last_update_time = time.time()
        
        # 初始化数据记录器（替代原有零散的变量）
        self.data_recorder = TrainingDataRecorder()
        
        # 告警标记（保持不变）
        self.alerts = {
            'iter_slow': False,
            'no_update': False,
            'ckpt_slow': False,
            'iter_interval_slow': False
        }
        
        # 正则表达式模式（保持不变）
        self.patterns = {
            # 仅匹配原始迭代行（以[时间戳]开头，排除INFO包装行）
            'raw_iteration': re.compile(
                r'^\s*\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+iteration\s+(?P<iter_num>\d+)/\s+\d+\s+\|.*?elapsed time per iteration \(ms\):\s+(?P<iter_time>[\d\.]+)'
            ),
            # 匹配save-checkpoint最大值
            'ckpt_time': re.compile(
                r'save-checkpoint\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)'
            ),
            # 匹配evaluate最大值
            'eval_time': re.compile(
                r'evaluate\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)'
            ),
            # 匹配ckpt归属的迭代号
            'ckpt_iter': re.compile(
                r'saving checkpoint at iteration\s+(\d+)\s+to'
            ),
            # 匹配eval归属的迭代号
            'eval_iter': re.compile(
                r'validation loss at iteration\s+(\d+)'
            )
        }
        
        # 运行标志（保持不变）
        self.running = True
        self.monitor_thread = None
        
    def parse_log_timestamp(self, timestamp_str):
        """解析日志中的时间戳字符串为datetime对象（保持不变）"""
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    
    def get_line_unique_id(self, iter_num, iter_time):
        """生成迭代行唯一标识（调用记录器方法）"""
        return self.data_recorder.get_line_unique_id(iter_num, iter_time)
    
    def parse_log_line(self, line):
        """解析日志行，提取关键信息（保持原有逻辑）"""
        # 1. 过滤掉INFO包装行（只保留原始迭代行）
        if "INFO - LogMonitor-MonitorThread" in line or "INFO - ClusterManager" in line:
            return None, None, None, None
        
        # 2. 匹配原始迭代行
        iter_match = self.patterns['raw_iteration'].match(line.strip())
        if iter_match:
            timestamp_str = iter_match.group('timestamp')
            iter_num = int(iter_match.group('iter_num'))
            iter_time = float(iter_match.group('iter_time'))
            log_timestamp = self.parse_log_timestamp(timestamp_str)
            
            # 生成唯一ID去重（调用记录器）
            unique_id = self.get_line_unique_id(iter_num, iter_time)
            if unique_id in self.data_recorder.get_processed_iter_lines():
                return None, None, None, None
            self.data_recorder.mark_iter_line_processed(unique_id)
            
            return 'iteration', iter_num, iter_time, log_timestamp
        
        # 3. 匹配ckpt耗时 + 归属迭代号
        ckpt_match = self.patterns['ckpt_time'].search(line)
        if ckpt_match:
            ckpt_max_time = float(ckpt_match.group('max'))
            # 查找该ckpt归属的迭代号（调用记录器获取last_valid_iter）
            ckpt_iter_match = self.patterns['ckpt_iter'].search(line)
            ckpt_iter = int(ckpt_iter_match.group(1)) if ckpt_iter_match else self.data_recorder.get_last_valid_iter()
            return 'ckpt_time', ckpt_iter, ckpt_max_time, None
        
        # 4. 匹配eval耗时 + 归属迭代号
        eval_match = self.patterns['eval_time'].search(line)
        if eval_match:
            eval_max_time = float(eval_match.group('max'))
            # 查找该eval归属的迭代号（调用记录器获取last_valid_iter）
            eval_iter_match = self.patterns['eval_iter'].search(line)
            eval_iter = int(eval_iter_match.group(1)) if eval_iter_match else self.data_recorder.get_last_valid_iter()
            return 'eval_time', eval_iter, eval_max_time, None
            
        return None, None, None, None
    
    def check_iteration_interval(self, current_iter):
        """
        检查迭代间隔（保持原有计算逻辑，仅替换数据来源为记录器）
        """
        last_valid_iter = self.data_recorder.get_last_valid_iter()
        if last_valid_iter is None or last_valid_iter != current_iter - 1:
            return
        
        # 1. 获取时间戳（从记录器获取）
        prev_timestamp = self.data_recorder.get_iter_timestamp(last_valid_iter)
        current_timestamp = self.data_recorder.get_iter_timestamp(current_iter)
        if prev_timestamp is None or current_timestamp is None:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 【iter:{last_valid_iter}->{current_iter}】 | 时间戳缺失，跳过间隔检查")
            return
        
        # 2. 计算实际间隔（毫秒）（逻辑不变）
        actual_interval = (current_timestamp - prev_timestamp).total_seconds() * 1000
        
        # 3. 获取上一迭代的额外耗时（从记录器获取）
        prev_iter_extra = self.data_recorder.get_iter_extra_time(last_valid_iter)
        prev_ckpt_time = prev_iter_extra.get('ckpt', 0.0)
        prev_eval_time = prev_iter_extra.get('eval', 0.0)
        total_extra_time = prev_ckpt_time + prev_eval_time
        
        # 4. 计算修正后间隔（逻辑不变）
        corrected_interval = actual_interval - total_extra_time
        corrected_interval = max(corrected_interval, 0)  # 确保不为负
        
        # 构建基础打印信息（逻辑不变）
        base_msg = f"[{datetime.now().strftime('%H:%M:%S')}] 【iter:{last_valid_iter}->{current_iter}】 | 实际间隔{actual_interval:.1f}ms"
        if total_extra_time > 0:
            base_msg += f" | 额外耗时{total_extra_time:.1f}ms | 修正后间隔{corrected_interval:.1f}ms"
        else:
            base_msg += f" | 修正后间隔{corrected_interval:.1f}ms"
        
        # 5. 计算参考值（从记录器获取数据）
        all_pure_iter_times = self.data_recorder.get_all_pure_iter_times()
        if len(all_pure_iter_times) < 2:
            print(f"{base_msg} (历史数据不足)")
            return
        
        # 6. 历史累计迭代平均（排除当前迭代）（逻辑不变）
        avg_pure_iter = self.data_recorder.calculate_avg_pure_iter(exclude_last=True)
        reference_value = avg_pure_iter + self.alert_threshold_ms
        
        # 7. 判断告警（逻辑不变）
        exceed_ms = corrected_interval - reference_value
        if exceed_ms > 0:
            if not self.alerts['iter_interval_slow']:
                self.trigger_alert(
                    f"【iter:{last_valid_iter}->{current_iter}】累计间隔过长！\n"
                    f"实际间隔: {actual_interval:.1f}ms | 修正后间隔: {corrected_interval:.1f}ms\n"
                    f"参考阈值 = 累计iter平均({avg_pure_iter:.1f}ms) + 告警阈值({self.alert_threshold_ms}ms) = {reference_value:.1f}ms | 超出: {exceed_ms:.1f}ms"
                )
                self.alerts['iter_interval_slow'] = True
            
            # 打印告警信息
            print(f"{base_msg} | (参考阈值 = 累计iter平均({avg_pure_iter:.1f}ms) + 告警阈值({self.alert_threshold_ms}ms) = {reference_value:.1f}ms | 超出: {exceed_ms:.1f}ms)")
        else:
            self.alerts['iter_interval_slow'] = False
            # 打印正常信息
            print(f"{base_msg} | (参考阈值 = 累计iter平均({avg_pure_iter:.1f}ms) + 告警阈值({self.alert_threshold_ms}ms) = {reference_value:.1f}ms)")
    
    def check_iteration_time(self, iter_num, iter_time_ms, log_timestamp):
        """检查迭代耗时（保持原有逻辑，仅替换数据存储为记录器）"""
        # 1. 记录时间戳（存入记录器）
        self.data_recorder.add_iter_timestamp(iter_num, log_timestamp)
        
        # 2. 获取当前迭代的额外耗时（从记录器获取）
        current_extra = self.data_recorder.get_iter_extra_time(iter_num)
        total_extra = current_extra.get('ckpt', 0.0) + current_extra.get('eval', 0.0)
        
        # 3. 计算累计迭代耗时（逻辑不变）
        pure_iter_time = max(iter_time_ms - total_extra, iter_time_ms * 0.1)
        
        # 4. 加入历史列表（存入记录器，保持原去重逻辑）
        self.data_recorder.add_iter_time(iter_time_ms, pure_iter_time)
        
        # 5. 检查迭代间隔（逻辑不变）
        self.check_iteration_interval(iter_num)
        
        # 6. 打印迭代信息（逻辑不变）
        all_pure_iter_times = self.data_recorder.get_all_pure_iter_times()
        if len(all_pure_iter_times) > 1:
            avg_pure = self.data_recorder.calculate_avg_pure_iter(exclude_last=True)
            pure_diff = pure_iter_time - avg_pure
            
            base_msg = f"[{datetime.now().strftime('%H:%M:%S')}] 【iter:{iter_num}】 | 总耗时{iter_time_ms:.1f}ms | 累计iter耗时{pure_iter_time:.1f}ms"
            if total_extra > 0:
                base_msg += f" | 额外耗时{total_extra:.1f}ms"
            base_msg += f" | (历史平均: {avg_pure:.1f}ms | 差值: {pure_diff:.1f}ms)"
            print(base_msg)
        else:
            base_msg = f"[{datetime.now().strftime('%H:%M:%S')}] 【首次iter:{iter_num}】 | 总耗时{iter_time_ms:.1f}ms | 累计iter耗时{pure_iter_time:.1f}ms"
            if total_extra > 0:
                base_msg += f" | 额外耗时{total_extra:.1f}ms"
            base_msg += " (无历史数据)"
            print(base_msg)
        
        # 7. 更新上一个有效迭代号（存入记录器）
        self.data_recorder.set_last_valid_iter(iter_num)
        self.last_update_time = time.time()
    
    def handle_checkpoint_time(self, ckpt_iter, ckpt_max_time_ms):
        """处理CKPT耗时（保持原有逻辑，仅替换数据存储为记录器）"""
        if ckpt_iter is None:
            print(f"\033[93m[警告] CKPT耗时{ckpt_max_time_ms:.1f}ms 无法归属iter，跳过\033[0m")
            return
        
        # 记录该迭代的CKPT耗时（存入记录器）
        self.data_recorder.add_iter_extra_time(ckpt_iter, 'ckpt', ckpt_max_time_ms)
        
        # 加入历史CKPT列表（存入记录器，保持原去重逻辑）
        self.data_recorder.add_ckpt_time(ckpt_max_time_ms)
        
        # 精简打印（逻辑不变）
        print(f"\033[92m[额外耗时] CKPT耗时{ckpt_max_time_ms:.1f}ms \033[0m")
        
        # CKPT耗时监控（逻辑不变，从记录器获取数据）
        all_ckpt_times = self.data_recorder.get_all_ckpt_times()
        if len(all_ckpt_times) > 1:
            avg_ckpt = self.data_recorder.calculate_avg_ckpt(exclude_last=True)
            ckpt_diff = ckpt_max_time_ms - avg_ckpt
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CKPT保存 | 耗时{ckpt_max_time_ms:.1f}ms (max值) | (累计平均: {avg_ckpt:.1f}ms | 差值: {ckpt_diff:.1f}ms)")
            
            if ckpt_diff > self.alert_threshold_ms:
                if not self.alerts['ckpt_slow']:
                    self.trigger_alert(
                        f"CKPT保存变慢！【iter:{ckpt_iter}】 CKPT耗时{ckpt_max_time_ms:.1f}ms, "
                        f"历史平均: {avg_ckpt:.1f}ms, 超出阈值: {self.alert_threshold_ms}ms"
                    )
                    self.alerts['ckpt_slow'] = True
            else:
                self.alerts['ckpt_slow'] = False
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 首次CKPT | 【iter:{ckpt_iter}】 耗时{ckpt_max_time_ms:.1f}ms (max值)")
        
        self.last_update_time = time.time()
    
    def handle_evaluate_time(self, eval_iter, eval_max_time_ms):
        """处理EVAL耗时（保持原有逻辑，仅替换数据存储为记录器）"""
        if eval_iter is None:
            print(f"\033[93m[警告] EVAL耗时{eval_max_time_ms:.1f}ms 无法归属iter，跳过\033[0m")
            return
        
        # 记录该迭代的EVAL耗时（存入记录器）
        self.data_recorder.add_iter_extra_time(eval_iter, 'eval', eval_max_time_ms)
        
        # 加入历史EVAL列表（存入记录器，保持原去重逻辑）
        self.data_recorder.add_eval_time(eval_max_time_ms)
        
        # 精简打印（逻辑不变）
        print(f"\033[92m[额外耗时] EVAL耗时{eval_max_time_ms:.1f}ms 归属【iter:{eval_iter}】\033[0m")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Evaluate | 【iter:{eval_iter}】 耗时{eval_max_time_ms:.1f}ms (max值)")
        
        self.last_update_time = time.time()
    
    def trigger_alert(self, message):
        """触发告警（保持不变）"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        alert_msg = f"\033[91m[告警信息] [{timestamp}] {message}\033[0m"
        print(alert_msg)
        
        # 如需发送告警，请通过 FEISHU_WEBHOOK_URL 配置机器人地址，
        # 并复用正式的通知模块，避免在源码中保存凭据。
    
    def monitor_no_update(self):
        """监控长时间无新内容（保持不变）"""
        while self.running:
            time_since_last_update = time.time() - self.last_update_time
            
            if time_since_last_update > self.no_update_threshold:
                if not self.alerts['no_update']:
                    self.trigger_alert(
                        f"长时间无新日志内容！已超过{self.no_update_threshold}秒"
                    )
                    self.alerts['no_update'] = True
            else:
                self.alerts['no_update'] = False
            
            time.sleep(5)
    
    def tail_log_file(self):
        """模拟tail -f功能，实时读取日志文件（保持不变）"""
        with open(self.log_file, 'r', encoding='utf-8') as f:
            f.seek(0, 2)  # 移动到文件末尾
            
            while self.running:
                line = f.readline()
                if line:
                    event_type, target_iter, value, log_timestamp = self.parse_log_line(line)
                    
                    if event_type:
                        if event_type == 'iteration':
                            self.check_iteration_time(target_iter, value, log_timestamp)
                        elif event_type == 'ckpt_time':
                            self.handle_checkpoint_time(target_iter, value)
                        elif event_type == 'eval_time':
                            self.handle_evaluate_time(target_iter, value)
                    else:
                        self.last_update_time = time.time()
                else:
                    time.sleep(0.1)  # 减少CPU占用

    def start(self):
        """启动监控（保持不变）"""
        print(f"开始监控日志文件: {self.log_file}")
        print(f"告警阈值: {self.alert_threshold_ms}ms | 无更新告警阈值: {self.no_update_threshold}秒\n")
        
        # 检查文件是否存在
        if not Path(self.log_file).exists():
            print(f"日志文件不存在: {self.log_file}")
            return
        
        # 启动无更新监控线程
        no_update_thread = threading.Thread(target=self.monitor_no_update, daemon=True)
        no_update_thread.start()
        
        # 主线程监控日志
        try:
            self.tail_log_file()
        except KeyboardInterrupt:
            print("\n监控已停止")
            # 可选：打印记录器中的数据统计
            print("\n=== 监控数据统计 ===")
            print(f"总迭代数: {len(self.data_recorder.get_all_iter_times())}")
            print(f"CKPT次数: {len(self.data_recorder.get_all_ckpt_times())}")
            print(f"EVAL次数: {len(self.data_recorder.get_all_eval_times())}")
            print(f"纯迭代平均耗时: {self.data_recorder.calculate_avg_pure_iter():.1f}ms")
        except Exception as e:
            print(f"监控出错: {e}")
        finally:
            self.running = False
    
    def stop(self):
        """停止监控（保持不变）"""
        self.running = False

def main():
    # 解析命令行参数（保持不变）
    parser = argparse.ArgumentParser(description='训练日志实时监控工具（集成数据记录器）')
    parser.add_argument('--log-file', '-f', required=True, help='要监控的日志文件路径')
    parser.add_argument('--alert-threshold', '-t', type=int, default=10, 
                        help='告警阈值(ms)，默认10ms')
    parser.add_argument('--no-update-threshold', '-to', type=int, default=600,
                        help='日志无更新告警阈值(秒)，默认600秒(10分钟)')
    
    args = parser.parse_args()
    
    # 创建监控器
    monitor = TrainingLogMonitor(
        log_file_path=args.log_file,
        alert_threshold_ms=args.alert_threshold,
        no_update_threshold=args.no_update_threshold
    )
    
    # 启动监控
    monitor.start()

if __name__ == "__main__":
    main()
