# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import  Dict, List, Optional, Set, Union, Callable, Tuple, Any
from cluster_manager.config.global_config import logger
from datetime import datetime
import numpy as np
from collections import defaultdict
import cluster_manager.config.global_config as global_config
#import matplotlib.pyplot as plt

log_prefix = "log"
iter_file_name = "iters.log"

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
        
        # lose | time | tflops | grad_norm | learning_rate
        self.training_data: Dict[str, Any] = defaultdict(dict)
        '''
        self.loss_data: Dict[int, float] = {}
        self.time_data: Dict[int, float] = {}
        self.tflops_data: Dict[int, float] = {}
        self.grad_norm_data: Dict[int, float] = {}
        self.learning_rate_data: Dict[int, float] = {}
        '''
        # 迭代核心管理（与原监控类完全一致）
        self.iter_log_timestamps: Dict[int, datetime] = {}     # {iter_num: 日志中的时间戳（datetime对象）}
        self.iter_extra_time_map: Dict[int, Dict[str, float]] = {}     # {iter_num: {'ckpt': xx, 'eval': xx}}
        self.processed_iter_lines: Set[str] = set() # 去重：已处理的迭代行唯一标识
        self.last_valid_iter: Optional[int] = None       # 上一个有效迭代号

    def get_line_unique_id(self, iter_num: int, iter_time: float) -> str:
        """生成迭代行唯一标识（用于去重）- 与原逻辑一致"""
        return f"{iter_num}_{iter_time}"

    # ========== 数据添加方法 ==========
    def add_iter_time(self, iter_time_ms: float) -> None:
        """添加迭代耗时数据（保持原去重逻辑）"""
        self.all_iter_times.append(iter_time_ms)

    '''
    def add_iter_time(self, iter_time_ms: float, pure_iter_time: float) -> None:
        """添加迭代耗时数据（保持原去重逻辑）"""
        if len(self.all_iter_times) == 0 or self.all_iter_times[-1] != iter_time_ms or self.all_pure_iter_times[-1] != pure_iter_time:
            self.all_iter_times.append(iter_time_ms)
            self.all_pure_iter_times.append(pure_iter_time)
    '''
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

    def record_iter_metrics(self, iter_num: int, data_dict: Dict[str, Any], log_parser_type: str = "base") -> Tuple[float, float]:
        """
        记录迭代训练指标（区分 base/Megatron 与 special 日志场景），返回 (loss, grad_norm)

        Args:
            iter_num: 迭代号
            data_dict: 解析后的日志数据字典
            log_parser_type: 日志解析器类型，"base" 表示 Megatron 日志，"special" 使用特定框架日志格式

        Returns:
            (loss, grad_norm) 用于后续异常检测
        """
        if log_parser_type == "base":
            # Megatron 场景：优先 lm_loss，其次 loss
            loss = data_dict.get('lm_loss') or data_dict.get('loss', 0.0)
            grad_norm = data_dict.get('gradient_norm', 0.0)
            learning_rate = data_dict.get('learning_rate', 0.0)
            tflops = data_dict.get('throughput_tflops_per_gpu', 0.0)
            elapsed_ms = data_dict.get('elapsed_time_ms', 0.0)
        else:
            # Special 场景：epoch 格式，loss 直接可用，梯度为 gnorm（已映射为 gradient_norm）
            # learning_rate 对应 lr（已映射），train_time (train_wall 秒) 替代 elapsed_time_ms
            loss = data_dict.get('loss', 0.0)
            grad_norm = data_dict.get('gradient_norm', 0.0)
            learning_rate = data_dict.get('learning_rate', 0.0)
            tflops = 0
            # Special 日志使用 train_time (train_wall，单位秒) 替代 elapsed_time_ms
            train_time_sec = float(data_dict.get('train_time', 0.0))
            elapsed_ms = train_time_sec * 1000.0

        loss_val = float(loss)
        grad_norm_val = float(grad_norm)

        self.training_data['loss'][iter_num] = loss_val
        self.training_data['grad_norm'][iter_num] = grad_norm_val
        self.training_data['learning_rate'][iter_num] = float(learning_rate)
        if log_parser_type == "base":
            self.training_data['TFLOP/s/GPU'][iter_num] = float(tflops)
        self.training_data['elapsed_time_ms'][iter_num] = float(elapsed_ms)
        self.set_last_valid_iter(iter_num)

        return loss_val, grad_norm_val

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
    
    def clear_data_dict(self):
        selt.training_data.clear()

    def print_loss(self, iter_num, n=5):
        metrics = ['loss', 'grad_norm', 'learning_rate', 'TFLOP/s/GPU', 'elapsed_time_ms']
    
        logger.info(f"Current iter: {iter_num}, Average of the last {n} training metric values:")
        for metric_key in metrics:
            metric_data = self.training_data.get(metric_key, {})
            if not metric_data:  # 无数据时跳过
                logger.info(f"{metric_key}: No data available")
                continue

            sorted_iters = sorted(metric_data.keys(), reverse=True)
            recent_n_iters = sorted_iters[:n]  # 取最近n个迭代号
            recent_n_values = [metric_data[iter_num] for iter_num in recent_n_iters]

            avg_value = sum(recent_n_values) / len(recent_n_values)

            if abs(avg_value) < 1e-6:  # 数值小于1e-6时用科学计数法
                avg_display = f"{avg_value:.8e}"
            else:
                avg_display = f"{round(avg_value, 6)}"

            logger.info(
                f"{metric_key}: "
                f"Values of the last {len(recent_n_values)} iterations = {recent_n_values} | "
                f"Average value = {avg_display}"
            )

    '''
    def plot_analysis(self, out_dir, output_image='loss_analysis.png'):
        # --- 绘图数据准备 ---
        base_iters = sorted(self.training_data['loss'].keys())
        base_losses = [self.training_data['loss'][i] for i in base_iters]

        # 单步耗时数据
        time_iters = sorted(self.training_data['time'].keys())
        time_values = [self.training_data['time'][i] for i in time_iters]

        # TFLOPs数据
        tflops_iters = sorted(self.training_data['tflops'].keys())
        tflops_values = [self.training_data['tflops'][i] for i in tflops_iters]

        # Grad Norm数据
        grad_iters = sorted(self.training_data['grad_norm'].keys())
        grad_norms = [self.training_data['grad_norm'][i] for i in grad_iters]

        # 学习率数据
        lr_iters = sorted(self.training_data['learning_rate'].keys())
        lr_values = [self.training_data['learning_rate'][i] for i in lr_iters]

        if base_iters:
            first_iter = base_iters[0]
            last_iter = base_iters[-1]
            file_prefix, file_ext = os.path.splitext(output_image)
            output_image = f"{file_prefix}_iter{first_iter}-{last_iter}{file_ext}"
        full_image_path = out_dir / output_image  # Path拼接，跨平台兼容

        fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, figsize=(12, 25), sharex=True)

        # ========== 图1: LM Loss 趋势 ==========
        if base_iters:
            ax1.plot(base_iters, base_losses, label='LM Loss', color='blue', linewidth=1.5)
            avg_loss = np.mean(base_losses)
            ax1.axhline(y=avg_loss, color='red', linestyle='--', linewidth=2, label=f'Avg: {avg_loss:.6f}')

            # ========== 核心：设置纵轴范围（避免差异放大） ==========
            loss_min, loss_max = min(base_losses), max(base_losses)
            loss_margin = (loss_max - loss_min) * 0.1  # 上下各留10%边距
            ax1.set_ylim(loss_min - loss_margin, loss_max + loss_margin)
        ax1.set_title('Figure 1: LM Loss Trend', fontsize=14)
        ax1.set_xlabel('Iter', fontsize=12)
        ax1.set_ylabel('LM Loss', fontsize=12)
        ax1.legend(loc='upper right')
        ax1.grid(True, linestyle='--', alpha=0.5)

        # ========== 图2: 单步耗时 (ms) ==========
        if time_iters:
            ax2.plot(time_iters, time_values, label='Elapsed Time (ms)', color='orange', linewidth=1.5)
            # 添加平均值水平线
            avg_time = np.mean(time_values)
            ax2.axhline(y=avg_time, color='red', linestyle='--', linewidth=2, label=f'Avg: {avg_time:.2f} ms')

            # 设置纵轴范围
            time_min, time_max = min(time_values), max(time_values)
            time_margin = (time_max - time_min) * 0.1
            ax2.set_ylim(time_min - time_margin, time_max + time_margin)
        ax2.set_title('Figure 2: Elapsed Time per Iteration', fontsize=14)
        ax2.set_xlabel('Iter', fontsize=12)
        ax2.set_ylabel('Time (ms)', fontsize=12)
        ax2.legend(loc='upper right')
        ax2.grid(True, linestyle='--', alpha=0.5)

        # ========== 图3: TFLOPs 趋势 ==========
        if tflops_iters:
            ax3.plot(tflops_iters, tflops_values, label='TFLOP/s/GPU', color='green', linewidth=1.5)
            # 添加平均值水平线
            avg_tflops = np.mean(tflops_values)
            ax3.axhline(y=avg_tflops, color='red', linestyle='--', linewidth=2, label=f'Avg: {avg_tflops:.2f} TFLOP/s/GPU')
            
            # 设置纵轴范围
            tflops_min, tflops_max = min(tflops_values), max(tflops_values)
            tflops_margin = (tflops_max - tflops_min) * 0.1
            ax3.set_ylim(tflops_min - tflops_margin, tflops_max + tflops_margin)
        ax3.set_title('Figure 3: Throughput (TFLOP/s/GPU)', fontsize=14)
        ax3.set_xlabel('Iter', fontsize=12)
        ax3.set_ylabel('TFLOP/s/GPU', fontsize=12)
        ax3.legend(loc='upper right')
        ax3.grid(True, linestyle='--', alpha=0.5)

        # ========== 图4: Grad Norm 趋势 ==========
        if grad_iters:
            ax4.plot(grad_iters, grad_norms, label='Grad Norm', color='purple', linewidth=1.5)
            # 添加平均值水平线
            avg_grad = np.mean(grad_norms)
            ax4.axhline(y=avg_grad, color='red', linestyle='--', linewidth=2, label=f'Avg: {avg_grad:.2f}')

            # 设置纵轴范围（处理尖峰，避免尖峰压缩其他数据）
            grad_min, grad_max = min(grad_norms), max(grad_norms)
            # 若存在尖峰（如当前图中的0.25），可手动设置上限，避免尖峰主导纵轴
            # 例如：grad_max = min(grad_max, avg_grad * 3)  # 限制为平均值的3倍
            grad_margin = (grad_max - grad_min) * 0.1
            ax4.set_ylim(grad_min - grad_margin, grad_max + grad_margin)
        ax4.set_title('Figure 4: Grad Norm Trend', fontsize=14)
        ax4.set_xlabel('Iter', fontsize=12)
        ax4.set_ylabel('Grad Norm', fontsize=12)
        ax4.legend(loc='upper right')
        ax4.grid(True, linestyle='--', alpha=0.5)

        # ========== 新增：图5: Learning Rate 趋势 ==========
        if lr_iters:
            ax5.plot(lr_iters, lr_values, label='Learning Rate', color='darkred', linewidth=1.5)
            # 添加平均值水平线（学习率通常是变化的，平均值仅作参考）
            avg_lr = np.mean(lr_values)
            ax5.axhline(y=avg_lr, color='black', linestyle='--', linewidth=2, label=f'Avg: {avg_lr:.6f}')

            # 设置纵轴范围（学习率是单调衰减，可手动设置更合理的范围）
            lr_min, lr_max = min(lr_values), max(lr_values)
            lr_margin = (lr_max - lr_min) * 0.1
            ax5.set_ylim(lr_min - lr_margin, lr_max + lr_margin)
        ax5.set_title('Figure 5: Learning Rate Trend', fontsize=14)
        ax5.set_xlabel('Iter', fontsize=12)
        ax5.set_ylabel('Learning Rate', fontsize=12)
        ax5.legend(loc='upper right')
        ax5.grid(True, linestyle='--', alpha=0.5)

        plt.tight_layout()
        plt.savefig(full_image_path, dpi=100, bbox_inches='tight')  # 优化保存参数，防止标签被裁剪
        logger.info(f"分析图表已保存为: {output_image}")

        # --- 性能指标统计报告（新增学习率统计）---
        logger.info("\n" + "="*50)

        base_avg_time = np.mean(base_losses)
        base_avg_tflops = np.mean(time_values)
        base_avg_grad = np.mean(tflops_values)
        base_avg_loss = np.mean(grad_norms)
        base_avg_lr = np.mean(lr_values)  # 新增：学习率平均值

        logger.info(f"{'Metric':<40} | {'Average Value':<15}")
        logger.info("-" * 60)
        logger.info(f"{'Avg LM Loss':<40} | {base_avg_loss:<15.6f}")
        logger.info(f"{'Avg Time per Iteration (ms)':<40} | {base_avg_time:<15.2f}")
        logger.info(f"{'Avg Throughput (TFLOP/s/GPU)':<40} | {base_avg_tflops:<15.2f}")
        logger.info(f"{'Avg Grad Norm':<40} | {base_avg_grad:<15.2f}")
        logger.info(f"{'Avg Learning Rate':<40} | {base_avg_lr:<15.6f}")  # 新增：打印学习率平均值
        logger.info("="*50 + "\n")

        self.clear_data_dict()
    '''
