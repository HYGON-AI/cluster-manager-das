# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
训练异常检测器 - 负责检测 loss/梯度/性能/训练轨迹异常

职责：
- Loss 异常检测（NaN/Inf/负数/连续增长/异常偏高）
- 梯度异常检测（NaN/Inf/为零/过大/过小）
- 性能下降检测（TFLOPS 下降/迭代时间上升）
- 训练轨迹异常检测（Loss 停滞/放缓/震荡/反弹/增加5倍，梯度范数增加5倍）
"""
import math
import logging
from datetime import datetime
from typing import Any, Optional, Dict, List, Callable

import pytz

from cluster_manager.config.global_config import logger
from cluster_manager.monitor.mega.training_data_recorder import TrainingDataRecorder


class AnomalyDetector:
    """
    训练异常检测器：检测 loss/grad/performance/trajectory 异常
    """

    def __init__(
        self,
        data_recorder: TrainingDataRecorder,
        job_name: Optional[str] = None,
        alert_callback: Optional[Callable] = None,
    ):
        """
        Args:
            data_recorder: 训练数据记录器
            job_name: 任务名称（用于告警消息）
            alert_callback: 飞书告警回调 (message: str) -> None
        """
        self.data_recorder = data_recorder
        self.job_name = job_name
        self.alert_callback = alert_callback

    # ===================== Loss/梯度异常检测 =====================

    def check_loss_and_grad(self, iter_num: int, loss: float, grad_norm: float):
        """检测 loss 和梯度异常"""
        GRAD_NORM_MAX_THRESHOLD = 30.0
        GRAD_NORM_MIN_THRESHOLD = 1e-6
        LOSS_ABNORMAL_THRESHOLD_RATIO = 3
        GRAD_ZERO_EPS = 1e-10
        alert_message = None
        alert_type = ""

        # Loss 检测
        if math.isnan(loss) or math.isinf(loss):
            alert_message = (f"iter {iter_num}：Loss为{'NaN' if math.isnan(loss) else 'Inf'}")
            alert_type = "loss_nan_inf"
        elif loss < 0:
            alert_message = (f"iter {iter_num}：Loss为负数（{loss:.4f}），理论值应非负")
            alert_type = "loss_negative"

        sorted_loss_iters = sorted(self.data_recorder.training_data['loss'].keys())
        sorted_loss_values = [self.data_recorder.training_data['loss'][it] for it in sorted_loss_iters]

        alert_step = 10
        if len(sorted_loss_values) >= alert_step:
            recent_loss = sorted_loss_values[-alert_step:]
            is_continue_rise = all(recent_loss[i] < recent_loss[i+1] for i in range(len(recent_loss)-1))
            if is_continue_rise:
                alert_message = (f"iter {iter_num}：Loss连续{alert_step}步增长（{[f'{x:.4f}' for x in recent_loss]}）")
                alert_type = "loss_continue_rise"

        if len(sorted_loss_values) >= 10:
            history_loss = sorted_loss_values[:-1]
            avg_history_loss = sum(history_loss) / len(history_loss)
            if loss > avg_history_loss * LOSS_ABNORMAL_THRESHOLD_RATIO:
                alert_message = (f"iter {iter_num}：Loss异常偏高（当前{loss:.4f} > 历史均值{avg_history_loss:.4f}×{LOSS_ABNORMAL_THRESHOLD_RATIO}）")
                alert_type = "loss_abnormal_high"

        # 梯度检测
        if math.isnan(grad_norm) or math.isinf(grad_norm):
            alert_message = (f"iter {iter_num}：梯度范数为{'NaN' if math.isnan(grad_norm) else 'Inf'}")
            alert_type = "grad_nan_inf"
        else:
            if abs(grad_norm) < GRAD_ZERO_EPS:
                alert_message = (f"iter {iter_num}：grad_norm=0（无梯度更新）")
                alert_type = "grad_zero"
            elif grad_norm > GRAD_NORM_MAX_THRESHOLD:
                alert_message = (f"iter {iter_num}：梯度范数过大（{grad_norm:.4f} > 阈值{GRAD_NORM_MAX_THRESHOLD}），可能梯度爆炸")
                alert_type = "grad_too_large"
            elif grad_norm < GRAD_NORM_MIN_THRESHOLD:
                alert_message = (f"iter {iter_num}：梯度范数过小（{grad_norm:.6f} < 阈值{GRAD_NORM_MIN_THRESHOLD}），可能梯度消失")
                alert_type = "grad_too_small"

        if alert_message:
            logger.warning(f"alert_type = {alert_type}")
            return {
                "event_type": "loss",
                "payload": {
                    "type": "loss",
                    "iter": iter_num,
                    "loss": loss,
                    "grad_norm": grad_norm,
                    "alert_type": alert_type,
                    "message": alert_message,
                },
                "feishu_message": self._build_loss_grad_feishu_msg(iter_num, alert_message),
            }

        # 性能下降检测
        #self.check_performance_degradation(iter_num)

        # 训练轨迹异常检测
        #self.check_training_trajectory(iter_num, loss, grad_norm)

        return None

    # ===================== 性能下降检测 =====================

    def check_performance_degradation(self, iter_num: int):
        """检测性能下降：TFLOP/s/GPU 下降、迭代时间上升"""
        alert_message = None
        alert_type = ""

        # TFLOP/s/GPU 性能下降
        if 'TFLOP/s/GPU' in self.data_recorder.training_data:
            tflops_data = self.data_recorder.training_data['TFLOP/s/GPU']
            if len(tflops_data) >= 10:
                sorted_iters = sorted(tflops_data.keys())
                sorted_tflops = [tflops_data[it] for it in sorted_iters]

                # 连续5步性能下降
                recent_tflops = sorted_tflops[-5:]
                is_continue_drop = all(recent_tflops[i] > recent_tflops[i+1] for i in range(len(recent_tflops)-1))
                if is_continue_drop:
                    alert_message = (f"iter {iter_num}：TFLOP/s/GPU连续5步下降（{[f'{x:.2f}' for x in recent_tflops]}）")
                    alert_type = "tflops_continue_drop"

                # 性能突然大幅下降
                if not alert_message:
                    history_tflops = sorted_tflops[:-1]
                    avg_tflops = sum(history_tflops) / len(history_tflops)
                    current_tflops = sorted_tflops[-1]
                    if current_tflops < avg_tflops * 0.5 and avg_tflops > 0:
                        alert_message = (f"iter {iter_num}：TFLOP/s/GPU突然大幅下降（当前{current_tflops:.2f} < 历史均值{avg_tflops:.2f}×0.5）")
                        alert_type = "tflops_sudden_drop"

                # 性能异常偏低
                if not alert_message and len(sorted_tflops) >= 20:
                    recent_20_tflops = sorted_tflops[-20:]
                    avg_recent = sum(recent_20_tflops[:-1]) / len(recent_20_tflops[:-1])
                    current_tflops = recent_20_tflops[-1]
                    if current_tflops < avg_recent * 0.7 and avg_recent > 0:
                        drop_ratio = (avg_recent - current_tflops) / avg_recent * 100
                        alert_message = (f"iter {iter_num}：TFLOP/s/GPU性能下降{drop_ratio:.1f}%（当前{current_tflops:.2f}，近期均值{avg_recent:.2f}）")
                        alert_type = "tflops_performance_drop"

        # 迭代时间异常上升
        if not alert_message and 'elapsed_time_ms' in self.data_recorder.training_data:
            elapsed_data = self.data_recorder.training_data['elapsed_time_ms']
            if len(elapsed_data) >= 10:
                sorted_iters = sorted(elapsed_data.keys())
                sorted_elapsed = [elapsed_data[it] for it in sorted_iters]

                # 连续5步迭代时间上升
                recent_elapsed = sorted_elapsed[-5:]
                is_continue_rise = all(recent_elapsed[i] < recent_elapsed[i+1] for i in range(len(recent_elapsed)-1))
                if is_continue_rise:
                    alert_message = (f"iter {iter_num}：迭代时间连续5步上升（{[f'{x:.0f}ms' for x in recent_elapsed]}）")
                    alert_type = "iter_time_continue_rise"

                # 迭代时间突然大幅上升
                if not alert_message:
                    history_elapsed = sorted_elapsed[:-1]
                    avg_elapsed = sum(history_elapsed) / len(history_elapsed)
                    current_elapsed = sorted_elapsed[-1]
                    if current_elapsed > avg_elapsed * 2 and avg_elapsed > 0:
                        alert_message = (f"iter {iter_num}：迭代时间突然大幅上升（当前{current_elapsed:.0f}ms > 历史均值{avg_elapsed:.0f}ms×2）")
                        alert_type = "iter_time_sudden_rise"

        if alert_message:
            logger.warning(f"性能下降检测: {alert_type}")
            feishu_msg = self._build_performance_feishu_msg(iter_num, alert_message)
            if self.alert_callback:
                self.alert_callback(feishu_msg)

    # ===================== 训练轨迹异常检测 =====================

    def check_training_trajectory(self, iter_num: int, current_loss: float, current_grad_norm: float = None):
        """检测训练轨迹异常：Loss 停滞/放缓/震荡/反弹/增加5倍，梯度范数增加5倍"""
        alert_message = None
        alert_type = ""

        if 'loss' not in self.data_recorder.training_data:
            return

        loss_data = self.data_recorder.training_data['loss']
        if len(loss_data) < 20:
            return

        sorted_iters = sorted(loss_data.keys())
        sorted_loss = [loss_data[it] for it in sorted_iters]

        # 1. Loss 停滞（最近20步变化率 < 0.1%）
        recent_20_loss = sorted_loss[-20:]
        loss_range = max(recent_20_loss) - min(recent_20_loss)
        avg_loss = sum(recent_20_loss) / len(recent_20_loss)
        if avg_loss > 0 and loss_range / avg_loss < 0.001:
            alert_message = (f"iter {iter_num}：Loss停滞，最近20步变化范围{loss_range:.6f}（均值{avg_loss:.4f}，变化率{loss_range/avg_loss*100:.3f}%）")
            alert_type = "loss_stagnation"

        # 2. Loss 下降速度异常放缓
        if not alert_message and len(sorted_loss) >= 60:
            early_30_loss = sorted_loss[-60:-30]
            late_30_loss = sorted_loss[-30:]
            early_drop = early_30_loss[0] - early_30_loss[-1]
            late_drop = late_30_loss[0] - late_30_loss[-1]
            if early_drop > 0 and late_drop < early_drop * 0.1:
                alert_message = (f"iter {iter_num}：Loss下降速度异常放缓，前30步下降{early_drop:.4f}，后30步下降{late_drop:.4f}（仅为前段的{late_drop/early_drop*100:.1f}%）")
                alert_type = "loss_slowdown"

        # 3. Loss 震荡异常
        if not alert_message and len(sorted_loss) >= 10:
            recent_10_loss = sorted_loss[-10:]
            oscillation_count = 0
            for i in range(1, len(recent_10_loss) - 1):
                if (recent_10_loss[i-1] < recent_10_loss[i] > recent_10_loss[i+1]) or \
                   (recent_10_loss[i-1] > recent_10_loss[i] < recent_10_loss[i+1]):
                    oscillation_count += 1
            if oscillation_count >= 5:
                alert_message = (f"iter {iter_num}：Loss震荡异常，最近10步出现{oscillation_count}次震荡（{[f'{x:.4f}' for x in recent_10_loss]}）")
                alert_type = "loss_oscillation"

        # 4. Loss 反弹
        if not alert_message and len(sorted_loss) >= 10:
            recent_10_loss = sorted_loss[-10:]
            min_loss = min(recent_10_loss[:-1])
            current_loss_val = recent_10_loss[-1]
            if min_loss > 0 and current_loss_val > min_loss * 1.2:
                rebound_ratio = (current_loss_val - min_loss) / min_loss * 100
                alert_message = (f"iter {iter_num}：Loss反弹，当前{current_loss_val:.4f}比近期最小值{min_loss:.4f}高出{rebound_ratio:.1f}%")
                alert_type = "loss_rebound"

        # 5. Loss 增加5倍
        if not alert_message and len(sorted_loss) >= 10:
            recent_10_loss = sorted_loss[-10:]
            min_loss = min(recent_10_loss[:-1])
            current_loss_val = recent_10_loss[-1]
            if min_loss > 0 and current_loss_val > min_loss * 5:
                increase_ratio = current_loss_val / min_loss
                alert_message = (f"iter {iter_num}：Loss增加5倍异常！当前{current_loss_val:.4f}，历史最小值{min_loss:.4f}，增加倍数{increase_ratio:.2f}倍")
                alert_type = "loss_increase_5x"

        # 6. 梯度范数增加5倍
        if not alert_message and current_grad_norm is not None:
            if 'grad_norm' in self.data_recorder.training_data:
                grad_norm_data = self.data_recorder.training_data['grad_norm']
                if len(grad_norm_data) >= 10:
                    sorted_grad_iters = sorted(grad_norm_data.keys())
                    sorted_grad_norm = [grad_norm_data[it] for it in sorted_grad_iters]
                    recent_10_grad = sorted_grad_norm[-10:]
                    min_grad = min(recent_10_grad[:-1])
                    if min_grad > 0 and current_grad_norm > min_grad * 5:
                        increase_ratio = current_grad_norm / min_grad
                        alert_message = (f"iter {iter_num}：梯度范数增加5倍异常！当前{current_grad_norm:.4f}，历史最小值{min_grad:.4f}，增加倍数{increase_ratio:.2f}倍")
                        alert_type = "grad_norm_increase_5x"

        if alert_message:
            logger.warning(f"训练轨迹异常检测: {alert_type}")
            feishu_msg = self._build_trajectory_feishu_msg(iter_num, alert_message)
            if self.alert_callback:
                self.alert_callback(feishu_msg)

    # ===================== 迭代耗时异常检测 =====================

    def check_iteration_time(self, iter_num: int, iter_time_ms: float) -> Optional[Dict[str, Any]]:
        """检查迭代耗时是否异常（单步 > 最近100步平均×5）"""
        all_iter_times = self.data_recorder.get_all_iter_times()
        recent_times = all_iter_times[-100:]

        if len(recent_times) < 2:
            self.data_recorder.add_iter_time(iter_time_ms)
            return None

        avg_time = sum(recent_times) / len(recent_times)
        threshold = avg_time * 5

        if iter_time_ms > threshold:
            return {
                "event_type": "slow",
                "payload": {
                    "type": "slow",
                    "iter": iter_num,
                    "iter_time": iter_time_ms,
                    "avg_time": avg_time,
                },
                "warn_msg": (
                    f"迭代耗时异常告警！\n"
                    f"迭代: {iter_num}\n"
                    f"当前耗时: {iter_time_ms:.1f}ms\n"
                    f"最近100次平均: {avg_time:.1f}ms\n"
                    f"超过阈值: {threshold:.1f}ms (5倍)"
                ),
            }

        self.data_recorder.add_iter_time(iter_time_ms)
        return None

    # ===================== 私有辅助方法 =====================

    def _build_loss_grad_feishu_msg(self, iter_num: int, alert_message: str) -> str:
        time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        job_name_display = self.job_name if self.job_name is not None else "未指定"
        return (
            f"----模型训练loss/grad 出现异常值----\n"
            f"任务ID: {job_name_display} \n"
            f"当前时间: {time_print} \n"
            f"当前iter: {iter_num} \n"
            f"异常信息: {alert_message} \n"
            f"请人工介入检查"
        )

    def _build_performance_feishu_msg(self, iter_num: int, alert_message: str) -> str:
        time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        job_name_display = self.job_name if self.job_name is not None else "未指定"
        return (
            f"----模型训练性能下降告警----\n"
            f"任务ID: {job_name_display} \n"
            f"当前时间: {time_print} \n"
            f"当前iter: {iter_num} \n"
            f"异常信息: {alert_message} \n"
            f"请人工介入检查"
        )

    def _build_trajectory_feishu_msg(self, iter_num: int, alert_message: str) -> str:
        time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        job_name_display = self.job_name if self.job_name is not None else "未指定"
        return (
            f"----模型训练轨迹异常告警----\n"
            f"任务ID: {job_name_display} \n"
            f"当前时间: {time_print} \n"
            f"当前iter: {iter_num} \n"
            f"异常信息: {alert_message} \n"
            f"请人工介入检查"
        )
