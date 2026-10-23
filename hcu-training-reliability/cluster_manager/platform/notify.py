# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# notify.py
import os
import json
import logging
import requests
import threading
import time
import math
from typing import Optional, List, Dict
from datetime import datetime, date
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cluster_manager.config.global_config import logger
import cluster_manager.config.global_config as global_config

class Notify:
    """飞书告警通知类，封装飞书机器人消息发送逻辑"""
    
    def __init__(self, alert_interval: int = 1800, job_name = None):
        # 优先使用传入的URL，否则读取环境变量
        self.webhook_url = global_config.FEISHU_WEBHOOK_URL
        if not self.webhook_url:
            logger.warning("Feishu webhook URL not set, Feishu alert disabled.")
            
        
        self.NORMAL_SEND_INTERVAL = alert_interval   # 正常场景(启动/运行)：30分钟
        self.ABNORMAL_SEND_INTERVAL   = 600
        
        self._init_state()
        
        self._send_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.is_running = False
        self.regular_notify_enabled = global_config.ENABLE_REGULAR_NOTIFY
        self.job_name = job_name
        self.total_iters = 0         # 总训练iter数
        self.save_interval = 0       # ckpt保存间隔
        self.eval_interval = 0       # eval间隔
        self.train_samples = 0

        self.TZ = pytz.timezone('Asia/Shanghai')

        self.workspace = f"{global_config.WORK_DIR}/workspace"
        self.token_record_path = os.path.join(self.workspace, "train_daily_token.json")
      
        # 新增：日报状态（上次报告的Token值和日期）
        self.last_report_tokens = 0.0
        self.last_report_date = None
        self._load_report_state()      # 加载持久化状态

    def _init_state(self):
        """初始化/重置状态的通用方法"""
        self._lock = threading.RLock()
        self.latest_iter: Optional[int] = None
        #self.latest_loss: Optional[float] = None
        self.latest_grad_norm: Optional[float] = None
        self.loss_history: List[float] = []  # 存储历史loss值
        self.last_recorded_iter: Optional[int] = None  # 上一次发送时的iter
        self.iter_fixed_start_time: Optional[float] = None  # iter开始固定的时间戳
        self.consumed_tokens = 0.0
        self.remaining_tokens = 0.0
        self.latest_global_batch_size = None # 全局批次大小
        self.latest_elapsed_time_ms = None  # 耗时(ms)
        self.latest_tflops_per_gpu = None   # TFLOP/s/GPU
        self.latest_batch_size = None
        self.consumed_samples = None

        self.recent_tflops: List[float] = []
        self.recent_elapsed_times: List[float] = []
        self.avg_ckpt_time_ms = 0.0  # 单次ckpt平均耗时(ms)
        self.avg_eval_time_ms = 0.0  # 单次eval平均耗时(ms)

    def _load_report_state(self):
        """从文件加载上次报告的状态"""
        try:
            if os.path.exists(self.token_record_path):
                with open(self.token_record_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.last_report_tokens = data.get("last_report_tokens", 0.0)
                self.last_report_date = data.get("last_report_date")
                logger.info(f"加载日报状态: tokens={self.last_report_tokens:.2f}B, date={self.last_report_date}")
        except Exception as e:
            logger.error(f"加载日报状态失败: {e}")

    def _save_report_state(self):
        """保存日报状态到文件"""
        try:
            data = {}
            if os.path.exists(self.token_record_path):
                with open(self.token_record_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            data["last_report_tokens"] = self.last_report_tokens
            data["last_report_date"] = self.last_report_date
            with open(self.token_record_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存日报状态失败: {e}")

    def _check_daily_report(self):
        """每天21点发送日报，统计昨天21点到今天21点的Token消耗（首次则统计启动至今）"""
        if self.latest_iter is None:
            return
        now = datetime.now(self.TZ)
        current_date = now.strftime("%Y-%m-%d")
        # 到达21点且今日尚未发送
        if now.hour >= 21 and self.last_report_date != current_date:
            with self._lock:
                if self.last_report_date == current_date:
                    return
                # 计算消耗差值
                if self.last_report_tokens == 0.0 and self.last_report_date is None:
                    # 首次报告：统计从启动到现在的消耗
                    consumed = max(0.0, self.consumed_tokens)
                    period_desc = f"启动至 {current_date} 21:00"
                else:
                    consumed = max(0.0, self.consumed_tokens - self.last_report_tokens)
                    period_desc = f"{self.last_report_date} 21:00 至 {current_date} 21:00"
                
                self._send_daily_token_report(consumed, period=period_desc)
                # 更新状态
                self.last_report_tokens = self.consumed_tokens
                self.last_report_date = current_date
                self._save_report_state()
                logger.info(f"21点日报已发送 | 周期: {period_desc} | 消耗: {consumed:.2f}B")


    def _send_daily_token_report(self, consumed: float, period: str):
        """发送每日Token消耗统计飞书消息"""
        current_time = datetime.now(self.TZ).strftime('%Y-%m-%d %H:%M:%S')
        job_name = self.job_name or "未指定"
        message = (
            f"----每日Token消耗统计----\n"
            f"任务ID: {job_name}\n"
            f"统计时间: {current_time}\n"
            f"统计时段: {period}\n"
            f"Token消耗: {consumed:.2f} B"
        )
        self.send_feishu_alert(message)
    
    def send_feishu_alert(self, message: str) -> bool:
        # 未配置URL直接返回失败
        if not self.webhook_url:
            logger.warning("Feishu webhook URL not set, Feishu alert disabled.")
            return False
        body = {
            "msg_type": "text",
            "content": {"text": message},
        }
        headers = {"Content-Type": "application/json"}

        result = {"success": False, "response_text": "", "exception": None}

        def post_request():
            try:
                session = requests.Session()
                session.mount("http://", HTTPAdapter(max_retries=0))
                session.mount("https://", HTTPAdapter(max_retries=0))
                response = session.post(url=self.webhook_url, json=body, headers=headers, timeout=10)
                response.raise_for_status()
                result["response_text"] = response.text
                result["success"] = True
            except Exception as e:
                result["exception"] = e

        thread = threading.Thread(target=post_request)
        thread.daemon = True  # 主线程退出时不会等待
        thread.start()
        thread.join(timeout=10)  # 最多等待 10 秒

        if thread.is_alive():
            logger.warning(f"Feishu alert sent timeout")
            return False
        if result["success"]:
            logger.info(f"Feishu alert sent successfully, response: {result['response_text']}")
            return True
        else:
            logger.warning(f"Failed to send Feishu alert: {result['exception']}")
            return False
    
    def update_ckpt_avg_time(self, ckpt_ms: float):
        self.avg_ckpt_time_ms = ckpt_ms

    def update_eval_avg_time(self, eval_ms: float):
        self.avg_eval_time_ms = eval_ms

    def update_train_total_config(self, total_iters: int, save_interval: int, eval_interval: int, train_samples: int):
        """更新总iter、保存间隔、评估间隔"""
        self.total_iters = total_iters
        self.save_interval = save_interval
        self.eval_interval = eval_interval
        self.train_samples = train_samples

    def update_iter_elapsed_time(self, elapsed_ms: float):
        """更新iter耗时，自动保留最近100条"""
        self.recent_elapsed_times.append(elapsed_ms)
        if len(self.recent_elapsed_times) > 100:
            self.recent_elapsed_times.pop(0)

    def update_training_data(self, data_dict: Dict, consumed_tokens: float, remaining_tokens: float):
        """更新最新的训练数据（供外部调用）"""
        with self._lock:
            self.latest_iter = data_dict['current_iter']
            latest_loss = float(data_dict.get('loss') or data_dict.get('lm_loss', 0.0))
            self.latest_grad_norm = float(data_dict.get('gradient_norm', 0.0))
            self.latest_global_batch_size = int(data_dict.get('global_batch_size', 0))
            self.latest_elapsed_time_ms = float(data_dict.get('elapsed_time_ms', 0.0))
            self.latest_tflops_per_gpu = float(data_dict.get('throughput_tflops_per_gpu', 0.0))
            self.latest_batch_size = float(data_dict.get('global_batch_size', 0.0))
            self.consumed_samples = float(data_dict.get('consumed_samples', 0.0))
            self.consumed_tokens = consumed_tokens
            self.remaining_tokens = remaining_tokens
            # 维护loss历史（只保留最近10个）
            self.loss_history.append(latest_loss)
            if len(self.loss_history) > 100:
                self.loss_history.pop(0)

            self.recent_tflops.append(self.latest_tflops_per_gpu)
            if len(self.recent_tflops) > 100:
                self.recent_tflops.pop(0)

            # 排除第一个iter的时间
            if len(self.loss_history) > 1:
                self.update_iter_elapsed_time(self.latest_elapsed_time_ms)

            if self.iter_fixed_start_time is None:
                self.iter_fixed_start_time = time.time()            
            if self.latest_iter != self.last_recorded_iter:
                self.iter_fixed_start_time = time.time()
    
    def _calculate_remaining_time(self) -> str:
        """核心：计算训练剩余时间，返回格式化字符串"""
        with self._lock:
            current_iter = self.latest_iter
            train_samples = self.train_samples
            consumed_samples = self.consumed_samples
            global_batch_size = self.latest_global_batch_size

            # 无有效数据 → 返回计算中
            if (not current_iter 
                or train_samples <= 0 
                or consumed_samples < 0 
                or global_batch_size <= 0 
                or consumed_samples >= train_samples):
                logger.info(f"[剩余时间计算] 条件不满足，返回计算中")
                return "计算中"

            remaining_samples = train_samples - consumed_samples  # 剩余样本数
            remaining_iters = remaining_samples / global_batch_size 

            # 2. 计算iter平均耗时（最近100次）
            if not self.recent_elapsed_times:
                return "计算中"
            avg_iter_ms = sum(self.recent_elapsed_times) / len(self.recent_elapsed_times)
            iter_remaining_ms = remaining_iters * avg_iter_ms

            # 3. 计算剩余CKPT次数 & 时间
            ckpt_remaining_ms = 0.0
            if self.save_interval > 0:
                ckpt_count = math.ceil(remaining_iters / self.save_interval)
                ckpt_remaining_ms = ckpt_count * self.avg_ckpt_time_ms

            # 4. 计算剩余Eval次数 & 时间
            eval_remaining_ms = 0.0
            if self.eval_interval > 0:
                eval_count = math.ceil(remaining_iters / self.eval_interval)
                eval_remaining_ms = eval_count * self.avg_eval_time_ms

            # 5. 总剩余时间(ms) → 转换为时分秒
            total_remaining_ms = iter_remaining_ms + ckpt_remaining_ms + eval_remaining_ms
            total_seconds = total_remaining_ms / 1000
            real_seconds = total_seconds / (global_config.EFFICIENCY_FACTOR if global_config.EFFICIENCY_FACTOR != 0 else 1)

            def _format(seconds):
                days = int(seconds // 86400)
                remaining = seconds % 86400
                hours = int(remaining // 3600)
                minutes = int((remaining % 3600) // 60)
                if days > 0:
                    return f"{days}d {hours}h {minutes}m"
                else:
                    return f"{hours}h {minutes}m"
            ideal_str = _format(total_seconds)
            real_str = _format(real_seconds)
            return f"理论剩余时间: {ideal_str}\n预估剩余时间(含故障): {real_str}"

    def _generate_message(self) -> str:
        """根据iter状态生成对应消息内容（核心分支逻辑）"""
        current_time = time.time()
        time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        job_name_display = self.job_name if self.job_name is not None else "未指定"
        remaining_time_str = self._calculate_remaining_time()       
 

        with self._lock:
            avg_loss = sum(self.loss_history) / len(self.loss_history) if self.loss_history else 0.0
            avg_tflops = sum(self.recent_tflops) / len(self.recent_tflops) if self.recent_tflops else 0.0
            avg_elapsed_ms = sum(self.recent_elapsed_times) / len(self.recent_elapsed_times) if self.recent_elapsed_times else 0.0
            real_size = len(self.loss_history)
            # 分支1：无iter信息 → 发送“未产生iter”告警
            if self.latest_iter is None:
                return ""

            # 分支2：有iter信息 → 判断是否变化
            current_iter = self.latest_iter

            if current_iter == self.last_recorded_iter:
                fixed_duration = (current_time - self.iter_fixed_start_time) / 60
                fixed_duration_str = f"{fixed_duration:.1f}" if fixed_duration < 1 else f"{int(fixed_duration)}"
                return (
                    f"----模型训练可能卡住----\n"
                    f"任务ID: {job_name_display} \n"
                    f"当前时间: {time_print} \n"
                    f"当前iter: {current_iter} \n"
                    f"告警: 当前iter已持续 {fixed_duration_str} 分钟未变化 \n"
                    f"请立即检查训练状态！"
                )
            
            # 分支3：迭代正常更新（合并 首次迭代 + 正常迭代 冗余代码）
            self.last_recorded_iter = current_iter
            self.iter_fixed_start_time = current_time
            return (
                f"----模型训练正常运行中----\n"
                f"任务ID: {job_name_display} \n"
                f"当前时间: {time_print} \n"
                f"当前iter: {current_iter} \n"
                f"最近{real_size}步平均loss: {avg_loss:.4f} \n"
                f"Global Batch Size: {self.latest_global_batch_size} \n"
                f"最近{real_size}步平均单步耗时: {avg_elapsed_ms:.1f}ms \n"
                f"最近{real_size}步平均throughput: {avg_tflops:.2f} TFLOP/s/GPU \n"
                f"已消耗的tokens: {self.consumed_tokens:.2f} B \n"
                f"剩余tokens: {self.remaining_tokens:.2f} B \n"
                f"{remaining_time_str} "
            )
            

    def _regular_send_loop(self):
        while not self._stop_event.is_set():
            self._check_daily_report()

            wait_sec = self.NORMAL_SEND_INTERVAL
            if self.latest_iter is None or self.latest_iter == self.last_recorded_iter:
                wait_sec = self.ABNORMAL_SEND_INTERVAL

            # 核心逻辑：生成消息 → 发送消息
            message = self._generate_message()
            if message != "" :
                self.send_feishu_alert(message)

            self._stop_event.wait(wait_sec)

    def start(self):
        if self.is_running:
            return

        if not self.regular_notify_enabled:
            logger.info("Regular notify is disabled by config (ENABLE_REGULAR_NOTIFY=false), skip starting thread.")
            return

        # 重置停止事件
        self._stop_event.clear()
        
        # 启动定期发送线程
        self._send_thread = threading.Thread(
            target=self._regular_send_loop, 
            daemon=True,
            name="FeishuNotifyThread"
        )
        self._send_thread.start()
        
        # 更新运行状态
        self.is_running = True
        logger.info(f"Feishu notify thread started (interval: {self.NORMAL_SEND_INTERVAL/60} mins)")     
        
    def stop(self):
        if not self.is_running:
            return
        
        # 触发停止事件
        self._stop_event.set()
        
        # 等待线程退出
        if self._send_thread and self._send_thread.is_alive():
            try:
                self._send_thread.join(timeout=5)
            except TimeoutError:
                logger.warning("Feishu notify thread exit timed out (5 seconds)")
        
        # 更新运行状态
        self._init_state()
        
        self.is_running = False
        logger.info("Feishu notify thread stopped")
                
    
