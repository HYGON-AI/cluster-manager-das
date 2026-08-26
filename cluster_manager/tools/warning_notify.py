# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
import pytz
import requests
import json
import time
import re
import os
import glob

# ===================== 配置项 =====================
URL = os.environ.get("FEISHU_WEBHOOK_URL", "")
MODEL_NAMES = [
    "qwen3-ar-32b-1024hcu"
]
WORKER_DIRS = [
    "/public/home/user/workspace"
]

SEQ_LEN = 32768
TIME_SLEEP = 600  # 检查间隔（秒）
ABNORMAL_THRESHOLD = 5.0
latest_iteration = -1

# 新增配置
HANG_THRESHOLD_SECONDS = 600  # 迭代数不变超10分钟告警
TARGET_TOTAL_ITERATIONS = 68500  # 指定总步数
last_recorded_iter = -1  # 上一次迭代数
last_recorded_time = datetime.now(pytz.timezone('Asia/Shanghai'))  # 上一次记录时间
last_recorded_cluster_time = pytz.timezone('Asia/Shanghai').localize(datetime(1970, 1, 1))
send_warning_message = -1
check_count = 0
print(f'{SEQ_LEN=}')
print(f'{TIME_SLEEP=}')
print(f'{HANG_THRESHOLD_SECONDS=} (迭代数不变告警阈值，秒)')
print(f'{TARGET_TOTAL_ITERATIONS=} (指定总训练步数)')


from collections import defaultdict
from typing import Dict, List
import numpy as np


class PeriodicTopNRankTFStats:
    """
    每 iter 接收 fastest / slowest ranks（数量不限制，由外部判定）
    每 period（如 50 iter）输出 TF 统计：
      median / mean / min
    """

    def __init__(self, period=50, topn=5):
        self.period = period
        self.topn = topn
        self.iter_idx = 0

        # 本周期数据（不限制 rank 数）
        self.fast_tf = defaultdict(list)   # rank -> [tf, tf, ...]
        self.slow_tf = defaultdict(list)

    # =========================
    # 每 iter 调用（不限制 rank 个数）
    # =========================
    def update_iter(
        self,
        ranks: List[int],
        tf_by_rank: Dict[int, float],
        category: str
    ) -> bool:
        """
        ranks      : 任意数量的 ranks（外部判定）
        tf_by_rank : rank -> tf
        category   : "fast" or "slow"
        """
        if category not in ("fast", "slow"):
            raise ValueError(f"Invalid category: {category}")

        if category == "fast":
            self.iter_idx += 1

        target = self.fast_tf if category == "fast" else self.slow_tf

        for rank in ranks:
            tf = tf_by_rank.get(rank)
            if tf is None:
                continue
            target[rank].append(tf)
        return self.iter_idx % self.period == 0

    # =========================
    # 周期统计
    # =========================
    def _tf_stats(self, tfs: List[float]) -> Dict[str, float]:
        return {
            "median": float(np.median(tfs)),
            "mean": float(np.mean(tfs)),
            "min": float(np.min(tfs)),
            "samples": len(tfs),
        }

    def _sorted_stats(self, tf_dict, reverse=False):
        stats = []
        for rank, tfs in tf_dict.items():
            if not tfs:
                continue
            stats.append((rank, self._tf_stats(tfs)))

        # 排序依据仍然是 median（最稳定）
        return sorted(
            stats,
            key=lambda x: x[1]["median"],
            reverse=reverse
        )

    def print_period_stats(self):
        # 先排序，再裁剪 topn
        slow_stats = self._sorted_stats(self.slow_tf, reverse=False)[:self.topn]
        fast_stats = self._sorted_stats(self.fast_tf, reverse=True)[:self.topn]

        print(f"\n====== TF Stats (Top {self.topn}) @ iter {self.iter_idx} ======")

        print("\n[Slow TF Stats]")
        for rank, s in slow_stats:
            print(
                f"Rank {rank:4d} : "
                f"median={s['median']:.2f}  "
                f"mean={s['mean']:.2f}  "
                f"min={s['min']:.2f}  "
                f"samples={s['samples']}"
            )

        print("\n[Fast TF Stats]")
        for rank, s in fast_stats:
            print(
                f"Rank {rank:4d} : "
                f"median={s['median']:.2f}  "
                f"mean={s['mean']:.2f}  "
                f"min={s['min']:.2f}  "
                f"samples={s['samples']}"
            )

    def get_period_stats(self):
        """仅返回TF Stats结构化数值（包含TopN个完整明细），不打印任何内容"""
        # 核心逻辑：排序+裁剪TopN（保留原有逻辑）
        slow_stats = self._sorted_stats(self.slow_tf, reverse=False)[:self.topn]
        fast_stats = self._sorted_stats(self.fast_tf, reverse=True)[:self.topn]

        # ========== 关键修改：构建TopN个完整明细数据 ==========
        # 慢TF TopN明细（每个条目包含rank、median、mean、min、samples）
        slow_tf_details = []
        for rank, s in slow_stats:
            slow_tf_details.append({
                "rank": rank,
                "median": s["median"],
                "mean": s["mean"],
                "min": s["min"],
                "samples": s["samples"]
            })
        
        # 快TF TopN明细（每个条目包含rank、median、mean、min、samples）
        fast_tf_details = []
        for rank, f in fast_stats:
            fast_tf_details.append({
                "rank": rank,
                "median": f["median"],
                "mean": f["mean"],
                "min": f["min"],
                "samples": f["samples"]
            })

        # 整理需要返回的核心数值（包含TopN明细+汇总值）
        stats_data = {
            "iter_idx": self.iter_idx,          # 当前迭代次数
            "topn": self.topn,                  # TopN数量
            # TopN完整明细（核心修改：返回所有TopN条目）
            "slow_tf_details": slow_tf_details, # 慢TF TopN明细列表
            "fast_tf_details": fast_tf_details # 快TF TopN明细列表
        }
        return stats_data
    
    def reset_period(self):
        self.fast_tf.clear()
        self.slow_tf.clear()


def filter_straggler_line(
    line: str,
    stats: PeriodicTopNRankTFStats
) -> bool:
    """
    解析 Megatron Etpt(TF) straggler 日志并更新 PeriodicTopNRankTFStats

    返回:
        True  -> 命中 period（如 50 iter），外部应触发输出
        False -> 否
    """
    tf_rank_pattern = re.compile(r"(\d+\.\d+)/(\d+)")
    if "^^^^ Bottom" in line:
        category = "bottom"
    elif "^^^^ Top" in line:
        category = "top"
    else:
        return False

    tf_by_rank = {}
    fast_ranks = []
    slow_ranks = []

    for tf, rank in tf_rank_pattern.findall(line):
        rank = int(rank)
        tf = float(tf)
        tf_by_rank[rank] = tf

        if category == "top":
            fast_ranks.append(rank)
        else:  # bottom
            slow_ranks.append(rank)

    if not tf_by_rank:
        return False

    hit_period = False

    if fast_ranks:
        hit_period |= stats.update_iter(
            ranks=fast_ranks,
            tf_by_rank=tf_by_rank,
            category="fast"
        )

    if slow_ranks:
        hit_period |= stats.update_iter(
            ranks=slow_ranks,
            tf_by_rank=tf_by_rank,
            category="slow"
        )

    return hit_period

def extract_cluster_log_info(log_line, log_stack, cluster_last_time_wrapper):
    """
    提取日志行中的时间和进程退出信息
    """
    global last_recorded_cluster_time
    global send_warning_message
    # 提取行首的时间戳
    timestamp_pattern = r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})'
    
    timestamp_match = re.search(timestamp_pattern, log_line)
    key_phrases = [
        "LogMonitor 启动成功",
        "替换故障节点:",  # 用该核心短语代表"节点:xxx 替换故障节点:xxx"
        "检测到进程退出",
        "状态切换"
    ]    
    if timestamp_match:
        log_time_str = timestamp_match.group(1)
        log_time = pytz.timezone('Asia/Shanghai').localize(datetime.strptime(log_time_str, "%Y-%m-%d %H:%M:%S"))
        # time_diff = (last_recorded_cluster_time - log_time).total_seconds()
        # print(f"log_time:{log_time}  last_recorded_cluster_time:{last_recorded_cluster_time} cluster_last_time_wrapper:{cluster_last_time_wrapper[0]}")
        if log_time <= last_recorded_cluster_time:
            return False
        
        for keyword in key_phrases:
            if keyword in log_line:
                log_stack.append(log_line)
                if cluster_last_time_wrapper[0] is None:
                    cluster_last_time_wrapper[0] = log_time
                if keyword == "检测到进程退出":
                    send_warning_message = 1
    return True             
        
def filter_cluster_manager_logs(line: str, log_stack, cluster_last_time_wrapper):
    """
    过滤出包含ClusterManager关键字的日志行并解析
    """
    if "ClusterManager" in line:
        return extract_cluster_log_info(line, log_stack, cluster_last_time_wrapper)
    return True

def send_message(message):
    """Send a message to the Feishu bot."""
    if not URL:
        print("FEISHU_WEBHOOK_URL is not configured; skip notification")
        return
    print("msg:", message)
    body = json.dumps({"msg_type": "text", "content": {"text": message}})
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url=URL, data=body, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message: {e}")


def send_message_from_log(model_name, training_log, is_test=False, abnormal_threshold=ABNORMAL_THRESHOLD):
    # 引用全局变量
    global latest_iteration, last_recorded_iter, last_recorded_time, last_recorded_cluster_time, check_count
    check_count += 1
    test_info = "【测试，请忽略】" if is_test else ""
    time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

    # 检查文件是否存在
    connected = True
    if not (os.path.exists(training_log) and connected):
        message = f"{test_info}警告: 未能连接{model_name}模型训练的远程服务器; 当前时间: {time_print}"
        send_message(message)
        return

    # 读取日志文件
    try:
        with open(training_log, 'r') as f:
            lines = f.readlines()
        if not lines:
            message = f"{test_info}警告: {model_name}日志文件为空; 当前时间: {time_print}"
            send_message(message)
            return
    except Exception as e:
        message = f"{test_info}警告: 读取{model_name}日志失败: {str(e)}; 当前时间: {time_print}"
        send_message(message)
        return

    # ===================== 核心解析逻辑（参考你的版本，新增总步数提取） =====================
    losses = []
    latest = True
    content = ''
    iterations = []
    iteration = ""
    all_iteration = 68500  # 兜底值
    consumed_tokens = 0.0
    time_per_step = 0.0
    TFLOP_per_step = 0.0
    time_to_finish = 0.0
    current_iter = None  # 用于hang检测

    cluster_last_time_wrapper = [None]

    # 修改迭代数正则：匹配 "iteration        1/  700000" 格式，提取当前迭代数+总迭代数
    iteration_pattern = r'iteration\s+(\d+)/\s*(\d+)'
    stats = PeriodicTopNRankTFStats(period=50, topn=5)
    log_stack = []
    # 反向遍历行
    for line in reversed(lines):
        next = filter_cluster_manager_logs(line, log_stack, cluster_last_time_wrapper)
        if check_count != 10:
            if next == True:
                continue
            else:
                break
    
         # 匹配掉队检测
        filter_straggler_line(line, stats)

        # 按|分割找loss字段
        try:
            tmp = [e.strip() for e in line.split('|')]
            loss_str = [e for e in tmp if e.startswith('lm loss')][0]
        except IndexError:
            continue

        # 匹配迭代数（新增提取总步数）
        if latest:
            match = re.search(iteration_pattern, line)
            if match:
                # 提取当前迭代数
                current_iter = int(match.group(1))
                latest_iteration = str(current_iter)
                # 提取日志中的总迭代数
                all_iteration = int(match.group(2))
                # 拼接content（保持和你原有格式一致）
                content = f"iteration        {current_iter}/  {all_iteration}"
                iterations.append(latest_iteration)
            latest = False

        # 匹配迭代数用于提取其他指标
        match = re.search(iteration_pattern, line)
        if match:
            # 提取当前迭代数（用于其他指标）
            iteration = match.group(1)
            # 再次确认总迭代数（避免首次匹配失败）
            all_iteration = int(match.group(2)) if match.group(2).isdigit() else 68500
        
        # 提取第一个loss行的其他指标
        if len(losses) == 0:
            try:
                # 提取consumed samples
                samples_match = re.search(r'consumed samples:\s(.*?)(\|)', line)
                if samples_match:
                    consumed_samples = int(samples_match.group(0).split()[2])
                    consumed_tokens = consumed_samples * SEQ_LEN / 1000 / 1000 / 1000

                # 提取elapsed time
                time_match = re.search(r'elapsed time(.*?)(\|)', line)
                if time_match:
                    time_per_step = float(time_match.group(0).split()[-2]) / 1000.0

                # 计算剩余时间（使用日志提取的总步数）
                time_to_finish = (all_iteration - int(iteration)) * time_per_step / 3600.0

                # 提取throughput
                tflops_match = re.search(r'throughput(.*?)(\|)', line)
                if tflops_match:
                    TFLOP_per_step = float(tflops_match.group(0).split()[-2])
            except:
                pass

        # 提取loss
        try:
            loss = float(loss_str.split(" ")[-1])
            losses.append(loss)
        except ValueError:
            continue

        # 最多收集50个loss
        if len(losses) == 50:
            break
    if log_stack:
        reversed_logs = log_stack[::-1]
        reversed_log_content = "\n  - ".join(reversed_logs)
        hang_msg = (
            f"{test_info}【紧急警告】{model_name}模型训练容错介入！\n"
            f"当前时间: {time_print}\n"
            f"容错信息：{reversed_log_content}"
            f"请立即人工介入检查！"
        )
        send_message(hang_msg)
        if cluster_last_time_wrapper[0] is not None:
            last_recorded_cluster_time = cluster_last_time_wrapper[0]
        cluster_last_time_wrapper = [None]

    if check_count != 10:
        return
    
    # 1. 获取掉队检测信息
    tf_stats_data = stats.get_period_stats()
    # ===================== Hang检测（触发后直接return） =====================
    if current_iter is not None:
        # 首次记录
        if last_recorded_iter == -1:
            last_recorded_iter = current_iter
            last_recorded_time = datetime.now(pytz.timezone('Asia/Shanghai'))
        # 检测hang：触发后发送告警并直接return
        elif current_iter == last_recorded_iter:
            time_diff = (datetime.now(pytz.timezone('Asia/Shanghai')) - last_recorded_time).total_seconds()
            if time_diff > HANG_THRESHOLD_SECONDS:
                hang_msg = f"{test_info}【紧急警告】{model_name}模型训练疑似Hang/挂掉！\n" \
                           f"当前时间: {time_print}\n" \
                           f"问题：迭代数{current_iter}已保持不变超过{time_diff/60:.1f}分钟\n" \
                           f"请立即人工介入检查！"
                send_message(hang_msg)
                return  
        # 迭代数更新，重置
        else:
            last_recorded_iter = current_iter
            last_recorded_time = datetime.now(pytz.timezone('Asia/Shanghai'))

    # ===================== 指定总步数剩余时间计算 =====================
    time_to_finish_target = 0.0
    if current_iter and time_per_step > 0:
        remaining_iter_target = max(0, TARGET_TOTAL_ITERATIONS - current_iter)
        time_to_finish_target = remaining_iter_target * time_per_step / 3600.0

    # ===================== 修复后的消息逻辑 =====================
    time_print = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    if len(losses) != 0:
        avg_loss = float("{:.3f}".format(sum(losses) / len(losses)))
        if int(iteration) > 2000 and (avg_loss <= 0 or max(losses) > min(losses) + abnormal_threshold):
            print(f"{avg_loss=}")
            print(f"{max(losses)=}")
            print(f"{min(losses)=}")
            message = f"{test_info}警告: {model_name}模型loss出现0、负数或者异常峰值, 怀疑训练出现问题;\n 当前时间: {time_print}"
        else:
            # message = f"{test_info}---------{model_name}---------\n 模型训练正在进行中\n 当前时间: {time_print} \n 进程: {content} \n 过去{len(losses)}步平均loss: {avg_loss} \n 已消耗的tokens量: {consumed_tokens:.3f} B \n 当前训练速度: {time_per_step:.3f} s/step \n 当前吞吐: {TFLOP_per_step:.1f} TFLOP/s/GPU \n 预计完成日志总步数({all_iteration}步)还需时间: {time_to_finish:.3f} 小时 \n 预计完成指定总步数({TARGET_TOTAL_ITERATIONS}步)还需时间: {time_to_finish_target:.3f} 小时"
            message = (
            f"{test_info}---------{model_name}---------\n"
            f" 模型训练正在进行中\n"
            f" 当前时间: {time_print} \n"
            f" 进程: {content} \n"
            f" 过去{len(losses)}步平均loss: {avg_loss} \n"
            f" 已消耗的tokens量: {consumed_tokens:.3f} B \n"
            f" 当前训练速度: {time_per_step:.3f} s/step \n"
            f" 当前吞吐: {TFLOP_per_step:.1f} TFLOP/s/GPU \n"
            f" 预计完成日志总步数({all_iteration}步)还需时间: {time_to_finish:.3f} 小时 \n"
            f" 预计完成指定总步数({TARGET_TOTAL_ITERATIONS}步)还需时间: {time_to_finish_target:.3f} 小时\n"
            f"\n"
            f"====== TF Stats (Top {tf_stats_data['topn']}) @ iter {tf_stats_data['iter_idx']} ======\n"
            f"\n[Slow TF Stats]\n"
        )

        # 遍历慢TF TopN明细，逐行添加
        for detail in tf_stats_data["slow_tf_details"]:
            message += (
                f"Rank {detail['rank']:4d} : median={detail['median']:.2f}  mean={detail['mean']:.2f}  min={detail['min']:.2f}  samples={detail['samples']}\n"
            )

        # 继续拼接快TF部分
        message += (
            f"\n[Fast TF Stats]\n"
        )
        # 遍历快TF TopN明细，逐行添加
        for detail in tf_stats_data["fast_tf_details"]:
            message += (
                f"Rank {detail['rank']:4d} : median={detail['median']:.2f}  mean={detail['mean']:.2f}  min={detail['min']:.2f}  samples={detail['samples']}\n"
            )

        # 去除首尾空行，保证格式整洁
        message = message.strip()
        stats.reset_period()
        send_message(message)
    else:
        message = f"{test_info}---------{model_name}---------\n 模型训练正在进行中\n 当前时间: {time_print} \n 进程: 尚未有任何loss信息"
        send_message(message)


# ===================== 主循环 =====================
time.sleep(10)

while True:
    try:
        # # for idx in range(0, len(WORKER_DIRS)):
        # #     WORKER_DIR = WORKER_DIRS[idx]
        # #     os.system(f'chmod 777 {WORKER_DIR} -R')

        # #     MODEL_NAME = MODEL_NAMES[idx]
        # #     training_log = sorted(glob.glob(f"{WORKER_DIR}/*"))[-1]
        # #     training_log = sorted(glob.glob(f"{training_log}/*"))[-1]
        # #     training_log = sorted(glob.glob(f"{training_log}/*"))[-1]
        # #     training_log = sorted(glob.glob(f"{training_log}/*"))[-1]
        # #     training_log = f"{training_log}/stdout.log"

        #     send_message_from_log(MODEL_NAME, training_log, is_test=False)
        if check_count == 10:
            check_count = 0
        send_message_from_log("示例训练任务", "/workspace/logs/cluster.log", is_test=False)
        # time.sleep(TIME_SLEEP)
        print("---------------------------------------------")
        time.sleep(10)
    except Exception as e:
        print(f"ERROR: {e}")
