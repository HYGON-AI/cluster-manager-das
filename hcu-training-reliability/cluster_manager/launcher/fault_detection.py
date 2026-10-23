# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import sys
import os
import subprocess
import argparse
from typing import List, Union, Optional, Dict, Any, Tuple
import re
from itertools import product
import re
import json
import time
import threading
import glob
import shlex
from datetime import datetime
from collections import defaultdict, deque
import statistics
import numpy as np

from cluster_manager.config.global_config import logger
import  cluster_manager.config.global_config as global_config
from cluster_manager.node_management.hostfile_handler import HostfileHandler
from cluster_manager.utils.string_utils import match_failed_nodes, node_expr_parser
from cluster_manager.utils.file_utils import read_hostfile
from cluster_manager.executor.cmd_executor import CmdExecutor

class FaultDetection:
    """故障检测模块，提供方法调用接口"""

    def __init__(self):
        self.last_fault_nodes = []
        self._state = defaultdict(dict)
        self._slurm_enabled = global_config.CLUSTER_SCHEDULE == "SLURM"

    def parse_fault_log(self, log_str: str) -> Dict[str, Optional[str]]:
        """
        解析日志内容，提取故障类型、描述、错误码、GPU ID 和训练步数。

        Args:
            log_str: 包含故障检查结果的字符串（可能多行）。

        Returns:
            包含以下字段的字典：
                fault_type:   故障类型
                description:  故障描述
                error_code:   错误码（NVIDIA Xid 号 或 AMD 错误码）
                gpu_id:       具体哪张 GPU（仅数字，None = 节点级）
                micro_step:   训练步数（用于 INF 精准判定）
        """
        result = {
            "fault_type": None,
            "description": None,
            "error_code": None,
            "gpu_id": None,
            "micro_step": None,
        }

        if not log_str or not isinstance(log_str, str):
            return result

        # 将 log_str 作为整体进行正则匹配，不按行切分
        # 正则模式 —— 修改捕获组，只捕获 GPU 编号数字部分
        # 格式: hcu_xid_76[HCU5] → 捕获错误码和数字
        hcu_xid_pattern = re.compile(r'hcu_xid_(\d+)\[HCU(\d+)\]')
        hcu_lose_pattern = re.compile(r'hcu_lose\[HCU(\d+)\]')
        ib_pcs_link_pattern = re.compile(r'ib_pcs_link_down\[([^\]]+)\]')
        ib_maxreadreq_pattern = re.compile(r'ib_maxreadreq_error\[([^\]]+)\]')
        ib_port_state_pattern = re.compile(r'ib_port_state_error\[([^\]]+)\]')
        storage_not_mount_pattern = re.compile(r'storage_not_mount(\[.*?\])+')
        connection_closed_pattern = re.compile(r'([^:]+):\s*Connection closed by .*')
        ssh_no_route_pattern = re.compile(r'([^:]+):\s*ssh: connect to host .* No route to host')

        result_list = []  # 存储解析到的候选故障条目
        # 1. HCU Xid 错误（最优先）
        match = hcu_xid_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 2,
                "description": match.group(0),
                "error_code": match.group(1),   # 错误码数字       
                "gpu_id": match.group(2),       # 仅数字        
                })

        # 2. HCU 丢失
        match = hcu_lose_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 2,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": match.group(1),       # 仅数字      
                })

        # 3. IB 相关故障（HCA 非 GPU，gpu_id 置 None）
        match = ib_pcs_link_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 1,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        match = ib_maxreadreq_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 1,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        match = ib_port_state_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 1,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        # 4. 存储挂载故障
        match = storage_not_mount_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 3,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        # 5. 连接关闭故障
        match = connection_closed_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 3,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        # 6. SSH 无路由故障
        match = ssh_no_route_pattern.search(log_str)
        if match:
            result_list.append({
                "fault_type": 3,
                "description": match.group(0),
                "error_code": None,
                "gpu_id": None,
            })

        # 7. 兼容带任意前缀的 VPC 高延迟故障标记。
        if re.search(r'\b[a-zA-Z0-9_-]*vpc_high_latency\b', log_str):
            result_list.append({
                "fault_type": 1,
                "description": "vpc_high_latency",
                "error_code": None,
                "gpu_id": None,
            })

        # 从 result_list 中选择最严重的故障条目返回（按 fault_type 降序排序）
        if result_list:
            def get_fault_type_value(fault):
                ft = fault.get("fault_type")
                if ft is None:
                    return 0
                return int(ft) if isinstance(ft, (int, str)) else 0

            result_list.sort(key=get_fault_type_value, reverse=True)
            return result_list[0]

        # 如果没有解析到任何故障条目，返回默认的空结果
        return result


    def parse_clush_output(self, output: str) -> list[str]:
        """解析 clush 输出：
        1）PASSED → 直接通过
        2）仅包含 ib_pcs_link_down[HCAx] 且 Counter < 5 → 降级通过
        """
        passed_nodes = []

        lines = output.splitlines()
        clean_lines = [line.strip() for line in lines if line.strip()]

        i = 0
        n = len(clean_lines)

        while i < n:
            line = clean_lines[i]

            if line == '---------------':
                i += 1
                if i >= n:
                    break

                # 节点表达式行
                node_line = clean_lines[i]
                i += 1

                # 去掉 (513) 这种计数
                node_expr = re.sub(r'\s*\(\d+\)$', '', node_line).strip()

                # 跳过分隔符
                if i < n and clean_lines[i] == '---------------':
                    i += 1

                result = None
                counter = None

                # 解析 block 内容
                while i < n:
                    current_line = clean_lines[i]

                    if current_line == '---------------':
                        break

                    # 解析 CHECK RESULT
                    if current_line.startswith('[CHECK RESULT]:'):
                        result = current_line.split(':', 1)[1].strip()
                        i += 1
                        continue

                    # 解析 Counter（在 CONTENT LIST 中）
                    if 'Counter:' in current_line:
                        match = re.search(r'Counter:\s*(\d+)', current_line)
                        if match:
                            counter = int(match.group(1))

                    # 忽略 clush 报错行
                    if current_line.startswith('clush:'):
                        i += 1
                        continue

                    i += 1

                is_passed = False

                # 正常通过
                if result == 'PASSED':
                    is_passed = True
                # ========== 新增过滤逻辑 ==========
                # VPC 高延迟属于可降级告警，不阻断节点。
                elif result and re.search(r'vpc_high_latency', result):
                    is_passed = True
                # =================================
                elif result and re.search(r'hcu_xid_1', result):
                    is_passed = True
                # =================================
                elif result and re.search(r'^ib_wrong_packet_error(\[HCA\d+\])+$', result):
                    is_passed = True
                # 降级容忍逻辑
                elif result:
                    errors = [e.strip() for e in result.split(',')]

                    # 判断是否全部是 ib_pcs_link_down[HCAx]
                    all_ib_link_down = all(
                        re.match(r'^ib_pcs_link_down\[HCA\d+\]+$', e)
                        for e in errors
                    )

                    if all_ib_link_down:
                        if counter is not None and counter < 20:
                            is_passed = True

                # 记录通过节点
                if is_passed:
                    passed_nodes.extend(node_expr_parser(node_expr))

            else:
                i += 1

        return passed_nodes

    def cross_check(self, hostfile):
        normal_nodes = []
        abnormal_nodes = []
        CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
        run_check_file = os.path.abspath(os.path.join(CURRENT_DIR, '../node_check/run_check.sh'))
        run_check_hostfile = os.path.abspath(hostfile)

        res_check_dir = os.path.dirname(run_check_file)
        check_nodes = 4

        cmd = f'cd {res_check_dir} ; bash {run_check_file} {run_check_hostfile} {check_nodes}'
        err_code, output = CmdExecutor.execute_command(
            cmd,
            capture_output=True,
            timeout=1200
        )
        if err_code != 0:
            raise RuntimeError(
                f"cross_check script failed, err={err_code}, cmd={cmd}, detail={output}"
            )

        res_check_pass = os.path.join(res_check_dir, 'node_checklog/host_check_pass')
        res_check_error = os.path.join(res_check_dir, 'node_checklog/host_check_error')
        if os.path.exists(res_check_pass):
            normal_nodes = HostfileHandler.read(res_check_pass)

        if os.path.exists(res_check_error):
            abnormal_nodes = HostfileHandler.read(res_check_error)
        return normal_nodes, abnormal_nodes
    def run_nhc_nodes(self, nodes: Union[List[str], str], timeout: int = 300):
        """
        对每个节点分别执行 nhc 和 scontrol，返回：
        {
            "node1": "xxx",
            "node2": "xxx",
            ...
        }
        """

        # 统一转成 list
        if isinstance(nodes, str):
            nodes = [nodes]

        nodes_info = {}

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        split_str = f">>>= \n{now}\n <<<=\n"

        for node in nodes:
            try:
                nhc_output = self._run_nhc(node, timeout)
            except Exception as e:
                nhc_output = f"[NHC ERROR]: {str(e)}"

            try:
                scontrol_output = self._run_scontrol(node, timeout)
            except Exception as e:
                scontrol_output = f"[SCONTROL ERROR]: {str(e)}"

            nodes_info[node] = (
                split_str
                + (nhc_output or "")
                + "\n"
                + (scontrol_output or "")
            )

        parts = []
        for node, content in nodes_info.items():
            parts.append(f"\n>>>= {node} <<<=\n")
            parts.append(str(content))
        res = '\n'.join(parts)
        self.write_run_nhc_error(res)

        return nodes_info

    
    def write_run_nhc_error(self, result: str):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        run_nhc_error_file = os.path.join(global_config.WORK_DIR,'run_nhc_error.txt')
        with open(run_nhc_error_file,'a') as f:
            f.write(f'\n{now}\n')
            f.write(result)
            f.write('-'*100)


    def run_nhc(self, hostfile: str, timeout: int = 300) -> Tuple[List[str], List[str], bool]:
        """
        执行 clush 命令，返回 PASSED 节点列表。
        :param hostfile: 节点列表文件路径
        :param timeout: 命令执行超时时间（秒），默认 300 秒
        :return: PASSED 节点列表
        """
        cmd = f"clush -f 1000 -b --hostfile {hostfile} run_nhc"
        all_nodes = read_hostfile(hostfile)

        err_code, output = CmdExecutor.execute_command(
            cmd,
            capture_output=True,
            timeout=timeout
        )
        output = output or ""

        if err_code != 0:
            logger.warning(
                f"[run_nhc] command failed, skip this round. err={err_code}, cmd={cmd}"
            )
            if output.strip():
                self.write_run_nhc_error(output)
            return [], [], False

        passed = self.parse_clush_output(output)
        failed = [node for node in all_nodes if node not in passed]

        if len(failed) > 0:
            self.write_run_nhc_error(output)

        return passed, failed, True

    def check_sinfo_R(self, hostfile, timeout=60):
        """执行节点健康检查 (sinfo -R)"""
        if not self._slurm_enabled:
            logger.debug("[NHCMonitor] Skip sinfo in CLUSTER_SCHEDULE=NONE.")
            return {}
        logger.info("[NHCMonitor] start nhc probe: sinfo -R")

        err_code, result = CmdExecutor.execute_command(
            "sinfo -R -o '%50E %12U %19H %6t %N'",
            capture_output=True,
            timeout=timeout
        )
        if err_code != 0:
            logger.warning(f"[NHCMonitor] sinfo -R failed, err={err_code}")
            if result and result.strip():
                logger.warning(f"[NHCMonitor] sinfo -R detail: {result}")
            return {}

        result = result or ""
        if not result.strip():
            logger.warning("[NHCMonitor] empty output from sinfo -R")
            return {}

        raw_text = result.strip()

        try:
            normal_nodes = read_hostfile(hostfile)
            error_nodes = match_failed_nodes(raw_text, normal_nodes)
        except Exception as e:
            logger.exception(f"[NHCMonitor] parse sinfo output failed: {e}")
            return {}

        return error_nodes

    def get_hcu_info(self,hostfile, timeout=120):
        _cmd = f"clush --hostfile={hostfile} -f 1024  'hy-smi -P -u --showmemuse --showmemavailable --json'"
        output = self._run_clush(_cmd,timeout)
        if output:
            return self._parse_hcu_output(output)

    def get_nodes_info(self,hostfile, timeout=120):

        if not self._slurm_enabled:
            logger.debug("[NHCMonitor] Skip scontrol node info in CLUSTER_SCHEDULE=NONE.")
            return []

        nodes = read_hostfile(hostfile)
        nodes = ",".join(nodes)
        _cmd = f"clush --hostfile={hostfile} -f 1024 -b 'scontrol show nodes {nodes}'"

        output = self._run_clush(_cmd,timeout)
        if output:
            return self._parse_nodes_output(output)

    def get_mem_info(self,hostfile, timeout=120):

        _cmd = f"""clush --hostfile={hostfile} -f 1024 -b 'free -m | grep -E "Mem|Swap"'"""
        # cmd = [
        # 'clush',
        # '--hostfile', hostfile,
        # '-f', 1024,
        # '-b', command   # command 原样传递，clush 会将其作为命令执行
        output = self._run_clush(_cmd,timeout)
        if output:
            return self._parse_mem_info(output)

    def _parse_mem_info(self, clush_output: str) -> List[Dict[str, Any]]:
        """
        解析 clush 命令输出，提取每个节点的 Mem 和 Swap 信息。

        参数:
            clush_output: clush 命令的标准输出字符串

        返回:
            字典列表，每个字典包含节点名及对应的 Mem 和 Swap 数据
        """
        # 修改正则表达式：匹配 Mem 和 Swap 信息
        mem_pattern = re.compile(r"^\s*Mem:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$")
        swap_pattern = re.compile(r"^\s*Swap:\s+(\d+)\s+(\d+)\s+(\d+)\s*$")

        lines = [line.rstrip() for line in clush_output.splitlines()]
        result = []
        i = 0
        n = len(lines)


        while i < n:
            # 寻找节点块开始标志（分隔线）
            if lines[i] == "---------------":
                
                if i + 2 >= n or lines[i + 2] != "---------------":
                    i += 1
                    continue
                node_name = lines[i + 1].strip()
                i += 3
                # logger.info(f"node_name:{node_name}")
                # 收集当前节点块的所有输出行（直到下一个分隔线或结束）
                block_lines = []
                while i < n and lines[i] != "---------------":
                    block_lines.append(lines[i])
                    i += 1

                # 解析 Mem 和 Swap
                mem_data = None
                swap_data = None
                for line in block_lines:
                    mem_match = mem_pattern.match(line)
                    if mem_match:
                        mem_data = {
                            "total": int(mem_match.group(1)),
                            "used": int(mem_match.group(2)),
                            "free": int(mem_match.group(3)),
                            "shared": int(mem_match.group(4)),
                            "buff/cache": int(mem_match.group(5)),
                            "available": int(mem_match.group(6)),
                        }
                        continue
                    swap_match = swap_pattern.match(line)
                    if swap_match:
                        swap_data = {
                            "total": int(swap_match.group(1)),
                            "used": int(swap_match.group(2)),
                            "free": int(swap_match.group(3)),
                        }

                # 只有同时获取到 Mem 和 Swap 才认为是有效节点
                if mem_data is not None and swap_data is not None:
                    result.append({
                        "node": node_name,
                        "Mem": mem_data,
                        "Swap": swap_data,
                    })
            else:
                i += 1
        return result



    def check_hcu_info(self, hostfile,timeout=120):
        """执行一次监控：获取数据、更新状态、记录日志、清理旧日志"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _cmd = [
            "clush",
            "--hostfile", hostfile,
            "-f", "1000",
            "hy-smi", "-P", "-u", "--showmemuse", "--showmemavailable", "--json"
        ]
        output = self._run_clush(_cmd,timeout)
        # 1. 执行命令获取原始输出
        # logger.info(f'-output:{output}')
        if output is None:
            return

        # 2. 保存原始输出到日志
        self._save_log(output, now)
        # logger.info(f'output:{output}')

        # 3. 解析当前数据
        current_data = self._parse_hcu_output(output)
        # logger.info(f'current_data:{current_data}')
        # 4. 更新状态并检测异常
        new_state, new_unhealthy, anomalies = self._update_state_and_detect(current_data, now)

        # 5. 更新内部共享状态（加锁）
        with self._lock:
            self._state = new_state
            self._unhealthy_ranks = new_unhealthy

        # 6. 记录异常到日志（可选，这里记录到同一日志文件）
        self._log_unhealthy(now, new_unhealthy)

        # 7. 清理超过一天的旧日志
        #self._clean_old_logs()
        if anomalies:
            return anomalies
    
    def _run_clush(self, _cmd, timeout):
        """执行 clush 命令，成功返回原始输出字符串，失败统一返回 None。"""
        try:
            if isinstance(_cmd, (list, tuple)):
                # list/tuple 入参统一做 shell 安全转义，避免空格/特殊字符导致命令被拆错。
                cmd = " ".join(shlex.quote(str(x)) for x in _cmd)
            else:
                cmd = str(_cmd)
            logger.info(f">= _run_clush 执行 {cmd} <=")
            try:
                parsed_timeout = int(timeout) if timeout is not None else 120
            except (TypeError, ValueError):
                parsed_timeout = 120
            if parsed_timeout <= 0:
                parsed_timeout = 120

            err_code, output = CmdExecutor.execute_command(
                cmd=cmd,
                capture_output=True,
                timeout=parsed_timeout
            )
            output = output or ""
            if err_code != 0:
                logger.warning(f"Error executing command (err={err_code}): {cmd}")
                if output.strip():
                    logger.warning(f"Command failure detail: {output[:1000]}")
                return None

            # 保护性兜底：理论上 err_code=0 时应是正常输出，若出现明显错误文本则视为失败。
            output_stripped = output.strip()
            if output_stripped:
                lowered = output_stripped.lower()
                suspicious_prefixes = (
                    "error executing ",
                    "command not found",
                    "no such file or directory",
                    "permission denied",
                )
                if any(prefix in lowered for prefix in suspicious_prefixes):
                    logger.warning(f"Unexpected error-like output with err=0, treat as failed: {cmd}")
                    logger.warning(f"Suspicious output detail: {output[:1000]}")
                    return None

            return output
        except Exception as e:
            logger.exception(f"Error executing command: {e}")
            return None
        
        
    def _run_nhc(self, nodes_str: str, timeout: int):
        cmd = f"clush -f 1024 -b -w {nodes_str} run_nhc"
        nhc_output = self._run_clush(cmd, timeout)
        if nhc_output:
            lines = nhc_output.splitlines()
            result_lines = [line for line in lines if "[CHECK RESULT]" in line]

            return " \n".join(result_lines)

    def _run_nodes_nhc(self, nodes: List, timeout: int):
        
        if not isinstance(nodes,str):
            nodes = ','.join(nodes)
        
        cmd = f"clush -f 1024 -b -w {nodes} run_nhc"
        nhc_output = self._run_clush(cmd, timeout)
        if nhc_output:
            passed = self.parse_clush_output(nhc_output)

            return passed

    def _run_scontrol(self, nodes_str: str, timeout: int):
        if not self._slurm_enabled:
            return ""
        cmd = f"scontrol show nodes {nodes_str}"
        scontrol_output = self._run_clush(cmd, timeout)
        if scontrol_output:
            lines = scontrol_output.splitlines()
            result_lines = [line for line in lines if "Reason=" in line]

            return " \n".join(result_lines)

    def _parse_nodes_output(self,output_text):

        if not output_text:
            logger.error("Error: No output returned from clush.")
            return []

        data = []
        parse_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 按 NodeName 分块
        node_blocks = re.split(r'\n(?=NodeName=)', output_text.strip())

        for block in node_blocks:
            node_match = re.search(r'NodeName=(\S+)', block)
            if not node_match:
                continue
            node = node_match.group(1)

            reason_match = re.search(r'Reason=(.*)', block)
            reason = reason_match.group(1).strip() if reason_match else ""

            if not reason:
                reason = f'{parse_time}，scontrol show node {node} 没有异常输出'

            data.append({
                "node": node,
                "reason": reason
            })

        return data



    def _check_nodes_output(self,output_text):

        if not output_text:
            logger.error("Error: No output returned from clush.")
            return []

        succ_nodes = []
        parse_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 按 NodeName 分块
        node_blocks = re.split(r'\n(?=NodeName=)', output_text.strip())

        for block in node_blocks:
            node_match = re.search(r'NodeName=(\S+)', block)
            if not node_match:
                continue
            node = node_match.group(1)

            reason_match = re.search(r'Reason=(.*)', block)
            reason = reason_match.group(1).strip() if reason_match else ""

            if not reason:
                succ_nodes.append(node)

            

        return succ_nodes



    def fix_json_format(self,output_text):
        # 替换所有没有逗号分隔的双引号之间的部分
        output_text = re.sub(r'"\s*"\s*', '", "', output_text)
        return output_text

    def _parse_hcu_output(self, output_text):
        """
        解析 clush 输出，返回结构:
        {
            "node1": {
                "card0": {"vram": float, "power": float, "use": float, "available":float},
                ...
            },
            ...
        }
        """
        # 修复格式
        output_text = self.fix_json_format(output_text)
        
        data = {}
        parse_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = output_text.splitlines()
        node_pattern = re.compile(r'^(\S+):')
        # logger.info(f'lines:{lines}')
        for line in lines:
            line = line.strip()
            if not line:
                continue

            m = node_pattern.match(line)
            # logger.info(f'm:{m}')
            if not m:
                continue

            node = m.group(1)
            rest = line[m.end():].strip()
            if not rest:
                continue

            try:
                json_data = json.loads(rest)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error: {e} for node: {node}")
                continue

            if node not in data:
                data[node] = {}

            for card_key, card_info in json_data.items():
                try:
                    vram = float(card_info.get("HCU memory use (%)", "0"))
                    power = float(card_info.get("Average Graphics Package Power (W)", "0"))
                    use = float(card_info.get("HCU use (%)", "0"))
                    available = float(card_info.get("Available memory size (MiB)", "0"))
                except (ValueError, TypeError):
                    continue

                data[node][card_key] = {
                    "vram": vram,
                    "power": power,
                    "use": use,
                    "available": available,
                    "time": parse_time
                }
        # logger.info(f'data:{data}')
        return data


    def check_nodes_in_squeue(self, nodeslist, timeout=120):
        if not self._slurm_enabled:
            return False
        if not isinstance(nodeslist, str):
            nodes_str = ','.join(nodeslist)
        else:
            nodes_str = nodeslist
        squeue_cmd = f'squeue --nodelist={nodes_str}'
        result = self._run_clush(squeue_cmd, timeout)
        if result:
            return self.check_squeue_nodes(result,len(nodeslist))
        else:
            return False
    

    def get_success_nodes(self, nodes, timeout=120):
        """
        返回同时满足：
        - nhc 检查通过
        - scontrol 状态正常
        的节点列表
        """
        if isinstance(nodes, str):
            nodes = [nodes]

        success_nodes = []

        for node in nodes:
            try:
                nhc_output = self._run_nhc(node, timeout)
            except Exception:
                continue

            try:
                scontrol_output = self._run_scontrol(node, timeout)
            except Exception:
                continue


            logger.debug(f'！！！！ {node} run_nhc：{nhc_output} 和 scontrol：{scontrol_output}！！！！')
            nhc_ok = False
            if nhc_output:
                for line in nhc_output.splitlines():
                    if "[CHECK RESULT]" in line and "PASSED" in line:
                        nhc_ok = True
                        break


            scontrol_ok = True
            if scontrol_output:
                for line in scontrol_output.splitlines():
                    if "Reason=" in line:
                        nhc_ok = False
                        break

            if nhc_ok and scontrol_ok:
                success_nodes.append(node)

        return success_nodes


    def check_squeue_nodes(self, output_text: str, threshold: int) -> bool:
        """
        解析 squeue 输出，判断是否满足条件

        参数：
            output_text: squeue 命令返回的字符串
            threshold: N 阈值

        返回：
            True / False
        """

        if not output_text:
            return False

        lines = [line.strip() for line in output_text.strip().splitlines() if line.strip()]

        # 至少需要 header + 1 行数据
        if len(lines) <= 1:
            return False

        header = lines[0]
        data_lines = lines[1:]

        # 找到 NODES 列索引（更稳健）
        headers = re.split(r'\s+', header)
        try:
            nodes_idx = headers.index("NODES")
            state_idx = headers.index("ST")
        except ValueError:
            # 输出格式异常
            return False

        total_nodes = 0
        running_jobs = 0

        for line in data_lines:
            cols = re.split(r'\s+', line)

            # 防御：列数不够
            if len(cols) <= max(nodes_idx, state_idx):
                continue

            try:
                nodes = int(cols[nodes_idx])
                state = cols[state_idx]
            except:
                continue

            total_nodes += nodes

            if state == "R":
                running_jobs += 1

        # ===== 规则判断 =====
        # 1）只有一个作业
        if len(data_lines) == 1:
            line = data_lines[0]
            cols = re.split(r'\s+', line)

            if len(cols) <= max(nodes_idx, state_idx):
                return False

            nodes = int(cols[nodes_idx])
            state = cols[state_idx]

            return state == "R" and nodes >= threshold

        # 2）多个作业：累加判断
        return total_nodes >= threshold




    # 1）显存泄漏 → 软故障
    # leak=True + usage持续增长,显存没有回落
    # → logger.warning(持续增长,显存没有回落)
    # 2）OOM → 硬故障（提前触发）
    # 1、全部显卡usage > 95%
    # → logger.warning(显存占用太大)
    # 2、部分usage > 95%，部分usage < 80%
    #   logger.warning(显存不均)
    # 3）异常节点 → 剔除
    # 某 cards usage >> others
    # → detect_filter_nodes

    def _calc_slope(self, seq: List[float]) -> float:
        """计算列表的线性回归斜率（最小二乘法）"""
        n = len(seq)
        if n < 2:
            return 0.0
        x = np.arange(n)
        y = np.array(seq)
        slope, _ = np.polyfit(x, y, 1)
        return slope

    def _is_stable(self, seq: List[float], rel_tol: float = 0.05) -> bool:
        """判断序列是否稳定（变异系数小于 rel_tol）"""
        if len(seq) < 2:
            return False
        mean_val = np.mean(seq)
        if mean_val == 0:
            return True   # 全零视为稳定
        std_val = np.std(seq)
        return (std_val / mean_val) < rel_tol


    def analyze_node_cards(self, node_name, cards, anomalies=None) -> None:
        """
        分析单节点所有卡的显存状态，检测泄漏、单卡OOM、节点级不均衡
        :param anomalies: 可选，用于收集异常信息的列表（每个元素为字典，包含异常详情）
        """
        if anomalies is None:
            anomalies = []   # 如果未传入，则创建一个空列表，但不会返回给调用者（仍只打日志）
        usages = []

        for card_key, metrics in cards.items():
            vram = metrics["vram"]
            available = metrics["available"]
            usage = vram / available if available > 0 else 0.0
            usages.append(usage)

            state_card = self._state[node_name].setdefault(card_key, {})
            history = state_card.setdefault("history", deque(maxlen=30))
            history.append(vram)

            # 1）显存泄漏检测
            if len(history) == history.maxlen:
                slope = self._calc_slope(list(history))
                mean_vram = np.mean(history)
                norm_slope = slope / max(mean_vram, 1)
                volatility = np.std(history) / max(mean_vram, 1)
                if norm_slope > 0.01 and volatility < 0.1:
                    msg = f"斜率={norm_slope:.4f}, 波动={volatility:.4f}"
                    logger.warning(f"[MEM_LEAK][{node_name}][{card_key}] {msg}")
                    anomalies.append({
                        "node": node_name,
                        "card": card_key,
                        "type": "Prob_MEM_LEAK",
                        "details": msg
                    })

            # 2）单卡 OOM 风险
            if usage > 0.95:
                msg = f"显存占用过高: {usage:.2%}"
                logger.warning(f"[OOM_RISK][{node_name}][{card_key}] {msg}")
                anomalies.append({
                    "node": node_name,
                    "card": card_key,
                    "type": "Single_Card_OOM_RISK",
                    "details": msg
                })

        # 节点级判断
        if not usages:
            return

        max_usage = max(usages)
        min_usage = min(usages)
        mean_usage = statistics.mean(usages)

        # 2.1 全卡 OOM
        if min_usage > 0.95:
            msg = f"所有卡显存占用过高: min={min_usage:.2%}, max={max_usage:.2%}"
            logger.error(f"[OOM_FATAL][{node_name}] {msg}")
            anomalies.append({
                "node": node_name,
                "type": "All_Card_OOM_RISK",
                "details": msg
            })

        # 2.2 严重不均衡
        if max_usage > 0.95 and min_usage < 0.8:
            msg = f"显存严重不均衡: max={max_usage:.2%}, min={min_usage:.2%}, mean={mean_usage:.2%}"
            logger.warning(f"[IMBALANCE][{node_name}] {msg}")
            anomalies.append({
                "node": node_name,
                "type": "IMBALANCE",
                "details": msg
            })

        # 2.3 一般不均衡
        if len(usages) > 1:
            std_usage = statistics.stdev(usages)
            if std_usage > 0.15:
                msg = f"显存分布不均: std={std_usage:.2%}, max={max_usage:.2%}, min={min_usage:.2%}, mean={mean_usage:.2%}"
                logger.warning(f"[IMBALANCE_WARN][{node_name}] {msg}")
                anomalies.append({
                    "node": node_name,
                    "type": "IMBALANCE_WARN",
                    "details": msg
                })


    def _update_state_and_detect(self, current_data, timestamp: float) -> Tuple[Dict, Dict]:
        """
        更新内部状态，并返回 (new_state, unhealthy_ranks)
        """
        from collections import defaultdict   # 确保导入

        new_state = defaultdict(dict)
        unhealthy_dict = {}
        anomalies = []

        for node, cards in current_data.items():
            # 先分析显存状态（会触发泄漏、OOM等日志）
            self.analyze_node_cards(node, cards, anomalies=anomalies)

            # 获取上一轮该节点的状态
            prev_node_state = self._state.get(node, {})

            for card_key, metrics in cards.items():
                vram = metrics["vram"]
                power = metrics["power"]
                use = metrics["use"]
                available = metrics["available"]

                # 从上一轮获取历史队列（若不存在则新建）
                prev_card_state = prev_node_state.get(card_key, {})
                history = prev_card_state.get("history", deque(maxlen=30))
                use_history = prev_card_state.get("use_history", deque(maxlen=30))
                power_history = prev_card_state.get("power_history", deque(maxlen=30))

                # 追加当前值
                history.append(vram)
                use_history.append(use)
                power_history.append(power)

                # ---- 卡死检测 ----
                # 条件：三个队列都满了，且都稳定，且平均利用率 < 5
                if (len(history) == history.maxlen and
                    len(use_history) == use_history.maxlen and
                    len(power_history) == power_history.maxlen and
                    self._is_stable(list(history)) and
                    self._is_stable(list(use_history)) and
                    self._is_stable(list(power_history)) and
                    np.mean(use_history) < 5):
                    msg = f"VRAM/use/power 长时间稳定且利用率低"
                    logger.error(
                        f"[HANG][{node}][{card_key}] "
                        f"VRAM/use/power 长时间稳定且利用率低"
                    )
                    anomalies.append({
                        "node": node,
                        "card": card_key,
                        "type": "HANG",
                        "details": msg
                        })
                    unhealthy_dict.setdefault(node, []).append(card_key)

                # ---- 保存新状态（包含所有历史队列）----
                new_state[node][card_key] = {
                    "power": power,
                    "use": use,
                    "available": available,
                    "history": history,
                    "use_history": use_history,
                    "power_history": power_history,
                }

            
            # 加到running_nodes中
        return new_state, unhealthy_dict, anomalies



    
def main():
    """
    主函数：解析命令行参数，调用 cross_check 执行节点交叉检测
    使用示例：python this_script.py --hostfile /path/to/your/hostfile
    """
    # 1. 解析命令行参数
    parser = argparse.ArgumentParser(description="节点交叉检测工具")
    parser.add_argument(
        "--hostfile", "-f",
        required=True,
        help="指定待检测节点的 hostfile 路径（必填）"
    )
    args = parser.parse_args()

    # 2. 验证 hostfile 是否存在
    if not os.path.exists(args.hostfile):
        print(f"错误：指定的 hostfile 不存在 → {args.hostfile}")
        exit(1)

    # 3. 创建类实例，调用 cross_check 方法
    # try:
    #     fault_detection = FaultDetection()  # 实例化类
    #     normal_nodes, abnormal_nodes = fault_detection.cross_check(args.hostfile)  # 调用方法

    #     # 4. 打印检测结果
    #     print("\n===== 节点交叉检测结果 =====")
    #     print(f"健康节点数量：{len(normal_nodes)}")
    #     if normal_nodes:
    #         print("健康节点列表：")
    #         for node in normal_nodes:
    #             print(f"  - {node}")

    #     print(f"\n异常节点数量：{len(abnormal_nodes)}")
    #     if abnormal_nodes:
    #         print("异常节点列表：")
    #         for node in abnormal_nodes:
    #             print(f"  - {node}")

    # except Exception as e:
    #     print(f"\n检测执行失败：{str(e)}")
    #     exit(1)
    # fault_detection = FaultDetection()
    # fault_detection.check_sinfo_R()

# 程序入口
if __name__ == "__main__":
    main()

    


