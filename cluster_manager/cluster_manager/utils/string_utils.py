# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import subprocess
from typing import Tuple, Optional, List
import errno
import re
from cluster_manager.config import *
from cluster_manager.config.global_config import logger
from collections import defaultdict


def split_outside_brackets(s: str) -> List[str]:
    """按逗号分割字符串但忽略方括号内部的逗号。"""
    parts = []
    buf = []
    depth = 0
    for ch in s:
        if ch == '[':
            depth += 1
            buf.append(ch)
        elif ch == ']':
            depth = max(depth - 1, 0)
            buf.append(ch)
        elif ch == ',' and depth == 0:
            part = ''.join(buf).strip()
            if part:
                parts.append(part)
            buf = []
        else:
            buf.append(ch)
    last = ''.join(buf).strip()
    if last:
        parts.append(last)
    return parts

def expand_range(expr: str) -> List[str]:
    """
    递归展开包含多个范围的节点表达式
    支持格式：
    - 逗号分隔：[2,4] → 2,4
    - 连字符范围：[01-10] → 01,02,...,10（保留数字位数）
    - 组合表达式：b11r[2,4]n[01-10] → 所有可能组合
    """
    # 找到第一个[的位置
    left = expr.find('[')
    if left == -1:
        return [expr]  # 没有范围，直接返回自身
    
    # 找到对应的]的位置
    right = expr.find(']', left)
    if right == -1:
        return [expr]  # 不完整的范围，返回原始字符串
    
    # 提取范围部分和前后固定部分
    prefix = expr[:left]
    range_str = expr[left+1:right]
    suffix = expr[right+1:]
    
    # 解析范围内容（支持逗号分隔的多个子范围）
    range_items = []
    for item in range_str.split(','):
        item = item.strip()
        # 处理连字符范围（如01-10）
        if '-' in item:
            start_str, end_str = item.split('-', 1)
            # 尝试解析为数字
            try:
                start = int(start_str)
                end = int(end_str)
                # 确定数字位数（用于补零，如01→2位）
                digit_len = max(len(start_str), len(end_str))
                # 生成范围内所有数字（包含两端）
                for num in range(start, end + 1):
                    # 按原始位数补零（如1→01）
                    range_items.append(f"{num:0{digit_len}d}")
            except ValueError:
                # 非数字范围，直接作为字符串处理
                range_items.append(item)
        else:
            # 单个项（如2或05）
            range_items.append(item)
    
    # 递归展开后缀，并组合所有可能的前缀+范围项+后缀
    expanded = []
    for item in range_items:
        # 拼接当前范围项和后缀，递归处理后缀中的范围
        for suffix_expanded in expand_range(f"{item}{suffix}"):
            expanded.append(f"{prefix}{suffix_expanded}")
    
    return expanded


def node_expr_parser(node_expr: str) -> List[str]:
    """
    b12r3n02
    b11r4n[20-22] (3)
    b11r4n20,b11r4n[22-23] (3)
    b11r4n[22-23] (3), b11r4n25
    b11r1n[04,09-11,14-17,19],b11r2n[05-08,11]
    """
    nodes = []
    for expr in split_outside_brackets(node_expr):
        node_expr_line = expr.split("(")[0].strip()
        nodes.extend(expand_range(node_expr_line))
    return nodes


def parse_sinfo_R(raw_text: str) -> dict:
    """
    解析 sinfo -R 输出，返回 {node: reason}
    """
    node_reason_map = {}

    lines = raw_text.strip().splitlines()
    if len(lines) <= 1:
        return node_reason_map  # 空或只有表头

    # 跳过表头
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue

        # 按列切分（注意 REASON 可能有空格，所以用最大分割）
        parts = line.split()
        if len(parts) < 5:
            logger.warning(f"skip invalid line: {line}")
            continue

        # NODELIST 在最后一列
        nodelist_expr = parts[-1]

        # REASON 
        reason = " ".join(parts[:-4])

        try:
            nodes = node_expr_parser(nodelist_expr)
        except Exception as e:
            logger.exception(f"parse node expr failed: {nodelist_expr}, err={e}")
            continue

        for node in nodes:
            node_reason_map[node] = reason

    return node_reason_map


def match_failed_nodes(raw_text: str, check_nodes: List[str]) -> dict:
    """
    返回 check_nodes 中出现在 sinfo -R 的节点及其 reason
    """
    node_reason_map = parse_sinfo_R(raw_text)

    result = {}
    for node in check_nodes:
        if node in node_reason_map:
            result[node] = node_reason_map[node]

    return result


# ----------------- 公共：识别 网络连接 节点 -----------------
def extract_failed_nodes_from_line(line: str) -> List[str]:
    """
    从日志中提取失败节点（支持单行/多行日志，自动覆盖所有常见失败场景，节点范围展开+去重）
    支持的失败格式：
    1. 单节点SSH解析失败：b11r4n21: ssh: Could not resolve hostname ...
    2. 单节点连接关闭：b11r4n20: Connection closed by ...
    3. 单节点权限拒绝：b11r4n20: Access denied by ...
    4. 单节点连接重置：b12r1n05: Connection reset by ...
    5. clush前缀单节点失败：clush: b12r3n02: exited with exit code ...
    6. clush前缀范围节点失败：clush: b11r4n[20-22] (3): exited with exit code 255
    7. clush前缀逗号分隔节点失败：clush: b11r4n20,b11r4n[22-23] (3): exited with exit code 255
    """
    # 判断是否包含目标报错关键词
    error_keywords = [
        "ssh: Could not resolve hostname",
        "Connection closed by",
        "Access denied by",
        "Connection reset by",
        "exited with exit code"
    ]

    if not any(key in line for key in error_keywords):
        return []
    
    m = re.search(r"(?:clush:\s*)?([^\s:]+\[[^\]]+\]|[^\s:]+(?=[: ]))", line)
    if not m:
        return []
   
    node_field = m.group(1) or m.group(2)
    return node_expr_parser(node_field)


def parse_proc_status_check(logfile) -> Tuple[List[str], List[str], List[str]]:
    """
    解析日志，返回 (failed_nodes, zero_nodes, nonzero_nodes)
    说明：
    - failed_nodes：通过多种失败行识别得到（展开表达式，去重、保持顺序）
    - 对于“节点行 + 数字行”形式的解析：
        支持 "expr (N)" 形式（在同一行有括号数字），也支持先出现节点行再出现单独数字行
    """
    failed_nodes: List[str] = []
    nodes: List[str] = []

    with open(logfile, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    
    i = 0
    while i < len(lines):
        line = lines[i]

        # 识别单行失败（ssh resolve / connection closed / access denied / clush）
        failed = extract_failed_nodes_from_line(line)
        if failed:
            failed_nodes.extend(failed)
            i += 1
            continue

        # 遇到分隔块：解析一个节点组及其值
        if line.startswith("---------------") and i + 2 < len(lines):
            # 第二行通常是节点表达式行，可能带 "(N)" 统计尾巴
            node_expr_line = lines[i + 1].split("(")[0].strip()
            
            for expr in split_outside_brackets(node_expr_line):
                nodes.extend(expand_range(expr))
            
            
        i += 1

    return nodes, failed_nodes

def compress_nodes(nodes):
    """
    ['g07r4n01','g07r4n02','g07r4n03']
        -> g07r4n[01-03]
    """
    if len(nodes) == 1:
        return nodes
    groups = defaultdict(list)

    # 解析 prefix + number
    pattern = re.compile(r"(.*?)(\d+)$")

    for node in nodes:
        m = pattern.match(node)
        if not m:
            continue

        prefix, num = m.groups()
        groups[prefix].append(int(num))

    results = []

    for prefix, nums in sorted(groups.items()):

        nums = sorted(set(nums))

        ranges = []
        start = nums[0]
        prev = nums[0]

        for n in nums[1:]:

            if n == prev + 1:
                prev = n
                continue

            ranges.append((start, prev))
            start = prev = n

        ranges.append((start, prev))

        parts = []

        for s, e in ranges:
            if s == e:
                parts.append(f"{s:02d}")
            else:
                parts.append(f"{s:02d}-{e:02d}")

        results.append(f"{prefix}[{','.join(parts)}]")

    return ",".join(results)

    
