# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import Optional, Callable, Dict, Any, Union
import time
from cluster_manager.config.global_config import logger

# Epoch-related patterns

# Epoch-related patterns

# Epoch-related patterns
EPOCH_PATTERNS = {
    'epoch': r'epoch\s+(\d+):\s+(\d+)\s+/\s+(\d+)',
    'loss': r'loss(?:[_\w]*)?=([\d.]+)', 
    'ntokens': r'ntokens=([\d.]+)', 
    'nsentences': r'nsentences=([\d.]+)', 
    'wps': r'wps=([\d.]+)', 
    'ups': r'ups=([\d.]+)', 
    'wpb': r'wpb=([\d.]+)', 
    'batch_size': r'bsz=([\d.]+)', 
    'num_updates': r'num_updates=(\d+)',
    'learning_rate': r'lr=([\d.eE-]+)', 
    'gradient_norm': r'gnorm=([\d.]+)', 
    'clip': r'clip=([\d.]+)', 
    'train_time': r'train_wall=([\d.]+)', 
    'fetch_data_time': r'fetch_data=([\d.]+)', 
    'cuda_active_gb': r'cuda_gb_active=([\d.]+)', 
    'cuda_allocated_gb': r'cuda_gb_allocated=([\d.]+)',
    'cuda_reserved_gb': r'cuda_gb_reserved=([\d.]+)',
    'cuda_free_gb': r'cuda_gb_free=([\d.]+)',
    'wall_time': r'wall=([\d.]+)'
}

# Iteration-related patterns
ITERATION_PATTERNS = {
    'iteration': r'iteration\s+(\d+)/\s*(\d+)',
    'consumed_samples': r'consumed samples:\s+(\d+)',
    'elapsed_time_ms': r'elapsed time per iteration \(ms\):\s+([\d.]+)', 
    'throughput_tflops_per_gpu': r'throughput per GPU \(TFLOP/s/GPU\):\s+([\d.]+)', 
    'learning_rate': r'learning rate:\s+([\d.Ee+-]+)', 
    'global_batch_size': r'global batch size:\s+(\d+)',
    'lm_loss': r'lm loss:\s+([\d.Ee+-]+)', 
    'loss_scale': r'loss scale:\s+([\d.]+)', 
    'gradient_norm': r'grad norm:\s+([\d.]+)', 
    'skipped_iterations': r'number of skipped iterations:\s+(\d+)',
    'nan_iterations': r'number of nan iterations:\s+(\d+)'
}

# Error-related patterns
ERROR_PATTERNS = {
    'prte_process_exit': r'Process name:\s*(\[prterun-[^\]]+\])\s*Exit code:\s*(-?\d+)',
    'fault_pid': r'Process name:\s*(\[\[\d+,\d+\],\d+\])',
    'remote_node': r'Remote daemon:\s*(\[\[\d+,\d+\],\d+\])\s*on node\s*([A-Za-z0-9_-]+)',
    'target_node': r'target node:\s*([A-Za-z0-9_-]+)',
    'remote_daemon': r'Remote daemon: \[(prterun-.+?)\] on node (\S+)',
    'rank_traceback': r'^\[rank(\d+)\]:\s*Traceback \(most recent call last\):',
    'traceback_error': r'Traceback \(most recent call last\):',
    'connection_closed': r'RuntimeError.*Connection closed by peer.*?\[(\d+\.\d+\.\d+\.\d+)\]:(\d+)',
    'ib_error': r'\[[^]]+\]:\[([A-Za-z0-9]+):(\d+):\d+:\d+\].*send completion with error: Work Request Flushed Error',
    'rank_pattern': r'(\d+\.\d+)/(\d+)',
    'nan_loss': r'AssertionError: Rank (\d+): found NaN in local forward loss calculation\. Device: \d+, node: ([A-Za-z0-9_-]+)',
    'inf': r'RuntimeError:.*?\bRank\s+(\d+).*?\binf\b', 
    'nan_grad': r'RuntimeError: Rank (\d+), node ([A-Za-z0-9_-]+), device \d+, iteration \d+: Unexpected result nan \(message=.*?found NaN in local grad norm.*?\)'
}

# Common patterns
COMMON_PATTERNS = {
    'reason': r'^(.+?)\s+(\S+)\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(.+)$',
    'ckpt_time': r'save-checkpoint\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)',
    'eval_time': r'evaluate\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)',
    'ckpt_iter': r'saving checkpoint at iteration\s+(\d+)\s+to',
    'eval_iter': r'validation loss at iteration\s+(\d+)',
    'iteration': r'^\s*\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*iteration\s+(?P<iter_num>\d+)/\s*(?P<total_iter>\d+).*?elapsed time per iteration \(ms\):\s+(?P<iter_time>[\d.]+)',
    'eval_interval': r'eval_interval\s+\.{3,}\s*(\d+)',
    'seq_length': r'seq_length\s+\.{3,}\s*(\d+)',
}

# 预编译所有正则（提升匹配效率）
def compile_patterns():
    """预编译所有正则表达式，返回编译后的字典"""
    compiled = {
        'EPOCH': {},
        'ITERATION': {},
        'ERROR': {},
        'COMMON': {}
    }
    # 编译Epoch相关
    for name, pattern in EPOCH_PATTERNS.items():
        compiled['EPOCH'][name] = re.compile(pattern)
    # 编译Iteration相关
    for name, pattern in ITERATION_PATTERNS.items():
        compiled['ITERATION'][name] = re.compile(pattern)
    # 编译Error相关
    for name, pattern in ERROR_PATTERNS.items():
        compiled['ERROR'][name] = re.compile(pattern)
    # 编译Common相关
    for name, pattern in COMMON_PATTERNS.items():
        compiled['COMMON'][name] = re.compile(pattern)
    return compiled

# 导出预编译的正则字典（全局可用）
COMPILED_PATTERNS = compile_patterns()

__all__ = [
    'EPOCH_PATTERNS',
    'ITERATION_PATTERNS',
    'ERROR_PATTERNS',
    'COMMON_PATTERNS',
    'COMPILED_PATTERNS'
]


def extract_patterns(log_line, patterns, result):
    """通用提取函数"""
    for key, pattern in patterns.items():  # 使用 .items() 遍历字典
        match = re.search(pattern, log_line)
        if match:
            result[key] = match.group(1)  # 这里可以根据需要应用转换

def parse_training_log(log_line):
    """
    解析训练日志，提取epoch/iteration信息
    """
    result: Dict[str, Any] = {}
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    pattern1 = EPOCH_PATTERNS['epoch']
    match1 = re.search(pattern1, log_line)
    if match1:
        result = {
            'type': 'log',
            'all' : log_line,
            'current_iter': int(match1.group(2)),  # Extract 'iter_num' using named group
            'total_iter': int(match1.group(3)),  # Using the second group for total
            'epoch_num': int(match1.group(1)),  # Extract the epoch number
        }
        
        # 提取所有epoch指标
        extract_patterns(log_line, EPOCH_PATTERNS, result)
        return result
    
    # 匹配第二种格式: iteration        2/    5000
    pattern2 = re.compile(COMMON_PATTERNS['iteration'])
    match2 = re.search(pattern2, log_line)
    
    if match2:
        result = {
            'type': 'log',
            'all' : log_line,
            'current_iter': int(match2.group('iter_num')),
            'timestamp': match2.group('timestamp'),
            'iter_time': float(match2.group('iter_time')),
            'total_iter': int(match2.group('total_iter')),
        }
        
        # 提取所有iteration指标
        extract_patterns(log_line, ITERATION_PATTERNS, result)
        return result
    
    #result['timestamp'] = timestamp

    ckpt_match = COMPILED_PATTERNS['COMMON']['ckpt_time'].search(log_line)
    if ckpt_match:
        ckpt_max_time = float(ckpt_match.group('max'))
        result = {'type': 'ckpt_time', 'max_time': ckpt_max_time}
        return result
    ckpt_iter_match = COMPILED_PATTERNS['COMMON']['ckpt_iter'].search(log_line)
    if ckpt_iter_match:
        ckpt_iter = int(ckpt_iter_match.group(1))
        result = {'type': 'ckpt_start', 'iter_num': ckpt_iter}
        return result

    return None

def parse_error_log(log_line):
    line = log_line.strip()
    # 初始化符合要求的嵌套结构
    exit_msg = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # 保留原时间格式
        "type": "exit",  # 外层固定为exit
        "data": {
            "type": "",   # 内层type：rank/node/proc
            "fault_pid": "",  # 保留原字段名fault_pid
            "exit_code": 1    # 新增exit_code，默认值1
        }
    }
    matched = False
    # 使用ERROR_PATTERNS进行错误日志匹配
    for key, pattern in ERROR_PATTERNS.items():
        try:
            match = re.search(pattern, line)
            if match:
                matched = True
                # 根据不同pattern设置data内的字段
                if key == 'prte_process_exit':
                    exit_code = int(match.group(2))
                    exit_msg['data'] = {
                        'type': 'proc',
                        'fault_pid': match.group(1),
                        'exit_code': exit_code,
                        'runtime': 'prte',
                    }
                    if exit_code >= 128:
                        exit_msg['data']['signal'] = exit_code - 128
                elif pattern == ERROR_PATTERNS['fault_pid']:
                    exit_msg['data']['type'] = 'rank'
                    exit_msg['data']['fault_pid'] = match.group(1)
                elif pattern in [ERROR_PATTERNS['remote_node'], ERROR_PATTERNS['remote_daemon']]:
                    exit_msg['data']['type'] = 'node'
                    exit_msg['data']['fault_pid'] = match.group(2)
                elif pattern == ERROR_PATTERNS['target_node']:
                    exit_msg['data']['type'] = 'node'
                    exit_msg['data']['fault_pid'] = match.group(1)
                elif key == 'rank_traceback':
                    exit_msg['data']['type'] = 'proc'
                    exit_msg['data']['fault_pid'] = 'Traceback (most recent call last):'
                    exit_msg['data']['traceback_rank'] = int(match.group(1))
                elif pattern == ERROR_PATTERNS['traceback_error']:
                    exit_msg['data']['type'] = 'proc'
                    exit_msg['data']['fault_pid'] = match.group()
                elif key == "connection_closed":
                    exit_msg["data"] = {
                        "type": "communication",
                        "peer_ip": match.group(1),
                        "peer_port": int(match.group(2)),
                        "fault_pid": match.group(),
                    }
                elif key == "ib_error":
                    exit_msg["data"] = {
                        "type": "communication",
                        "host": match.group(1),
                        "fault_pid": match.group(),
                    }
                elif key == "nan_loss" or key == 'nan_grad':
                    exit_msg["type"] = "loss"
                    exit_msg["data"] = {
                        "type": "NaN",
                        "rank": match.group(1),
                        "node": match.group(2)
                    }
                elif key == "inf":
                    exit_msg["type"] = "inf"
                    exit_msg["data"] = {
                        "type": "INF",
                        "rank": match.group(1)
                    }
                else:
                    exit_msg['data']['type'] = 'proc'
                    exit_msg['data']['fault_pid'] = match.group()
                break
        except Exception as e:
            logger.exception(f"正则表达式错误 {key}: {e}")
    
    if exit_msg['data'].get('connection_closed'):
        exit_msg['data']['type'] = 'node'
        exit_msg['data']['fault_pid'] = exit_msg['data'].get('connection_closed')
    
    if matched:
        return exit_msg
    else:
        return {}
    
