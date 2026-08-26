# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import Any, Dict
import time
from cluster_manager.config.global_config import logger

# Epoch-related patterns

# Epoch-related patterns

LOG_TIMESTAMP_PATTERN = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\]')
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
    'fault_pid': r'Process name: \[(prterun-.+?)\]',
    'remote_node': r'Remote daemon:\s*(\[\[\d+,\d+\],\d+\])\s*on node\s*([A-Za-z0-9_-]+)',
    'remote_daemon': r'Remote daemon: \[(prterun-.+?)\] on node (\S+)',
    'traceback_error': r'Traceback \(most recent call last\):',
    'primary_message': r'Primary job\s+terminated normally, but\s+1 process returned',
    'connection_closed': r'RuntimeError.*Connection closed by peer.*?\[(\d+\.\d+\.\d+\.\d+)\]:(\d+)',
    'ib_error': r'\[[^]]+\]:\[([A-Za-z0-9]+):(\d+):\d+:\d+\].*send completion with error: Work Request Flushed Error',
    'rank_pattern': r'(\d+\.\d+)/(\d+)',
    'terminated': r'.*行\s+\d+:\s+\d+\s+Terminated.*'
}

# Common patterns
COMMON_PATTERNS = {
    'reason': r'^(.+?)\s+(\S+)\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(.+)$',
    'ckpt_time': r'save-checkpoint\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)',
    'eval_time': r'evaluate\s+[\.]+:\s*\((?P<min>[\d\.]+)\s*,\s*(?P<max>[\d\.]+)\)',
    'ckpt_iter': r'saving checkpoint at iteration\s+(\d+)\s+to',
    'eval_iter': r'validation loss at iteration\s+(\d+)',
    'iteration': r'.*?\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+iteration\s+(?P<iter_num>\d+)/\s+\d+\s+\|.*?elapsed time per iteration \(ms\):\s+(?P<iter_time>[\d\.]+)'
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

def extract_log_timestamp(log_line: str) -> str:
    """从日志行头部提取原生时间戳，无则返回当前系统时间"""
    match = LOG_TIMESTAMP_PATTERN.search(log_line)
    if match:
        return match.group(1)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

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
    timestamp = extract_log_timestamp(log_line)
    pattern1 = EPOCH_PATTERNS['epoch']
    match1 = re.search(pattern1, log_line)
    if match1:
        result = {
            'type': 'log',
            'all' : log_line,
            'total_iter': int(match1.group(3)),  # Using the second group for total
            'epoch_num': int(match1.group(1)),  # Extract the epoch number
            'epoch_match': match1.group(0),
            'timestamp': timestamp,
        }
        
        # 提取所有epoch指标
        extract_patterns(log_line, EPOCH_PATTERNS, result)
        if 'num_updates' in result:
            result['current_iter'] = int(result['num_updates'])
        else:
            result['current_iter'] = int(match1.group(2))
        return result
    
    # 匹配第二种格式: iteration        2/    5000
    pattern2 = re.compile(ITERATION_PATTERNS['iteration'])
    match2 = re.search(pattern2, log_line)
    
    if match2:
        result = {
            'type': 'log',
            'current_iter': int(match2.group(1)),
            'total_iter': int(match2.group(2)),
            'iteration_match': match2.group(0)
        }
        
        # 提取所有iteration指标
        extract_patterns(log_line, ITERATION_PATTERNS, result)
        return result
    
    result['timestamp'] = timestamp

    return None


def parse_error_log(log_line):
    line = log_line.strip()
    result = {}
    
    # 使用ERROR_PATTERNS进行错误日志匹配
    for key, pattern in ERROR_PATTERNS.items():
        try:
            match = re.search(pattern, line)
            if match:
                result['event_type'] = 'LOG_EXIT'
                result['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                if pattern == ERROR_PATTERNS['fault_pid']:
                    result['type'] = 'rank'
                    result['fault_pid'] = match.group(1)
                elif pattern == ERROR_PATTERNS['remote_node'] or pattern == ERROR_PATTERNS['remote_daemon']:
                    result['type'] = 'node'
                    result['fault_pid'] = match.group(2)
                elif pattern == ERROR_PATTERNS['traceback_error']:
                    result['type'] = 'proc'
                    result['fault_pid'] = match.group()
                else:
                    result['type'] = 'proc'
                    result['fault_pid'] = match.group()

                break
        except Exception as e:
            logger.exception(f"正则表达式错误 {key}: {e}")
    

    if result.get('connection_closed'):
        result['type'] = 'node'
        result["fault_pid"] = result.get('connection_closed')

    return result



def create_analyze_special_log():
    def parse(line: str) -> tuple[str, dict]:

        # 初始化空结果字典
        result = {}
        
        # 1. 分别解析训练日志和错误日志
        train_log_res = parse_training_log(line)   
        error_log_res = parse_error_log(line)     
        
        # 2. 合并字典（错误日志字段若与训练日志重名，会覆盖训练日志，可根据需求调整）
        if train_log_res:
            result.update(train_log_res)
        if error_log_res:
            result.update(error_log_res)
            # logger.info(f'-----------error_log_res:{error_log_res}-----------')
        
        if result:
            return result
        
        return None

    return parse



if __name__ == '__main__':
    log_file = '/workspace/logs/training.log'

    parse = create_analyze_special_log()

    with open(log_file,'r') as f:
        for line in f.readlines():
            res = parse(line)
            if res:
                print(res)
