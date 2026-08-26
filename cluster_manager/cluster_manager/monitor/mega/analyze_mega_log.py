# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import Optional, Callable, Dict, Any, Union
import time
from ..log_patterns import parse_training_log, parse_error_log

def create_analyze_mega_log():
                        # -> Optional[Dict[str, Any]]
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
        
        if result:
            return result
        
        return None

    return parse



if __name__ == '__main__':
    log_file = '/workspace/logs/llama.log'

    parse = create_analyze_mega_log()

    with open(log_file,'r') as f:
        for line in f.readlines():
            res = parse(line)
            if res:
                print(res)
