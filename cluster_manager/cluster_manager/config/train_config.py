# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# cluster_manager/config/train_config.py
"""
Megatron 训练配置模块

职责：
1. 从 shell/yaml/json 文件加载训练参数
2. 提供全局配置访问接口

配置定义在 global_config.py 中：
- ARG_MAP: 参数映射
- MEGATRON_TARGET_ARGS: 自定义目标参数
- DEFAULT_TRAIN_CONFIG: 默认训练配置
"""
import ast
import os
import re
import json
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# 全局配置缓存（统一存储）
_TRAIN_CONFIG: Dict = {}

# 从 global_config 导入配置定义（避免循环导入，在函数内部导入）


def _resolve_string_vars(var_value: str, known_vars: dict) -> str:
    """解析字符串中的变量引用"""
    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return str(known_vars.get(var_name, match.group(0)))
    return re.sub(r'\$\{(\w+)\}|\$(\w+)', replacer, var_value)


def _safe_eval_arithmetic(expr_str: str, known_vars: dict):
    """计算仅包含数字、已知变量和基础运算符的 Shell 算术表达式。"""
    clean_expr = expr_str.replace('$', '')
    if len(clean_expr) > 256:
        return None

    safe_values = {
        key: value
        for key, value in known_vars.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }

    def evaluate(node):
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
                return node.value
            raise ValueError("unsupported literal")
        if isinstance(node, ast.Name):
            if node.id not in safe_values:
                raise ValueError(f"unknown variable: {node.id}")
            return safe_values[node.id]
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = evaluate(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.BinOp):
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
        raise ValueError("unsupported arithmetic expression")

    try:
        return int(evaluate(ast.parse(clean_expr, mode="eval")))
    except (SyntaxError, TypeError, ValueError, ZeroDivisionError, OverflowError):
        return None


def _parse_shell_script(file_path: str) -> dict:
    """解析 Shell 脚本中的 Megatron 参数"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 构建变量上下文
    var_pattern = re.compile(r'^\s*(\w+)\s*=\s*(.+?)\s*(?:#.*|$)')
    arith_pattern = re.compile(r'^\$\(\(\s*(.+?)\s*\)\)$')
    context = {}
    
    for line in content.splitlines():
        match = var_pattern.match(line)
        if not match:
            continue
        var_name, var_value = match.group(1), match.group(2).strip('"').strip("'")
        
        arith_match = arith_pattern.match(var_value)
        if arith_match:
            calc_result = _safe_eval_arithmetic(arith_match.group(1), context)
            context[var_name] = calc_result if calc_result is not None else var_value
            continue
        
        resolved_value = _resolve_string_vars(var_value, context)
        if '$' in resolved_value:
            context[var_name] = None
        else:
            try:
                context[var_name] = int(resolved_value)
            except ValueError:
                context[var_name] = resolved_value
    
    # 提取 Megatron 参数
    arg_pattern = re.compile(r'--([a-zA-Z0-9_-]+)\s+([^\s#\\]+)')
    parsed_args = {}
    
    for line in content.splitlines():
        for match in arg_pattern.finditer(line):
            arg_name = match.group(1)  # 例如: tensor-model-parallel-size
            raw_value = match.group(2).strip('"').strip("'")
            final_value = _resolve_string_vars(raw_value, context)
            
            if '$' in final_value:
                parsed_args[arg_name] = None
            else:
                try:
                    parsed_args[arg_name] = int(final_value)
                except ValueError:
                    parsed_args[arg_name] = final_value
    
    # 处理布尔标志
    if re.search(r'--sequence-parallel(?:\s|$)', content):
        parsed_args['sequence-parallel'] = True
    
    # 调试输出
    logger.debug(f"解析到的参数: {parsed_args}")
    
    return parsed_args


def load_train_config(config_path: str):
    """加载训练配置（启动时调用一次）"""
    global _TRAIN_CONFIG
    
    # 从 global_config 导入配置定义（避免循环导入）
    from cluster_manager.config.global_config import MEGATRON_TARGET_ARGS, PERSISTENT_KEYS, DEFAULT_TRAIN_CONFIG
    
    if not config_path or not os.path.exists(config_path):
        _TRAIN_CONFIG = DEFAULT_TRAIN_CONFIG.copy()
        logger.warning("配置文件不存在或未指定，使用默认配置")
        return
    
    suffix = os.path.splitext(config_path)[1].lower()
    
    try:
        if suffix in [".sh", ".bash"]:
            raw = _parse_shell_script(config_path)
        elif suffix in [".yaml", ".yml"]:
            import yaml
            with open(config_path) as f:
                raw = yaml.safe_load(f) or {}
        elif suffix == ".json":
            with open(config_path) as f:
                raw = json.load(f) or {}
        else:
            _TRAIN_CONFIG = DEFAULT_TRAIN_CONFIG.copy()
            return
        
        result = DEFAULT_TRAIN_CONFIG.copy()
        
        # 解析 PERSISTENT_KEYS 中的参数（Megatron 原生参数形式）
        for key in PERSISTENT_KEYS:
            # key 格式: --tensor-model-parallel-size
            # raw 中存储的格式: tensor-model-parallel-size（不带 --）
            clean_key = key.lstrip('-')  # 去掉前面的 --
            if clean_key in raw and raw[clean_key] is not None:
                result[key] = raw[clean_key]
            elif key in raw and raw[key] is not None:
                result[key] = raw[key]
        
        # 解析自定义目标参数
        for target_arg in MEGATRON_TARGET_ARGS:
            clean_arg = target_arg.lstrip('-')
            if clean_arg in raw and raw[clean_arg] is not None:
                result[target_arg] = raw[clean_arg]
            elif target_arg in raw and raw[target_arg] is not None:
                result[target_arg] = raw[target_arg]
        
        _TRAIN_CONFIG = result
        
        logger.info(f"成功加载训练配置: {config_path}")
        
    except Exception as e:
        logger.error(f"加载训练配置失败: {e}")
        _TRAIN_CONFIG = DEFAULT_TRAIN_CONFIG.copy()


def get_config(key: str, default=None):
    """
    获取配置参数值（统一获取方式）
    
    Args:
        key: 参数名，如 "tp", "pp", "--save-interval"
        default: 默认值
    
    Returns:
        参数值
    """
    return _TRAIN_CONFIG.get(key, default)


def get_train_config() -> dict:
    """获取所有训练配置"""
    return _TRAIN_CONFIG.copy() if _TRAIN_CONFIG else {}


def get_megatron_config() -> dict:
    """获取所有配置（MEGATRON_TARGET_ARGS + PERSISTENT_KEYS）"""
    from cluster_manager.config.global_config import MEGATRON_TARGET_ARGS, PERSISTENT_KEYS
    result = {}
    # 获取自定义参数
    for key in MEGATRON_TARGET_ARGS:
        if key in _TRAIN_CONFIG:
            result[key] = _TRAIN_CONFIG[key]
    # 获取落盘参数
    for key in PERSISTENT_KEYS:
        if key in _TRAIN_CONFIG:
            result[key] = _TRAIN_CONFIG[key]
    return result


def get_persistent_config() -> dict:
    """获取落盘参数（容错重启时对比，区分模型是否变化）"""
    from cluster_manager.config.global_config import PERSISTENT_KEYS
    result = {}
    for key in PERSISTENT_KEYS:
        if key in _TRAIN_CONFIG:
            result[key] = _TRAIN_CONFIG[key]
    return result


def set_megatron_config(key: str, value):
    """设置配置参数"""
    _TRAIN_CONFIG[key] = value
