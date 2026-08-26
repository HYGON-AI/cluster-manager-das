# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import json
import hashlib
import threading
from enum import Enum, auto
from datetime import datetime

from cluster_manager.config.global_config import logger
import cluster_manager.config.global_config as global_config
from cluster_manager.utils.string_utils import compress_nodes


# {
#   "job_name": "gpt_moe_train"
#   "run_state": "RUNNING",
#   "required_nodes_num": 32,
#   "slots_per_node": 8,
#   "model_config": {
#     "world_size": 256,
#     "tp": 4,
#     "pp": 8,
#     "dp": 8,
#     "ep": 8,
#     "num_experts": 32,
#     "micro_batch_size": 4,
#     "global_batch_size": 1024,
#     "gradient_accumulation_steps": 4
#   },
#   "active_nodes": [
#     "node01",
#     "node02",
#     "node03"
#   ],
#   "start_time": "2026-03-10T22:10:31",
#   "timestamp": "2026-03-10T22:10:31"
# }

class RunState(Enum):
    INIT = auto()
    STARTING = auto()
    PENDING = auto()
    RUNNING = auto()
    HANG = auto()
    LOSS_DIVERGENCE = auto()
    RECOVERING = auto()
    FAULT = auto()
    FAULT_RECOVER = auto()
    COMPLETE = auto()


class RunStateManager:

    def __init__(self, node_pool_proxy):

        self.node_pool_proxy = node_pool_proxy
        self.lock = threading.Lock()

        self.run_state = RunState.INIT
        self.start_time = None
        self.train_config_path = f"{global_config.WORK_DIR}/workspace/running_state.json"

    # ------------------------------------------------
    # config value 读取
    # ------------------------------------------------

    def _get_cfg_value(self, cfg, key):

        value = cfg.get(key, -1)

        if value is None:
            return -1

        return value


    def _print_runtime_args_compare(self, saved_config, current_config):
        """紧凑输出配置对比，便于观察差异"""
        
        # 合并所有 key
        all_keys = set(saved_config.keys()) | set(current_config.keys())
        
        # 找出有差异的 key
        diff_keys = []
        for key in all_keys:
            saved_val = saved_config.get(key, "N/A")
            curr_val = current_config.get(key, "N/A")
            if saved_val != curr_val:
                diff_keys.append(key)
        
        # 输出概要
        logger.info(f"[Config Compare] total={len(all_keys)}, diff={len(diff_keys)}")
        
        # 输出差异项（单行紧凑格式）
        if diff_keys:
            diff_str = ", ".join(f"{k}: {saved_config.get(k, 'N/A')} -> {current_config.get(k, 'N/A')}" 
                                 for k in sorted(diff_keys))
            logger.info(f"[Config Compare] {diff_str}")

    def compare_runtime_args(self, saved_config: dict, current_config: dict, ignore_keys: set | None = None) -> dict:
        """
        比较 saved_config 与 current_config 的差异，返回差异字典
        忽略 ignore_keys 中的字段（默认忽略 'run_state'）
        
        返回格式：
        {
            "key1": (saved_val, current_val),
            "key2": (saved_val, current_val),
            ...
        }
        """
        if ignore_keys is None:
            ignore_keys = {"run_state"}  # 默认忽略运行状态字段

        diff = {}

        # 合并所有 key
        all_keys = set(saved_config.keys()) | set(current_config.keys())
        for key in all_keys:
            if key in ignore_keys:
                continue

            saved_val = saved_config.get(key, "N/A")
            curr_val = current_config.get(key, "N/A")

            if saved_val != curr_val:
                diff[key] = (saved_val, curr_val)

        has_diff = bool(diff)
        return has_diff, diff
   # ------------------------------------------------
    # get state
    # ------------------------------------------------
    def get_state(self, runtime_args):

        with self.lock:

            if not os.path.exists(self.train_config_path):

                logger.warning("State file not found")

                return RunState.INIT

            try:

                with open(self.train_config_path) as f:
                    data = json.load(f)

                # 只对比 megatron_config 部分
                saved_megatron = data.get("megatron_config", {})
                current_megatron = runtime_args.get("megatron_config", {})

                self._print_runtime_args_compare(saved_megatron, current_megatron)

                has_diff, diff = self.compare_runtime_args(
                    saved_megatron,
                    current_megatron,
                    ignore_keys=set()
                )
                
                if has_diff:
                    logger.warning("[Config Compare] mismatch -> INIT")
                    return RunState.INIT

                state_str = data.get("run_state", "INIT")
                state = RunState[state_str]
                logger.info(f"[StateManager] loaded state from file: {state}")
                if state == RunState.RUNNING:
                    logger.info("[StateManager] detected RUNNING -> transition to PENDING")
                    return RunState.PENDING
                logger.info(f"[StateManager] state={state}")
                return state

            except Exception as e:

                logger.error(f"Read state file failed: {e}")

                return RunState.INIT

    # ------------------------------------------------
    # 设置状态
    # ------------------------------------------------
    def set_state(self, new_state: RunState, runtime_args):
        with self.lock:
            old_state = runtime_args.get("run_state")
            logger.info(f"RunState transition: {old_state.name if old_state else 'None'} -> {new_state.name}")
            runtime_args["run_state"] = new_state
            self._persist_state(runtime_args)

    def _make_json_serializable(self, obj):
        """
        递归转换不可 JSON 序列化对象
        """

        if isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}

        elif isinstance(obj, list):
            return [self._make_json_serializable(v) for v in obj]

        elif isinstance(obj, tuple):
            return tuple(self._make_json_serializable(v) for v in obj)

        elif isinstance(obj, RunState):
            return obj.name

        return obj
    # ------------------------------------------------
    # 保存状态
    # ------------------------------------------------

    def _persist_state(self, runtime_args):

        try:

            data = self._make_json_serializable(runtime_args)

            with open(self.train_config_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.debug("RunState persisted")

        except Exception as e:

            logger.error(f"Persist run_state failed: {e}")

    # ------------------------------------------------
    # load full state
    # ------------------------------------------------

    def load_full_state(self):

        if not os.path.exists(self.train_config_path):

            return None

        try:

            with open(self.train_config_path) as f:

                return json.load(f)

        except Exception as e:

            logger.error(f"Load state failed: {e}")

            return None
