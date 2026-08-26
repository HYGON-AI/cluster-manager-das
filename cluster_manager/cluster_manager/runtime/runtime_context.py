#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from cluster_manager.runtime.run_state_manager import RunStateManager, RunState
from cluster_manager.parallel.topology_validator import ParallelTopologyValidator
from cluster_manager.config.global_config import logger
from cluster_manager.config.train_config import get_train_config
import cluster_manager.config.global_config as global_config

class RetryInfo:
    """
    封装训练任务的重试信息：
      - retry_count: 当前重试次数
      - last_retry_time: 上一次重试时间
    提供安全的读写和更新接口。
    """

    def __init__(self):
        self._retry_count = 0
        self._last_retry_time = None

    # ---------- 访问接口 ----------
    @property
    def retry_count(self) -> int:
        return self._retry_count

    @property
    def last_retry_time(self):
        return self._last_retry_time

    # ---------- 修改接口 ----------
    def increment(self, timestamp=None):
        """增加一次重试次数，并更新 last_retry_time"""
        self._retry_count += 1
        self._last_retry_time = timestamp

    def reset(self):
        """重置重试信息"""
        self._retry_count = 0
        self._last_retry_time = None

    def update(self, count: int = None, timestamp=None):
        """直接更新重试信息"""
        if count is not None:
            self._retry_count = count
        if timestamp is not None:
            self._last_retry_time = timestamp

    def to_dict(self) -> dict:
        """对外输出 dict，方便序列化或日志"""
        return {
            "retry_count": self._retry_count,
            "last_retry_time": self._last_retry_time,
        }

class RuntimeContext:
    """
    RuntimeContext 统一管理：
    - NodePool
    - RunState（通过 RunStateManager）
    - RetryInfo
    - TrainingJob 配置
    """

    def __init__(self, runtime_args: dict, node_pool_proxy):

        self.job_name = runtime_args["job_name"]  # 后续多任务并行准备，唯一标识

        # --------------------------------
        # job runtime config
        # --------------------------------
        self.runtime_args = runtime_args

        # --------------------------------
        # training config cache
        # --------------------------------
        self.train_cfg_cache = self._build_runtime_args(runtime_args)

        # --------------------------------
        # node pool
        # --------------------------------
        self.node_pool_proxy = node_pool_proxy

        # --------------------------------
        # run state manager
        # --------------------------------
        self.run_state_manager = RunStateManager(node_pool_proxy)

        # --------------------------------
        # run state 初始化
        # --------------------------------
        try:
            self.run_state = self.get_state()
        except Exception as e:
            global_config.logger.warning(
                f"[RuntimeContext] get_state failed, fallback INIT: {e}"
            )
            self.run_state = RunState.INIT

        # --------------------------------
        # runtime info
        # --------------------------------
        self.slots_file = None
        self.master = None

        # --------------------------------
        # retry info
        # --------------------------------
        self.retry_info = RetryInfo()
        self.recovery_phase = None          # 'stopping' or 'starting'

    # ------------------------------------------------
    # 构建 runtime_args
    # ------------------------------------------------
    def _build_runtime_args(self, runtime_args):

        # ----------------------------
        # 基础参数（始终存在）
        # ----------------------------
        required_nodes_num = runtime_args["required_nodes_num"]
        slots_per_node = runtime_args["slots_per_node"]
        job_name = runtime_args["job_name"]

        world_size = ParallelTopologyValidator.get_world_size({
            "required_nodes_num": required_nodes_num,
            "slots_per_node": slots_per_node
        })

        # ----------------------------
        # 运行时配置（非 Megatron 参数）
        # ----------------------------
        runtime_config = {
            "job_name": job_name,
            "required_nodes_num": required_nodes_num,
            "slots_per_node": slots_per_node,
        }

        # ----------------------------
        # 从全局配置获取训练参数（已在 main.py 中提前加载）
        # ----------------------------
        train_cfg = get_train_config()

        # ----------------------------
        # Megatron 配置（用于对比，决定模型是否变化）
        # ----------------------------
        megatron_config = {}
        
        for key in global_config.PERSISTENT_KEYS:
            if key == "--world-size":
                megatron_config[key] = world_size
            elif key in train_cfg:
                megatron_config[key] = train_cfg[key]

        # ----------------------------
        # 计算派生参数 dp / edp
        # ----------------------------
        tp = train_cfg.get("--tensor-model-parallel-size")
        pp = train_cfg.get("--pipeline-model-parallel-size")
        cp = train_cfg.get("--context-parallel-size")
        ep = train_cfg.get("--expert-model-parallel-size")
        etp = train_cfg.get("--expert-tensor-parallel-size")

        megatron_config["--dp"] = ParallelTopologyValidator.calc_dp(world_size, tp, pp, cp)
        megatron_config["--edp"] = ParallelTopologyValidator.calc_edp(world_size, ep, etp, pp)

        # ----------------------------
        # 拓扑校验
        # ----------------------------
        ParallelTopologyValidator.validate({
            "world_size": world_size,
            "tp": tp,
            "pp": pp,
            "cp": cp,
            "ep": ep,
            "etp": etp,
        })

        return {
            "runtime_config": runtime_config,
            "megatron_config": megatron_config,
        }

    # ---------------------------
    # 获取当前运行状态
    # ---------------------------
    def get_state(self):
        return self.run_state_manager.get_state(self.train_cfg_cache)

    # ---------------------------
    # 设置运行状态
    # ---------------------------
    def set_state(self, new_state: RunState):
        self.run_state = new_state
        self.run_state_manager.set_state(
            new_state,
            self.train_cfg_cache
        )

    # ---------------------------
    # 读取完整状态
    # ---------------------------
    def load_full_state(self):
        return self.run_state_manager.load_full_state()

        # ------------------------------------------------
    # 解析 fault rank
    # ------------------------------------------------
    def _parse_rank_from_fault(self, fault_info: str) -> int:
        """
        从 OpenMPI / RCCL 日志字符串中解析 rank

        支持两种格式：

        1)
        [prterun-a05r3n07-802797@1,76]

        2)
        Process name: [[24917,1],18]
        """

        s = str(fault_info)

        # --------------------------------
        # 格式1
        # --------------------------------
        if "@" in s:
            parts = s.split(",")

            if len(parts) < 2:
                raise ValueError(f"invalid rank string: {fault_info}")

            raw_str = parts[1]
            rank_str = "".join(c for c in raw_str if c.isdigit())

        # --------------------------------
        # 格式2
        # --------------------------------
        else:

            last_comma_idx = s.rfind(",")

            if last_comma_idx == -1:
                raise ValueError(f"invalid rank string: {fault_info}")

            bracket_idx = s.find("]", last_comma_idx + 1)

            if bracket_idx == -1:
                raise ValueError(f"invalid rank string: {fault_info}")

            rank_str = s[last_comma_idx + 1: bracket_idx]

        return int(rank_str)
        
    def handle_runtime_fault(self, fault_info, fault_type, fault_reason=None):
        """
        统一处理 runtime 故障

        fault_type:
            rank  -> 单 rank 故障
            node  -> 节点故障
            proc  -> 训练进程异常
        """

        logger = global_config.logger

        try:

            # --------------------------------
            # rank fault
            # --------------------------------
            if fault_type in ("global_rank", "rank"):
                slots_per_node = self.runtime_args["slots_per_node"]
                # 解析rank（兼容两种类型）
                if fault_type == "global_rank":
                    rank = int(fault_info)
                else:
                    rank = self._parse_rank_from_fault(fault_info)
                # 安全校验rank值
                if not isinstance(rank, int) or rank < 0:
                    logger.error(f"[RuntimeContext] 无效的rank值: {rank}")
                    return False

                nodeno = rank // slots_per_node
                logger.info(
                    f"[RuntimeContext] rank fault detected "
                    f"rank={rank} nodeno={nodeno}"
                )
                self.node_pool_proxy.add_fault_nodes_no(
                    nodeno, fault_reason=fault_reason
                )

            # --------------------------------
            # node fault
            # --------------------------------
            elif fault_type == "node":

                logger.info(
                    f"[RuntimeContext] node fault detected: {fault_info}"
                )

                self.node_pool_proxy.add_fault_nodes([fault_info], fault_reason)

            # --------------------------------
            # process fault
            # --------------------------------
            elif fault_type == "proc":
                #
                logger.info(
                    "[RuntimeContext] process failure detected, restarting job"
                )

            else:

                logger.warning(
                    f"[RuntimeContext] unknown fault type: {fault_type}"
                )
                return False

            return True
        except Exception as e:

            logger.error(
                f"[RuntimeContext] handle_runtime_fault failed: {e}"
            )
            return False
