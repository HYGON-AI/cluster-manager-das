#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Megatron 并行拓扑验证器

基于训练脚本获取的 Megatron 参数，完成各种并行的通信检测。
使用 PyTorch 分布式异步接口进行通信验证。
"""

import os
import sys

# 添加项目路径，支持独立运行（必须在其他模块导入之前执行）
if __name__ == "__main__":
    current_file = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from cluster_manager.config.global_config import logger
from cluster_manager.config.train_config import get_config, set_megatron_config

# PyTorch 分布式通信
try:
    import torch
    import torch.distributed as dist
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning("PyTorch not available, communication check will be limited")


def init_distributed(backend: str = "nccl", timeout_minutes: int = 30) -> bool:
    """
    初始化 PyTorch 分布式环境
    
    支持两种初始化方式：
    1. 环境变量方式：需要设置 MASTER_ADDR 和 MASTER_PORT
    2. TCP 方式：需要设置 DIST_URL 和 DIST_PORT 环境变量
    
    Args:
        backend: 通信后端，可选 "nccl" 或 "gloo"
        timeout_minutes: 初始化超时时间（分钟）
    
    Returns:
        bool: 是否成功初始化
    """
    if not TORCH_AVAILABLE:
        logger.warning("[Distributed] PyTorch not available")
        return False
    
    if dist.is_initialized():
        logger.info("[Distributed] Already initialized")
        return True
    
    # 从环境变量获取 rank 信息（兼容 mpirun 和 torchrun）
    if "OMPI_COMM_WORLD_RANK" in os.environ:
        rank = int(os.environ["OMPI_COMM_WORLD_RANK"])
        world_size = int(os.environ["OMPI_COMM_WORLD_SIZE"])
        local_rank = int(os.environ.get("OMPI_COMM_WORLD_LOCAL_RANK", 0))
    else:
        rank = int(os.environ.get("RANK", 0))
        world_size = int(os.environ.get("WORLD_SIZE", 1))
        local_rank = int(os.environ.get("LOCAL_RANK", 0))
    
    # 绑定 GPU
    device = None
    if torch.cuda.is_available():
        torch.cuda.set_device(local_rank)
        device = torch.device(f"cuda:{local_rank}")
        #logger.info(f"[Distributed] Rank {rank} bound to GPU {local_rank}")
    else:
        device = torch.device("cpu")
    
    # 设置默认的 MASTER_ADDR 和 MASTER_PORT
    master_addr = os.environ.get("MASTER_ADDR", os.environ.get("DIST_URL", ""))
    master_port = os.environ.get("MASTER_PORT", os.environ.get("DIST_PORT", "29500"))
    
    # 如果没有设置 MASTER_ADDR，尝试从 hostfile 的第一个节点获取
    if not master_addr:
        import socket
        # 使用当前主机作为 master
        master_addr = socket.gethostname()
        logger.warning(f"[Distributed] MASTER_ADDR not set, using local hostname: {master_addr}")
    
    # 设置环境变量（PyTorch 分布式初始化需要）
    os.environ["MASTER_ADDR"] = master_addr
    os.environ["MASTER_PORT"] = str(master_port)
    os.environ["RANK"] = str(rank)
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["LOCAL_RANK"] = str(local_rank)
    
    try:
        # 初始化分布式
        init_method = f"tcp://{master_addr}:{master_port}"
        
        # 根据是否有 GPU 自动选择 backend
        actual_backend = backend
        if backend == "nccl" and not torch.cuda.is_available():
            actual_backend = "gloo"
            logger.warning("[Distributed] NCCL backend requested but CUDA not available, using gloo")
        
        dist.init_process_group(
            backend=actual_backend,
            init_method=init_method,
            rank=rank,
            world_size=world_size,
            timeout=timedelta(minutes=timeout_minutes)
        )
        
        '''
        logger.info(f"[Distributed] Initialized successfully: "
                   f"rank={rank}, world_size={world_size}, local_rank={local_rank}, "
                   f"backend={actual_backend}, master={master_addr}:{master_port}, device={device}")
        '''
        return True
        
    except Exception as e:
        logger.error(f"[Distributed] Failed to initialize: {e}")
        return False


def cleanup_distributed():
    """清理分布式环境"""
    if TORCH_AVAILABLE and dist.is_initialized():
        dist.destroy_process_group()
        logger.info("[Distributed] Cleaned up")


@dataclass
class ProcessGroup:
    """进程组信息"""
    name: str
    ranks: List[int]
    size: int
    group: Optional[object] = None  # torch.distributed.ProcessGroup
    

@dataclass
class TopologyInfo:
    """拓扑信息"""
    world_size: int
    tp: int  # tensor model parallel size
    pp: int  # pipeline model parallel size
    dp: int  # data parallel size
    cp: int  # context parallel size
    ep: int  # expert model parallel size (MoE)
    etp: int  # expert tensor parallel size (MoE)
    edp: int = 1  # expert data parallel size (MoE)
    
    # 计算得到的分组信息
    tp_groups: List[ProcessGroup] = field(default_factory=list)
    pp_groups: List[ProcessGroup] = field(default_factory=list)
    dp_groups: List[ProcessGroup] = field(default_factory=list)
    dp_cp_groups: List[ProcessGroup] = field(default_factory=list)
    ep_groups: List[ProcessGroup] = field(default_factory=list)
    
    def is_moe(self) -> bool:
        """是否为 MoE 模型"""
        return self.ep > 1


@dataclass
class CommunicationResult:
    """通信检测结果"""
    group_name: str
    group_type: str
    success: bool
    latency_ms: float
    error_msg: str = ""
    timestamp: str = ""


class ParallelTopologyValidator:
    """
    Megatron 并行拓扑验证器
    
    Dense:
        tp * pp * cp * dp = world_size
    
    MoE:
        etp * ep * pp * edp = world_size
    
    Relation:
        dp = ep * edp
    """

    @staticmethod
    def get_world_size(cfg):
        nodes = cfg.get("required_nodes_num", 1)
        slots = cfg.get("slots_per_node", 1)
        return nodes * slots

    @staticmethod
    def calc_dp(world_size, tp, pp, cp):
        tp_num = int(tp or 1)
        pp_num = int(pp or 1)
        cp_num = int(cp or 1)
        word_num = int(world_size)
        denom = tp_num * pp_num * cp_num

        if denom == 0:
            raise ValueError("tp * pp * cp cannot be zero")

        if word_num % denom != 0:
            raise ValueError(
                f"world_size({word_num}) not divisible by tp*pp*cp ({denom})"
            )

        return word_num // denom

    @staticmethod
    def calc_edp(world_size, ep, etp, pp):
        ep_num = int(ep or 1)
        etp_num = int(etp or 1)
        pp_num = int(pp or 1)
        denom = ep_num * etp_num * pp_num

        if denom == 0:
            raise ValueError("ep * etp * pp cannot be zero")

        if world_size % denom != 0:
            raise ValueError(
                f"world_size({world_size}) not divisible by ep*etp*pp ({denom})"
            )

        return world_size // denom

    # ========================================
    # 从全局配置获取并行参数
    # ========================================
    
    @classmethod
    def get_parallel_params_from_config(cls) -> Dict[str, int]:
        """从全局配置获取 Megatron 并行参数"""
        params = {
            "tp": 1,
            "pp": 1,
            "cp": 1,
            "ep": 1,
            "etp": 1
        }
        
        # 从全局配置获取（已通过 train_config.py 加载）
        param_mapping = {
            "--tensor-model-parallel-size": "tp",
            "--pipeline-model-parallel-size": "pp",
            "--context-parallel-size": "cp",
            "--expert-model-parallel-size": "ep",
            "--expert-tensor-parallel-size": "etp",
        }
        
        for arg_name, param_name in param_mapping.items():
            value = get_config(arg_name)
            if value is not None:
                params[param_name] = int(value)
        
        #logger.info(f"[TopologyValidator] Parallel params: {params}")
        return params

    # ========================================
    # Megatron-LM 分组策略实现
    # ========================================
    
    @classmethod
    def build_tp_groups(cls, world_size: int, tp: int, pp: int, dp: int, cp: int = 1) -> List[ProcessGroup]:
        """
        构建 Tensor Model Parallel 进程组
        
        TP group: 相同 pp_rank 和 dp_rank 的 ranks 组成一个 TP 组
        """
        groups = []
        num_groups = pp * dp * cp
        
        for group_idx in range(num_groups):
            pp_rank = group_idx // (dp * cp)
            dp_cp_rank = group_idx % (dp * cp)
            base = pp_rank * (tp * dp * cp) + dp_cp_rank * tp
            ranks = [base + i for i in range(tp)]
            groups.append(ProcessGroup(
                name=f"tp_group_{group_idx}",
                ranks=ranks,
                size=tp
            ))
        
        return groups

    @classmethod
    def build_pp_groups(cls, world_size: int, tp: int, pp: int, dp: int, cp: int = 1) -> List[ProcessGroup]:
        """
        构建 Pipeline Model Parallel 进程组
        
        PP group: 相同 tp_rank 和 dp_rank 的 ranks 组成一个 PP 组
        """
        groups = []
        num_groups = tp * dp * cp
        stride = tp * dp * cp
        
        for group_idx in range(num_groups):
            base = group_idx
            ranks = [base + i * stride for i in range(pp)]
            groups.append(ProcessGroup(
                name=f"pp_group_{group_idx}",
                ranks=ranks,
                size=pp
            ))
        
        return groups

    @classmethod
    def build_dp_groups(cls, world_size: int, tp: int, pp: int, dp: int, cp: int = 1) -> List[ProcessGroup]:
        """
        构建 Data Parallel 进程组
        
        DP group: 相同 tp_rank 和 pp_rank 的 ranks 组成一个 DP 组
        """
        groups = []
        num_groups = tp * pp
        
        for group_idx in range(num_groups):
            pp_rank = group_idx // tp
            tp_rank = group_idx % tp
            
            ranks = []
            for dp_rank in range(dp):
                for cp_rank in range(cp):
                    rank = (pp_rank * tp * dp * cp + 
                            dp_rank * tp * cp + 
                            cp_rank * tp + 
                            tp_rank)
                    ranks.append(rank)
            
            groups.append(ProcessGroup(
                name=f"dp_group_{group_idx}",
                ranks=sorted(ranks),
                size=dp * cp
            ))
        
        return groups

    @classmethod
    def build_dp_cp_groups(cls, world_size: int, tp: int, pp: int, dp: int, cp: int = 1) -> List[ProcessGroup]:
        """
        构建 DP-CP 组 (Data Parallel + Context Parallel)
        """
        groups = []
        num_groups = tp
        
        for tp_rank in range(num_groups):
            ranks = []
            for pp_rank in range(pp):
                for dp_rank in range(dp):
                    for cp_rank in range(cp):
                        rank = (pp_rank * tp * dp * cp + 
                                dp_rank * tp * cp + 
                                cp_rank * tp + 
                                tp_rank)
                        ranks.append(rank)
            
            groups.append(ProcessGroup(
                name=f"dp_cp_group_{tp_rank}",
                ranks=sorted(ranks),
                size=pp * dp * cp
            ))
        
        return groups

    @classmethod
    def build_ep_groups(cls, world_size: int, tp: int, pp: int, ep: int, etp: int = 1) -> List[ProcessGroup]:
        """
        构建 Expert Model Parallel 进程组 (MoE)
        """
        if ep <= 1:
            return []
        
        groups = []
        edp = world_size // (ep * etp * pp) if (ep * etp * pp) > 0 else 1
        num_groups = etp * pp
        
        for group_idx in range(num_groups):
            base = group_idx * ep
            ranks = [(base + i) % world_size for i in range(ep)]
            groups.append(ProcessGroup(
                name=f"ep_group_{group_idx}",
                ranks=sorted(ranks),
                size=ep
            ))
        
        return groups

    # ========================================
    # 进程组管理 - 预创建和销毁
    # ========================================
    
    @staticmethod
    def create_torch_groups_for_topology(topology: TopologyInfo, rank: int, 
                                          timeout_seconds: int = 60) -> Dict[str, object]:
        """
        为拓扑中的所有进程组预先创建 PyTorch ProcessGroup
        
        重要：dist.new_group 是集体操作，需要所有 rank 都参与调用，
        即使某些 rank 不属于该 group。否则会导致死锁。
        
        Args:
            topology: 拓扑信息
            rank: 当前进程 rank
            timeout_seconds: 超时时间（秒）
        
        Returns:
            Dict[str, ProcessGroup]: 组名到 PyTorch ProcessGroup 的映射
        """
        if not TORCH_AVAILABLE or not dist.is_initialized():
            return {}
        
        torch_groups = {}
        
        # 辅助函数：为单个 ProcessGroup 创建 torch group
        # 重要：所有 rank 都必须调用 dist.new_group，这是一个集体操作
        def create_group(pg: ProcessGroup) -> Optional[object]:
            # 检查组大小，单 rank 组无需创建
            if len(pg.ranks) < 2:
                return None
            
            try:
                # 关键：所有 rank 都必须调用 new_group，即使不在 ranks 列表中
                # 不在 ranks 中的 rank 会得到 None 或无效的 group
                torch_group = dist.new_group(
                    ranks=pg.ranks, 
                    timeout=timedelta(seconds=timeout_seconds)
                )
                
                # 只有在 group 中的 rank 才保存返回值
                if rank in pg.ranks:
                    return torch_group
                else:
                    return None
                    
            except Exception as e:
                logger.error(f"[GroupCreation] Failed to create group {pg.name}: {e}")
                return None
        
        # 创建 TP groups - 所有 rank 都参与 new_group 调用
        for pg in topology.tp_groups:
            tg = create_group(pg)
            if tg is not None:
                torch_groups[pg.name] = tg
                pg.group = tg
        
        # 创建 PP groups - 所有 rank 都参与 new_group 调用
        for pg in topology.pp_groups:
            tg = create_group(pg)
            if tg is not None:
                torch_groups[pg.name] = tg
                pg.group = tg
        
        # 创建 DP groups - 所有 rank 都参与 new_group 调用
        for pg in topology.dp_groups:
            tg = create_group(pg)
            if tg is not None:
                torch_groups[pg.name] = tg
                pg.group = tg
        
        # 创建 EP groups - 所有 rank 都参与 new_group 调用
        for pg in topology.ep_groups:
            tg = create_group(pg)
            if tg is not None:
                torch_groups[pg.name] = tg
                pg.group = tg
        
        #logger.info(f"[GroupCreation] Created {len(torch_groups)} torch process groups")
        return torch_groups
    
    @staticmethod
    def destroy_torch_groups(torch_groups: Dict[str, object]):
        if not TORCH_AVAILABLE:
            return

        destroyed_count = 0
        for group_name, torch_group in torch_groups.items():
            # 跳过 None
            if torch_group is None:
                continue

            try:
                dist.destroy_process_group(torch_group)
                destroyed_count += 1
            except Exception as e:
                error_msg = str(e)
                if "Invalid process group" not in error_msg:
                    logger.warning(f"[GroupCleanup] Failed to destroy group {group_name}: {e}")
                # 对于 "Invalid process group specified" 错误，静默跳过

        #if destroyed_count > 0:
            #logger.info(f"[GroupCleanup] Destroyed {destroyed_count} torch process groups")

    # ========================================
    # 通信性能测试
    # ========================================
    
    @staticmethod
    def benchmark_allreduce(group: ProcessGroup, rank: int, world_size: int, 
                            num_iterations: int = 100, data_size_mb: float = 1.0) -> Dict:
        """
        AllReduce 性能基准测试
        
        Args:
            group: 进程组（已包含预创建的 torch group）
            rank: 当前进程 rank
            world_size: 总进程数
            num_iterations: 迭代次数
            data_size_mb: 数据大小 (MB)
        """
        if not TORCH_AVAILABLE:
            return {"error": "PyTorch not available"}
        
        if rank not in group.ranks:
            return {"error": "Rank not in group"}
        
        if not dist.is_initialized():
            return {"error": "Distributed not initialized"}
        
        # 检查组大小，单 rank 组无需通信测试
        group_size = len(group.ranks)
        if group_size < 2:
            return {
                "group_name": group.name,
                "operation": "AllReduce",
                "data_size_mb": data_size_mb,
                "num_iterations": num_iterations,
                "note": f"Group size = {group_size}, no communication needed",
                "latency_mean_ms": 0.0,
                "latency_std_ms": 0.0,
                "latency_min_ms": 0.0,
                "latency_max_ms": 0.0,
                "bandwidth_gbps": 0.0,
            }
        
        try:
            import time
            import numpy as np
            import socket
            
            # 获取当前节点信息用于错误诊断
            local_hostname = socket.gethostname()
            
            # 使用预创建的进程组
            torch_group = group.group
            if torch_group is None:
                return {"error": "Torch group not pre-created"}
            
            # 计算数据大小
            num_elements = int(data_size_mb * 1024 * 1024 / 4)  # float32 = 4 bytes
            tensor = torch.randn(num_elements)
            if torch.cuda.is_available():
                tensor = tensor.cuda()
            
            # 预热
            for _ in range(10):
                dist.all_reduce(tensor, op=dist.ReduceOp.SUM, group=torch_group)
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
            
            # 正式测试
            latencies = []
            for _ in range(num_iterations):
                start = time.perf_counter()
                dist.all_reduce(tensor, op=dist.ReduceOp.SUM, group=torch_group)
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
                end = time.perf_counter()
                latencies.append((end - start) * 1000)  # ms
            
            # 计算统计信息
            latencies = np.array(latencies)
            data_size_bytes = num_elements * 4
            bandwidth_gbps = (data_size_bytes * 8 / 1e9) / (latencies.mean() / 1000)
            
            return {
                "group_name": group.name,
                "operation": "AllReduce",
                "data_size_mb": data_size_mb,
                "num_iterations": num_iterations,
                "latency_mean_ms": float(latencies.mean()),
                "latency_std_ms": float(latencies.std()),
                "latency_min_ms": float(latencies.min()),
                "latency_max_ms": float(latencies.max()),
                "bandwidth_gbps": float(bandwidth_gbps),
            }
            
        except Exception as e:
            import socket
            error_msg = str(e)
            diagnostic_info = {
                "error": error_msg,
                "group_name": group.name,
                "group_ranks": group.ranks,
                "current_rank": rank,
                "local_hostname": socket.gethostname(),
            }
            
            if "socketStartConnect" in error_msg or "Software caused connection abort" in error_msg:
                diagnostic_info["error_type"] = "network_connection"
                diagnostic_info["hint"] = "Network connectivity issue - check firewall, network config, and node reachability"
            elif "ncclSystemError" in error_msg:
                diagnostic_info["error_type"] = "nccl_system"
                diagnostic_info["hint"] = "NCCL system error - check NCCL_DEBUG=INFO for details"
            
            return diagnostic_info
    
    @staticmethod
    def benchmark_send_recv(group: ProcessGroup, rank: int, world_size: int,
                           num_iterations: int = 100, data_size_mb: float = 1.0) -> Dict:
        """
        Send/Recv 性能基准测试 (PP)
        
        测试 PP 组中相邻 rank 之间的点对点通信性能
        采用配对方式：偶数 local_rank 发送，奇数 local_rank 接收
        """
        if not TORCH_AVAILABLE:
            return {"error": "PyTorch not available"}
        
        if rank not in group.ranks:
            return {"error": "Rank not in group"}
        
        if not dist.is_initialized():
            return {"error": "Distributed not initialized"}
        
        # 检查组大小，单 rank 组无需通信测试
        group_size = len(group.ranks)
        if group_size < 2:
            return {
                "group_name": group.name,
                "operation": "Send/Recv",
                "data_size_mb": data_size_mb,
                "num_iterations": num_iterations,
                "note": f"Group size = {group_size}, no communication needed",
                "latency_mean_ms": 0.0,
                "latency_std_ms": 0.0,
                "latency_min_ms": 0.0,
                "latency_max_ms": 0.0,
                "bandwidth_gbps": 0.0,
            }
        
        try:
            import time
            import numpy as np
            
            # 使用预创建的进程组
            torch_group = group.group
            if torch_group is None:
                return {"error": "Torch group not pre-created"}
            
            local_rank = group.ranks.index(rank)
            
            num_elements = int(data_size_mb * 1024 * 1024 / 4)
            send_tensor = torch.randn(num_elements)
            recv_tensor = torch.zeros(num_elements)
            if torch.cuda.is_available():
                send_tensor = send_tensor.cuda()
                recv_tensor = recv_tensor.cuda()
            
            # 确定当前 rank 的通信角色
            is_sender = (local_rank % 2 == 0) and (local_rank < group_size - 1)
            is_receiver = (local_rank % 2 == 1)
            
            # 如果是最后一个 rank 且 group_size 为奇数，则不参与通信
            if local_rank == group_size - 1 and group_size % 2 == 1:
                return {
                    "group_name": group.name,
                    "operation": "Send/Recv",
                    "data_size_mb": data_size_mb,
                    "num_iterations": num_iterations,
                    "note": "Last rank in odd-sized group, no partner",
                    "latency_mean_ms": 0.0,
                    "latency_std_ms": 0.0,
                    "latency_min_ms": 0.0,
                    "latency_max_ms": 0.0,
                    "bandwidth_gbps": 0.0,
                }
            
            partner_local_rank = local_rank + 1 if is_sender else local_rank - 1
            partner_global_rank = group.ranks[partner_local_rank]
            
            # 预热
            for _ in range(10):
                if is_sender:
                    dist.send(send_tensor, dst=partner_global_rank, group=torch_group)
                elif is_receiver:
                    dist.recv(recv_tensor, src=partner_global_rank, group=torch_group)
            if torch.cuda.is_available():
                torch.cuda.synchronize()
            
            # 正式测试
            latencies = []
            for _ in range(num_iterations):
                start = time.perf_counter()
                if is_sender:
                    dist.send(send_tensor, dst=partner_global_rank, group=torch_group)
                elif is_receiver:
                    dist.recv(recv_tensor, src=partner_global_rank, group=torch_group)
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
                end = time.perf_counter()
                latencies.append((end - start) * 1000)
            
            latencies = np.array(latencies)
            data_size_bytes = num_elements * 4
            bandwidth_gbps = (data_size_bytes * 8 / 1e9) / (latencies.mean() / 1000)
            
            return {
                "group_name": group.name,
                "operation": "Send/Recv",
                "data_size_mb": data_size_mb,
                "num_iterations": num_iterations,
                "partner_rank": partner_global_rank if (is_sender or is_receiver) else None,
                "role": "sender" if is_sender else ("receiver" if is_receiver else "idle"),
                "latency_mean_ms": float(latencies.mean()),
                "latency_std_ms": float(latencies.std()),
                "latency_min_ms": float(latencies.min()),
                "latency_max_ms": float(latencies.max()),
                "bandwidth_gbps": float(bandwidth_gbps),
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    @staticmethod
    def benchmark_all_to_all(group: ProcessGroup, rank: int, world_size: int,
                             num_iterations: int = 100, data_size_mb: float = 1.0) -> Dict:
        """
        All-to-All 性能基准测试 (专家间通信)
        
        MoE 中用于专家之间的 token 分发和收集
        """
        if not TORCH_AVAILABLE:
            return {"error": "PyTorch not available"}
        
        if rank not in group.ranks:
            return {"error": "Rank not in group"}
        
        if not dist.is_initialized():
            return {"error": "Distributed not initialized"}
        
        # 检查组大小，单 rank 组无需通信测试
        group_size = len(group.ranks)
        if group_size < 2:
            return {
                "group_name": group.name,
                "operation": "All-to-All",
                "data_size_mb": data_size_mb,
                "num_iterations": num_iterations,
                "note": f"Group size = {group_size}, no communication needed",
                "latency_mean_ms": 0.0,
                "latency_std_ms": 0.0,
                "latency_min_ms": 0.0,
                "latency_max_ms": 0.0,
                "bandwidth_gbps": 0.0,
            }
        
        try:
            import time
            import numpy as np
            
            # 使用预创建的进程组
            torch_group = group.group
            if torch_group is None:
                return {"error": "Torch group not pre-created"}

            # 计算每个 pair 的元素数量，确保至少为 1
            num_elements_per_pair = max(1, int(data_size_mb * 1024 * 1024 / 4 / group_size))
            
            input_tensor = torch.randn(group_size, num_elements_per_pair)
            output_tensor = torch.zeros(group_size, num_elements_per_pair)
            
            if torch.cuda.is_available():
                input_tensor = input_tensor.cuda()
                output_tensor = output_tensor.cuda()
            
            # 预热：每次迭代后同步确保预热完整
            for _ in range(10):
                dist.all_to_all_single(output_tensor, input_tensor, group=torch_group)
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
            
            # 正式测试
            latencies = []
            for _ in range(num_iterations):
                start = time.perf_counter()
                dist.all_to_all_single(output_tensor, input_tensor, group=torch_group)
                if torch.cuda.is_available():
                    torch.cuda.synchronize()
                end = time.perf_counter()
                latencies.append((end - start) * 1000)
            
            latencies = np.array(latencies)
            total_data_bytes = num_elements_per_pair * 4 * group_size * group_size
            bandwidth_gbps = (total_data_bytes * 8 / 1e9) / (latencies.mean() / 1000)
            
            return {
                "group_name": group.name,
                "operation": "All-to-All",
                "data_size_mb": data_size_mb,
                "group_size": group_size,
                "num_iterations": num_iterations,
                "latency_mean_ms": float(latencies.mean()),
                "latency_std_ms": float(latencies.std()),
                "latency_min_ms": float(latencies.min()),
                "latency_max_ms": float(latencies.max()),
                "bandwidth_gbps": float(bandwidth_gbps),
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    @classmethod
    def benchmark_all_groups(cls, topology: TopologyInfo, rank: int = 0,
                            num_iterations: int = 100, data_size_mb: float = 1.0) -> List[Dict]:
        """
        对所有进程组进行性能基准测试
        
        每个 rank 会找到自己所属的进程组进行测试，确保所有 rank 都能参与基准测试
        
        注意：
        1. 每种类型的通信测试之间会进行全局同步（barrier）
        2. 所有 rank 都必须参与 barrier，否则会导致死锁
        3. 对于不参与特定测试的 rank，会记录跳过信息但仍参与同步
        """
        results = []
        
        # ========================================
        # TP 测试 (AllReduce)
        # ========================================
        tp_tested = False
        for group in topology.tp_groups:
            if rank in group.ranks:
                result = cls.benchmark_allreduce(
                    group, rank, topology.world_size,
                    num_iterations, data_size_mb
                )
                result["group_type"] = "TP"
                result["group_name"] = group.name
                results.append(result)
                tp_tested = True
                break
        
        # 如果当前 rank 不属于任何 TP group，记录跳过信息
        if not tp_tested:
            results.append({
                "group_type": "TP",
                "group_name": "N/A",
                "note": f"Rank {rank} not in any TP group",
                "skipped": True
            })
        
        # TP 测试完成后同步，确保所有 rank 都完成 TP 测试再进行 PP 测试
        # 重要：所有 rank 都必须参与 barrier
        if TORCH_AVAILABLE and dist.is_initialized():
            dist.barrier()
        
        # ========================================
        # PP 测试 (Send/Recv)
        # ========================================
        pp_tested = False
        for group in topology.pp_groups:
            if rank in group.ranks:
                result = cls.benchmark_send_recv(
                    group, rank, topology.world_size,
                    num_iterations, data_size_mb
                )
                result["group_type"] = "PP"
                result["group_name"] = group.name
                results.append(result)
                pp_tested = True
                break
        
        # 如果当前 rank 不属于任何 PP group，记录跳过信息
        if not pp_tested:
            results.append({
                "group_type": "PP",
                "group_name": "N/A",
                "note": f"Rank {rank} not in any PP group",
                "skipped": True
            })
        
        # PP 测试完成后同步
        if TORCH_AVAILABLE and dist.is_initialized():
            dist.barrier()
        
        # ========================================
        # DP 测试 (AllReduce)
        # ========================================
        dp_tested = False
        for group in topology.dp_groups:
            if rank in group.ranks:
                result = cls.benchmark_allreduce(
                    group, rank, topology.world_size,
                    num_iterations, data_size_mb
                )
                result["group_type"] = "DP"
                result["group_name"] = group.name
                results.append(result)
                dp_tested = True
                break
        
        # 如果当前 rank 不属于任何 DP group，记录跳过信息
        if not dp_tested:
            results.append({
                "group_type": "DP",
                "group_name": "N/A",
                "note": f"Rank {rank} not in any DP group",
                "skipped": True
            })
        
        # DP 测试完成后同步
        if TORCH_AVAILABLE and dist.is_initialized():
            dist.barrier()
        
        # ========================================
        # EP 测试 (All-to-All for MoE)
        # ========================================
        ep_tested = False
        for group in topology.ep_groups:
            if rank in group.ranks:
                result = cls.benchmark_all_to_all(
                    group, rank, topology.world_size,
                    num_iterations, data_size_mb
                )
                result["group_type"] = "EP"
                result["group_name"] = group.name
                results.append(result)
                ep_tested = True
                break
        
        # 如果当前 rank 不属于任何 EP group，记录跳过信息
        if not ep_tested:
            results.append({
                "group_type": "EP",
                "group_name": "N/A",
                "note": f"Rank {rank} not in any EP group (or EP disabled)",
                "skipped": True
            })
        
        # 所有测试完成后最终同步
        if TORCH_AVAILABLE and dist.is_initialized():
            dist.barrier()
        return results

    # ========================================
    # 拓扑验证
    # ========================================
    
    @classmethod
    def validate(cls, cfg: Dict) -> TopologyInfo:
        """
        验证拓扑并构建进程组
        
        Args:
            cfg: 配置字典，包含 world_size 或 required_nodes_num/slots_per_node
        """
        # 获取 world_size
        world_size = cfg.get("world_size", 1)
        if world_size == 1:
            world_size = cls.get_world_size(cfg)
        
        # 从全局配置获取并行参数
        params = cls.get_parallel_params_from_config()
        
        tp = params.get("tp", 1)
        pp = params.get("pp", 1)
        cp = params.get("cp", 1)
        ep = params.get("ep", 1)
        etp = params.get("etp", 1)
        
        # 计算 dp
        dp = cls.calc_dp(world_size, tp, pp, cp)
        
        # 计算 edp (MoE)
        edp = 1
        if ep > 1:
            edp = cls.calc_edp(world_size, ep, etp, pp)
        
        # 构建拓扑信息
        topology = TopologyInfo(
            world_size=world_size,
            tp=tp,
            pp=pp,
            dp=dp,
            cp=cp,
            ep=ep,
            etp=etp,
            edp=edp
        )
        
        # 构建进程组
        topology.tp_groups = cls.build_tp_groups(world_size, tp, pp, dp, cp)
        topology.pp_groups = cls.build_pp_groups(world_size, tp, pp, dp, cp)
        topology.dp_groups = cls.build_dp_groups(world_size, tp, pp, dp, cp)
        topology.dp_cp_groups = cls.build_dp_cp_groups(world_size, tp, pp, dp, cp)
        topology.ep_groups = cls.build_ep_groups(world_size, tp, pp, ep, etp)
        
        # 验证拓扑
        cls._validate_topology(topology)
        
        # MoE 校验
        num_experts = get_config("--num-experts")
        if num_experts:
            if num_experts % ep != 0:
                raise RuntimeError(
                    f"num_experts {num_experts} not divisible by ep {ep}"
                )
        
        #logger.info(f"[TopologyValidator] Topology: world_size={world_size}, tp={tp}, pp={pp}, dp={dp}, cp={cp}, ep={ep}")
        
        return topology
    
    @classmethod
    def _validate_topology(cls, topology: TopologyInfo) -> bool:
        """验证拓扑配置是否合法"""
        if topology.is_moe():
            expected = topology.etp * topology.ep * topology.pp * topology.edp
            if expected != topology.world_size:
                raise ValueError(
                    f"MoE topology mismatch: etp({topology.etp}) * ep({topology.ep}) * "
                    f"pp({topology.pp}) * edp({topology.edp}) = {expected} != world_size({topology.world_size})"
                )
        else:
            expected = topology.tp * topology.pp * topology.cp * topology.dp
            if expected != topology.world_size:
                raise ValueError(
                    f"Dense topology mismatch: tp({topology.tp}) * pp({topology.pp}) * "
                    f"cp({topology.cp}) * dp({topology.dp}) = {expected} != world_size({topology.world_size})"
                )
        return True


# ========================================
# 独立测试入口
# ========================================
def test_topology_validator():
    """
    独立测试函数，支持两种模式：
    
    模式1：通过 MEGATRON_SCRIPT_PATH 环境变量解析参数
       - 设置 MEGATRON_SCRIPT_PATH 环境变量指向训练脚本
       - global_config 会自动解析训练脚本中的并行参数
       - 此时 --tp/--pp/--dp 等参数不应设置
       
    模式2：手动指定并行参数
       - 不设置 MEGATRON_SCRIPT_PATH
       - 必须通过 --tp/--pp/--dp 等参数指定并行配置
       
    注意：两种模式互斥，不能同时使用
    """
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Megatron 并行拓扑验证器")
    
    # 基本参数
    parser.add_argument("--world_size", type=int, default=None, help="总进程数")
    
    # 并行参数（仅在不使用 MEGATRON_SCRIPT_PATH 时需要）
    parser.add_argument("--tp", type=int, default=None, help="Tensor Parallel Size")
    parser.add_argument("--pp", type=int, default=None, help="Pipeline Parallel Size")
    parser.add_argument("--dp", type=int, default=None, help="Data Parallel Size")
    parser.add_argument("--cp", type=int, default=1, help="Context Parallel Size (默认: 1)")
    parser.add_argument("--ep", type=int, default=1, help="Expert Parallel Size (MoE, 默认: 1)")
    parser.add_argument("--etp", type=int, default=1, help="Expert Tensor Parallel Size (MoE, 默认: 1)")
    
    # 输出选项
    parser.add_argument("--output-dir", type=str, default="./topology_results", help="输出目录")
    parser.add_argument("--output-file", type=str, default=None, help="输出文件名（可选）")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    parser.add_argument("--json", action="store_true", help="以 JSON 格式输出结果")
    
    # 通信测试选项
    parser.add_argument("--benchmark", action="store_true", help="运行通信性能基准测试")
    parser.add_argument("--benchmark-iterations", type=int, default=100, help="基准测试迭代次数")
    parser.add_argument("--benchmark-size", type=float, default=1.0, help="基准测试数据大小 (MB)")
    
    # 分布式初始化选项
    parser.add_argument("--backend", type=str, default="nccl", choices=["nccl", "gloo"], 
                        help="分布式通信后端 (默认: nccl)")
    parser.add_argument("--no-init", action="store_true", 
                        help="不自动初始化分布式环境（用于已初始化的场景）")
    
    args = parser.parse_args()
    
    # 获取 rank 信息（用于分布式运行）
    rank_info = get_rank_info()
    rank = rank_info["rank"]
    world_size_from_env = rank_info["world_size"]
    
    # 只有 rank 0 打印信息
    def log(msg):
        if rank == 0 or args.verbose:
            print(msg)
    
    log("=" * 60)
    log("Megatron 并行拓扑验证器")
    log("=" * 60)
    
    # 判断使用哪种模式（仅通过环境变量判断）
    use_script_mode = os.environ.get("MEGATRON_SCRIPT_PATH") is not None
    
    # 检查参数互斥性
    if use_script_mode:
        # 模式1：从训练脚本解析参数
        has_manual_params = any([args.tp is not None, args.pp is not None, args.dp is not None])
        if has_manual_params:
            log(f"\n⚠ 警告: 使用 MEGATRON_SCRIPT_PATH 模式时，--tp/--pp/--dp 参数将被忽略")
        
        log(f"\n模式: 从训练脚本解析参数")
        log(f"训练脚本: {os.environ.get('MEGATRON_SCRIPT_PATH')}")
        
        # global_config 会自动解析 MEGATRON_SCRIPT_PATH，直接获取参数
        params = ParallelTopologyValidator.get_parallel_params_from_config()
        
    else:
        # 模式2：手动指定参数
        log(f"\n模式: 手动指定并行参数")
        
        # 检查必需参数
        if args.tp is None or args.pp is None or args.dp is None:
            log(f"\n✗ 错误: 未设置 MEGATRON_SCRIPT_PATH，必须手动指定 --tp, --pp, --dp 参数")
            return 1
        
        params = {
            "tp": args.tp,
            "pp": args.pp,
            "dp": args.dp,
            "cp": args.cp,
            "ep": args.ep,
            "etp": args.etp,
        }
    
    log(f"\n--- 并行参数 ---")
    for key, value in params.items():
        log(f"  {key}: {value}")
    
    # 确定 world_size
    if args.world_size is not None:
        world_size = args.world_size
    elif world_size_from_env > 0:
        world_size = world_size_from_env
    else:
        # 从并行参数计算
        tp = params.get("tp", 1)
        pp = params.get("pp", 1)
        cp = params.get("cp", 1)
        dp = params.get("dp", 1)
        world_size = tp * pp * cp * dp
        log(f"  计算的 world_size: {world_size}")
    
    # 配置
    config = {
        "world_size": world_size,
    }
    
    log(f"\n--- 运行时配置 ---")
    log(f"  world_size: {world_size}")
    log(f"  rank: {rank}")
    if rank_info["hostname"]:
        log(f"  hostname: {rank_info['hostname']}")
    
    # 初始化分布式环境（用于通信测试）
    dist_initialized = False
    torch_groups = {}  # 预创建的进程组
    
    if not args.no_init and TORCH_AVAILABLE:
        log(f"\n--- 初始化分布式环境 ---")
        dist_initialized = init_distributed(backend=args.backend)
        if dist_initialized:
            log(f"  ✓ 分布式环境初始化成功")
        else:
            log(f"  ✗ 分布式环境初始化失败，通信测试将不可用")
    elif args.no_init:
        log(f"\n--- 跳过分布式初始化 (--no-init) ---")
        dist_initialized = dist.is_initialized() if TORCH_AVAILABLE else False
    
    # 结果字典
    result = {
        "status": "unknown",
        "world_size": world_size,
        "rank": rank,
        "rank_info": rank_info,
        "params": params,
        "topology": None,
        "error": None,
    }
    
    try:
        # 临时设置全局配置（用于 validate 函数读取）
        if params.get("tp"):
            set_megatron_config("--tensor-model-parallel-size", params["tp"])
        if params.get("pp"):
            set_megatron_config("--pipeline-model-parallel-size", params["pp"])
        if params.get("cp"):
            set_megatron_config("--context-parallel-size", params["cp"])
        if params.get("ep"):
            set_megatron_config("--expert-model-parallel-size", params["ep"])
        if params.get("etp"):
            set_megatron_config("--expert-tensor-parallel-size", params["etp"])
        
        # 验证拓扑
        topology = ParallelTopologyValidator.validate(config)
        
        log(f"\n--- 拓扑信息 ---")
        log(f"World Size: {topology.world_size}")
        log(f"TP: {topology.tp}, PP: {topology.pp}, DP: {topology.dp}, CP: {topology.cp}")
        log(f"EP: {topology.ep}, ETP: {topology.etp}, EDP: {topology.edp}")
        log(f"Is MoE: {topology.is_moe()}")
        
        # 拓扑验证
        expected = topology.tp * topology.pp * topology.cp * topology.dp
        if expected != topology.world_size:
            result["status"] = "mismatch"
            result["error"] = f"Topology mismatch: tp*pp*cp*dp = {expected} != world_size = {topology.world_size}"
            log(f"\n✗ {result['error']}")
        else:
            result["status"] = "valid"
            log(f"\n✓ 拓扑验证通过")
        
        # 转换拓扑信息为可序列化格式
        result["topology"] = {
            "world_size": topology.world_size,
            "tp": topology.tp,
            "pp": topology.pp,
            "dp": topology.dp,
            "cp": topology.cp,
            "ep": topology.ep,
            "etp": topology.etp,
            "edp": topology.edp,
            "is_moe": topology.is_moe(),
            "tp_groups": [{"name": g.name, "ranks": g.ranks, "size": g.size} for g in topology.tp_groups],
            "pp_groups": [{"name": g.name, "ranks": g.ranks, "size": g.size} for g in topology.pp_groups],
            "dp_groups": [{"name": g.name, "ranks": g.ranks, "size": g.size} for g in topology.dp_groups],
            "dp_cp_groups": [{"name": g.name, "ranks": g.ranks, "size": g.size} for g in topology.dp_cp_groups],
            "ep_groups": [{"name": g.name, "ranks": g.ranks, "size": g.size} for g in topology.ep_groups],
        }
        
        # 通信性能基准测试
        if args.benchmark:
            if not TORCH_AVAILABLE:
                log(f"\n--- 通信性能基准测试 ---")
                log(f"  ✗ 跳过: PyTorch 不可用 (TORCH_AVAILABLE=False)")
                result["benchmark"] = {"error": "PyTorch not available"}
            elif not dist_initialized:
                log(f"\n--- 通信性能基准测试 ---")
                log(f"  ✗ 跳过: 分布式环境未初始化")
                result["benchmark"] = {"error": "Distributed not initialized"}
            else:
                log(f"\n--- 通信性能基准测试 ---")
                
                # 预先创建所有进程组
                log(f"  预创建所有进程组...")
                torch_groups = ParallelTopologyValidator.create_torch_groups_for_topology(
                    topology, rank, timeout_seconds=60
                )
                log(f"  ✓ 已创建 {len(torch_groups)} 个进程组")
                
                # 运行基准测试
                benchmark_results = ParallelTopologyValidator.benchmark_all_groups(
                    topology, rank,
                    num_iterations=args.benchmark_iterations,
                    data_size_mb=args.benchmark_size
                )
                result["benchmark"] = benchmark_results
                
                # 基准测试结果打印：所有 rank 都打印，方便调试
                for bench_result in benchmark_results:
                    if "error" in bench_result:
                        error_msg = bench_result.get('error', 'unknown')
                        if "Rank not in group" in error_msg or "not in group" in error_msg:
                            pass
                        else:
                            print(f"[Rank {rank}] {bench_result.get('group_type', 'unknown')}: error - {error_msg}")
                    elif bench_result.get("skipped"):
                        # 跳过的测试
                        print(f"[Rank {rank}] {bench_result.get('group_type', 'unknown')}: skipped - {bench_result.get('note', 'N/A')}")
                    else:
                        # 正常完成的测试
                        print(f"[Rank {rank}] {bench_result.get('group_type', 'unknown')} - {bench_result.get('operation', 'N/A')}: "
                            f"{bench_result.get('latency_mean_ms', 0):.2f} ms, "
                            f"{bench_result.get('bandwidth_gbps', 0):.2f} GB/s")
                
                # 销毁所有进程组
                log(f"  销毁所有进程组...")
                ParallelTopologyValidator.destroy_torch_groups(torch_groups)
                log(f"  ✓ 已销毁所有进程组")
        
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        log(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保清理进程组
        if torch_groups:
            ParallelTopologyValidator.destroy_torch_groups(torch_groups)
    
    # 收集所有 rank 的结果到 rank 0
    all_results = None
    if dist_initialized and TORCH_AVAILABLE:
        # 使用 gather_object 收集所有 rank 的结果
        import torch.distributed as dist
        
        # 所有 rank 只发送精简数据，减少数据冗余
        result_to_send = {
            "rank": rank,
            "rank_info": rank_info,
            "benchmark": result.get("benchmark"),
        }
        
        # 准备收集容器（只有 rank 0 需要足够大的容器）
        if rank == 0:
            all_results = [None for _ in range(world_size)]
        
        # 所有 rank 都调用 gather_object
        dist.gather_object(result_to_send, all_results if rank == 0 else None, dst=0)
        
        # 同步确保所有 rank 都完成收集
        dist.barrier()
    else:
        # 非分布式环境，只有当前结果
        all_results = [{
            "rank": rank,
            "rank_info": rank_info,
            "benchmark": result.get("benchmark"),
        }]
    
    # 保存结果（只有 rank 0 执行）
    if rank == 0:
        os.makedirs(args.output_dir, exist_ok=True)
        
        if args.output_file:
            output_file = os.path.join(args.output_dir, args.output_file)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(args.output_dir, f"topology_result_{timestamp}.json")
        
        # 构建包含所有 rank 结果的输出
        output_data = {
            "summary": {
                "world_size": world_size,
                "timestamp": datetime.now().isoformat(),
                "params": params,
                "topology": result.get("topology"),
            },
            "ranks": all_results if all_results else [result]
        }
        
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2, default=str)
        log(f"\n结果已保存到: {output_file}")
        log(f"  包含 {len(all_results) if all_results else 1} 个 rank 的结果")
    
    # JSON 输出
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    
    log(f"\n{'=' * 60}")
    log("测试完成")
    log("=" * 60)
    
    return 0 if result["status"] == "valid" else 1


def get_rank_info():
    """获取当前进程的 rank 信息"""
    info = {
        "hostname": "",
        "rank": 0,
        "world_size": 0,
        "local_rank": 0,
    }
    
    import socket
    info["hostname"] = socket.gethostname()
    
    # 从环境变量获取 rank 信息
    if "RANK" in os.environ:
        info["rank"] = int(os.environ["RANK"])
    elif "OMPI_COMM_WORLD_RANK" in os.environ:
        info["rank"] = int(os.environ["OMPI_COMM_WORLD_RANK"])
    
    if "WORLD_SIZE" in os.environ:
        info["world_size"] = int(os.environ["WORLD_SIZE"])
    elif "OMPI_COMM_WORLD_SIZE" in os.environ:
        info["world_size"] = int(os.environ["OMPI_COMM_WORLD_SIZE"])
    
    if "LOCAL_RANK" in os.environ:
        info["local_rank"] = int(os.environ["LOCAL_RANK"])
    
    return info


if __name__ == "__main__":
    sys.exit(test_topology_validator())

