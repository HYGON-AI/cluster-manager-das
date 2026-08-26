# Copyright 2023 The DLRover Authors. All rights reserved.
# Modifications Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import time
from dataclasses import dataclass
from datetime import timedelta
import torch
import torch.distributed as dist
import socket
from cluster_manager.config.global_config import logger
node_name = socket.gethostname()

def record_execution_time(func):
    def wrapper(*args, **kwargs):
        t = func(*args, **kwargs)
        local_rank = int(os.environ["LOCAL_RANK"])
        write_time_to_file(t, local_rank)
        return t

    return wrapper


def log_execution_time(func):
    def wrapper(*args, **kwargs):
        local_rank = int(os.environ["LOCAL_RANK"])
        func_name = func.__name__
        t = func(*args, **kwargs)
        t = round(t, 3)
        return t

    return wrapper


def mock_error():
    if os.environ.get("MOCK_ERR_RANK"):
        err_rank = int(os.environ["MOCK_ERR_RANK"])
        local_rank = int(os.environ["LOCAL_RANK"])
        if err_rank == local_rank:
            raise ValueError("Mock network error!")


#@log_execution_time
def init_process_group(
    protocol: str, timeout: timedelta = timedelta(seconds=180)
):
    start = time.time()
    dist.init_process_group(protocol, timeout=timeout)
    elapsed_time = time.time() - start
    return elapsed_time


def get_network_check_timeout() -> timedelta:
    default_timeout_seconds = 180
    timeout = int(
        os.environ.get("NETWORK_CHECK_TIMEOUT", default_timeout_seconds)
    )
    if timeout <= 0:
        timeout = default_timeout_seconds

    return timedelta(seconds=timeout)


@log_execution_time
def bm_allgather(local_rank, shape, use_gpu):
    world_size = dist.get_world_size()
    device = torch.device(f"cuda:{local_rank}" if use_gpu else "cpu")
    data = torch.randn(shape, dtype=torch.float32).to(device)
    tensor_list = [
        torch.zeros_like(data).to(device) for _ in range(world_size)
    ]

    if use_gpu:
        elapsed_time = _execute_nccl_comm(local_rank, dist.all_gather, tensor_list, data)
    else:
        elapsed_time = _execute_cpu_comm(local_rank, dist.all_gather, tensor_list, data)

    gb_unit = 1024 * 1024 * 1024
    algobw = shape * 4 / gb_unit / (elapsed_time / 1000)
    busbw = algobw * (world_size - 1) / world_size
    algobw = round(algobw, 2)
    busbw = round(busbw, 2)
    elapsed_time = round(elapsed_time, 3)
    if local_rank == 0:
        logger.info(f"{node_name} {local_rank} AllGather Perf: world size = {world_size}, algobw={algobw} GB/s, busbw={busbw} GB/s.")
    mock_error()
    return elapsed_time


@log_execution_time
def bm_allreduce(local_rank, shape, use_gpu):
    world_size = dist.get_world_size()
    device = torch.device(f"cuda:{local_rank}" if use_gpu else "cpu")
    data = torch.randn(shape, dtype=torch.float32).to(device)
    a = shape
    if use_gpu:
        elapsed_time = _execute_nccl_comm(local_rank, dist.all_reduce, data)
    else:
        elapsed_time = _execute_cpu_comm(local_rank, dist.all_reduce, data)

    # edge protection
    elapsed_time = 0.001 if elapsed_time == 0.0 else elapsed_time

    gb_unit = 1024 * 1024 * 1024
    algobw = shape * 4 / gb_unit / (elapsed_time / 1000)
    busbw = algobw * 2 * (world_size - 1) / world_size
    algobw = round(algobw, 2)
    busbw = round(busbw, 2)
    elapsed_time = round(elapsed_time, 3)
    if local_rank == 0:
        logger.info(f"{node_name} {local_rank} AllReduce Perf: world size = {world_size}, algobw={algobw} GB/s, busbw={busbw} GB/s.")
    mock_error()
    return elapsed_time

@log_execution_time
def bm_alltoall(shape, use_gpu):
    world_size = dist.get_world_size()
    local_rank = int(os.environ["LOCAL_RANK"])
    device = torch.device(f"cuda:{local_rank}" if use_gpu else "cpu")
    
    # 为 all2all 准备数据：每个 rank 有 world_size 个张量
    # 假设 shape 是单个张量的形状，我们创建一组张量
    tensor_list = []
    for i in range(world_size):
        tensor = torch.randn(shape, dtype=torch.float32).to(device)
        tensor_list.append(tensor)
    
    # 准备输出张量列表
    output_tensor_list = [torch.zeros_like(tensor) for tensor in tensor_list]
    
    if use_gpu:
        elapsed_time = _execute_nccl_comm(dist.all_to_all, output_tensor_list, tensor_list)
    else:
        elapsed_time = _execute_cpu_comm(dist.all_to_all, output_tensor_list, tensor_list)

    # edge protection
    elapsed_time = 0.001 if elapsed_time == 0.0 else elapsed_time

    # 计算带宽
    gb_unit = 1024 * 1024 * 1024
    # all2all 的总数据量：每个 rank 发送 world_size 个张量，每个张量大小为 shape
    total_data_size = world_size * shape.numel() * 4  # 4 bytes for float32
    algobw = total_data_size / gb_unit / (elapsed_time / 1000)
    
    # all2all 的 bus bandwidth 计算与 allreduce 不同
    # 对于 all2all，通常考虑每个链接的带宽
    busbw = algobw * (world_size - 1) / world_size
    
    algobw = round(algobw, 2)
    busbw = round(busbw, 2)
    elapsed_time = round(elapsed_time, 3)
    
    if local_rank == 0:
        logger.info(f"{node_name} {local_rank} All2All Perf: world size = {world_size}, algobw={algobw} GB/s, busbw={busbw} GB/s.")
    
    mock_error()
    return elapsed_time

@log_execution_time
def _execute_nccl_comm(local_rank ,comm_op, *args):
    # warm up
    for _ in range(1):
        a= comm_op(*args)
    logger.info(f"333333333 local_rank:{local_rank}")
    torch.cuda.synchronize(device=local_rank)

    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record(stream=torch.cuda.current_stream())

    round_num = 1
    for _ in range(round_num):
        logger.info(f"444444444444 local_rank:{local_rank}")
        comm_op(*args)
    logger.info(f"5555555555555 local_rank:{local_rank}")
    end.record(stream=torch.cuda.current_stream())
    end.synchronize()
    elapsed_time = start.elapsed_time(end) / round_num
    return elapsed_time


@log_execution_time
def _execute_cpu_comm(comm_op, *args):
    # warm up
    for _ in range(10):
        comm_op(*args)

    round_num = 20
    start = time.time()
    for _ in range(round_num):
        comm_op(*args)
    elapsed_time = time.time() - start
    return elapsed_time


def matmul(local_rank, use_cuda, round_num=10, verbose=False):
    device = torch.device(f"cuda:{local_rank}" if use_cuda else "cpu")

    if use_cuda:
        m = k = n = 16384
    else:
        m = k = n = 128

    if use_cuda and torch.cuda.is_bf16_supported():
        dtype = torch.bfloat16
    else:
        dtype = torch.float32

    tensor1 = torch.randn(m, k, dtype=dtype, device=device)
    tensor2 = torch.randn(k, n, dtype=dtype, device=device)
    mm_out = torch.empty(m, n, dtype=dtype, device=device)

    if use_cuda:
        elapsed_time = _execute_gpu_matmul(tensor1, tensor2, round_num, mm_out)
    else:
        elapsed_time = _execute_cpu_matmul(tensor1, tensor2, round_num, mm_out)
    elapsed_time = round(elapsed_time, 3)
    if verbose:
        logger.info(f"{node_name} {local_rank} per matmul is {elapsed_time} milliseconds with m={m},k={k},n={n} using {dtype}")
    return elapsed_time


def _execute_gpu_matmul(tensor1, tensor2, round_num, out=None):
    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record(stream=torch.cuda.current_stream())

    for _ in range(round_num):
        with torch.no_grad():
            torch.matmul(tensor1, tensor2, out=out)

    end.record(stream=torch.cuda.current_stream())
    end.synchronize()
    elapsed_time = start.elapsed_time(end) / round_num
    return elapsed_time


@log_execution_time
def _execute_cpu_matmul(tensor1, tensor2, round_num, out=None):
    start = time.time()
    for _ in range(round_num):
        with torch.no_grad():
            torch.matmul(tensor1, tensor2, out=out)

    elapsed_time = time.time() - start
    return elapsed_time

# def write_time_to_file(time, local_rank):
#     data = {"time": time, "local_rank": local_rank}
#     root = ConfigPath.NETWORK_CHECK_DATA_DIR
#     os.makedirs(root, exist_ok=True)
#     path = os.path.join(root, f"{local_rank}.txt")
#     with open(path, "w") as f:
#         f.write(json.dumps(data))


@dataclass
class DeviceBenchEnv:
    device_name: str = "Unknown"
    torch_version: str = "Unknown"
    cuda_version: str = "Unknown"
    cann_version: str = "Unknown"
