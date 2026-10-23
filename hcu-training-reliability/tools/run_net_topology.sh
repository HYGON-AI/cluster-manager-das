#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# =============================================================================
# Network Topology Validator Launch Script
# 网络拓扑验证器启动脚本
# =============================================================================
#
# 使用方式：
#   ./run_net_topology.sh <hostfile_path> [options]
#
# 示例：
#   ./run_net_topology.sh /path/to/hostfile
#   ./run_net_topology.sh ./hostfile
#
# 参数：
#   $1 - hostfile 路径（必填）
#
# =============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Python 脚本路径
PYTHON_SCRIPT="${PROJECT_ROOT}/cluster_manager/parallel/net_topology_validator.py"

# =============================================================================
# 参数解析
# =============================================================================

# 检查是否提供了 hostfile 参数
if [[ $# -lt 1 ]]; then
    echo -e "${RED}Error: 缺少 hostfile 参数${NC}"
    echo ""
    echo "使用方式："
    echo "  $0 <hostfile_path>"
    echo ""
    echo "示例："
    echo "  $0 /path/to/hostfile"
    echo "  $0 ./hostfile"
    exit 1
fi

# 必填：hostfile 路径（从命令行参数获取）
HOSTFILE="$1"

# 总进程数（可选，如果不设置则从 hostfile 自动计算）
# 格式：NUM_PROCESSES=16 或 NUM_PROCESSES=""（自动计算）
NUM_PROCESSES=""

# 方式1：通过训练脚本解析并行参数（推荐）
# 设置 MEGATRON_SCRIPT_PATH 后，将自动从训练脚本解析 tp/pp/dp 等参数
# 此时下面的 TP_SIZE/PP_SIZE/DP_SIZE 等参数将被忽略
MEGATRON_SCRIPT_PATH="${MEGATRON_SCRIPT_PATH:-/workspace/train/run.sh}"

# 方式2：手动指定并行参数
# 如果 MEGATRON_SCRIPT_PATH 为空，则使用以下参数
# 注意：tp * pp * dp * cp 必须等于 NUM_PROCESSES
TP_SIZE=8
PP_SIZE=2
DP_SIZE=1
CP_SIZE=1
EP_SIZE=1        # MoE only
ETP_SIZE=1       # MoE only

# 输出目录
OUTPUT_DIR="./topology_results"

# 环境配置
NCCL_ENV="${NCCL_ENV:-/workspace/hcu_megatron/requirements/env.sh}"
CONDA_INIT_PATH="${CONDA_INIT_PATH:-/opt/conda/etc/profile.d/conda.sh}"
CONDA_ENV="${CONDA_ENV:-hcu-training}"

# 是否运行通信性能基准测试
RUN_BENCHMARK=true
BENCHMARK_ITERATIONS=10
BENCHMARK_DATA_SIZE_MB=1.0

# 是否详细输出
VERBOSE=false

# =============================================================================
# 以下内容无需修改
# =============================================================================

# 导出 MEGATRON_SCRIPT_PATH（如果设置了）
if [[ -n "$MEGATRON_SCRIPT_PATH" ]]; then
    export MEGATRON_SCRIPT_PATH
fi

# 导出环境变量
export HIP_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
export HSA_FORCE_FINE_GRAIN_PCIE=1
export CUDA_DEVICE_MAX_CONNECTIONS=1

# 分布式通信环境变量
# MASTER_ADDR: 主节点地址，使用 hostfile 中的第一个节点
# MASTER_PORT: 通信端口，默认 29500
if [[ -f "$HOSTFILE" ]]; then
    export MASTER_ADDR=$(head -n 1 "$HOSTFILE" | awk '{print $1}')
    echo -e "${GREEN}[INFO]${NC} MASTER_ADDR: $MASTER_ADDR (from hostfile)"
else
    export MASTER_ADDR=$(hostname)
    echo -e "${YELLOW}[WARN]${NC} HOSTFILE not found, using local hostname as MASTER_ADDR: $MASTER_ADDR"
fi
export MASTER_PORT=${MASTER_PORT:-29501}
echo -e "${GREEN}[INFO]${NC} MASTER_PORT: $MASTER_PORT"

# 检查 hostfile 文件是否存在
if [[ ! -f "$HOSTFILE" ]]; then
    echo -e "${RED}Error: hostfile 不存在: $HOSTFILE${NC}"
    exit 1
fi

# 从 hostfile 自动计算 NUM_PROCESSES（如果未手动设置）
# 计算方式：节点数 * 8（每节点 8 个 GPU）
if [[ -z "$NUM_PROCESSES" ]]; then
    NUM_PROCESSES=$(($(cat ${HOSTFILE} | sort | uniq | wc -l) * 8))
    echo -e "${GREEN}[INFO]${NC} NUM_PROCESSES auto-calculated: $NUM_PROCESSES (nodes * 8)"
fi

# 构建 Python 参数（必须在 NUM_PROCESSES 计算之后）
PYTHON_ARGS=""

if [[ -z "$MEGATRON_SCRIPT_PATH" ]]; then
    # 方式2：使用手动指定的参数（MEGATRON_SCRIPT_PATH 为空时）
    PYTHON_ARGS="$PYTHON_ARGS --tp $TP_SIZE"
    PYTHON_ARGS="$PYTHON_ARGS --pp $PP_SIZE"
    PYTHON_ARGS="$PYTHON_ARGS --dp $DP_SIZE"
    PYTHON_ARGS="$PYTHON_ARGS --cp $CP_SIZE"
    PYTHON_ARGS="$PYTHON_ARGS --ep $EP_SIZE"
    PYTHON_ARGS="$PYTHON_ARGS --etp $ETP_SIZE"
fi
# 注意：MEGATRON_SCRIPT_PATH 不作为参数传递，仅通过环境变量导出
# global_config 会自动解析 MEGATRON_SCRIPT_PATH 指向的训练脚本

PYTHON_ARGS="$PYTHON_ARGS --world_size $NUM_PROCESSES"
PYTHON_ARGS="$PYTHON_ARGS --output-dir $OUTPUT_DIR"

if [[ "$RUN_BENCHMARK" == "true" ]]; then
    PYTHON_ARGS="$PYTHON_ARGS --benchmark"
    PYTHON_ARGS="$PYTHON_ARGS --benchmark-iterations $BENCHMARK_ITERATIONS"
    PYTHON_ARGS="$PYTHON_ARGS --benchmark-size $BENCHMARK_DATA_SIZE_MB"
fi

if [[ "$VERBOSE" == "true" ]]; then
    PYTHON_ARGS="$PYTHON_ARGS --verbose"
fi

# 打印配置信息
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}Network Topology Validator${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "Hostfile:         $HOSTFILE"
echo -e "Num Processes:    $NUM_PROCESSES"
echo -e "Output Dir:       $OUTPUT_DIR"

if [[ -n "$MEGATRON_SCRIPT_PATH" ]]; then
    echo -e "Mode:             Parse from training script"
    echo -e "Train Script:     $MEGATRON_SCRIPT_PATH"
else
    echo -e "Mode:             Manual parameters"
    echo -e "TP Size:          $TP_SIZE"
    echo -e "PP Size:          $PP_SIZE"
    echo -e "DP Size:          $DP_SIZE"
    echo -e "CP Size:          $CP_SIZE"
    if [[ "$EP_SIZE" -gt 1 ]]; then
        echo -e "EP Size:          $EP_SIZE"
        echo -e "ETP Size:         $ETP_SIZE"
    fi
fi

echo -e "${BLUE}============================================================${NC}"

# 构建环境初始化命令
ENV_INIT_CMDS=""

# NCCL 环境
if [[ -n "$NCCL_ENV" && -f "$NCCL_ENV" ]]; then
    ENV_INIT_CMDS="$ENV_INIT_CMDS source $NCCL_ENV &&"
    echo -e "${GREEN}[INFO]${NC} Using NCCL_ENV: $NCCL_ENV"
fi

# Conda 环境
if [[ -n "$CONDA_INIT_PATH" && -f "$CONDA_INIT_PATH" ]]; then
    ENV_INIT_CMDS="$ENV_INIT_CMDS source $CONDA_INIT_PATH && conda activate $CONDA_ENV &&"
    echo -e "${GREEN}[INFO]${NC} Using Conda env: $CONDA_ENV"
fi

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 运行 mpirun
echo -e "\n${YELLOW}[INFO]${NC} Starting topology validation...\n"

mpirun -np ${NUM_PROCESSES} \
    --hostfile ${HOSTFILE} \
    --allow-run-as-root \
    --bind-to none \
    -mca plm_rsh_no_tree_spawn 1 \
    -x HIP_VISIBLE_DEVICES \
    -x HSA_FORCE_FINE_GRAIN_PCIE \
    -x CUDA_DEVICE_MAX_CONNECTIONS \
    -x MEGATRON_SCRIPT_PATH \
    -x MASTER_ADDR \
    -x MASTER_PORT \
    bash -c "
        # 导出 OpenMPI 环境变量为 PyTorch 格式
        export WORLD_SIZE=\${OMPI_COMM_WORLD_SIZE:-$NUM_PROCESSES}
        export RANK=\${OMPI_COMM_WORLD_RANK:-0}
        export LOCAL_RANK=\${OMPI_COMM_WORLD_LOCAL_RANK:-0}
        ${ENV_INIT_CMDS}
        python -u ${PYTHON_SCRIPT} ${PYTHON_ARGS}
    "

EXIT_CODE=$?

echo -e "\n${BLUE}============================================================${NC}"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}✓ Topology validation completed successfully${NC}"
else
    echo -e "${RED}✗ Topology validation failed with exit code: $EXIT_CODE${NC}"
fi
echo -e "${BLUE}============================================================${NC}"

exit $EXIT_CODE
