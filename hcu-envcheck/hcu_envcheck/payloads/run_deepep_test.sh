#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# In-project DeepEP launcher (renamed from run_test.sh), mirroring
# third-party/DeepEP/tests/run_test.sh (ci-release): the master address/port
# and GPU list come from the environment so concurrent test groups rendezvous
# on their own first node. Extended with an intranode mode that runs the
# single-node test directly, as documented by the upstream tests readme.
# The upstream file itself is never modified.

# rocSHMEM
export DEEPEP_ENABLE_LL_LAYERED_OPT=1

# export ROCSHMEM_IB_GID_INDEX=0      # ROCE
# export ROCSHMEM_GDR_DISABLE_XDP=1   # disable xdp

# common
[[ ${HCU_GPUS_PER_NODE:-} =~ ^[1-9][0-9]*$ ]] || { echo 'explicit HCU_GPUS_PER_NODE required' >&2; exit 2; }
[[ -n ${HCU_DEVICE_SELECTION_FILE:-} ]] || { echo 'preflight device selection record required' >&2; exit 2; }
SELECTOR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/device_selection.py
source "${SELECTOR%/*}/../logging.sh"
python_bin=''
for candidate in python3 python; do
    command -v "$candidate" >/dev/null 2>&1 && { python_bin=$candidate; break; }
done
[[ -n $python_bin && -r $SELECTOR ]] || { echo 'device selection Python/runtime unavailable' >&2; exit 69; }
SELECT=("$python_bin" -B "$SELECTOR" exec --count "$HCU_GPUS_PER_NODE" --record "$HCU_DEVICE_SELECTION_FILE" --)

# 单机节点内测试：按上游 readme 直接 python 运行（工作目录须为 tests 目录）
if [ "$#" -eq 1 ] && [ "$1" = "intranode" ]; then
    python_bin=""
    for candidate in python3 python; do
        command -v "$candidate" >/dev/null 2>&1 && python_bin=$candidate && break
    done
    if [ -z "$python_bin" ]; then
        echo "python missing in selected environment"
        exit 69
    fi
    if [ ! -r ./test_intranode.py ]; then
        echo "test_intranode.py missing under $(pwd)"
        exit 69
    fi
    case $HCU_GPUS_PER_NODE in 2|4|8) ;; *) echo 'intranode supports 2, 4 or 8 GPUs' >&2; exit 2 ;; esac
    hcu_log_command TEST-COMMAND "$(hostname -s)" "$python_bin" ./test_intranode.py --num-processes "$HCU_GPUS_PER_NODE"
    hcu_log_raw '单机测试程序输出'
    exec "${SELECT[@]}" "$python_bin" ./test_intranode.py --num-processes "$HCU_GPUS_PER_NODE"
fi

# test
# 用法:
#   1 个参数: intranode                 (单机)
#   2 个参数: <node-rank> <lowl|norm>   (nnodes 默认为 2)
#   3 个参数: <nnodes> <node-rank> <lowl|norm>
if [ "$#" -eq 3 ]; then
    NNODES=$1
    NODE_RANK=$2
    MODE=$3
elif [ "$#" -eq 2 ]; then
    NNODES=2
    NODE_RANK=$1
    MODE=$2
else
    echo "Usage: $0 [nnodes] <node-rank> <lowl|norm> | intranode"
    echo "  1 arg : intranode (single node)"
    echo "  2 args: <node-rank> <lowl|norm>  (nnodes defaults to 2)"
    exit 1
fi

# NNODES is the node count of this test group (from OMPI_COMM_WORLD_SIZE),
# never a GPU count; each node reuses its preflight device selection.
if ! [[ "$NNODES" =~ ^[2-9][0-9]*$ ]]; then
    echo "Error: nnodes must be an integer >= 2 (got '$NNODES')"
    exit 1
fi
if ! [[ "$NODE_RANK" =~ ^[0-9]+$ ]] || [ "$NODE_RANK" -ge "$NNODES" ]; then
    echo "Error: node-rank must be 0..$((NNODES-1)) (got '$NODE_RANK')"
    exit 1
fi

if [ "$MODE" = "lowl" ]; then
    # 测试节点间低延迟
    if (( NODE_RANK == 0 )); then
        hcu_log_command TEST-COMMAND "$(hostname -s)" torchrun --nproc-per-node=1 --nnodes="$NNODES" --node-rank="$NODE_RANK" --master-addr="${MASTER_ADDR:?MASTER_ADDR required}" --master-port="${MASTER_PORT:?MASTER_PORT required}" ./test_low_latency.py --num-processes "$HCU_GPUS_PER_NODE"
        hcu_log_raw 'MPI 汇集各节点输出'
    fi
    exec "${SELECT[@]}" torchrun --nproc-per-node=1 --nnodes="$NNODES" --node-rank="$NODE_RANK" --master-addr="${MASTER_ADDR:?MASTER_ADDR required}" --master-port="${MASTER_PORT:?MASTER_PORT required}" ./test_low_latency.py --num-processes "$HCU_GPUS_PER_NODE"
elif [ "$MODE" = "norm" ]; then
    # 测试节点间高吞吐
    (( HCU_GPUS_PER_NODE == 8 )) || { echo 'norm requires 8 GPUs per node' >&2; exit 2; }
    if (( NODE_RANK == 0 )); then
        hcu_log_command TEST-COMMAND "$(hostname -s)" torchrun --nproc-per-node=1 --nnodes="$NNODES" --node-rank="$NODE_RANK" --master-addr="${MASTER_ADDR:?MASTER_ADDR required}" --master-port="${MASTER_PORT:?MASTER_PORT required}" ./test_internode.py --num-processes "$HCU_GPUS_PER_NODE"
        hcu_log_raw 'MPI 汇集各节点输出'
    fi
    exec "${SELECT[@]}" torchrun --nproc-per-node=1 --nnodes="$NNODES" --node-rank="$NODE_RANK" --master-addr="${MASTER_ADDR:?MASTER_ADDR required}" --master-port="${MASTER_PORT:?MASTER_PORT required}" ./test_internode.py --num-processes "$HCU_GPUS_PER_NODE"
else
    echo "Error: MODE must be 'lowl' or 'norm' (got '$MODE')"
    echo "Usage: $0 [nnodes] <node-rank> <lowl|norm>"
    exit 1
fi
