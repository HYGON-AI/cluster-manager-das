#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

NP=2
GPUS_PER_NODE=8

MASTER_ADDR=b12r1n08
MASTER_PORT=23333
DTK_ENV="/opt/dtk-25.04.1/env.sh"              # where env.sh of dtk
NCCL_ENV="/public/home/user/labs/hcu_megatron/requirements/env.sh"
export TORCH_CPP_LOG_LEVEL=error
#pkill -f ft_launcher
HOSTFILE=/public/home/user/labs/hcu_megatron/hostfile
LOGFILE="/public/home/user/workspace/new_labs/ft_hcu_megatron/kill_out.log"
for arg in "$@"; do
    case $arg in
        --host=*)
            MASTER_ADDR="${arg#*=}"
            ;;
        --port=*)
            MASTER_PORT="${arg#*=}"
            ;;
        --gpus=*)
            NP="${arg#*=}"
            ;;
        --hostfile=*)
            HOSTFILE="${arg#*=}"
            ;;
        --logfile=*)
            LOGFILE="${arg#*=}"
            ;;

    esac
done



NNODES=$NP
export HIP_VISIBLE_DEVICES=0,1,2,3,4,5,6,7  # 根据实际情况调整
echo "Starting distributed training with $NP nodes, $GPUS_PER_NODE GPUs per node"

mpirun -np ${NP} \
  --hostfile ${HOSTFILE} \
  --bind-to none \
  --mca plm_rsh_no_tree_spawn 1 \
  --allow-run-as-root \
  -x NNODES=${NNODES} \
  -x MASTER_ADDR=${MASTER_ADDR} \
  -x MASTER_PORT=${MASTER_PORT} \
  -x GPUS_PER_NODE=${GPUS_PER_NODE} \
  -x HIP_VISIBLE_DEVICES \
  bash -c "
    source ~/.bashrc && conda activate megatron_train && \
    source /opt/dtk-25.04.1/env.sh && \
    source /public/home/user/labs/hcu_megatron/requirements/env.sh && \
    bash /public/home/user/workspace/new_labs/ft_hcu_megatron/examples/llama/trains.sh
  "  2>&1 | tee  ${LOGFILE}
wait
