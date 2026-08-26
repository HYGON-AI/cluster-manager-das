#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
for para in $*
do
    if [[ $para == --profiling* ]];then
        profiling=${para#*=}
    fi
done

HOSTFILE=${1}
HOSTFILE=$(realpath -e "${HOSTFILE}")
GPUS=$(($(cat ${HOSTFILE}|sort|uniq |wc -l)*8))
HOST="$(cat ${HOSTFILE} |sed -n "1p"|awk -F ' ' '{print $1}')"
PORT="25905"

MEGATRON_PATH="/public/home/user/hcu_megatron"
TRAIN_PATH=${MEGATRON_PATH}/examples/gpt3

# Those variables need to modify
DTK_ENV=""                                                               # where env.sh of dtk
DATA_PATH="/public/home/user/dataset/gpt_datasets_samples/redpajama_text_document"                                                             # path to redpajama_text_document
TOKENIZER_MODEL_PATH="/public/home/user/dataset/gpt_datasets_samples/tokenizer.model"                                                  # path to tokenizer.model
CHECKPOINT_PATH="./ckpt"                                                       # path to ckpt
NCCL_ENV=${MEGATRON_PATH}/requirements/env.sh                            # Please adjust the variables based on the actual NET being used
LAUNCH_WITH_BINDING=${MEGATRON_PATH}/requirements/launch_with_binding.sh # Please adjust the variables based on the actual NET

# Conda and training script configuration
CONDA_INIT_PATH="/public/home/user/anaconda3/etc/profile.d/conda.sh"  # path to conda init script
CONDA_ENV="cluster_manager"                                               # conda environment name
TRAIN_SCRIPT="${TRAIN_PATH}/train_gpt_567B_$((${GPUS} / 8))nodes.sh"     # training script path



# Runs
module load mpi/hpcx/2.18.0/gcc-8.5.0/shca

mpirun -np ${GPUS}  --hostfile ${HOSTFILE} \
        --allow-run-as-root \
        --bind-to none \
        --mca plm_rsh_no_tree_spawn 1 \
	    -wdir ${TRAIN_PATH} \
        bash -c "
        source ${CONDA_INIT_PATH} && \
        conda activate ${CONDA_ENV} && \
        source ${NCCL_ENV} && \
        ${TRAIN_SCRIPT} \
        ${HOST} \
        ${PORT} \
        --data_path=$DATA_PATH \
        --tokenizer_path=$TOKENIZER_MODEL_PATH \
        --checkpoint_path=$CHECKPOINT_PATH \
        --launch_with_binding=${LAUNCH_WITH_BINDING} \
        --profiling=$profiling"
wait
