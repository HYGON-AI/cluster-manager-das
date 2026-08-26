# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
for para in "$@"; do
    case "$para" in
        --profiling=*)
            profiling="${para#*=}"
            ;;
        --host=*)
            HOST="${para#*=}"
            ;;
        --gpus=*)
            GPUS="${para#*=}"
            ;;
        --hostfile=*)
            HOSTFILE="${para#*=}"
            ;;
        --logfile=*)
            LOGFILE="${para#*=}"
            ;;
        --scriptfile=*)
            SCRIPTFILE="${para#*=}"
            ;;
        --ckptfile=*)
            CKPTFILE="${para#*=}"
            ;;
        *)
            echo "Warning: Unknown parameter $para"
            ;;
    esac
done

CURRENT_DIR=$( cd "$( dirname "$0" )" && pwd )
MEGATRON_PATH=$( dirname $( dirname ${CURRENT_DIR}))

# Those variables need to modify
DTK_ENV="/opt/dtk-25.04.1/env.sh"                                                               # where env.sh of dtk
DATA_PATH="/public/share/project/data/datasets/oscar-1GB-head/oscar-1GB_head-llama3.2_text_document"                                                             # path to oscar-1GB_head-llama2_text_document
TOKENIZER_MODEL_PATH="/public/share/project/data/model_weights/llama3.2/tokenizer.model"                                                  # path to tokenizer.model
CHECKPOINT_PATH="/public/share/project/zy/ckpt"                                                       # path to ckpt
NCCL_ENV=${MEGATRON_PATH}/requirements/env.sh                            # Please adjust the variables based on the actual NET being used
LAUNCH_WITH_BINDING=${MEGATRON_PATH}/requirements/launch_with_binding.sh # Please adjust the variables based on the actual NET being used

# Those variables no need to modify
#HOSTFILE="hostfile_$(basename "$0" | sed -E 's/^run_(.+)\.sh$/\1/')"
#GPUS=$(($(cat ${HOSTFILE}|sort|uniq |wc -l)*8))
GPUS=${GPUS:-"8"}
#HOST="$(cat ${HOSTFILE} |sed -n "1p"|awk -F ' ' '{print $1}')"
HOST=${HOST:-"b11r3n03"}
PORT=$(( RANDOM % 5001 + 20000 ))

# Runs Llama3 70B model
source ${NCCL_ENV}
mpirun -np ${GPUS}  --hostfile ${HOSTFILE} \
                    --allow-run-as-root \
                    --bind-to none \
                    --mca plm_rsh_no_tree_spawn 1 \
                    bash -c "
                    source /public/home/user/requirements/etc/profile.d/conda.sh && \
                    conda activate llama && \
                    source ${DTK_ENV} && \
                    source ${NCCL_ENV} && \
                    /public/home/user/hcu_megatron/examples/llama/train_llama3_70b_8nodes.sh \
                    ${HOST} \
                    ${PORT} \
                    --data_path=$DATA_PATH \
                    --tokenizer_path=$TOKENIZER_MODEL_PATH \
                    --checkpoint_path=$CHECKPOINT_PATH \
                    --profiling=$profiling" >> ${LOGFILE} 2>&1

wait
