#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

# Environment variables for performance tuning
export CUDA_DEVICE_MAX_CONNECTIONS=${CUDA_DEVICE_MAX_CONNECTIONS:-1}
#export LOG_LEVEL=${LOG_LEVEL:-INFO}
#export NCCL_IB_TIMEOUT=${NCCL_IB_TIMEOUT:-19}
#export NVTE_FWD_LAYERNORM_SM_MARGIN=${NVTE_FWD_LAYERNORM_SM_MARGIN:-16}
#export NVTE_BWD_LAYERNORM_SM_MARGIN=${NVTE_BWD_LAYERNORM_SM_MARGIN:-16}
#export NCCL_P2P_NET_CHUNKSIZE=${NCCL_P2P_NET_CHUNKSIZE:-2097152}
#export NCCL_AVOID_RECORD_STREAMS=${NCCL_AVOID_RECORD_STREAMS:-1}

export PYTHONPATH=/public/home/user/workspace/new_labs/ft_hcu_megatron/Megatron-LM:$PYTHONPATH
export LD_LIBRARY_PATH=/opt/dtk-25.04.1/cuda/cuda-11/targets/x86_64-linux/lib:$LD_LIBRARY_PATH
CHECKPOINT_PATH=/public/share/project/user/ckpt-llama #<Specify path>
TENSORBOARD_LOGS_PATH=/public/home/user/ckpt/llama/tensorboard #<Specify path>
TOKENIZER_ARG=/public/home/user/model_weights/llama2/tokenizer.model # Path to tokenizer model, or "MOCK"
DATA_ARG=/public/home/user/datasets/oscar-1GB-head/oscar-1GB_head-llama2_text_document #<Specify path and file prefix>_text_document

# Create directories if they don't exist
mkdir -p "$(dirname "$CHECKPOINT_PATH")"
mkdir -p "$(dirname "$TENSORBOARD_LOGS_PATH")"
export TORCH_CPP_LOG_LEVEL=error
# Distributed training setup
GPUS_PER_NODE=${GPUS_PER_NODE:-8}
NUM_NODES=${NNODES:-1}
MASTER_ADDR=${MASTER_ADDR:-b11r1n10}
MASTER_PORT=${MASTER_PORT:-6000}
NODE_RANK=${OMPI_COMM_WORLD_RANK:-0}
WORLD_SIZE=$(($GPUS_PER_NODE*$NUM_NODES))


MiN_NODES=2


echo "=== Node Configuration ==="
echo "NODE_RANK: $NODE_RANK"
echo "NNODES: $NUM_NODES" 
echo "GPUS_PER_NODE: $GPUS_PER_NODE"
echo "WORLD_SIZE: $WORLD_SIZE"
echo "MASTER_ADDR: $MASTER_ADDR"
echo "MASTER_PORT: $MASTER_PORT"
echo "hostname": $HOSTNAME


# Path to the pretrain_gpt.py script, assuming this script is run from the root of the Megatron-LM repository
PRETRAIN_SCRIPT_PATH="/public/home/user/workspace/new_labs/ft_hcu_megatron/pretrain_gpt.py"

# Fixed model and training parameters
TP_SIZE=4     
CP_SIZE=1     
PP_SIZE=2     
MICRO_BATCH_SIZE=1
GLOBAL_BATCH_SIZE=128
NUM_LAYERS=8 
DTYPE="fp16"
SEQ_LENGTH=1024
MAX_POSITION_EMBEDDINGS=1024

# Data cache path (useful for both mock and real data)
DATA_CACHE_PATH="${PWD}/benchmark_cache_llama3_8b_fp8"
mkdir -p "$DATA_CACHE_PATH"
if [ $NODE_RANK -eq 0 ]; then
    RDZV_CONF="is_host=1"
else
    RDZV_CONF="is_host=0"
fi



DISTRIBUTED_ARGS=(
    --nproc_per_node $GPUS_PER_NODE
    --nnodes $NUM_NODES
    # --nnodes $NUM_NODES
    --node_rank $NODE_RANK
    --master_addr $MASTER_ADDR
    --master_port $MASTER_PORT
    --rdzv_endpoint ${MASTER_ADDR}:${MASTER_PORT}
    --max-restarts 10
    --rdzv_backend c10d
    --rdzv-conf $RDZV_CONF
    --monitor-interval 5
    --ft-log-level INFO
    --ft-rank-heartbeat-timeout 1000
    --ft-initial-rank-heartbeat-timeout 1000
    --ft-restart-policy min-healthy
    --ft-use-infra-group-rank false
    --ft-enable-nic-monitor False
    --ft-rdzv-impl legacy
)

MODEL_ARGS=(

    --inprocess-restart
    --distributed-backend nccl
    --disable-gloo-process-groups
    --ckpt-fully-parallel-load
    --num-layers $NUM_LAYERS
    --hidden-size 2048
    --ffn-hidden-size 14336
    --num-attention-heads 32
    --group-query-attention
    --num-query-groups 8
    --kv-channels 128
    --seq-length $SEQ_LENGTH
    --max-position-embeddings $MAX_POSITION_EMBEDDINGS
    --position-embedding-type rope
    --rotary-base 1000000 
    --rotary-percent 1.0
    --attention-dropout 0.0
    --hidden-dropout 0.0
    --swiglu
    --init-method-std 0.0134
    --attention-backend auto
    --apply-layernorm-1p 
    --untie-embeddings-and-output-weights
    --disable-bias-linear 
    --use-ckpt-memory-cache
    --normalization RMSNorm
)

TRAINING_ARGS=(
    --transformer-impl transformer_engine
    --use-mcore-models 
    --micro-batch-size 1
    --global-batch-size 256
    --train-iters 20
    --weight-decay 0.1 
    --adam-beta1 0.9 
    --adam-beta2 0.95 
    --init-method-std 0.006 
    --clip-grad 1.0 
    --bf16
    --disable-bias-linear
    --attention-dropout 0
    --hidden-dropout 0
    --swiglu
    --lr 3.0e-5 
    --lr-decay-style cosine 
    --min-lr 3.0e-6
    --lr-warmup-iters 1
    --ckpt-format torch_dist
    --ddp-average-in-collective
    --overlap-grad-reduce
    --use-flash-attn
)

# Conditional arguments based on DTYPE (FP8)
DTYPE_ARGS=()
if [[ "$DTYPE" == "fp8" ]]; then
    DTYPE_ARGS+=(
        "--fp8-format hybrid"
        "--fp8-amax-history-len 1024"
        "--fp8-amax-compute-algo max"
        "--fp8-param-gather"
    )
fi

# Model parallelism arguments
MODEL_PARALLEL_ARGS=(
    --tensor-model-parallel-size $TP_SIZE
    --context-parallel-size $CP_SIZE
    --pipeline-model-parallel-size $PP_SIZE # Not explicitly set in llama script options, assume 1 if not multi-node PP
    --sequence-parallel  # Always enable sequence parallelism with TP_SIZE=2
)

# Distributed Data Parallel (DDP) arguments
# From original script's ddp_args
DDP_ARGS=(
    --use-distributed-optimizer
    --overlap-grad-reduce
    --overlap-param-gather
    --sequence-parallel
    
)
TRAINING_ARGS+=("${DDP_ARGS[@]}")


# Data arguments (conditional for mock vs real data)
DATA_ARGS_LIST=()
if [[ "$TOKENIZER_ARG" == "MOCK" ]] || [[ "$DATA_ARG" == "MOCK" ]] || [[ -z "$TOKENIZER_ARG" ]]; then
    DATA_ARGS_LIST+=(
        "--mock-data"
        "--tokenizer-type NullTokenizer"
        "--vocab-size 128256" 
        "--data-cache-path ${DATA_CACHE_PATH}"
        "--tiktoken-pattern v2" 
        "--split '99,1,0'"
        "--no-create-attention-mask-in-dataloader"
        "--no-mmap-bin-files"
        "--num-workers 1"
    )
else
    # Settings for real data
    DATA_ARGS_LIST+=(
        "--data-path $DATA_ARG"
        "--tokenizer-type Llama2Tokenizer" 
        "--tokenizer-model $TOKENIZER_ARG"
        "--data-cache-path ${DATA_CACHE_PATH}"
        "--split '949,50,1'"
        "--no-create-attention-mask-in-dataloader"
        "--no-mmap-bin-files"
        "--num-workers 1"
        # Note: --vocab-size might be inferred by HuggingFaceTokenizer or might need to be explicit.
        "--vocab-size 128256"
    )
fi

EVAL_AND_LOGGING_ARGS=(
    --log-interval 1
    --eval-iters 32
    --eval-interval 100
    --save-interval 1000
    --log-throughput
    --profile
    --profile-step-start 4
    --profile-step-end 6
    --distributed-timeout-minutes 60
    --save "$CHECKPOINT_PATH"
    --load "$CHECKPOINT_PATH" 
    --tensorboard-dir "$TENSORBOARD_LOGS_PATH"
    --distributed-backend nccl
    --ckpt-format torch_dist  
    --ddp-average-in-collective
    --overlap-grad-reduce
    --use-flash-attn

)

# Ensure pretrain_gpt.py is found
if [ ! -f "$PRETRAIN_SCRIPT_PATH" ]; then
    echo "Error: pretrain_gpt.py not found at $PRETRAIN_SCRIPT_PATH"
    echo "Please ensure you are running this script from the root of the Megatron-LM repository, and pretrain_gpt.py is present."
    exit 1
fi

echo '========================================'
echo WORLD_SIZE=$WORLD_SIZE
echo "NNODES=$NUM_NODES, MASTER_ADDR=$MASTER_ADDR, MASTER_PORT=$MASTER_PORT"
echo '========================================'

# Run the training command
ft_launcher ${DISTRIBUTED_ARGS[@]} \
    "$PRETRAIN_SCRIPT_PATH" \
    ${MODEL_ARGS[@]} \
    ${TRAINING_ARGS[@]} \
    ${DTYPE_ARGS[@]} \
    ${MODEL_PARALLEL_ARGS[@]} \
    ${DATA_ARGS_LIST[@]} \
    ${EVAL_AND_LOGGING_ARGS[@]}
