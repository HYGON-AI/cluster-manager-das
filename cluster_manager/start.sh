#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

WORK_DIR=$(pwd)
LOG_DIR="/path/to/logs"

ENABLE_HW_CHECK=False
ENABLE_NHC_FAULT_HANDLE=False
export FEISHU_WEBHOOK_URL=""

export WORK_DIR="${WORK_DIR:-/opt/workspace}"
export LOG_DIR="${LOG_DIR:-${WORK_DIR}/hcu_megatron/examples/aibenchmark}"
export MPI_LAUNCH_TIMEOUT="${MPI_LAUNCH_TIMEOUT:-300}"
export MAX_RESTART_TIMES="${MAX_RESTART_TIMES:-3}"
export TRAIN_ALERT_THRESHOLD="${TRAIN_ALERT_THRESHOLD:-20000}"
export TRAIN_NO_UPDATE_THRESHOLD="${TRAIN_NO_UPDATE_THRESHOLD:-600}"
export LOG_PARSER_TYPE="${LOG_PARSER_TYPE:-base}"
export STARTUP_NO_LOG_TIMEOUT_SEC=3600
export SBATCH_SCRIPT="${SBATCH_SCRIPT:-${WORK_DIR}/sbatch.sh}"
export ENABLE_HW_CHECK="${ENABLE_HW_CHECK:-true}"
export INTERVAL_MONITOR=180
export ENABLE_NHC_FAULT_HANDLE="${ENABLE_NHC_FAULT_HANDLE:-true}"
export CLUSTER_LAUNCH_MODE="${CLUSTER_LAUNCH_MODE:-mpi}"
export CLUSTER_SCHEDULE="${CLUSTER_SCHEDULE:-NONE}"

TRIAN_PATH="/path/to/train.sh"
export MEGATRON_SCRIPT_PATH="${TRIAN_PATH}"
NODES_NUM=1024
SLOTS=8
RUN_PATH="/path/to/run.sh"

HOSTFILE="/path/to/hostfile"
JOB_ID="${JOB_ID:-}"
JOB_NAME="${JOB_NAME:-}"

source /path/to/conda.sh
conda activate conda_env_name


nohup python /path/to/hcu_cluster_manager/cluster_manager/main.py \
  --nodes_num "${NODES_NUM}" \
  --slots "${SLOTS}" \
  --exec "${RUN_PATH}" \
  --hostfile "${HOSTFILE}" \
  --job_id "${JOB_ID}" \
  --job_name "${JOB_NAME}" \
  --sbatch_script "${SBATCH_SCRIPT}" >> /path/to/logs/cluster0506.log 2>&1 &
