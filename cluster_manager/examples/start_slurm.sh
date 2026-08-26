#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

# Slurm allocation + mpirun example.
export WORK_DIR="${WORK_DIR:-$(pwd)}"
export LOG_DIR="${LOG_DIR:-/path/to/logs}"
export CLUSTER_LAUNCH_MODE=mpi
export CLUSTER_SCHEDULE=SLURM
export LOG_PARSER_TYPE="${LOG_PARSER_TYPE:-base}"

export MEGATRON_SCRIPT_PATH="/path/to/train.sh"
NODES_NUM=4
SLOTS=8
RUN_PATH="/path/to/run.sh"
HOSTFILE="/path/to/hostfile"
JOB_ID="1234"
JOB_NAME="slurm_example"
SBATCH_SCRIPT="/path/to/sbatch.sh"

python /path/to/hcu_cluster_manager/cluster_manager/cluster_manager/main.py \
  --nodes_num "${NODES_NUM}" \
  --slots "${SLOTS}" \
  --exec "${RUN_PATH}" \
  --hostfile "${HOSTFILE}" \
  --job_id "${JOB_ID}" \
  --job_name "${JOB_NAME}" \
  --sbatch_script "${SBATCH_SCRIPT}"
