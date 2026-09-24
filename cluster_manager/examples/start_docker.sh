#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
LOG_DIR="${LOG_DIR:-${WORK_DIR}/logs}"

export WORK_DIR
export LOG_DIR
export CLUSTER_LAUNCH_MODE="${CLUSTER_LAUNCH_MODE:-docker}"
export CLUSTER_SCHEDULE=NONE
export LOG_PARSER_TYPE="${LOG_PARSER_TYPE:-base}"
export MPI_LAUNCH_TIMEOUT="${MPI_LAUNCH_TIMEOUT:-300}"
export MPI_HOST_SSH_PORT="${MPI_HOST_SSH_PORT:-22}"
export DOCKER_CONTAINER_SSH_PORT="${MPIRUN_PLM_RSH_ARGS:-36000}"
export MPIRUN_ALLOW_RUN_AS_ROOT="${MPIRUN_ALLOW_RUN_AS_ROOT:-1}"

export DOCKER_CONTAINER_NAME="${DOCKER_CONTAINER_NAME:-cluster-manager}"
export DOCKER_IMAGE="${DOCKER_IMAGE:?Set DOCKER_IMAGE to the training image}"
export DOCKER_HOST_SHARE_ROOT="${DOCKER_HOST_SHARE_ROOT:-}"
export DOCKER_CONTAINER_SHARE_ROOT="${DOCKER_CONTAINER_SHARE_ROOT:-}"
export DOCKER_REUSE_CONTAINER="${DOCKER_REUSE_CONTAINER:-1}"
export DOCKER_REMOVE_CONTAINER_ON_STOP="${DOCKER_REMOVE_CONTAINER_ON_STOP:-1}"
export MPI_FORWARD_ENV="${MPI_FORWARD_ENV:-LD_LIBRARY_PATH}"
export PYTHON_BIN="${PYTHON_BIN:-python3}"

export MEGATRON_SCRIPT_PATH="${MEGATRON_SCRIPT_PATH:-}"
SLOTS="${SLOTS:-8}"
RUN_PATH="${RUN_PATH:-${DOCKER_EXEC_PATH:-/workspace/train/run.sh}}"
HOSTFILE="${HOSTFILE:?Set HOSTFILE to the training hostfile}"
if [[ ! -r "${HOSTFILE}" ]]; then
    echo "Hostfile not found or unreadable: ${HOSTFILE}" >&2
    exit 2
fi
# Manually set the requested training node count; do not infer it from HOSTFILE.
NODES_NUM="${NODES_NUM:-1}"

mkdir -p "${LOG_DIR}"
cd "${SCRIPT_DIR}/.." || exit 1
CLUSTER_MANAGER_MAIN="${CLUSTER_MANAGER_MAIN:-${SCRIPT_DIR}/../cluster_manager/main.py}"
if [[ ! -f "${CLUSTER_MANAGER_MAIN}" ]]; then
    echo "Cluster manager entrypoint not found: ${CLUSTER_MANAGER_MAIN}" >&2
    exit 2
fi
nohup "${PYTHON_BIN}" "${CLUSTER_MANAGER_MAIN}" \
  --nodes_num "${NODES_NUM}" \
  --slots "${SLOTS}" \
  --exec "${RUN_PATH}" \
  --hostfile "${HOSTFILE}" >> "${LOG_DIR}/cluster_manager.log" 2>&1 &
