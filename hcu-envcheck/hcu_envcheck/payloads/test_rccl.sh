#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# rccl-tests 单测示例
#
# 单机单进程（1 进程驱动 N 卡，走 -g）:
#     bash test_rccl.sh single
#     GPUS_PER_NODE=4 MAXBYTES=8g bash test_rccl.sh single
#
# 多机多进程（必须使用编译了 MPI 的 rccl-tests，每卡 1 进程，不带 -g）:
#     # 使用 controller 生成的 slotsfile，每行 node slots=N，N 等于 GPUS_PER_NODE
#     bash test_rccl.sh multi ./slotsfile
#     SSH_PORT=11451 bash test_rccl.sh multi ./slotsfile
#     OP=all_gather_perf bash test_rccl.sh multi ./slotsfile
#
# 常用环境变量:
#     RCCL_TESTS   rccl-tests 的 build 目录，默认 /opt/rccl-test/build
#     OP           测试算子，默认 all_reduce_perf
#     GPUS_PER_NODE 每节点卡数，默认 8
#     IFNAME       指定可用网卡（-x NCCL_SOCKET_IFNAME），默认取 env.sh 里的值
#     SSH_PORT     mpirun 拉起远端进程用的 ssh 端口（--mca plm_rsh_args '-p N'），
#                  容器内 sshd 常不是 22，跨机必须按实际端口指定；留空则用默认 22
#     DTK_ENV      dtk 的 env.sh 路径，留空则不 source

set -o pipefail

CURRENT_DIR=$( cd "$( dirname "$0" )" && pwd )
source "$CURRENT_DIR/../logging.sh"
MEGATRON_PATH=$( dirname $( dirname ${CURRENT_DIR}))

# Those variables need to modify
RCCL_TESTS=${RCCL_TESTS:-/opt/rccl-test/build}                # rccl-tests 编译产物目录
DTK_ENV=${DTK_ENV:-""}                                        # where env.sh of dtk
if [ -z "${NCCL_ENV+x}" ] && [ ! -f "${MEGATRON_PATH}/requirements/env.sh" ]; then NCCL_ENV=""; fi
NCCL_ENV=${NCCL_ENV-${MEGATRON_PATH}/requirements/env.sh}     # 显式空值表示沿用已加载环境

# Those variables no need to modify
OP=${OP:-all_reduce_perf}       # all_reduce_perf / all_gather_perf / reduce_scatter_perf / alltoall_perf ...
GPUS_PER_NODE=${GPUS_PER_NODE:-8}
MINBYTES=${MINBYTES:-8}         # -b
MAXBYTES=${MAXBYTES:-4g}        # -e
STEPFACTOR=${STEPFACTOR:-2}     # -f
ITERS=${ITERS:-20}              # -n
WARMUP=${WARMUP:-5}             # -w
CHECK=${CHECK-1}                # 显式空值表示沿用 rccl-tests 默认值
SSH_PORT=${SSH_PORT:-""}        # mpirun 拉起远端进程的 ssh 端口，留空用默认 22

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

MODE=${1:-single}
shift 2>/dev/null

log_info() {
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} == 1 ]]; then
        [[ $* == 'rccl test done'* ]] || return 0
        printf '%s\n' "$*"
    else echo -e "${GREEN}[INFO ]${NC} $*"; fi
}
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

prepare_env() {
    if [ -n "${DTK_ENV}" ]; then
        log_info "source ${DTK_ENV}"
        source "${DTK_ENV}" || return
    fi
    if [ -n "${NCCL_ENV}" ]; then
        if [ ! -f "${NCCL_ENV}" ]; then
            log_error "NCCL_ENV not found: ${NCCL_ENV}"
            exit 1
        fi
        log_info "source ${NCCL_ENV}"
        source "${NCCL_ENV}" || return
    fi
    # 允许命令行覆盖 env.sh 里的网卡
    if [ -n "${IFNAME}" ]; then
        export NCCL_SOCKET_IFNAME=${IFNAME}
        export GLOO_SOCKET_IFNAME=${IFNAME}
    fi
    log_info "NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME}  NCCL_IB_HCA=${NCCL_IB_HCA}"

    BIN=${RCCL_BIN:-${RCCL_TESTS}/${OP}}
    if [ ! -x "${BIN}" ]; then
        log_error "rccl-tests binary not found: ${BIN}"
        log_error "请确认 RCCL_TESTS=${RCCL_TESTS} 指向 rccl-tests 的 build 目录（多机需带 MPI 编译: make MPI=1 MPI_HOME=... HIP_HOME=...）"
        exit 1
    fi
}

# 工程包装层可传入共享的 rank 环境脚本与任务 guard；独立运行不受影响。
set_rank_command() {
    RANK_CMD=("${BIN}")
    if [ -n "${HCU_RCCL_RANK_SCRIPT:-}" ]; then
        RANK_CMD=(bash "${HCU_RCCL_RANK_SCRIPT}" "${OP}")
    fi
    if [ -n "${HCU_RCCL_GUARD_SCRIPT:-}" ]; then
        RANK_CMD=(env -u BASH_ENV -u ENV bash --noprofile --norc
                  "${HCU_RCCL_GUARD_SCRIPT}" run "${HCU_TASK_TOKEN}" -- "${RANK_CMD[@]}")
    fi
    if [ -n "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" ] && [ "${HCU_CLUSTER_TIMEOUT_SECONDS}" != 0 ]; then
        RANK_CMD=(timeout --signal=TERM --kill-after=5s
                  "${HCU_CLUSTER_TIMEOUT_SECONDS}s" "${RANK_CMD[@]}")
    fi
    CHECK_ARGS=()
    [ -z "${CHECK}" ] || CHECK_ARGS=(-c "${CHECK}")
}

# 单机单进程：./all_reduce_perf -b 8 -e 1g -f 2 -g 4
run_single() {
    local ngpus=${GPUS_PER_NODE}
    # 支持 test_rccl.sh single -g 4 这样直接透传参数
    log_info "单机单进程: ${BIN} -b ${MINBYTES} -e ${MAXBYTES} -f ${STEPFACTOR} -g ${ngpus} $*"
    local -a launch=("${RANK_CMD[@]}" -b "${MINBYTES}" \
           -e ${MAXBYTES} \
           -f ${STEPFACTOR} \
           -g ${ngpus} \
           -n ${ITERS} \
           -w ${WARMUP} \
           "${CHECK_ARGS[@]}" \
           "$@")
    hcu_log_command TEST-COMMAND "$(hostname -s)" "$BIN" -b "$MINBYTES" -e "$MAXBYTES" -f "$STEPFACTOR" -g "$ngpus" -n "$ITERS" -w "$WARMUP" "${CHECK_ARGS[@]}" "$@"
    hcu_log_raw '单机测试程序输出'
    "${launch[@]}"
}

# 多机多进程：mpirun --allow-run-as-root -np 16 --hostfile <file> -x LD_LIBRARY_PATH -x ROCM_PATH ./all_reduce_perf -b 8 -e 1g -f 2
run_multi() {
    local slotsfile=$1
    shift 2>/dev/null

    if [ -z "${slotsfile}" ] || [ ! -f "${slotsfile}" ]; then
        log_error "用法: bash test_rccl.sh multi <slotsfile>   # 每行 node slots=N，N 等于 GPUS_PER_NODE"
        exit 1
    fi

    # 直接使用持久化的 slotsfile，不生成或删除临时 MPI hostfile。
    # 注意: Open MPI 5 (PRRTE) 下 "-H <ip>:8" 不被接受（IP 形式解析不到本地节点，
    # 即使 -np 1 也会报 "not enough slots"），必须用 --hostfile + slots=N。
    local nodenum np
    if [[ ! ${GPUS_PER_NODE} =~ ^[1-9][0-9]*$ ]]; then
        log_error "GPUS_PER_NODE 必须为正整数"
        return 1
    fi
    if ! nodenum=$(awk -v slots="slots=${GPUS_PER_NODE}" 'NF && $1 !~ /^#/ {
        if (NF != 2 || $1 !~ /^[A-Za-z0-9][A-Za-z0-9_.-]*$/ || $2 != slots || seen[$1]++) exit 1
        count++
    } END {if (!count) exit 1; print count}' "${slotsfile}"); then
        log_error "slotsfile 无效，要求节点不重复且每行 slots=${GPUS_PER_NODE}: ${slotsfile}"
        return 1
    fi
    np=$((nodenum * GPUS_PER_NODE))

    log_info "多机多进程: ${nodenum} 节点 / ${np} 进程 (每卡 1 进程，不带 -g)"
    log_info "slotsfile: ${slotsfile}"
    log_info "hosts: $(paste -sd' ' "${slotsfile}")"

    # mpirun 默认走 22 端口 ssh 到远端节点拉起进程。容器里的 sshd 常映射到别的
    # 端口，此时必须用 plm_rsh_args 传 -p，否则跨机会卡在连接超时/被拒。
    local rsh_args=()
    if [ -n "${SSH_PORT}" ]; then
        rsh_args=(--mca plm_rsh_args "-p ${SSH_PORT}")
        log_info "ssh port: ${SSH_PORT}"
    fi

    local root_args=() hcu_env_args=() name
    [ "${HCU_ALLOW_ROOT_MPI:-1}" != 1 ] || root_args=(--allow-run-as-root)
    if [ -n "${HCU_RCCL_RANK_SCRIPT:-}" ]; then
        for name in HCU_GPUS_PER_NODE HCU_DEVICE_SELECTION_DIR HCU_GROUP_HOSTFILE NCCL_TOPO_FILE UCX_NET_DEVICES NCCL_PXN_DISABLE RCCL_PXN_GPU_BALANCE \
                    RCCL_NET_PLANE NCCL_NET_PLUGIN NCCL_PLUGIN_P2P ROCM_HOME DTK_ROOT \
                    DTK_HOME DTK_PATH RCCL_BIN_DIR RCCL_TESTS_BIN_DIR RCCL_TESTS_PATH \
                    RCCL_TESTS_HOME RCCL_HOME RCCL_PATH; do
            [ -z "${!name:-}" ] || hcu_env_args+=(-x "${name}=${!name}")
        done
    fi
    local -a launch=("${MPIRUN_BIN:-mpirun}" "${root_args[@]}" \
           -np ${np} \
           --hostfile "${slotsfile}" \
           --bind-to none \
           --map-by slot --rank-by slot \
           --mca plm_rsh_no_tree_spawn 1 \
           "${rsh_args[@]}" \
           -x LD_LIBRARY_PATH \
           -x ROCM_PATH \
           -x PATH \
           -x NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME} \
           -x NCCL_IB_HCA \
           -x NCCL_ALGO \
           -x NCCL_NET_GDR_LEVEL \
           -x NCCL_NET_GDR_READ \
           -x RCCL_SDMA_COPY_ENABLE \
           -x NCCL_DEBUG=${NCCL_DEBUG:-WARN} \
           "${hcu_env_args[@]}" \
           "${RANK_CMD[@]}" -b "${MINBYTES}" \
                  -e ${MAXBYTES} \
                  -f ${STEPFACTOR} \
                  -n ${ITERS} \
                  -w ${WARMUP} \
                  "${CHECK_ARGS[@]}" \
                  "$@")
    hcu_log_command TEST-COMMAND "$(hostname -s)" "$BIN" -b "$MINBYTES" -e "$MAXBYTES" -f "$STEPFACTOR" -n "$ITERS" -w "$WARMUP" "${CHECK_ARGS[@]}" "$@"
    hcu_log_command MPI-COMMAND "$(hostname -s)" "${launch[@]}"
    hcu_log_raw 'MPI 汇集各节点输出'
    "${launch[@]}"
    local ret=$?
    return ${ret}
}

prepare_env || exit $?
set_rank_command

case "${MODE}" in
    single) run_single "$@" ;;
    multi)  run_multi  "$@" ;;
    *)
        log_error "未知模式: ${MODE}"
        echo "用法: bash test_rccl.sh {single|multi <slotsfile>} [额外的 rccl-tests 参数]"
        exit 1
        ;;
esac

ret=$?
if [ ${ret} -eq 0 ]; then
    log_info "rccl test done, 关注输出末尾的 'Avg bus bandwidth' 与 '#wrong' 是否为 0"
else
    log_error "rccl test failed, exit code=${ret}"
fi
exit ${ret}
