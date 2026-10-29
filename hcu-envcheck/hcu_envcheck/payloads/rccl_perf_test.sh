#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# rccl_perf_test.sh —— RCCL 全项集合通信性能测试 + 必需的规模基准比对
#
# 由 hcu-cluster-run <场景> rccl 调用：
#   组首节点运行此脚本；单节点直接执行，多节点通过 MPI 在组内执行。
#   （组 slotsfile 自动作为第一个参数传入；也可手动: ./rccl_perf_test.sh <slotsfile>）
#
# 职责：读取组 slotsfile，核对每节点 slots=C，直接用于 MPI 分配，
#       默认依次跑完整 10 项 rccl-test，由统一 reporting 模块比较性能：
#       >= 基准(可留 RCCL_MARGIN 余量) => PASS；低于 => FAIL 并打印实测/基准。
#       基准须覆盖当前 np 的全部待测项；缺失报错，不跳项、不降级为功能测试。
#
# 基准文件（rccl_baseline.conf；--baseline-file 可指定，默认查找路径见下）：
#   行格式 "<np> <test> <busbw> [margin]"，np = 总卡数 = 节点数 × 每节点卡数。
#   按当前卡数精确取该规模的基准；无对应规模直接报错，不借用其他规模。
#   第 4 列 margin 可选：该项允许低于基准的百分比（覆盖全局 RCCL_MARGIN；
#   波动大的项可单独放宽）。本脚本不内置基准数据。
#
# 通信与测量配置来源（高 -> 低）: env.sh 激活的环境变量 -> cluster_env.conf
# 站点默认（$_SELF_DIR/../cluster_env.conf，CLUSTER_ENV_FILE 可覆盖）-> 脚本内置兜底。
# 公共命令只下发规模、端口、基准及内部执行控制，不透传任意载荷参数。
# 多节点每个 rank 再 source HCU_CLUSTER_ENV_SCRIPT。HCU_TASK_TOKEN 用于任务清理。
# 独立运行保留原命令 --allow-run-as-root；入口在容器 rccl-tests 下设为 1，宿主机场景仍显式控制。
#
# 用法:
#   ./rccl_perf_test.sh <slotsfile> [options]
# options:
#   --port <n>          mpirun ssh 端口(plm_rsh_args -p)，独立运行默认 25901；入口宿主机场景传 22
#   --gpus-per-node <n> 每节点参与卡数，默认 8，不接受 hostfile 卡数覆盖
#   --tests <csv>       worker 内部阶段选择；普通公共调用固定完整十项
#   --baseline-file <f> 基准文件路径（优先级见下）。查找顺序:
#                       RCCL_BASELINE_B64 / RCCL_BASELINE_TEXT 显式环境内容
#                       -> --baseline-file -> $RCCL_BASELINE -> $RCCL_BASELINE_FILE
#                       -> $PWD/rccl_baseline.conf -> 本脚本同目录 -> ../baselines/
#   --log-dir <path>    日志目录，默认 ./rccl_perf_logs/<组名或时间戳>
#   --skip-idle-check   跳过开跑前的本机计算卡空闲检查（默认检查；
#                       兼容已完成预检的调用方设置 CLUSTER_IDLE_CHECKED=1）
#   --prepare-only      仅预检并生成基准计划，不启动测试
#   --plan-file <path>  基准计划 TSV，默认 <log-dir>/rccl-plan.tsv
#   --raw-only          仅执行及保存原始日志；由控制端 Python 判定性能
#   --dry-run           只打印将执行的命令
# 环境变量：RCCL_BIN_DIR、NCCL_TOPO_FILE、NCCL_SOCKET_IFNAME、UCX_NET_DEVICES、
#   RCCL_MARGIN 和 RCCL_ARG_B/E/F/N/W；在目标 --env-script 中 export。
#
# 退出码: 0 = 全部 PASS；1 = 参数/环境错误；2 = 存在 FAIL 项
set -uo pipefail

_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" 2>/dev/null && pwd)"
_REPO_ROOT="$(cd "$_SELF_DIR/../.." 2>/dev/null && pwd)"

# ---------- 站点默认值（可选 cluster_env.conf） ----------
# 通信与测量优先级（高 -> 低）: env.sh 激活的环境变量 > cluster_env.conf
# 站点默认 > 本脚本内置兜底。conf 缺失时静默走内置默认，行为不变。
_SITE_CONF="${CLUSTER_ENV_FILE:-$_REPO_ROOT/cluster_run/cluster_env.conf}"
if [[ -f "$_SITE_CONF" ]]; then
    set +u; source "$_SITE_CONF"; set -u
    printf '>>> 站点配置: %s\n' "$_SITE_CONF" >&2
fi
# 站点附加库（如 RCCL 网络插件目录）：必须无条件前置，未设置的变量也覆盖不到
# 已有值；env.sh 已 export 的 LD_LIBRARY_PATH 在此之前，站点附加仍在最前且不重复。
if [[ -n "${RCCL_EXTRA_LD_PATH:-}" ]]; then
    case ":${LD_LIBRARY_PATH:-}:" in
        *":${RCCL_EXTRA_LD_PATH}:"*) ;;
        *) export LD_LIBRARY_PATH="${RCCL_EXTRA_LD_PATH}:${LD_LIBRARY_PATH:-}" ;;
    esac
fi

# ---------- 颜色 (tty 自动检测) ----------
if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_BLD=$'\033[1m'; C_RST=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_BLD=""; C_RST=""
fi
log() { printf '%s\n' "$*"; }
die() { printf '%s[ERROR]%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

# ---------- 默认值（env.sh 激活的环境 > cluster_env.conf 站点默认 > 命令行最高） ----------
HOSTFILE="${1:-${GROUP_HOSTFILE:-}}"
[[ -n "$HOSTFILE" && "$HOSTFILE" != -* ]] && shift || HOSTFILE="${GROUP_HOSTFILE:-}"
SSH_PORT="${SSH_PORT:-${DEFAULTS_CONTAINER_SSH_PORT:-25901}}"
HCU_ALLOW_ROOT_MPI="${HCU_ALLOW_ROOT_MPI:-1}"
GPUS_PER_NODE=8
TESTS_CSV=""
MARGIN="${RCCL_MARGIN:-1}"
BASELINE_FILE=""
BIN_DIR="${RCCL_BIN_DIR:-${RCCL_TESTS_BIN_DIR:-}}"
# env.sh 设置的实际运行变量（NCCL_*/UCX_NET_DEVICES）优先于 conf 的 RCCL_* 配置名。
TOPO_FILE="${NCCL_TOPO_FILE:-${RCCL_TOPO_FILE:-/usr/local/built-in-508-topo-input-tj-default.xml}}"
IFACE="${NCCL_SOCKET_IFNAME:-${RCCL_IFACE:-eth0}}"
UCX_DEV="${UCX_NET_DEVICES:-${RCCL_UCX_DEV:-ib0}}"
# Site defaults come from the user's working MPI command. env.sh values and
# cluster_env.conf both feed these; no user payload arguments override them.
export NCCL_PXN_DISABLE="${NCCL_PXN_DISABLE:-0}"
export RCCL_PXN_GPU_BALANCE="${RCCL_PXN_GPU_BALANCE:-1}"
export NCCL_NET_PLUGIN="${NCCL_NET_PLUGIN:-shca}"
export NCCL_PLUGIN_P2P="${NCCL_PLUGIN_P2P:-ib}"
export NCCL_NET_GDR_LEVEL="${NCCL_NET_GDR_LEVEL:-4}"
export NCCL_NET_GDR_READ="${NCCL_NET_GDR_READ:-1}"
ARG_B="${RCCL_ARG_B:-4}"; ARG_E="${RCCL_ARG_E:-1G}"; ARG_F="${RCCL_ARG_F:-2}"
ARG_N="${RCCL_ARG_N:-20}"; ARG_W="${RCCL_ARG_W:-5}"
LOG_DIR=""
DRY_RUN=0
SKIP_IDLE=0
PREPARE_ONLY=0
RAW_ONLY=0
PLAN_FILE=""

while (($#)); do
    case "$1" in
        --port)           SSH_PORT="${2:-}"; shift ;;
        --gpus-per-node)  GPUS_PER_NODE="${2:-}"; shift ;;
        --tests)          TESTS_CSV="${2:-}"; shift ;;
        --baseline-file)  BASELINE_FILE="${2:-}"; shift ;;
        --log-dir)        LOG_DIR="${2:-}"; shift ;;
        --skip-idle-check) SKIP_IDLE=1 ;;
        --prepare-only)   PREPARE_ONLY=1 ;;
        --plan-file)      PLAN_FILE="${2:-}"; [[ -n "$PLAN_FILE" ]] || die "--plan-file 缺少路径"; shift ;;
        --raw-only)       RAW_ONLY=1 ;;
        --dry-run)        DRY_RUN=1 ;;
        -h|--help)        sed -n '2,/^set -uo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                die "未知参数: $1（-h 看用法）" ;;
    esac
    shift
done
[[ "$PREPARE_ONLY" != 1 || "$RAW_ONLY" != 1 ]] || die "--prepare-only 与 --raw-only 不能同时使用"

export NCCL_SOCKET_IFNAME="$IFACE" NCCL_TOPO_FILE="$TOPO_FILE" UCX_NET_DEVICES="$UCX_DEV"
NETWORK_VARS=(NCCL_SOCKET_IFNAME NCCL_PXN_DISABLE RCCL_PXN_GPU_BALANCE RCCL_NET_PLANE
              NCCL_NET_PLUGIN NCCL_PLUGIN_P2P NCCL_NET_GDR_LEVEL NCCL_NET_GDR_READ NCCL_TOPO_FILE UCX_NET_DEVICES)

[[ -n "$HOSTFILE" || -n "${NODE_NAME:-}" ]] || die "缺少 hostfile（第一个参数 / GROUP_HOSTFILE），或 -g 1 形态的 NODE_NAME"
if [[ -n "$HOSTFILE" ]]; then
    [[ -f "$HOSTFILE" ]] || die "hostfile 不存在: $HOSTFILE"
fi
[[ "$GPUS_PER_NODE" =~ ^[1-9][0-9]*$ ]] \
    || die "--gpus-per-node 应为正整数，得到: $GPUS_PER_NODE"
[[ "$MARGIN" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "RCCL_MARGIN 应为非负数字，得到: $MARGIN"
[[ "$SSH_PORT" =~ ^[1-9][0-9]*$ && "$SSH_PORT" -le 65535 ]] || die "--port 应为 1~65535"
[[ "${HCU_ALLOW_ROOT_MPI:-0}" =~ ^[01]$ ]] || die "HCU_ALLOW_ROOT_MPI 必须为 0 或 1"
[[ -z "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" || "$HCU_CLUSTER_TIMEOUT_SECONDS" =~ ^[0-9]+(\.[0-9]+)?$ ]] \
    || die "HCU_CLUSTER_TIMEOUT_SECONDS 必须为非负秒数"

# The controller's saved MPI allocation must match the explicit card count C.
declare -a NODES=()
HOSTLIST=""
NP=0
if [[ -z "$HOSTFILE" ]]; then
    NODES+=("${NODE_NAME}")
else
    while read -r host extra || [[ -n ${host:-} ]]; do
        host=${host%$'\r'}
        [[ -n $host && $host != \#* ]] || continue
        [[ $host =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "invalid group member: $host"
        extra=${extra%$'\r'}
        [[ $extra == "slots=$GPUS_PER_NODE" ]] || die "invalid RCCL slotsfile: expected $host slots=$GPUS_PER_NODE"
        for existing in "${NODES[@]}"; do [[ $existing != "$host" ]] || die "duplicate member: $host"; done
        NODES+=("$host")
    done < "$HOSTFILE"
fi
((${#NODES[@]} > 0)) || die "empty group"
for host in "${NODES[@]}"; do HOSTLIST+="${HOSTLIST:+,}${host}:${GPUS_PER_NODE}"; done
NP=$((${#NODES[@]} * GPUS_PER_NODE))
export HCU_GPUS_PER_NODE="$GPUS_PER_NODE"

# Prefer explicit settings and the environment; retain the user's binary fallback.
resolve_test_bin() {
    local name=$1 root candidate
    if [[ -n "$BIN_DIR" ]]; then
        printf '%s' "$BIN_DIR/$name"
        return
    fi
    candidate=$(command -v "$name" 2>/dev/null) || candidate=""
    if [[ -n "$candidate" ]]; then printf '%s' "$candidate"; return; fi
    for root in "${RCCL_TESTS_PATH:-}" "${RCCL_TESTS_HOME:-}" "${RCCL_HOME:-}" "${RCCL_PATH:-}" \
                "${DTK_ROOT:-}" "${DTK_HOME:-}" "${DTK_PATH:-}" "${ROCM_PATH:-}" "${ROCM_HOME:-}"; do
        [[ -n "$root" ]] || continue
        for candidate in "$root/$name" "$root/bin/$name" "$root/build/$name" \
                         "$root/rccl-test/build/$name" "$root/rccl-tests/build/$name"; do
            [[ -x "$candidate" ]] && { printf '%s' "$candidate"; return; }
        done
    done
    # Last fallback preserves the supplied, known-working deployment command.
    # Explicit settings, PATH and selected toolkit roots still have priority.
    printf '/opt/rccl-test/build/%s' "$name"
}
MPI_BIN="${MPIRUN_BIN:-${MPI_BIN:-}}"
if ((${#NODES[@]} > 1)); then
    if [[ -z "$MPI_BIN" ]]; then MPI_BIN=$(command -v mpirun 2>/dev/null) || MPI_BIN=""; fi
    if [[ -z "$MPI_BIN" ]]; then
        for root in "${MPI_HOME:-}" "${MPI_ROOT:-}" "${OPENMPI_HOME:-}" "${OMPI_HOME:-}"; do
            [[ -n "$root" && -x "$root/bin/mpirun" ]] && { MPI_BIN="$root/bin/mpirun"; break; }
        done
    fi
    [[ -n "$MPI_BIN" || "$DRY_RUN" == 1 ]] || die "多节点需要 mpirun；通过 env.sh 的 PATH/MPI_HOME 或 MPIRUN_BIN 配置"
    MPI_BIN="${MPI_BIN:-mpirun}"
    if [[ "$DRY_RUN" != 1 && "$EUID" == 0 && "${HCU_ALLOW_ROOT_MPI:-0}" != 1 ]]; then
        die "root MPI 未授权；应由统一入口自动设置 HCU_ALLOW_ROOT_MPI=1"
    fi
fi
TASK_GUARD_BODY=""
if [[ -n "${HCU_TASK_TOKEN:-}" ]]; then
    [[ "$HCU_TASK_TOKEN" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{15,95}$ ]] || die "HCU_TASK_TOKEN 格式错误"
    if [[ -n ${HCU_TASK_GUARD_FILE:-} ]]; then
        [[ -f $HCU_TASK_GUARD_FILE && ! -L $HCU_TASK_GUARD_FILE && -r $HCU_TASK_GUARD_FILE ]] \
            || die "共享任务清理载荷不可读: $HCU_TASK_GUARD_FILE"
    else
        [[ -r "$_SELF_DIR/task_guard.sh" ]] || die "缺少任务清理载荷: $_SELF_DIR/task_guard.sh"
        TASK_GUARD_BODY=$(< "$_SELF_DIR/task_guard.sh")
    fi
fi

# ---------- 必需基准表：显式环境内容或基准文件，不能缺项 ----------
# 查找顺序: RCCL_BASELINE_B64 / RCCL_BASELINE_TEXT（用户显式配置）
#           > --baseline-file 指定 > $RCCL_BASELINE > $RCCL_BASELINE_FILE
#           > $PWD/rccl_baseline.conf > 本脚本同目录 > ../baselines/（仓库布局）
BASE_SRC=""
np_lines=0
declare -A BASELINE=()
declare -A BASELINE_MARGIN=()   # 按项 margin（基准文件第 4 列, 可选; 缺省用 RCCL_MARGIN）
_load_baseline_from_text() {    # <基准文本>
    local bnp btest bval bmargin
    while read -r bnp btest bval bmargin _ || [[ -n "${bnp:-}" ]]; do
        bnp=${bnp%$'\r'}; btest=${btest%$'\r'}; bval=${bval%$'\r'}; bmargin=${bmargin%$'\r'}
        [[ -n "${bnp:-}" && "$bnp" != \#* ]] || continue
        [[ "$bnp" == "$NP" ]] || continue
        [[ "$bval" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "基准数值不合法: ${bnp} ${btest} ${bval}"
        awk -v v="$bval" 'BEGIN { exit !(v > 0) }' || die "基准值必须大于 0: ${bnp} ${btest} ${bval}"
        [[ -z "${BASELINE[$btest]+x}" ]] || die "基准条目重复: np=${NP} test=${btest}"
        BASELINE[$btest]="$bval"
        if [[ -n "${bmargin:-}" && "$bmargin" != \#* ]]; then
            [[ "$bmargin" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "基准第4列 margin 不合法: ${bnp} ${btest} ${bval} ${bmargin}"
            awk -v m="$bmargin" 'BEGIN { exit !(m < 100) }' || die "margin 必须小于 100: ${btest} ${bmargin}"
            BASELINE_MARGIN[$btest]="$bmargin"
        fi
        np_lines=$((np_lines + 1))
    done
}
_load_baseline_from_file() {    # <基准文件路径>
    local f=$1
    [[ -f "$f" ]] || die "基准文件不存在: $f"
    _load_baseline_from_text < "$f"
}
if [[ -n "${RCCL_BASELINE_B64:-}" ]]; then
    # 可选的 base64 基准内容（单行，无路径依赖，容器内同样可用）。
    if _btext=$(printf '%s' "$RCCL_BASELINE_B64" | base64 -d 2>/dev/null) && [[ -n "$_btext" ]]; then
        _load_baseline_from_text <<< "$_btext"
        BASE_SRC="注入内容(np=${NP})"
    else
        die "RCCL_BASELINE_B64 解码失败（base64 不可用或内容损坏）"
    fi
elif [[ -n "${RCCL_BASELINE_TEXT:-}" ]]; then
    # 兜底: 直接以文本形式注入
    _load_baseline_from_text <<< "$RCCL_BASELINE_TEXT"
    BASE_SRC="注入内容(np=${NP})"
elif [[ -n "$BASELINE_FILE" ]]; then
    _load_baseline_from_file "$BASELINE_FILE"
    BASE_SRC="file:${BASELINE_FILE} (np=${NP})"
elif [[ -n "${RCCL_BASELINE:-${RCCL_BASELINE_FILE:-}}" ]]; then
    BASELINE_FILE="${RCCL_BASELINE:-$RCCL_BASELINE_FILE}"
    _load_baseline_from_file "$BASELINE_FILE"
    BASE_SRC="file:${BASELINE_FILE} (np=${NP})"
else
    for _d in "./rccl_baseline.conf" \
              "$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" 2>/dev/null && pwd)/rccl_baseline.conf" \
              "$_REPO_ROOT/cluster_run/baselines/rccl_baseline.conf"; do
        [[ -n "$_d" && -f "$_d" ]] || continue
        _load_baseline_from_file "$_d"
        BASE_SRC="file:${_d} (np=${NP})"
        break
    done
fi
[[ -n "$BASE_SRC" ]] || die "未找到性能基准；请通过 --baseline-file 或 RCCL_BASELINE_FILE 提供当前 np=${NP} 的完整基准"
((np_lines > 0)) || die "基准中没有 np=${NP} 的条目；不会借用其他规模或降级成功"
base_src="$BASE_SRC"

# ---------- 测试项 ----------
ALL_TESTS=(all_reduce all_gather broadcast reduce reduce_scatter gather scatter alltoall alltoallv sendrecv)
declare -a TESTS=()
if [[ -n "$TESTS_CSV" ]]; then
    IFS=',' read -r -a TESTS <<< "$TESTS_CSV"
    for t in "${TESTS[@]}"; do
        valid=0
        for known in "${ALL_TESTS[@]}"; do [[ "$t" != "$known" ]] || valid=1; done
        ((valid)) || die "未知测试项 ${t}（可选: ${ALL_TESTS[*]}）"
    done
else
    TESTS=("${ALL_TESTS[@]}")
fi
declare -a MISSING_BASELINES=()
declare -A SEEN_TESTS=()
for t in "${TESTS[@]}"; do
    [[ -z "${SEEN_TESTS[$t]+x}" ]] || die "测试项重复: ${t}"
    SEEN_TESTS[$t]=1
    [[ -n "${BASELINE[$t]+x}" ]] || MISSING_BASELINES+=("$t")
done
((${#TESTS[@]} > 0)) || die "测试项不能为空"
((${#MISSING_BASELINES[@]} == 0)) || die "基准缺项: np=${NP} tests=${MISSING_BASELINES[*]}；请补齐后执行，本次没有启动测试，不会静默跳项"
awk -v m="$MARGIN" 'BEGIN { exit !(m < 100) }' || die "RCCL_MARGIN 必须小于 100"

LOG_DIR="${LOG_DIR:-./rccl_perf_logs/${GROUP_NAME:-$(date +%Y%m%d-%H%M%S)}}"
mkdir -p "$LOG_DIR" || die "无法创建日志目录: $LOG_DIR"
LOG_DIR=$(cd "$LOG_DIR" && pwd) || die "无法定位日志目录"
PLAN_FILE="${PLAN_FILE:-$LOG_DIR/rccl-plan.tsv}"
[[ "$LOG_DIR$PLAN_FILE" != *$'\t'* && "$LOG_DIR$PLAN_FILE" != *$'\n'* ]] || die "日志及计划路径不能含制表符或换行"

# 在任何测试启动前检查全部选中项，避免运行中途才发现可执行文件缺失。
if [[ "$DRY_RUN" != 1 ]]; then
    for t in "${TESTS[@]}"; do
        _bin=$(resolve_test_bin "${t}_perf")
        [[ -x "$_bin" ]] || die "RCCL 可执行文件不存在或不可执行: $_bin"
    done
    if ((${#NODES[@]} > 1)); then
        command -v "$MPI_BIN" >/dev/null 2>&1 || die "mpirun 不存在或不可执行: $MPI_BIN"
    fi
fi

# 默认独立调用也使用同一解析器，目标节点 raw-only/prepare-only 无需 Python。
if [[ "$RAW_ONLY" != 1 && "$PREPARE_ONLY" != 1 && "$DRY_RUN" != 1 ]]; then
    REPORT_PYTHON="${HCU_REPORT_PYTHON:-python3}"
    command -v "$REPORT_PYTHON" >/dev/null 2>&1 || die "独立性能判定需要 Python；集群入口应使用 --raw-only"
    REPORT_ROOT=$(cd "$_SELF_DIR/../.." && pwd) || die "无法定位 reporting 模块"
    PYTHONPATH="$REPORT_ROOT${PYTHONPATH:+:$PYTHONPATH}" "$REPORT_PYTHON" -c 'import hcu_envcheck.reporting' \
        || die "无法加载统一 reporting 模块"
fi

# ---------- 本机计算卡空闲检查（VRAM/HCU/KFD） ----------
# 默认执行本机占用检查；显式跳过或调用方标记已完成预检时不重复检查。
if [[ "$SKIP_IDLE" == "0" && "$DRY_RUN" == "0" && "${CLUSTER_IDLE_CHECKED:-0}" != "1" ]]; then
    HSMI="${HY_SMI_BIN:-$(command -v hy-smi 2>/dev/null)}"
    if [[ -z "$HSMI" ]]; then
        log "${C_YEL}[警告]${C_RST} hy-smi 不可用，跳过计算卡空闲检查"
    else
        busy=""
        while read -r hcu _t _p _pf _c vram util _; do
            [[ "$hcu" =~ ^[0-9]+$ ]] || continue
            v="${vram%\%}"; u="${util%\%}"
            over=$(awk -v v="$v" -v u="$u" 'BEGIN { print (v > 5 || u > 5) ? 1 : 0 }')
            [[ "$over" == "1" ]] && busy+="${busy:+; }卡${hcu} VRAM=${vram} HCU=${util}"
        done <<< "$("$HSMI" 2>/dev/null)"
        pids_out=$("$HSMI" --showpids 2>/dev/null)
        if ! grep -qE "No KFD PIDs currently running|[Ee]rror|[Ff]ailed" <<< "$pids_out"; then
            busy+="${busy:+; }存在KFD进程"
        fi
        if [[ -n "$busy" ]]; then
            die "本机计算卡被占用，拒绝开跑（--skip-idle-check 可跳过）: ${busy}"
        fi
        log "${C_GRN}[IDLE]${C_RST} $(hostname): 计算卡空闲检查通过"
    fi
fi

# ---------- 组内共享的 rank 环境和任务清理脚本只生成一次 ----------
prepare_rank_scripts() {
    local name assignment rank_body shared_script_dir guard_path
    source "$_SELF_DIR/../state_runtime.sh" || die 'cannot load state runtime helpers'
    shared_script_dir=${HCU_RCCL_SCRIPT_DIR:-$LOG_DIR}
    guard_path=${HCU_TASK_GUARD_FILE:-$shared_script_dir/task-guard.sh}
    TEST_SCRIPT="$_SELF_DIR/test_rccl.sh"
    [[ -r "$TEST_SCRIPT" ]] || die "缺少 RCCL 启动脚本: $TEST_SCRIPT"

    # 所有 rank 在共享路径上加载自身环境，再恢复包装层选定的通信参数。
    rank_body="$(declare -f resolve_test_bin)"$'\nset -e\n'
    if [[ -n "${HCU_CLUSTER_ENV_SCRIPT:-}" ]]; then
        printf -v assignment 'source %q\n' "$HCU_CLUSTER_ENV_SCRIPT"
        rank_body+="$assignment"
    fi
    for name in HCU_GPUS_PER_NODE HCU_DEVICE_SELECTION_DIR HCU_GROUP_HOSTFILE; do
        [[ -n ${!name:-} ]] || continue
        printf -v assignment 'export %s=%q\n' "$name" "${!name}"
        rank_body+="$assignment"
    done
    for name in "${NETWORK_VARS[@]}"; do
        [[ -n "${!name:-}" ]] || continue
        printf -v assignment 'export %s=%q\n' "$name" "${!name}"
        rank_body+="$assignment"
    done
    rank_body+=$'BIN_DIR="${RCCL_BIN_DIR:-${RCCL_TESTS_BIN_DIR:-}}"\n'
    rank_body+=$'bin=$(resolve_test_bin "$1")\nshift\n'
    rank_body+=$'[[ ${HCU_GPUS_PER_NODE:-} =~ ^[1-9][0-9]*$ && -n ${HCU_DEVICE_SELECTION_DIR:-} && -r ${HCU_GROUP_HOSTFILE:-} ]] || { echo "explicit GPU count/device selection missing" >&2; exit 2; }\n'
    rank_body+=$'rank=${OMPI_COMM_WORLD_RANK:-0}\nnode_index=$((rank / HCU_GPUS_PER_NODE + 1))\n'
    rank_body+=$'node=$(awk -v row="$node_index" \'NF&&$1!~/^#/ {if(++n==row){print $1;exit}}\' "$HCU_GROUP_HOSTFILE")\n'
    rank_body+=$'[[ -n $node ]] || exit 2\npython_bin=; for p in python3 python; do command -v "$p" >/dev/null && { python_bin=$p; break; }; done\n'
    printf -v assignment 'exec "$python_bin" -B %q exec --count "$HCU_GPUS_PER_NODE" --record "$HCU_DEVICE_SELECTION_DIR/$node.json" --node "$node" -- "$bin" "$@"\n' "$_SELF_DIR/device_selection.py"
    rank_body+="$assignment"

    # Immutable snapshots are reused across stages. Changed target settings
    # get a content-specific variant, never overwrite an earlier command.
    RANK_FILE=$(hcu_cache_script "$shared_script_dir/rank-body.sh" "$rank_body") || die 'cannot publish rank script'
    if [[ -n ${HCU_TASK_TOKEN:-} && -n ${HCU_TASK_GUARD_FILE:-} ]]; then
        # The controller published this immutable run guard before dispatch.
        # Explicit shared guards are validated above, never rewritten here.
        GUARD_FILE=$HCU_TASK_GUARD_FILE
    elif [[ -n "$TASK_GUARD_BODY" ]]; then
        GUARD_FILE=$(hcu_cache_script "$guard_path" "$TASK_GUARD_BODY"$'\n') || die 'cannot publish guard script'
    else
        GUARD_FILE=''
    fi
}

# ---------- 保留原 MPI/rank 启动协议，解析与报告由 Python 统一处理 ----------
build_command() {               # <test name> ; 结果写入 MP_CMD
    local test=$1 bin cards=$GPUS_PER_NODE
    ((${#NODES[@]} > 1)) || cards=$NP
    bin=$(resolve_test_bin "$test")

    # NCCL_ENV 显式留空：worker 已在组首 source，rank 脚本在各节点 source；
    # 不再使用 test_rccl.sh 原项目布局下的 Megatron 默认路径。
    MP_CMD=(env NCCL_ENV= RCCL_BIN="$bin" OP="$test"
            GPUS_PER_NODE="$cards" MINBYTES="$ARG_B" MAXBYTES="$ARG_E"
            STEPFACTOR="$ARG_F" ITERS="$ARG_N" WARMUP="$ARG_W" CHECK=
            SSH_PORT="$SSH_PORT" IFNAME="$IFACE" MPIRUN_BIN="$MPI_BIN"
            HCU_ALLOW_ROOT_MPI="$HCU_ALLOW_ROOT_MPI"
            HCU_RCCL_RANK_SCRIPT="$RANK_FILE" HCU_RCCL_GUARD_SCRIPT="$GUARD_FILE"
            bash "$TEST_SCRIPT")
    if ((${#NODES[@]} == 1)); then
        MP_CMD+=(single)
    else
        MP_CMD+=(multi "$HOSTFILE")
    fi
}

# ---------- 预检计划与原始执行记录 ----------
write_plan() {
    local temporary t
    mkdir -p "$(dirname "$PLAN_FILE")" || die "无法创建基准计划目录"
    temporary=$(mktemp "${PLAN_FILE}.tmp.XXXXXX") || die "无法创建临时基准计划"
    {
        printf 'test\tnp\tbaseline_busbw_gbps\tmargin_pct\n'
        for t in "${TESTS[@]}"; do
            printf '%s\t%s\t%s\t%s\n' "$t" "$NP" "${BASELINE[$t]}" "${BASELINE_MARGIN[$t]:-$MARGIN}"
        done
    } > "$temporary" || { rm -f -- "$temporary"; die "无法写入基准计划"; }
    chmod 644 -- "$temporary" || { rm -f -- "$temporary"; die "无法设置基准计划读取权限"; }
    mv -f -- "$temporary" "$PLAN_FILE" || { rm -f -- "$temporary"; die "无法保存基准计划: $PLAN_FILE"; }
}

prepare_rank_scripts
if [[ "$DRY_RUN" != 1 && ( "$RAW_ONLY" != 1 || ${HCU_DIRECT_GROUP_LOG:-0} != 1 ) ]]; then
    write_plan
fi
if [[ "$PREPARE_ONLY" == 1 ]]; then
    if [[ "$DRY_RUN" == 1 ]]; then
        log ">>> RCCL 预检计划（未执行现场检查）: np=$NP tests=${TESTS[*]} plan=$PLAN_FILE"
    else
        log ">>> RCCL 预检通过: np=$NP tests=${TESTS[*]} plan=$PLAN_FILE"
    fi
    exit 0
fi

if [[ ${HCU_DIRECT_GROUP_LOG:-0} != 1 ]]; then
    log "RCCL 原始测试 ($(date '+%F %T'))"
    log ">>> 节点: $HOSTLIST (np=$NP)"
    log ">>> 基准: $base_src"
    log ">>> 测试项: ${TESTS[*]}"
    log ">>> 日志目录: $LOG_DIR"
fi
FAILED=0
if [[ "$DRY_RUN" != 1 ]]; then
    printf 'test\texit_code\telapsed_seconds\tlog\n' > "$LOG_DIR/execution.tsv" \
        || die "无法创建 RCCL 执行记录"
fi
idx=0
for t in "${TESTS[@]}"; do
    idx=$((idx + 1))
    build_command "${t}_perf"
    # argv 安全转义，保留完整 rank 环境及 guard 启动命令，便于复现。
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} != 1 ]]; then
        { printf '#!/usr/bin/env bash\nexec '; printf '%q ' "${MP_CMD[@]}"; printf '\n'; } > "$LOG_DIR/${t}.command.sh" \
            || die "无法写入命令记录: $LOG_DIR/${t}.command.sh"
    fi
    if [[ "$DRY_RUN" == 1 ]]; then
        printf '[%d/%d] %s\n    ' "$idx" "${#TESTS[@]}" "$t"
        printf '%q ' "${MP_CMD[@]}"
        printf '\n'
        continue
    fi
    tlog="$LOG_DIR/${t}.log"
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} != 1 ]]; then
        printf '\n%s\n[TEST] item=%s\n' '--------------------------------------------------------------------------------' "$t"
    fi
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} != 1 ]]; then
        printf '[COMMAND] node=%s command=' "${HOSTLIST%%,*}"
        printf '%q ' "${MP_CMD[@]}"
        printf '\n'
    fi
    [[ ${HCU_DIRECT_GROUP_LOG:-0} == 1 ]] || printf '[%d/%d] %s start\n' "$idx" "${#TESTS[@]}" "$t"
    t0=$(date +%s)
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} == 1 ]]; then
        "${MP_CMD[@]}" 2>&1 </dev/null
        rc=$?
    else
        "${MP_CMD[@]}" > "$tlog" 2>&1 </dev/null
        rc=$?
    fi
    dt=$(( $(date +%s) - t0 ))
    if [[ ${HCU_DIRECT_GROUP_LOG:-0} == 1 ]]; then log_cell=''; else log_cell=$tlog; fi
    printf '%s\t%s\t%s\t%s\n' "$t" "$rc" "$dt" "$log_cell" >> "$LOG_DIR/execution.tsv" \
        || die "无法写入 RCCL 执行记录"
    [[ ${HCU_DIRECT_GROUP_LOG:-0} == 1 ]] || printf '[%d/%d] %s end rc=%s elapsed=%ss\n' "$idx" "${#TESTS[@]}" "$t" "$rc" "$dt"
    if ((rc != 0)); then
        FAILED=1
        [[ "$rc" != 130 && "$rc" != 143 ]] || exit "$rc"
    fi
done
[[ "$DRY_RUN" != 1 ]] || exit 0
if [[ "$RAW_ONLY" == 1 ]]; then
    ((FAILED == 0)) || exit 2
    exit 0
fi
# 独立调用保留原性能验收语义；所有指标解析和报告只有 Python 一份实现。
PYTHONPATH="$REPORT_ROOT${PYTHONPATH:+:$PYTHONPATH}" "$REPORT_PYTHON" -m hcu_envcheck.reporting \
    rccl-local --log-dir "$LOG_DIR" --plan-file "$PLAN_FILE"
exit $?
