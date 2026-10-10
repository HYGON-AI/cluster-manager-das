#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# rccl_perf_test.sh —— RCCL 全项集合通信性能测试 + 必需的规模基准比对
#
# 由 hcu-cluster-run <场景> rccl 调用（默认 profile=rccl-tests）：
#   组首节点运行此脚本；单节点直接执行，多节点通过 MPI 在组内执行。
#   （组 hostfile 自动作为第一个参数传入；也可手动: ./rccl_perf_test.sh <hostfile>）
#
# 职责：读 hostfile（每行: 节点 [slots=N]，slots 默认 8），拼 mpirun --host 列表，
#       默认依次跑完整 10 项 rccl-test，解析 busbw 峰值并比较性能：
#       >= 基准(可留 --margin 余量) => PASS；低于 => FAIL 并打印实测/基准。
#       基准须覆盖当前 np 的全部待测项；缺失报错，不跳项、不降级为功能测试。
#
# 基准文件（rccl_baseline.conf，与总入口同目录；--baseline-file 可指定）：
#   行格式 "<np> <test> <busbw> [margin]"，np = 总卡数 = 节点数 × slots。
#   按当前卡数精确取该规模的基准；无对应规模直接报错，不借用其他规模。
#   第 4 列 margin 可选：该项允许低于基准的百分比（覆盖全局 --margin；
#   GEMM/波动大的项可单独放宽）。本脚本不内置基准数据。
#
# 配置来源（高 -> 低）: 命令行参数 -> env.sh 激活的环境变量 -> cluster_env.conf
# 站点默认（$_SELF_DIR/../cluster_env.conf，CLUSTER_ENV_FILE 可覆盖）-> 脚本内置兜底。
# 多节点每个 rank 再 source HCU_CLUSTER_ENV_SCRIPT。HCU_TASK_TOKEN 用于任务清理。
# 独立运行保留原命令 --allow-run-as-root；入口在容器 rccl-tests 下设为 1，宿主机场景仍显式控制。
#
# 用法:
#   ./rccl_perf_test.sh <hostfile> [options]
# options:
#   --port <n>          mpirun ssh 端口(plm_rsh_args -p)，独立运行默认 25901；入口宿主机场景传 22
#   --default-slots <n> hostfile 未写 slots 时的默认值，默认取配置 DEFAULTS_SLOTS（8）
#   --tests <csv>       只跑指定项，如 all_reduce,broadcast（默认全部 10 项）
#   --margin <pct>      允许低于基准的百分比，默认取配置 RCCL_MARGIN（1）；
#                       基准文件第 4 列可按项覆盖
#   --baseline-file <f> 基准文件路径（优先级见下）。查找顺序:
#                       RCCL_BASELINE_B64 / RCCL_BASELINE_TEXT 显式环境内容
#                       -> --baseline-file -> $RCCL_BASELINE -> $RCCL_BASELINE_FILE
#                       -> $PWD/rccl_baseline.conf -> 本脚本同目录 -> ../baselines/
#   --bin-dir <path>    rccl-test 可执行目录，优先 RCCL_BIN_DIR，其次 PATH/环境根目录
#   --topo <file>       NCCL_TOPO_FILE，env.sh 未设置时用 /usr/local/built-in-508-topo-input-tj-default.xml
#   --iface <name>      NCCL_SOCKET_IFNAME，env.sh 未设置时用 eth0
#   --ucx <dev>         UCX_NET_DEVICES，env.sh 未设置时用 ib0
#   -b/-e/-f/-n/-w      透传给 *_perf（默认取配置 RCCL_ARG_*）
#   --log-dir <path>    日志目录，默认 ./rccl_perf_logs/<组名或时间戳>
#   --skip-idle-check   跳过开跑前的本机计算卡空闲检查（独立运行本脚本时默认检查；
#                       经 hcu-cluster-run 调起时其占用检查阶段已查过，
#                       本脚本会自动跳过, 无需传本参数）
#   --dry-run           只打印将执行的命令
#
# 退出码: 0 = 全部 PASS；1 = 参数/环境错误；2 = 存在 FAIL 项
set -uo pipefail

_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" 2>/dev/null && pwd)"

# ---------- 站点默认值（可选 cluster_env.conf） ----------
# 优先级（高 -> 低）: 显式命令行参数 > env.sh 激活的环境变量 > cluster_env.conf
# 站点默认 > 本脚本内置兜底。conf 缺失时静默走内置默认，行为不变。
_SITE_CONF="${CLUSTER_ENV_FILE:-$_SELF_DIR/../cluster_env.conf}"
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
DEFAULT_SLOTS="${DEFAULTS_SLOTS:-8}"
MAX_SLOTS="${DEFAULTS_MAX_SLOTS:-8}"
TESTS_CSV=""
MARGIN="${RCCL_MARGIN:-1}"
BASELINE_FILE=""
BIN_DIR="${RCCL_BIN_DIR:-${RCCL_TESTS_BIN_DIR:-}}"
BIN_DIR_ARG=""; TOPO_ARG=""; IFACE_ARG=""; UCX_ARG=""
# env.sh 设置的实际运行变量（NCCL_*/UCX_NET_DEVICES）优先于 conf 的 RCCL_* 配置名。
TOPO_FILE="${NCCL_TOPO_FILE:-${RCCL_TOPO_FILE:-/usr/local/built-in-508-topo-input-tj-default.xml}}"
IFACE="${NCCL_SOCKET_IFNAME:-${RCCL_IFACE:-eth0}}"
UCX_DEV="${UCX_NET_DEVICES:-${RCCL_UCX_DEV:-ib0}}"
# Site defaults come from the user's working MPI command. env.sh values and
# cluster_env.conf both feed these; explicit --iface/--topo/--ucx override both.
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

while (($#)); do
    case "$1" in
        --port)           SSH_PORT="${2:-}"; shift ;;
        --default-slots)  DEFAULT_SLOTS="${2:-}"; shift ;;
        --tests)          TESTS_CSV="${2:-}"; shift ;;
        --margin)         MARGIN="${2:-}"; shift ;;
        --baseline-file)  BASELINE_FILE="${2:-}"; shift ;;
        --bin-dir)        BIN_DIR="${2:-}"; BIN_DIR_ARG="$BIN_DIR"; shift ;;
        --topo)           TOPO_FILE="${2:-}"; TOPO_ARG="$TOPO_FILE"; shift ;;
        --iface)          IFACE="${2:-}"; IFACE_ARG="$IFACE"; shift ;;
        --ucx)            UCX_DEV="${2:-}"; UCX_ARG="$UCX_DEV"; shift ;;
        -b)               ARG_B="${2:-}"; shift ;;
        -e)               ARG_E="${2:-}"; shift ;;
        -f)               ARG_F="${2:-}"; shift ;;
        -n)               ARG_N="${2:-}"; shift ;;
        -w)               ARG_W="${2:-}"; shift ;;
        --log-dir)        LOG_DIR="${2:-}"; shift ;;
        --skip-idle-check) SKIP_IDLE=1 ;;
        --dry-run)        DRY_RUN=1 ;;
        -h|--help)        sed -n '2,/^set -uo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                die "未知参数: $1（-h 看用法）" ;;
    esac
    shift
done

export NCCL_SOCKET_IFNAME="$IFACE" NCCL_TOPO_FILE="$TOPO_FILE" UCX_NET_DEVICES="$UCX_DEV"
NETWORK_VARS=(NCCL_SOCKET_IFNAME NCCL_PXN_DISABLE RCCL_PXN_GPU_BALANCE RCCL_NET_PLANE
              NCCL_NET_PLUGIN NCCL_PLUGIN_P2P NCCL_NET_GDR_LEVEL NCCL_NET_GDR_READ NCCL_TOPO_FILE UCX_NET_DEVICES)

[[ -n "$HOSTFILE" || -n "${NODE_NAME:-}" ]] || die "缺少 hostfile（第一个参数 / GROUP_HOSTFILE），或 -g 1 形态的 NODE_NAME"
if [[ -n "$HOSTFILE" ]]; then
    [[ -f "$HOSTFILE" ]] || die "hostfile 不存在: $HOSTFILE"
fi
[[ "$DEFAULT_SLOTS" =~ ^[1-9][0-9]*$ && "$DEFAULT_SLOTS" -le "$MAX_SLOTS" ]] \
    || die "--default-slots 应为 1~${MAX_SLOTS}，得到: $DEFAULT_SLOTS"
[[ "$MARGIN" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "--margin 应为非负数字，得到: $MARGIN"
[[ "$SSH_PORT" =~ ^[1-9][0-9]*$ && "$SSH_PORT" -le 65535 ]] || die "--port 应为 1~65535"
[[ "${HCU_ALLOW_ROOT_MPI:-0}" =~ ^[01]$ ]] || die "HCU_ALLOW_ROOT_MPI 必须为 0 或 1"
[[ -z "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" || "$HCU_CLUSTER_TIMEOUT_SECONDS" =~ ^[0-9]+(\.[0-9]+)?$ ]] \
    || die "HCU_CLUSTER_TIMEOUT_SECONDS 必须为非负秒数"

# ---------- 解析 hostfile: 节点 [slots=N]；无 hostfile 时用 NODE_NAME 单机 ----------
declare -a NODES=() SLOTS=()
HOSTLIST=""
NP=0
lineno=0
if [[ -z "$HOSTFILE" ]]; then
    # 独立运行时只测 NODE_NAME 对应本节点，slots 取默认值。
    NODES+=("${NODE_NAME}"); SLOTS+=("$DEFAULT_SLOTS")
    HOSTLIST="${NODE_NAME}:${DEFAULT_SLOTS}"
    NP=$DEFAULT_SLOTS
else
while read -r host tok _ || [[ -n "${host:-}" ]]; do
    host=${host%$'\r'}; tok=${tok%$'\r'}
    lineno=$((lineno + 1))
    [[ -n "${host:-}" ]] || continue
    [[ "$host" == \#* ]] && continue
    [[ "$host" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "hostfile 节点名不合法: $host"
    for existing in "${NODES[@]}"; do [[ "$existing" != "$host" ]] || die "hostfile 节点重复: $host"; done
    s=$DEFAULT_SLOTS
    if [[ -n "${tok:-}" && "$tok" != \#* ]]; then
        if [[ "$tok" =~ ^slots=([0-9]+)$ ]]; then s="${BASH_REMATCH[1]}"
        elif [[ "$tok" =~ ^[0-9]+$ ]]; then s="$tok"
        else die "hostfile 第 ${lineno} 行格式不正确: $host $tok"; fi
    fi
    if [[ "$s" == "0" ]]; then
        log "${C_YEL}[跳过]${C_RST} ${host}: slots=0"
        continue
    fi
    [[ "$s" =~ ^[1-9][0-9]*$ && "$s" -le "$MAX_SLOTS" ]] || die "${host}: slots 应为 1~${MAX_SLOTS}"
    NODES+=("$host"); SLOTS+=("$s")
    HOSTLIST+="${HOSTLIST:+,}${host}:${s}"
    NP=$((NP + s))
done < "$HOSTFILE"
fi
((${#NODES[@]} > 0)) || die "hostfile 中没有可用节点"

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
    [[ -r "$_SELF_DIR/task_guard.sh" ]] || die "缺少任务清理载荷: $_SELF_DIR/task_guard.sh"
    TASK_GUARD_BODY=$(< "$_SELF_DIR/task_guard.sh")
fi

# ---------- 必需基准表：显式环境内容或基准文件，不能缺项 ----------
# 查找顺序: RCCL_BASELINE_B64 / RCCL_BASELINE_TEXT（用户显式配置）
#           > --baseline-file 指定 > $RCCL_BASELINE > $RCCL_BASELINE_FILE
#           > $PWD/rccl_baseline.conf > 本脚本同目录 > ../baselines/（仓库布局）
BASE_SRC=""
np_lines=0
declare -A BASELINE=()
declare -A BASELINE_MARGIN=()   # 按项 margin（基准文件第 4 列, 可选; 缺省用全局 --margin）
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
              "$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" 2>/dev/null && pwd)/../baselines/rccl_baseline.conf"; do
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
awk -v m="$MARGIN" 'BEGIN { exit !(m < 100) }' || die "--margin 必须小于 100"

LOG_DIR="${LOG_DIR:-./rccl_perf_logs/${GROUP_NAME:-$(date +%Y%m%d-%H%M%S)}}"
mkdir -p "$LOG_DIR" || die "无法创建日志目录: $LOG_DIR"

# ---------- 本机计算卡空闲检查（判据与 dcu_idle_check.sh 一致的内置精简版） ----------
# 本脚本常经 stdin 注入执行, 目标侧不一定有公共脚本文件, 故内置。
# 经总入口预检后（CLUSTER_IDLE_CHECKED=1）自动跳过——
# 总入口的占用检查阶段已逐节点查过, 避免重复检查与竞态。
if [[ "$SKIP_IDLE" == "0" && "$DRY_RUN" == "0" && "${CLUSTER_IDLE_CHECKED:-0}" != "1" ]]; then
    HSMI="hy-smi"
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

# ---------- 单机直启 / 多机 MPI；每个执行进程有独立环境和任务标记 ----------
build_command() {               # <test name> ; 结果写入 MP_CMD
    local test=$1 cards=1 name rank_body assignment
    local -a rank_cmd=() env_forward=() root_args=()
    ((${#NODES[@]} > 1)) || cards=$NP
    rank_body="$(declare -f resolve_test_bin)"$'\n'
    rank_body+='set -e
readonly -a hcu_rccl_args=("$@")
if [[ -n ${hcu_rccl_args[0]} ]]; then source "${hcu_rccl_args[0]}"; fi
'
    # These are the selected GROUP communication settings, applied AFTER each
    # rank sources its local runtime. %q preserves spaces/metacharacters as data.
    for name in "${NETWORK_VARS[@]}"; do
        [[ -n "${!name:-}" ]] || continue
        printf -v assignment 'export %s=%q\n' "$name" "${!name}"
        rank_body+="$assignment"$'\n'
    done
    rank_body+='BIN_DIR="${hcu_rccl_args[1]:-${RCCL_BIN_DIR:-${RCCL_TESTS_BIN_DIR:-}}}"
[[ -z ${hcu_rccl_args[2]} ]] || export UCX_NET_DEVICES="${hcu_rccl_args[2]}"
[[ -z ${hcu_rccl_args[3]} ]] || export NCCL_SOCKET_IFNAME="${hcu_rccl_args[3]}"
[[ -z ${hcu_rccl_args[4]} ]] || export NCCL_TOPO_FILE="${hcu_rccl_args[4]}"
bin=$(resolve_test_bin "${hcu_rccl_args[5]}")
exec "$bin" "${hcu_rccl_args[@]:6}"'
    rank_cmd=(bash -c "$rank_body" hcu-rccl-rank "${HCU_CLUSTER_ENV_SCRIPT:-}" "$BIN_DIR_ARG"
              "$UCX_ARG" "$IFACE_ARG" "$TOPO_ARG" "$test"
              -g "$cards" -b "$ARG_B" -e "$ARG_E" -f "$ARG_F" -n "$ARG_N" -w "$ARG_W")
    local rank_file="$LOG_DIR/rank-body.sh" guard_file="$LOG_DIR/task-guard.sh"
    if ((${#NODES[@]} > 1)); then
        # mpirun 把 rank argv 以空格拼接送远端 shell 重新分词：内联的多行脚本文本
        # 必然断裂。rank 本体与 task guard 落盘到全组可见的 LOG_DIR，rank 命令只
        # 携带无空格的路径与参数，远端分词结果与本地 argv 一一对应。
        printf '%s\n' "$rank_body" > "$rank_file" || die "无法写入 rank 脚本: $rank_file"
        rank_cmd=(bash "$rank_file" "${HCU_CLUSTER_ENV_SCRIPT:-}" "$BIN_DIR_ARG"
                  "$UCX_ARG" "$IFACE_ARG" "$TOPO_ARG" "$test"
                  -g "$cards" -b "$ARG_B" -e "$ARG_E" -f "$ARG_F" -n "$ARG_N" -w "$ARG_W")
        if [[ -n "$TASK_GUARD_BODY" ]]; then
            printf '%s\n' "$TASK_GUARD_BODY" > "$guard_file" || die "无法写入 guard 脚本: $guard_file"
        fi
    fi
    if [[ -n "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" && "$HCU_CLUSTER_TIMEOUT_SECONDS" != 0 ]]; then
        rank_cmd=(timeout --signal=TERM --kill-after=5s "${HCU_CLUSTER_TIMEOUT_SECONDS}s" "${rank_cmd[@]}")
    fi
    if [[ -n "$TASK_GUARD_BODY" ]]; then
        if ((${#NODES[@]} > 1)); then
            # 文件调用中无 -c 的 $0 标签位：直接以 run TOKEN 起始，与
            # hcu_main 的 mode/token 位置对齐。
            rank_cmd=(env -u BASH_ENV -u ENV bash --noprofile --norc "$guard_file"
                      run "$HCU_TASK_TOKEN" -- "${rank_cmd[@]}")
        else
            rank_cmd=(env -u BASH_ENV -u ENV bash --noprofile --norc -c "$TASK_GUARD_BODY"
                      hcu-task-guard run "$HCU_TASK_TOKEN" -- "${rank_cmd[@]}")
        fi
    fi
    if ((${#NODES[@]} == 1)); then
        MP_CMD=("${rank_cmd[@]}")
        return
    fi
    if [[ "${HCU_ALLOW_ROOT_MPI:-0}" == 1 ]]; then
        root_args=(--allow-run-as-root)
    else
        unset OMPI_ALLOW_RUN_AS_ROOT OMPI_ALLOW_RUN_AS_ROOT_CONFIRM PRTE_ALLOW_RUN_AS_ROOT PRTE_ALLOW_RUN_AS_ROOT_CONFIRM
    fi
    for name in PATH LD_LIBRARY_PATH ROCM_PATH ROCM_HOME DTK_ROOT DTK_HOME DTK_PATH RCCL_BIN_DIR \
                RCCL_TESTS_BIN_DIR RCCL_TESTS_PATH RCCL_TESTS_HOME RCCL_HOME RCCL_PATH; do
        [[ -z "${!name:-}" ]] || env_forward+=(-x "$name")
    done
    for name in "${NETWORK_VARS[@]}"; do
        [[ -z "${!name:-}" ]] || env_forward+=(-x "$name=${!name}")
    done
    MP_CMD=("$MPI_BIN" "${root_args[@]}" --mca plm_rsh_args "-p ${SSH_PORT}"
            --host "$HOSTLIST" -np "$NP" --map-by slot --bind-to none --wdir "$PWD"
            "${env_forward[@]}" "${rank_cmd[@]}")
}

# 从 rccl-test 输出解析 busbw 峰值:
# 数据行: size count type redop root time algbw busbw #wrong time algbw busbw #wrong
# 取 out-of-place($8) 与 in-place($12) busbw 的最大值; 表解析不到则回退 Avg bus bandwidth
parse_busbw() {                 # parse_busbw <logfile>
    local f=$1 v
    v=$(awk '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            if (NF >= 8  && $8  ~ /^[0-9]+(\.[0-9]+)?$/ && $8  > m) m = $8
            if (NF >= 12 && $12 ~ /^[0-9]+(\.[0-9]+)?$/ && $12 > m) m = $12
        }
        END { if (m > 0) printf "%.2f", m }
    ' "$f")
    if [[ -z "$v" ]]; then
        v=$(awk -F: '/Avg bus bandwidth/ { gsub(/[[:space:]]/, "", $2); printf "%.2f", $2 }' "$f")
    fi
    printf '%s' "$v"
}

# Display the four columns requested in the collective table. These are
# independent peaks across tested message sizes, not necessarily one row.
# Keep the original max(out/in busbw) acceptance rule; a one-column baseline
# must not be silently reused as four independent metric thresholds.
parse_layout_peaks() {
    awk '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            if (NF < 13) next
            if ($7 !~ /^[0-9]+(\.[0-9]+)?$/ || $8 !~ /^[0-9]+(\.[0-9]+)?$/ ||
                $11 !~ /^[0-9]+(\.[0-9]+)?$/ || $12 !~ /^[0-9]+(\.[0-9]+)?$/) next
            if (!seen || $7 > oa) oa=$7
            if (!seen || $8 > ob) ob=$8
            if (!seen || $11 > ia) ia=$11
            if (!seen || $12 > ib) ib=$12
            seen=1
        }
        END { if (seen) printf "%.2f %.2f %.2f %.2f", oa,ob,ia,ib; else printf "- - - -" }
    ' "$1"
}

record_result() {              # status, reason; numeric evidence from current test
    local status=$1 reason=$2
    ROWS+=("${t}|${status}|${out_alg}|${out_bus}|${in_alg}|${in_bus}|${busbw}|${base}|${item_margin}|${threshold}|${delta}|${rc}|${reason}")
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$t" "$status" "$out_alg" "$out_bus" "$in_alg" "$in_bus" "$busbw" "$base" "$item_margin" "$threshold" "$delta" "$rc" "$reason" \
        >> "$LOG_DIR/rccl-summary.tsv" || die "无法写入 RCCL 汇总"
}

# ---------- 开跑 ----------
echo "${C_BLD}RCCL 性能测试${C_RST}  ($(date '+%F %T'))"
echo ">>> 节点: ${HOSTLIST}  (np=${NP})"
echo ">>> 基准: ${base_src}${MARGIN:+  余量: 低于基准 ${MARGIN}% 内算过}"
echo ">>> 模式: $(if ((${#NODES[@]} == 1)); then echo "单机直启 -g ${NP}"; else echo '多节点 MPI，每 rank -g 1'; fi)"
echo ">>> 用例参数: -b ${ARG_B} -e ${ARG_E} -f ${ARG_F} -n ${ARG_N} -w ${ARG_W}"
echo ">>> 测试项: ${TESTS[*]}"
echo ">>> 判据: max(out-of-place busbw, in-place busbw) 峰值 >= 当前 np 基准 × (1-margin%)"
echo ">>> 四列带宽分别取本次消息尺寸范围的峰值；单位 GB/s，不代表同一尺寸行"
for name in "${NETWORK_VARS[@]}"; do
    echo ">>> ${name}=${!name:-<未设置，不伪造值>}"
done
echo ">>> 日志目录: ${LOG_DIR}"
echo

FAILED=0
declare -a ROWS=()
if [[ "$DRY_RUN" != 1 ]]; then
    printf 'test\tstatus\tout_of_place_algbw_gbps\tout_of_place_busbw_gbps\tin_place_algbw_gbps\tin_place_busbw_gbps\tpeak_busbw_gbps\tbaseline_busbw_gbps\tmargin_pct\tthreshold_busbw_gbps\tdelta_pct\treturncode\treason\n' \
        > "$LOG_DIR/rccl-summary.tsv" || die "无法创建 RCCL 汇总"
fi
idx=0
for t in "${TESTS[@]}"; do
    idx=$((idx + 1))
    base="${BASELINE[$t]}"
    item_margin="${BASELINE_MARGIN[$t]:-$MARGIN}"
    threshold=$(awk -v b="$base" -v m="$item_margin" 'BEGIN { printf "%.4f", b*(1-m/100) }')
    out_alg=-; out_bus=-; in_alg=-; in_bus=-; busbw=-; delta=-
    build_command "${t}_perf"
    # Keep a safely quoted, complete launch command, including the guard and
    # rank bootstrap, rather than hiding it behind a successful dry-run receipt.
    { printf '#!/usr/bin/env bash\nexec '; printf '%q ' "${MP_CMD[@]}"; printf '\n'; } > "$LOG_DIR/${t}.command.sh" \
        || die "无法写入命令记录: $LOG_DIR/${t}.command.sh"

    if [[ "$DRY_RUN" == "1" ]]; then
        printf '[%d/%d] %s\n    %s\n' "$idx" "${#TESTS[@]}" "$t" "${MP_CMD[*]}"
        continue
    fi

    printf '[%d/%d] %-16s 基准 %-8s 运行中...' "$idx" "${#TESTS[@]}" "$t" "$base"
    tlog="${LOG_DIR}/${t}.log"
    t0=$(date +%s)
    # </dev/null 必须有: 本脚本常经 stdin 注入执行(bash -s), mpirun 会吞掉 stdin 里剩余的脚本文本
    "${MP_CMD[@]}" > "$tlog" 2>&1 </dev/null
    rc=$?
    dt=$(( $(date +%s) - t0 ))

    if ((rc != 0)); then
        printf '\r[%d/%d] %-16s %s[FAIL]%s 执行失败 rc=%s (%ss) 日志: %s\n' \
            "$idx" "${#TESTS[@]}" "$t" "$C_RED" "$C_RST" "$rc" "$dt" "$tlog"
        tail -5 "$tlog" | sed 's/^/      | /'
        record_result FAIL EXECUTION_FAILED
        FAILED=1
        [[ "$rc" == 130 || "$rc" == 143 ]] && exit "$rc"
        continue
    fi

    busbw=$(parse_busbw "$tlog")
    if [[ -z "$busbw" ]]; then
        busbw=-
        printf '\r[%d/%d] %-16s %s[FAIL]%s 未解析到 busbw (%ss) 日志: %s\n' \
            "$idx" "${#TESTS[@]}" "$t" "$C_RED" "$C_RST" "$dt" "$tlog"
        record_result FAIL OUTPUT_PARSE_FAILED
        FAILED=1
        continue
    fi

    read -r out_alg out_bus in_alg in_bus <<< "$(parse_layout_peaks "$tlog")"
    delta=$(awk -v v="$busbw" -v b="$base" 'BEGIN { printf "%+.2f", (v-b)/b*100 }')

    # busbw >= base * (1 - margin/100) 即 PASS；margin 优先用该项自己的（基准文件第 4 列），
    # 缺省回退全局 --margin（适合波动大的项如 alltoall 单独放宽容差）
    ok=$(awk -v v="$busbw" -v b="$base" -v m="$item_margin" 'BEGIN { print (v >= b * (1 - m/100)) ? 1 : 0 }')
    if [[ "$ok" == "1" ]]; then
        printf '\r[%d/%d] %-16s %s[PASS]%s busbw=%-8s 基准=%-8s (%ss)\n' \
            "$idx" "${#TESTS[@]}" "$t" "$C_GRN" "$C_RST" "$busbw" "$base" "$dt"
        record_result PASS MEETS_BASELINE
    else
        gap=$(awk -v v="$busbw" -v b="$base" 'BEGIN { printf "%.1f", (b - v) / b * 100 }')
        printf '\r[%d/%d] %-16s %s[FAIL]%s busbw=%-8s 基准=%-8s 低 %s%% (容差 %s%%) (%ss) 日志: %s\n' \
            "$idx" "${#TESTS[@]}" "$t" "$C_RED" "$C_RST" "$busbw" "$base" "$gap" "$item_margin" "$dt" "$tlog"
        record_result FAIL BELOW_BASELINE
        FAILED=1
    fi
done

[[ "$DRY_RUN" == "1" ]] && exit 0

# ---------- 汇总 ----------
echo
echo "================= RCCL 测试汇总 (np=${NP}) ================="
printf '%-18s %-6s %10s %10s %10s %10s %10s %9s %s\n' "测试项" "结果" "out-algbw" "out-busbw" "in-algbw" "in-busbw" "基准" "偏差%" "说明"
{
    printf '# RCCL 性能验收（np=%s）\n\n' "$NP"
    printf -- '- 节点：`%s`\n- 基准来源：`%s`\n' "$HOSTLIST" "$BASE_SRC"
    printf -- '- 四列为各消息尺寸的独立峰值，单位 GB/s；判定沿用原脚本 busbw 峰值与当前规模基准比较。\n'
    printf -- '- 范围：%s；计划 %s 项，完成 %s 项。\n\n' "${TESTS_CSV:-完整10项}" "${#TESTS[@]}" "${#ROWS[@]}"
    printf '| Collective | 结果 | out-of-place algbw | out-of-place busbw | in-place algbw | in-place busbw | busbw 基准 | 容差%% | 判定下限 | 偏差%% | 原因 |\n'
    printf '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n'
} > "$LOG_DIR/rccl-summary.md" || die "无法创建 RCCL Markdown 汇总"
pass_n=0; fail_n=0
for row in "${ROWS[@]}"; do
    IFS='|' read -r t r oa ob ia ib v b margin limit difference rc note <<< "$row"
    printf '%-18s %-6s %10s %10s %10s %10s %10s %9s %s\n' "$t" "$r" "$oa" "$ob" "$ia" "$ib" "$b" "$difference" "$note"
    printf '| %s_perf | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
        "$t" "$r" "$oa" "$ob" "$ia" "$ib" "$b" "$margin" "$limit" "$difference" "$note" \
        >> "$LOG_DIR/rccl-summary.md" || die "无法写入 RCCL Markdown 汇总"
    if [[ "$r" == "PASS" ]]; then
        pass_n=$((pass_n + 1))
    else
        fail_n=$((fail_n + 1))
    fi
done
echo "------------------------------------------------------------"
if ((FAILED == 0)); then
    echo "${C_GRN}${C_BLD}结果: 全部 ${pass_n} 项性能达标${C_RST}"
else
    echo "${C_RED}${C_BLD}结果: ${fail_n} 项未达标${C_RST} / 共 $((pass_n + fail_n)) 项，详细日志: ${LOG_DIR}/"
fi
echo ">>> 汇总: ${LOG_DIR}/rccl-summary.md / ${LOG_DIR}/rccl-summary.tsv"

((FAILED == 0)) || exit 2
exit 0
