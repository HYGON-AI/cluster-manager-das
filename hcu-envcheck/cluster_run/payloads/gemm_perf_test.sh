#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# gemm_perf_test.sh —— DCU 单机 GEMM 性能测试 + 基准比对
#
# 由 hcu-cluster-run <场景> gemm --profile rocblas 在已 source env.sh 的节点调用。
# 保留单机逐卡/逐形状测试；组内调度由总入口负责，也可在激活环境后独立运行。
# 只读取显式参数和环境变量，不隐式加载旧 cluster_env.conf/common.sh。
#
# 职责：本机逐卡跑一组 GEMM 形状（rocblas-bench），解析 TFLOPS 与基准比对。
#   GEMM 是单卡/单机属性, 不需要多机 mpirun —— 所以走 -g 1, 每台机器
#   独立测、独立判定, 汇总层自动分桶（哪台机器哪张卡慢一眼可见）。
#
# 基准文件 gemm_baseline.conf（../baselines/ 或本脚本同目录; --baseline-file 可指定）：
#   行格式 "<m>x<n>x<k> <dtype> <tflops> [margin]"
#   margin 列可选: 该形状允许低于基准的百分比。GEMM 受频率/温度影响比 rccl 大,
#   默认容差取 3（而不是 rccl 的 1）, 单形状可再放宽。
#   无基准文件时进入"采集模式": 只测量打印, 不判定 PASS/FAIL（用于先实测取基准）。
#
# 用法:
#   ./gemm_perf_test.sh [options]
# options:
#   --bin <path>        rocblas-bench 路径（默认 $GEMM_BIN/$ROCBLAS_BENCH -> PATH ->
#                       $GEMM_BIN_DIR/$ROCBLAS_PATH/$DTK_ROOT/$ROCM_PATH 等环境根目录）
#   --shapes <csv>      形状列表, 如 4096x4096x4096,8192x8192x8192
#                       （默认: 基准文件里的全部形状; 无基准文件时用内置采集集）
#   --dtype <t>         f16_r|f32_r|bf16_r（默认 f16_r, HPC 验收常用半精度）
#   --cards <csv>       只测指定卡, 如 0,1（默认全部, 按 hy-smi 枚举）
#   --margin <pct>      全局容差, 默认 3; 基准文件第 4 列可按形状覆盖
#   --baseline-file <f> 基准文件（默认: $GEMM_BASELINE_B64 注入 -> ../baselines/ ->
#                       同目录 gemm_baseline.conf）
#   --iters <n>         每形状迭代次数, 默认 10
#   --log-dir <path>    日志目录, 默认 ./gemm_perf_logs/<主机名>
#   --skip-idle-check   跳过本机占用检查（经总入口调起时自动跳过）
#   --dry-run           只打印将执行的命令
#
# 输出: 每形状每卡一行 [PASS]/[FAIL]（采集模式打 [INFO]）, 总入口自动统计标记。
# 退出码: 0 = 全部 PASS（或采集模式完成）；1 = 参数/环境错误；2 = 存在 FAIL
set -uo pipefail

_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" 2>/dev/null && pwd)"

if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_BLD=$'\033[1m'; C_RST=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_BLD=""; C_RST=""
fi
log() { printf '%s\n' "$*"; }
die() { printf '%s[ERROR]%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

GEMM_BIN="${GEMM_BIN:-${ROCBLAS_BENCH:-}}"
SHAPES_CSV="${GEMM_SHAPES:-}"
DTYPE="${GEMM_DTYPE:-f16_r}"
CARDS_CSV="${GEMM_CARDS:-}"
MARGIN="${GEMM_MARGIN:-3}"
BASELINE_FILE=""
ITERS="${GEMM_ITERS:-10}"
LOG_DIR=""
DRY_RUN=0
SKIP_IDLE=0

while (($#)); do
    case "$1" in
        --bin)            GEMM_BIN="${2:-}"; shift ;;
        --shapes)         SHAPES_CSV="${2:-}"; shift ;;
        --dtype)          DTYPE="${2:-}"; shift ;;
        --cards)          CARDS_CSV="${2:-}"; shift ;;
        --margin)         MARGIN="${2:-}"; shift ;;
        --baseline-file)  BASELINE_FILE="${2:-}"; shift ;;
        --iters)          ITERS="${2:-}"; shift ;;
        --log-dir)        LOG_DIR="${2:-}"; shift ;;
        --skip-idle-check) SKIP_IDLE=1 ;;
        --dry-run)        DRY_RUN=1 ;;
        -h|--help)        sed -n '2,/^set -uo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                die "未知参数: $1（-h 看用法）" ;;
    esac
    shift
done
[[ "$MARGIN" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "--margin 应为非负数字"
[[ "$ITERS" =~ ^[1-9][0-9]*$ ]] || die "--iters 应为正整数"
case "$DTYPE" in f16_r|f32_r|bf16_r) ;; *) die "--dtype 只支持 f16_r|f32_r|bf16_r" ;; esac
[[ -z "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" || "$HCU_CLUSTER_TIMEOUT_SECONDS" =~ ^[0-9]+(\.[0-9]+)?$ ]] \
    || die "HCU_CLUSTER_TIMEOUT_SECONDS 必须为非负秒数"

# ---------- rocblas-bench 定位 ----------
if [[ -z "$GEMM_BIN" ]]; then
    GEMM_BIN=$(command -v rocblas-bench 2>/dev/null) || GEMM_BIN=""
fi
if [[ -z "$GEMM_BIN" ]]; then
    for _root in "${GEMM_BIN_DIR:-}" "${ROCBLAS_PATH:-}" "${ROCBLAS_ROOT:-}" \
                 "${DTK_ROOT:-}" "${DTK_HOME:-}" "${DTK_PATH:-}" "${ROCM_PATH:-}" "${ROCM_HOME:-}"; do
        [[ -n "$_root" ]] || continue
        for _c in "$_root/rocblas-bench" "$_root/bin/rocblas-bench" \
                  "$_root/rocblas/bin/rocblas-bench"; do
            [[ -x "$_c" ]] && { GEMM_BIN="$_c"; break 2; }
        done
    done
fi
if [[ "$DRY_RUN" != 1 ]] && { [[ -z "$GEMM_BIN" ]] || ! command -v "$GEMM_BIN" >/dev/null 2>&1; }; then
    die "找不到 rocblas-bench；用 --bin 或 env.sh 的 GEMM_BIN/PATH/DTK_ROOT/ROCM_PATH 配置"
fi
TASK_GUARD_BODY=""
if [[ -n "${HCU_TASK_TOKEN:-}" ]]; then
    [[ "$HCU_TASK_TOKEN" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{15,95}$ ]] || die "HCU_TASK_TOKEN 格式错误"
    [[ -r "$_SELF_DIR/task_guard.sh" ]] || die "缺少任务清理载荷: $_SELF_DIR/task_guard.sh"
    TASK_GUARD_BODY=$(< "$_SELF_DIR/task_guard.sh")
fi

# ---------- 基准表 ----------
# 查找顺序: GEMM_BASELINE_B64 注入 > --baseline-file > ../baselines/ > 同目录 gemm_baseline.conf
# 均无 -> 采集模式（只测不判）
declare -A BASELINE=() BASELINE_MARGIN=()
declare -a BASE_ORDER=()
COLLECT_MODE=0
_load_baseline() {
    local key dt val mg
    while read -r key dt val mg _ || [[ -n "${key:-}" ]]; do
        key=${key%$'\r'}; dt=${dt%$'\r'}; val=${val%$'\r'}; mg=${mg%$'\r'}
        [[ -n "${key:-}" && "$key" != \#* ]] || continue
        [[ "$dt" == "$DTYPE" ]] || continue
        [[ "$val" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "基准数值不合法: $key $dt $val"
        BASELINE[$key]="$val"
        BASE_ORDER+=("$key")
        if [[ -n "${mg:-}" && "$mg" != \#* ]]; then
            [[ "$mg" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "基准 margin 列不合法: $key $dt $val $mg"
            BASELINE_MARGIN[$key]="$mg"
        fi
    done
}
BASE_SRC=""
if [[ -n "${GEMM_BASELINE_B64:-}" ]]; then
    _btext=$(printf '%s' "$GEMM_BASELINE_B64" | base64 -d 2>/dev/null) || die "GEMM_BASELINE_B64 解码失败"
    _load_baseline <<< "$_btext"; BASE_SRC="注入内容"
elif [[ -n "$BASELINE_FILE" ]]; then
    [[ -f "$BASELINE_FILE" ]] || die "基准文件不存在: $BASELINE_FILE"
    _load_baseline < "$BASELINE_FILE"; BASE_SRC="file:$BASELINE_FILE"
elif [[ -n "$_SELF_DIR" ]]; then
    # 仓库布局: 脚本在 payloads/, 基准在 ../baselines/; 平铺部署回退同目录
    for _bf in "${_SELF_DIR}/../baselines/gemm_baseline.conf" "${_SELF_DIR}/gemm_baseline.conf"; do
        [[ -f "$_bf" ]] || continue
        _load_baseline < "$_bf"; BASE_SRC="file:${_bf}"
        break
    done
    unset _bf
fi
if [[ -z "$BASE_SRC" || ${#BASELINE[@]} -eq 0 ]]; then
    COLLECT_MODE=1
fi

# ---------- 形状集 ----------
declare -a SHAPES=()
if [[ -n "$SHAPES_CSV" ]]; then
    IFS=',' read -r -a SHAPES <<< "$SHAPES_CSV"
elif ((COLLECT_MODE == 0)); then
    SHAPES=(${BASE_ORDER[@]+"${BASE_ORDER[@]}"})
else
    # 采集集: 大形状看峰值算力, 中形状看常见负载
    SHAPES=(8192x8192x8192 4096x4096x4096 2048x2048x8192)
fi
for s in "${SHAPES[@]}"; do
    [[ "$s" =~ ^[1-9][0-9]*x[1-9][0-9]*x[1-9][0-9]*$ ]] || die "形状格式应为正整数 MxNxK, 得到: $s"
    if ((COLLECT_MODE == 0)); then
        [[ -n "${BASELINE[$s]+x}" ]] || die "基准缺少形状 $s ($DTYPE)，不能按零基准判 PASS"
    fi
done

# ---------- 卡枚举 ----------
HSMI="${HY_SMI_BIN:-$(command -v hy-smi 2>/dev/null)}"
declare -a CARDS=()
if [[ -n "$CARDS_CSV" ]]; then
    IFS=',' read -r -a CARDS <<< "$CARDS_CSV"
elif [[ -n "$HSMI" ]]; then
    while read -r hcu _; do
        [[ "$hcu" =~ ^[0-9]+$ ]] && CARDS+=("$hcu")
    done <<< "$("$HSMI" 2>/dev/null)"
fi
if ((${#CARDS[@]} == 0)); then
    [[ "$DRY_RUN" == 1 ]] || die "未能枚举计算卡；请在 env.sh 配置 hy-smi/HY_SMI_BIN 或使用 --cards 明确卡号"
    CARDS=(0)
fi
for card in "${CARDS[@]}"; do [[ "$card" =~ ^[0-9]+$ ]] || die "非法卡号: $card"; done

# ---------- 本机占用检查（同 rccl 逻辑: 总入口查过则跳过） ----------
if [[ "$SKIP_IDLE" == "0" && "$DRY_RUN" == "0" && "${CLUSTER_IDLE_CHECKED:-0}" != "1" && -n "$HSMI" ]]; then
    busy=""
    while read -r hcu _t _p _pf _c vram util _; do
        [[ "$hcu" =~ ^[0-9]+$ ]] || continue
        v="${vram%\%}"; u="${util%\%}"
        over=$(awk -v v="$v" -v u="$u" 'BEGIN { print (v > 5 || u > 5) ? 1 : 0 }')
        [[ "$over" == "1" ]] && busy+="${busy:+; }卡${hcu} VRAM=${vram} HCU=${util}"
    done <<< "$("$HSMI" 2>/dev/null)"
    [[ -n "$busy" ]] && die "本机计算卡被占用，拒绝开跑（--skip-idle-check 可跳过）: ${busy}"
fi

LOG_DIR="${LOG_DIR:-./gemm_perf_logs/$(hostname)}"
mkdir -p "$LOG_DIR" || die "无法创建日志目录: $LOG_DIR"

# ---------- 开跑 ----------
echo "${C_BLD}GEMM 性能测试${C_RST}  $(hostname)  ($(date '+%F %T'))"
echo ">>> bin: ${GEMM_BIN:-<dry-run 未定位>}  dtype: ${DTYPE}  iters: ${ITERS}"
echo ">>> 卡: ${CARDS[*]}  形状: ${SHAPES[*]}"
if ((COLLECT_MODE)); then
    echo ">>> ${C_YEL}采集模式${C_RST}: 无基准文件, 只测量不判定（结果可直接整理进 gemm_baseline.conf）"
else
    echo ">>> 基准: ${BASE_SRC}  全局容差: ${MARGIN}%（基准第4列可按形状覆盖）"
fi
echo ">>> 日志目录: ${LOG_DIR}"
echo

# rocblas-bench 输出解析: 数据行的 rocblas-Gflops 列（不同版本列名有差异, 取含
# Gflops 的表头定位列号; 解析不到时给 FAIL 并保留原始日志）
run_one() {                     # run_one <card> <shape> ; 输出 TFLOPS 到 stdout, 失败输出空
    local card=$1 shape=$2
    local m n k
    IFS='x' read -r m n k <<< "$shape"
    local out="${LOG_DIR}/gemm_c${card}_${shape}.log"
    local -a command=("$GEMM_BIN" -f gemm -r "$DTYPE"
        --transposeA N --transposeB N -m "$m" -n "$n" -k "$k"
        --alpha 1 --beta 0 -i "$ITERS")
    if [[ -n "${HCU_CLUSTER_TIMEOUT_SECONDS:-}" && "$HCU_CLUSTER_TIMEOUT_SECONDS" != 0 ]]; then
        command=(timeout --signal=TERM --kill-after=5s "${HCU_CLUSTER_TIMEOUT_SECONDS}s" "${command[@]}")
    fi
    if [[ -n "$TASK_GUARD_BODY" ]]; then
        command=(env -u BASH_ENV -u ENV bash --noprofile --norc -c "$TASK_GUARD_BODY"
                 hcu-task-guard run "$HCU_TASK_TOKEN" -- "${command[@]}")
    fi
    HIP_VISIBLE_DEVICES=$card "${command[@]}" > "$out" 2>&1 </dev/null
    local rc=$?
    ((rc == 0)) || return "$rc"
    awk -F '[,[:space:]]+' '
        { sub(/^[[:space:]]+/, ""); sub(/[[:space:]]+$/, "") }
        /rocblas-Gflops|hipblaslt-Gflops|Gflops/ && !colset {
            for (i = 1; i <= NF; i++) if ($i ~ /Gflops/) { col = i; colset = 1 }
            next
        }
        colset && $col ~ /^[0-9]+(\.[0-9]+)?$/ { if ($col > best) best = $col }
        END { if (best > 0) printf "%.2f", best / 1000 }
    ' "$out"
}

FAILED=0
total=0; pass_n=0; fail_n=0
for shape in "${SHAPES[@]}"; do
    base="${BASELINE[$shape]:-}"
    item_margin="${BASELINE_MARGIN[$shape]:-$MARGIN}"
    for card in "${CARDS[@]}"; do
        total=$((total + 1))
        if [[ "$DRY_RUN" == "1" ]]; then
            IFS='x' read -r m n k <<< "$shape"
            echo "[dry-run] HIP_VISIBLE_DEVICES=$card ${GEMM_BIN:-rocblas-bench} -f gemm -r $DTYPE -m $m -n $n -k $k -i $ITERS"
            continue
        fi
        printf '卡%-2s %-20s 运行中...' "$card" "$shape"
        t0=$(date +%s)
        tflops=$(run_one "$card" "$shape")
        rc=$?
        [[ "$rc" == 130 || "$rc" == 143 ]] && exit "$rc"
        dt=$(( $(date +%s) - t0 ))
        if [[ -z "$tflops" ]]; then
            printf '\r%s[FAIL]%s 卡%-2s %-20s 执行失败或未解析到 Gflops (%ss) 日志: %s\n' \
                "$C_RED" "$C_RST" "$card" "$shape" "$dt" "${LOG_DIR}/gemm_c${card}_${shape}.log"
            FAILED=1; fail_n=$((fail_n + 1))
            continue
        fi
        if ((COLLECT_MODE)); then
            printf '\r%s[INFO]%s 卡%-2s %-20s %s TFLOPS (%ss)   # 基准行: %s %s %s\n' \
                "$C_YEL" "$C_RST" "$card" "$shape" "$tflops" "$dt" "$shape" "$DTYPE" "$tflops"
            pass_n=$((pass_n + 1))
            continue
        fi
        ok=$(awk -v v="$tflops" -v b="$base" -v m="$item_margin" 'BEGIN { print (v >= b * (1 - m/100)) ? 1 : 0 }')
        if [[ "$ok" == "1" ]]; then
            printf '\r%s[PASS]%s 卡%-2s %-20s %s TFLOPS  基准 %s (%ss)\n' \
                "$C_GRN" "$C_RST" "$card" "$shape" "$tflops" "$base" "$dt"
            pass_n=$((pass_n + 1))
        else
            gap=$(awk -v v="$tflops" -v b="$base" 'BEGIN { printf "%.1f", (b - v) / b * 100 }')
            printf '\r%s[FAIL]%s 卡%-2s %-20s %s TFLOPS  基准 %s 低 %s%%（容差 %s%%）(%ss)\n' \
                "$C_RED" "$C_RST" "$card" "$shape" "$tflops" "$base" "$gap" "$item_margin" "$dt"
            FAILED=1; fail_n=$((fail_n + 1))
        fi
    done
done

[[ "$DRY_RUN" == "1" ]] && exit 0
echo
echo "================= GEMM 汇总 $(hostname) ================="
if ((COLLECT_MODE)); then
    echo "采集完成 ${pass_n}/${total} 项（失败 ${fail_n}）; 按上方 '# 基准行:' 整理进 gemm_baseline.conf 即可启用判定"
else
    if ((FAILED == 0)); then
        echo "${C_GRN}${C_BLD}结果: 全部 ${pass_n} 项达标${C_RST}"
    else
        echo "${C_RED}${C_BLD}结果: ${fail_n} 项未达标${C_RST} / 共 ${total} 项, 日志: ${LOG_DIR}/"
    fi
fi
((FAILED == 0)) || exit 2
exit 0
