#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# dcu_idle_check.sh —— DCU 计算卡空闲检查（公共前置脚本）
#
# 职责：hy-smi 检查本机计算卡是否空闲。VRAM% / HCU% 超过阈值即视为被占用（默认 5%）。
#
# 用途：
#   - 独立运行:  ./dcu_idle_check.sh  （在有 hy-smi 的宿主机/容器里）
#   - 由 hcu-cluster-run 主动测试预检逐节点调用（"计算卡占用检查"阶段）
#   - 被其他 -s 载荷脚本 source 或调用（rccl_perf_test.sh 默认开跑前调用）
#
# 用法:
#   ./dcu_idle_check.sh [options]
# options:
#   --vram-max <pct>   VRAM% 阈值，默认 5（超过算占用）
#   --util-max <pct>   HCU% 阈值，默认 5
#   --cards <csv>      只检查指定卡号，如 0,1,2,3（默认全部）
#   --quiet            只输出结论行（IDLE / BUSY:<原因>）
#   -h, --help
#
# 退出码: 0 = 空闲可用；1 = hy-smi 不可用等环境错误；2 = 有卡被占用
set -uo pipefail

if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_RST=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_RST=""
fi
die() { printf '%s[ERROR]%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

VRAM_MAX="${DEFAULTS_VRAM_MAX:-5}"
UTIL_MAX="${DEFAULTS_UTIL_MAX:-5}"
CARDS_CSV=""
QUIET=0
while (($#)); do
    case "$1" in
        --vram-max) VRAM_MAX="${2:-}"; shift ;;
        --util-max) UTIL_MAX="${2:-}"; shift ;;
        --cards)    CARDS_CSV="${2:-}"; _CARDS_GIVEN=1; shift ;;
        --quiet)    QUIET=1 ;;
        -h|--help)  sed -n '2,/^set -uo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          die "未知参数: $1（-h 看用法）" ;;
    esac
    shift
done
[[ "$VRAM_MAX" =~ ^[0-9]+$ ]] || die "--vram-max 应为整数"
[[ "$UTIL_MAX" =~ ^[0-9]+$ ]] || die "--util-max 应为整数"
# 与其他取值选项一致拒绝空值：静默回退到"检查全部卡"会在其他卡忙时误判 BUSY 且无提示。
[[ "${_CARDS_GIVEN:-0}" != 1 || -n "$CARDS_CSV" ]] || die "--cards 需要卡号列表（如 0,1,2）"
[[ -z "$CARDS_CSV" ]] || [[ "$CARDS_CSV" =~ ^[0-9]+(,[0-9]+)*$ ]] || die "--cards 应为逗号分隔的卡号（如 0,1,2）"

# hy-smi 定位: PATH -> /opt/hyhal/bin（宿主机常见只挂载未进 PATH）
HSMI="hy-smi"
if ! command -v hy-smi >/dev/null 2>&1; then
    [[ -x /opt/hyhal/bin/hy-smi ]] && HSMI=/opt/hyhal/bin/hy-smi \
        || die "hy-smi 不可用（PATH 与 /opt/hyhal/bin 均未找到）"
fi

declare -A CARD_FILTER=()
if [[ -n "$CARDS_CSV" ]]; then
    IFS=',' read -r -a _cards <<< "$CARDS_CSV"
    for c in "${_cards[@]}"; do CARD_FILTER[$c]=1; done
fi

# ---------- VRAM% / HCU% 阈值检查 ----------
smi_out=$("$HSMI" 2>/dev/null) || die "hy-smi 执行失败"
declare -a BUSY_REASONS=()
card_cnt=0
while read -r hcu _temp _pwr _perf _cap vram util _rest; do
    [[ "$hcu" =~ ^[0-9]+$ ]] || continue
    if [[ -n "$CARDS_CSV" && -z "${CARD_FILTER[$hcu]+x}" ]]; then continue; fi
    card_cnt=$((card_cnt + 1))
    v="${vram%\%}"; u="${util%\%}"
    # 小数向上取整比较（如 0.0% -> 0）
    v_int=$(awk -v x="$v" 'BEGIN { printf "%d", (x == int(x)) ? x : int(x) + 1 }')
    u_int=$(awk -v x="$u" 'BEGIN { printf "%d", (x == int(x)) ? x : int(x) + 1 }')
    ((v_int > VRAM_MAX)) && BUSY_REASONS+=("卡${hcu} VRAM=${vram}(>${VRAM_MAX}%)")
    ((u_int > UTIL_MAX)) && BUSY_REASONS+=("卡${hcu} HCU=${util}(>${UTIL_MAX}%)")
done <<< "$smi_out"
((card_cnt > 0)) || die "hy-smi 输出中未解析到任何卡（格式变化?）"

# ---------- 结论 ----------
host=$(hostname)
if ((${#BUSY_REASONS[@]} == 0)); then
    if [[ "$QUIET" == "1" ]]; then echo "IDLE"
    else echo "${C_GRN}[IDLE]${C_RST} ${host}: ${card_cnt} 卡全部空闲 (VRAM<=${VRAM_MAX}% HCU<=${UTIL_MAX}%)"; fi
    exit 0
else
    reasons=$(IFS='; '; echo "${BUSY_REASONS[*]}")
    if [[ "$QUIET" == "1" ]]; then echo "BUSY:${reasons}"
    else
        echo "${C_RED}[BUSY]${C_RST} ${host}: 计算卡被占用 -- ${reasons}"
        [[ "$QUIET" == "0" ]] && "$HSMI" 2>/dev/null | head -14
    fi
    exit 2
fi
