#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
#
# common.sh —— 工具集公共引导：加载 cluster_env.conf、统一颜色/日志/die
#
# 用法（在各脚本顶部）：
#   source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
#
# 加载优先级（低 -> 高）：cluster_env.conf 默认值 < 环境变量 < 命令行参数。
# 各脚本的命令行解析在其后执行，自然拥有最高优先级。

# 工具目录（所有脚本共用）
TOOLS_DIR="${TOOLS_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
export TOOLS_DIR

# 共享集群约定（单一来源）：容器/镜像/端口/基线路径等。
# cluster_env.conf 含集群内部信息, 不入库——从 cluster_env.conf.example 复制后
# 按本集群填写。缺失时用 example 的中性默认值兜底并提示一次。
CLUSTER_ENV_FILE="${CLUSTER_ENV_FILE:-${TOOLS_DIR}/cluster_env.conf}"
if [[ -f "$CLUSTER_ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$CLUSTER_ENV_FILE"
elif [[ -f "${TOOLS_DIR}/cluster_env.conf.example" ]]; then
    # shellcheck disable=SC1091
    source "${TOOLS_DIR}/cluster_env.conf.example"
    printf '[提示] 未找到 %s, 已用 cluster_env.conf.example 的中性默认值（建议复制并按集群填写）\n' \
        "$CLUSTER_ENV_FILE" >&2
fi

# ---------- 颜色：对应流是终端才带色，进文件/管道自动关 ----------
ce_color_init() {               # ce_color_init <fd>  -> 设置变量名前缀
    local fd=$1
    if [ -t "$fd" ]; then
        CE_RED=$'\033[31m'; CE_GRN=$'\033[32m'; CE_YEL=$'\033[33m'
        CE_CYN=$'\033[36m'; CE_BLD=$'\033[1m'; CE_DIM=$'\033[2m'; CE_RST=$'\033[0m'
    else
        CE_RED=""; CE_GRN=""; CE_YEL=""; CE_CYN=""; CE_BLD=""; CE_DIM=""; CE_RST=""
    fi
}
ce_color_init 1

# ---------- 统一日志 ----------
ce_log()  { printf '%s\n' "$*"; }      # 结果输出（随 stdout 着色策略）
ce_info() { printf '%s\n' "$*" >&2; }  # 过程日志（随 stderr）
ce_die()  { printf '%s[ERROR]%s %s\n' "$CE_RED" "$CE_RST" "$*" >&2; exit 1; }

# ---------- 数值取默认：ce_or <值> <默认> ----------
ce_or() { [[ -n "$1" ]] && printf '%s' "$1" || printf '%s' "$2"; }
