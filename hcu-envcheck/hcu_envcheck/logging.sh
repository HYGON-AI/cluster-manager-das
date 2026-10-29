#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Presentation only. Never evaluates argv, loads an environment, or runs tests.
hcu_log_start() {
    local order=$1 suite=$2 item=$3 environment=$4 nodes=$5 leader=$6 scale=$7
    printf '\n%s\n【开始】%s · %s · %s\n%s\n\n' \
        '================================================================================' "$order" "$suite" "$item" \
        '================================================================================'
    printf '测试环境：%s\n测试规模：%s\n参与节点：%s\n启动节点：%s\n开始时间：%s\n\n' \
        "$environment" "$scale" "$nodes" "$leader" "$(date '+%F %T%z')"
}
hcu_log_command() {
    local label=$1 node=$2 argument quoted text title
    shift 2
    case $label in
        MPI-COMMAND) title='MPI 启动命令' ;;
        TEST-COMMAND) title='实际测试命令' ;;
        *) title='执行命令' ;;
    esac
    printf -v text '\n【%s｜在 %s 执行】\n工作目录：%s\n' "$title" "$node" "$PWD"
    if (( $# )); then
        printf -v quoted '%q' "$1"; text+=$quoted; shift
        for argument in "$@"; do
            printf -v quoted '%q' "$argument"
            if [[ $argument == -* ]]; then text+=$' \\\n  '"$quoted"
            else text+=" $quoted"; fi
        done
    fi
    printf '%s\n【命令结束】\n\n' "$text"
}
hcu_log_raw() {
    printf '【测试原始输出｜%s】\n%s\n' "$1" \
        '--------------------------------------------------------------------------------'
}
hcu_log_end() {
    local order=$1 suite=$2 item=$3 status=$4 rc=$5 elapsed=$6 cleanup=$7 outcome cleanup_text
    case $status in
        PASS) outcome=执行成功 ;; WARN) outcome=执行完成（含警告） ;;
        TIMEOUT) outcome=执行超时 ;; CANCELLED) outcome=执行取消 ;;
        NA) outcome=不适用 ;; *) outcome=执行失败 ;;
    esac
    case $cleanup in CONFIRMED) cleanup_text=已确认 ;; PENDING) cleanup_text=由外层任务确认 ;; *) cleanup_text=未确认 ;; esac
    printf '\n%s\n【结束】%s · %s · %s\n执行结果：%s（%s），退出码 %s\n执行耗时：%s 秒\n任务清理：%s\n判定报告：summary.md\n' \
        '--------------------------------------------------------------------------------' "$order" "$suite" "$item" \
        "$outcome" "$status" "$rc" "$elapsed" "$cleanup_text"
}
