#!/bin/sh
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Bare-metal node check example.
#
# Normal use:
#   1. Edit examples/baremetal-nodes.txt.
#   2. Make sure the control host can SSH to every node without embedding a
#      password in this file.
#   3. Run: ./examples/check-nodes.sh
#
# This runs one node-check round: host/HCU/RDMA inventory, IB state,
# ib_write_bw and run_nhc. It never adds, removes or recovers Kubernetes taints.

set -eu

resolve_script_dir() {
    target=$1
    while [ -h "$target" ]; do
        target_dir=$(CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd) || return 1
        link=$(readlink "$target") || return 1
        case "$link" in
            /*) target=$link ;;
            *) target=$target_dir/$link ;;
        esac
    done
    CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd
}

EXAMPLE_DIR=$(resolve_script_dir "$0") || {
    printf 'cannot resolve example directory\n' >&2
    exit 70
}
# 项目根目录：根据当前脚本所在的 examples 目录自动向上一级定位。
PROJECT_ROOT=$(CDPATH= cd -P -- "$EXAMPLE_DIR/.." && pwd)
# 检测工具入口：默认使用项目根目录下的 hcu-envcheck.sh。
TOOL=$PROJECT_ROOT/hcu-envcheck.sh

# 待检测节点列表：默认读取 examples/baremetal-nodes.txt。
# 一般情况下，用户只需要修改该节点列表文件；也可以通过 NODES_FILE
# 环境变量指定其他文件。
NODES_FILE=${NODES_FILE:-"$EXAMPLE_DIR/baremetal-nodes.txt"}

# 以下是常用检测参数。目标集群规格不同时，可通过同名环境变量覆盖默认值，
# 无需修改脚本主体。例如：
#   EXPECTED_DEVICES=16 ./examples/check-nodes.sh

# 每个节点预期安装并可识别的 HCU 数量，默认每节点 8 张。
EXPECTED_DEVICES=${EXPECTED_DEVICES:-8}

# 每个节点要求的最少 RDMA HCA 数量；这些 HCA 还必须存在 Active+LinkUp 端口。
EXPECTED_RDMA_DEVICES=${EXPECTED_RDMA_DEVICES:-4}

# 单条 HCA 路径 ib_write_bw 的最低平均带宽阈值，单位为 Gbit/s。
IB_MINIMUM_AVERAGE_GBPS=${IB_MINIMUM_AVERAGE_GBPS:-100}

# 同时采集节点静态信息的最大 SSH 并发数；不代表 ib_write_bw 并发数。
# 带宽测试仍由命令中的 --ib-concurrency 1 控制，默认串行执行。
CONCURRENCY=${CONCURRENCY:-16}

# 检测结果根目录。目录可以重复使用，每次运行会自动创建
# nodes_check_时间戳 子目录，避免覆盖或混合历史结果。
OUTPUT_ROOT=${OUTPUT_ROOT:-"$PROJECT_ROOT/out"}

# 目标节点上的 Python 3 解释器。host-python 模式下，它同时代表待检查的
# Python 软件环境；默认从目标节点 PATH 中查找 python3。
REMOTE_PYTHON=${REMOTE_PYTHON:-python3}

# 训练软件环境模式，可选 host-python、conda 或 docker。
# 使用 conda/docker 时，还需按脚本后文要求设置对应的环境变量。
SOFTWARE_MODE=${SOFTWARE_MODE:-host-python}

# 需要检查的 Python 包，多个包用空格分隔，例如 "torch numpy"。
# 默认为空：不检查 Python 依赖，也不会导入 Torch。
PYTHON_PACKAGES=${PYTHON_PACKAGES:-}

start_info() {
    printf '[start] %s\n' "$*"
}

# 仅用于执行前回显节点选择，不参与真正的节点解析。实际解析仍由
# hcu-envcheck 的 nodes-file 解析器负责，支持逗号列表和 bracket range。
node_selection_summary() {
    (
        set -f
        separator=
        node_line=
        while IFS= read -r node_line || [ -n "$node_line" ]; do
            node_line=${node_line%%#*}
            set -- $node_line
            [ "$#" -gt 0 ] || continue
            printf '%s%s' "$separator" "$1"
            separator=,
        done < "$NODES_FILE"
        printf '\n'
    )
}

[ -x "$TOOL" ] || {
    printf 'hcu-envcheck entry point is not executable: %s\n' "$TOOL" >&2
    exit 69
}
[ -r "$NODES_FILE" ] || {
    printf 'node list is not readable: %s\n' "$NODES_FILE" >&2
    exit 66
}
SELECTED_NODES=$(node_selection_summary)

# ib_write_bw sends active traffic. An interactive user only needs to type yes;
# automation can set CONFIRM_NODES_IDLE=yes after independently proving that
# every selected node is idle.
if [ "${CONFIRM_NODES_IDLE:-}" != "yes" ]; then
    if [ -t 0 ]; then
        start_info '节点检查会执行一轮主机/HCU/RDMA 信息采集、ibstat、ib_write_bw 和 NHC。'
        start_info '该检查不会添加、删除或恢复 Kubernetes taint。'
        start_info "请先确认节点列表中的所有节点均无训练任务：$NODES_FILE"
        start_info '输入 yes 继续：'
        IFS= read -r answer
        [ "$answer" = "yes" ] || {
            printf '[start] 已取消：未输入 yes。\n' >&2
            exit 64
        }
    else
        printf '[start] ERROR: 非交互执行需要设置 CONFIRM_NODES_IDLE=yes。\n' >&2
        exit 64
    fi
fi

start_info "selected nodes: $SELECTED_NODES"
start_info "node list: $NODES_FILE"
start_info 'rounds: 1; ib_write_bw concurrency: 1'
start_info "ib_write_bw minimum average: ${IB_MINIMUM_AVERAGE_GBPS} Gbit/s"
start_info "output root: $OUTPUT_ROOT"
start_info "software mode: $SOFTWARE_MODE"

set -- baremetal-cluster \
    --nodes-file "$NODES_FILE" \
    --transport ssh \
    --concurrency "$CONCURRENCY" \
    --command-timeout 420 \
    --remote-python "$REMOTE_PYTHON" \
    --expected-devices "$EXPECTED_DEVICES" \
    --samples 1 \
    --busy-sample-quorum 1 \
    --sample-interval 0 \
    --software-mode "$SOFTWARE_MODE" \
    --require-rdma \
    --minimum-rdma-devices "$EXPECTED_RDMA_DEVICES" \
    --expected-rdma-protocol ib \
    --enable-node-health-checks \
    --confirm-nodes-idle \
    --nhc-timeout 600 \
    --ib-tool ib_write_bw \
    --ib-protocol ib \
    --ib-port 1 \
    --ib-control-port 18515 \
    --ib-message-bytes 1048576 \
    --ib-iters 1000 \
    --ib-minimum-average-gbps "$IB_MINIMUM_AVERAGE_GBPS" \
    --ib-concurrency 1 \
    --ib-max-tests 64 \
    --output-dir "$OUTPUT_ROOT"

case "$SOFTWARE_MODE" in
    host-python)
        ;;
    conda)
        : "${CONDA_PREFIX:?set CONDA_PREFIX to the absolute training environment path}"
        : "${CONDA_STORAGE:?set CONDA_STORAGE to shared or node-local}"
        set -- "$@" \
            --conda-prefix "$CONDA_PREFIX" \
            --conda-storage "$CONDA_STORAGE"
        ;;
    docker)
        : "${DOCKER_IMAGE:?set DOCKER_IMAGE to the exact training image}"
        set -- "$@" \
            --docker-image "$DOCKER_IMAGE" \
            --container-python "${CONTAINER_PYTHON:-python3}"
        ;;
    *)
        printf 'SOFTWARE_MODE must be host-python, conda or docker\n' >&2
        exit 64
        ;;
esac

# Empty by default: no Python package is checked and Torch is not imported.
# Example opt-in: PYTHON_PACKAGES="torch numpy" ./examples/check-nodes.sh
for package_name in $PYTHON_PACKAGES; do
    set -- "$@" --require-python-package "$package_name"
done

[ -z "${SSH_USER:-}" ] || set -- "$@" --ssh-user "$SSH_USER"
[ -z "${SSH_PORT:-}" ] || set -- "$@" --ssh-port "$SSH_PORT"
[ -z "${SSH_CONFIG_FILE:-}" ] || set -- "$@" --ssh-config-file "$SSH_CONFIG_FILE"
[ -z "${IDENTITY_FILE:-}" ] || set -- "$@" --identity-file "$IDENTITY_FILE"

if [ -n "$PYTHON_PACKAGES" ]; then
    start_info "python checks: $PYTHON_PACKAGES"
else
    start_info 'python checks: skipped'
fi

exec "$TOOL" "$@"
