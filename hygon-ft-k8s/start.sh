#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
#
# Hygon FT 一键入口：
#   bash start.sh install              安装/更新 Kubernetes 容错组件
#   bash start.sh cleanup              清理安装器创建的 Kubernetes 资源和节点状态
#   bash start.sh status               查看组件、taint 和 FaultEvent 状态
#   bash start.sh recover node95       检测并恢复一个 taint 节点
#   bash start.sh help                 查看完整说明
#
# 可以直接修改下方“用户配置区”，也可以通过同名环境变量覆盖。

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# 登录节点执行恢复检查时使用的项目代码目录。
PROJECT_ROOT="${PROJECT_ROOT:-$ROOT_DIR}"

###############################################################################
# 用户配置区
###############################################################################

# 可选共享目录，只用于提供默认训练 YAML、日志和登录节点离线依赖路径。
# FT 控制面和恢复检查 Pod 的代码、依赖均来自 FT_CONTROLLER_IMAGE。
SHARED_ROOT="${SHARED_ROOT:-/public/home/user/code}"

# 登录节点 recover 命令的可选离线 Python 依赖目录。
# install/status/cleanup 和 Pod 内运行时不依赖该目录。
PYTHON_DEPS_DIR="${PYTHON_DEPS_DIR-$SHARED_ROOT/hygon-ft-runtime-deps}"

# check_status.sh 展示的外部持久化训练/launcher 日志目录。
FT_LOG_DIR="${FT_LOG_DIR:-/public/home/user/logs}"

# Operator、Webhook、NodeHealth Agent、ft-launcher 和恢复检查 Pod 使用的统一镜像。
# start.sh 不负责构建或分发镜像，执行 install 前必须确保目标节点能够拉取或已加载。
FT_CONTROLLER_IMAGE="${FT_CONTROLLER_IMAGE:-${RUNTIME_IMAGE:-hygon/ft-controller:latest}}"

# system 节点运行 operator/webhook；training 节点运行 nodehealth-agent。
# 支持逗号或空格分隔，两个集合不能重叠。
SYSTEM_NODES="${SYSTEM_NODES-node35}"
TRAINING_NODES="${TRAINING_NODES-node95,node98}"
APPLY_SYSTEM_TAINT="${APPLY_SYSTEM_TAINT:-true}"

# 可选的 PyTorchJob 或 Volcano Job YAML，仅用于 install 阶段前置检查。
# 使用 TRAIN_YAML= 可以只安装 FT 控制面，不检查具体训练任务。
TRAIN_YAML="${TRAIN_YAML-$SHARED_ROOT/hcu_megatron/examples/gpt3/volcano_train.yaml}"

# taint 恢复默认配置。RECOVERY_NODE 也可以由 recover 后的第一个参数提供。
# 需要去除taint的节点
RECOVERY_NODE="${RECOVERY_NODE:-}"
# 正常的参考节点
RECOVERY_NORMAL_NODES="${RECOVERY_NORMAL_NODES:-}"
# taint节点的检查次数
RECOVERY_CHECK_TIMES="${RECOVERY_CHECK_TIMES:-2}"
# taint节点的检查间隔时间
RECOVERY_CHECK_INTERVAL_SECONDS="${RECOVERY_CHECK_INTERVAL_SECONDS:-30}"
# 单次 IB 带宽检查或 NHC 检查允许执行的最长时间
RECOVERY_CHECK_TIMEOUT_SECONDS="${RECOVERY_CHECK_TIMEOUT_SECONDS:-600}"
RECOVERY_REMOVE_TAINT="${RECOVERY_REMOVE_TAINT:-true}"
# taint的键
RECOVERY_TAINT_KEY="${RECOVERY_TAINT_KEY:-ft.hygon.io/node-unhealthy}"
# taint的影响
RECOVERY_TAINT_EFFECT="${RECOVERY_TAINT_EFFECT:-NoSchedule}"
TAINT_RECOVERY_IMAGE="${TAINT_RECOVERY_IMAGE:-$FT_CONTROLLER_IMAGE}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# cleanup 等待 namespace 和工作负载删除完成的最长时间。
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-300}"
# 默认删除 install.sh 为 training 节点写入的 accelerator label。
REMOVE_ACCELERATOR_LABEL="${REMOVE_ACCELERATOR_LABEL:-true}"

###############################################################################

fail() {
  printf '[start] ERROR: %s\n' "$*" >&2
  exit 1
}

info() {
  printf '[start] %s\n' "$*"
}

usage() {
  cat <<'EOF'
用法：
  bash start.sh install
      使用 FT_CONTROLLER_IMAGE 安装或更新 Kubernetes 容错组件。该操作会
      创建/更新 CRD、RBAC、operator、webhook、nodehealth-agent、节点标签
      和 system 节点 taint；不会构建镜像、安装 Volcano 或提交训练任务。

  bash start.sh cleanup
      删除 install 创建的 hygon-ft namespace（包括其中的 Deployment、DaemonSet、
      Pod、Service、ConfigMap、Secret、ServiceAccount、Lease 和 FaultEvent），
      同时删除 MutatingWebhookConfiguration、CRD、ClusterRole/Binding，并撤销
      node-role.hygon.io/system、node-role.hygon.io/training 标签以及
      training 节点的 accelerator.hygon.io/enabled 标签和
      node-role.hygon.io/system、ft.hygon.io/node-unhealthy taint。
      不删除集群外的 FT_LOG_DIR，也不删除用户自行提交的训练 Job/Pod。

  bash start.sh status
      只读查看容错组件、FaultEvent、节点标签、节点 taint 和持久化日志。

  bash start.sh recover NODE [NORMAL_NODES]
      对一个 taint 节点执行恢复检测。NORMAL_NODES 可使用逗号分隔；
      不提供时由程序自动选择两个 Ready 且未被同类 taint 隔离的节点。
      所有轮次均通过后，默认只删除 ft.hygon.io/node-unhealthy:NoSchedule。

  bash start.sh help

常见示例：
  FT_CONTROLLER_IMAGE=hygon/ft-controller:latest \
  TRAIN_YAML=/path/to/train.yaml \
  SYSTEM_NODES=node35 \
  TRAINING_NODES=node37,node38 \
  bash start.sh install
  bash start.sh cleanup
  bash start.sh status
  bash start.sh recover node95 node39,node97

  # 非交互清理：
  CONFIRM_HYGON_FT_CLEANUP=yes bash start.sh cleanup

  # 非交互自动化；调用方必须先确认三个节点均无训练任务：
  CONFIRM_TAINT_RECOVERY=yes bash start.sh recover node95 node39,node97

  # 登录节点的 Python 已安装 kubernetes 时，恢复不需要离线依赖目录：
  PYTHON_DEPS_DIR= bash start.sh recover node95 node39,node97
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

validate_bool() {
  local name="$1"
  local value="$2"
  [[ "$value" == "true" || "$value" == "false" ]] ||
    fail "$name must be true or false"
}

validate_positive_integer() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] ||
    fail "$name must be a positive integer"
}

python_path_value() {
  local value="$ROOT_DIR"
  if [[ -n "$PYTHON_DEPS_DIR" ]]; then
    value="$PYTHON_DEPS_DIR:$value"
  fi
  if [[ -n "${PYTHONPATH:-}" ]]; then
    value="$value:$PYTHONPATH"
  fi
  printf '%s\n' "$value"
}

python_can_import_recovery_dependencies() {
  env PYTHONPATH="$(python_path_value)" "$PYTHON_BIN" -c \
    'import kubernetes; import hygon_ft.nodehealth.taint_recovery' \
    >/dev/null 2>&1
}

confirm_recovery_activity() {
  if [[ "${CONFIRM_TAINT_RECOVERY:-}" == "yes" ]]; then
    return 0
  fi
  if [[ ! -t 0 ]]; then
    fail "non-interactive recovery requires CONFIRM_TAINT_RECOVERY=yes"
  fi
  cat <<'EOF'
[start] taint 恢复会执行多轮 ibstat、ib_write_bw、NHC 和检查 Pod。
[start] 请先确认目标节点及两个正常参考节点均无训练任务。
[start] 输入 yes 继续：
EOF
  local answer
  IFS= read -r answer
  [[ "$answer" == "yes" ]] || fail "taint recovery cancelled"
}

confirm_cleanup_activity() {
  if [[ "${CONFIRM_HYGON_FT_CLEANUP:-}" == "yes" ]]; then
    return 0
  fi
  if [[ ! -t 0 ]]; then
    fail "non-interactive cleanup requires CONFIRM_HYGON_FT_CLEANUP=yes"
  fi
  cat <<'EOF'
[start] cleanup 将删除 hygon-ft namespace 内的全部资源、Hygon FT 的
[start] cluster-scoped webhook/RBAC/CRD，并撤销组件写入的节点标签和 taint。
[start] 集群外持久化日志和用户自行提交的训练 Job/Pod 将保留。
[start] 输入 yes 继续：
EOF
  local answer
  IFS= read -r answer
  [[ "$answer" == "yes" ]] || fail "cleanup cancelled"
}

run_install() {
  require_command kubectl
  require_command bash

  [[ -n "$FT_CONTROLLER_IMAGE" ]] || fail "FT_CONTROLLER_IMAGE is empty"
  if [[ -n "$TRAIN_YAML" ]]; then
    [[ -f "$TRAIN_YAML" ]] || fail "TRAIN_YAML does not exist: $TRAIN_YAML"
  fi

  info "Kubernetes context: $(kubectl config current-context)"
  info "controller image: $FT_CONTROLLER_IMAGE"
  if [[ -n "$TRAIN_YAML" ]]; then
    info "validate workload: $TRAIN_YAML"
  else
    info "validate workload: skipped (control-plane-only install)"
  fi
  if [[ -n "$SYSTEM_NODES" ]]; then
    info "system nodes: $SYSTEM_NODES"
  else
    info "system nodes: use existing node-role.hygon.io/system=true labels"
  fi
  if [[ -n "$TRAINING_NODES" ]]; then
    info "training nodes: $TRAINING_NODES"
  else
    info "training nodes: use existing node-role.hygon.io/training=true labels"
  fi

  export FT_CONTROLLER_IMAGE SYSTEM_NODES TRAINING_NODES APPLY_SYSTEM_TAINT
  bash "$ROOT_DIR/scripts/install.sh" "$TRAIN_YAML"
}

run_cleanup() {
  require_command kubectl
  require_command bash
  validate_positive_integer CLEANUP_TIMEOUT_SECONDS "$CLEANUP_TIMEOUT_SECONDS"
  validate_bool REMOVE_ACCELERATOR_LABEL "$REMOVE_ACCELERATOR_LABEL"
  [[ -f "$ROOT_DIR/scripts/uninstall.sh" ]] ||
    fail "cleanup script does not exist: $ROOT_DIR/scripts/uninstall.sh"

  info "Kubernetes context: $(kubectl config current-context)"
  info "cleanup timeout: ${CLEANUP_TIMEOUT_SECONDS}s"
  confirm_cleanup_activity

  export CLEANUP_TIMEOUT_SECONDS REMOVE_ACCELERATOR_LABEL
  export CONFIRM_HYGON_FT_CLEANUP=yes
  bash "$ROOT_DIR/scripts/uninstall.sh"
}

run_status() {
  require_command kubectl
  export FT_LOG_DIR
  bash "$ROOT_DIR/scripts/check_status.sh"
}

run_recover() {
  local node="${1:-$RECOVERY_NODE}"
  local normal_nodes="${2:-$RECOVERY_NORMAL_NODES}"
  local -a command

  [[ -n "$node" ]] || fail "target node is required: bash start.sh recover NODE [NORMAL_NODES]"
  validate_positive_integer RECOVERY_CHECK_TIMES "$RECOVERY_CHECK_TIMES"
  validate_positive_integer RECOVERY_CHECK_INTERVAL_SECONDS "$RECOVERY_CHECK_INTERVAL_SECONDS"
  validate_positive_integer RECOVERY_CHECK_TIMEOUT_SECONDS "$RECOVERY_CHECK_TIMEOUT_SECONDS"
  validate_bool RECOVERY_REMOVE_TAINT "$RECOVERY_REMOVE_TAINT"
  require_command kubectl
  require_command "$PYTHON_BIN"

  if ! python_can_import_recovery_dependencies; then
    if [[ -n "$PYTHON_DEPS_DIR" ]]; then
      fail "cannot import kubernetes/hygon_ft with PYTHON_DEPS_DIR=$PYTHON_DEPS_DIR"
    fi
    fail "cannot import kubernetes/hygon_ft; install kubernetes for $PYTHON_BIN or set PYTHON_DEPS_DIR"
  fi

  confirm_recovery_activity

  command=(
    "$PYTHON_BIN" -m hygon_ft.nodehealth.taint_recovery
    --node "$node"
    --namespace hygon-ft
    --image "$TAINT_RECOVERY_IMAGE"
    --taint-key "$RECOVERY_TAINT_KEY"
    --taint-effect "$RECOVERY_TAINT_EFFECT"
    --check-times "$RECOVERY_CHECK_TIMES"
    --check-interval-seconds "$RECOVERY_CHECK_INTERVAL_SECONDS"
    --check-timeout-seconds "$RECOVERY_CHECK_TIMEOUT_SECONDS"
  )
  if [[ -n "$normal_nodes" ]]; then
    command+=(--normal-nodes "$normal_nodes")
  fi
  if [[ "$RECOVERY_REMOVE_TAINT" == "true" ]]; then
    command+=(--remove-taint)
  fi

  info "target taint node: $node"
  if [[ -n "$normal_nodes" ]]; then
    info "normal reference nodes: $normal_nodes"
  else
    info "normal reference nodes: auto-select two Ready non-tainted nodes"
  fi
  info "rounds: $RECOVERY_CHECK_TIMES; interval: ${RECOVERY_CHECK_INTERVAL_SECONDS}s"
  info "checker image: $TAINT_RECOVERY_IMAGE"
  info "remove taint after PASS: $RECOVERY_REMOVE_TAINT"

  env PYTHONPATH="$(python_path_value)" "${command[@]}"
}

main() {
  local action="${1:-help}"
  if (( $# > 0 )); then
    shift
  fi
  case "$action" in
    install|start)
      (( $# == 0 )) || fail "$action does not accept positional arguments"
      run_install
      ;;
    cleanup|clean|uninstall)
      (( $# == 0 )) || fail "$action does not accept positional arguments"
      run_cleanup
      ;;
    status)
      (( $# == 0 )) || fail "status does not accept positional arguments"
      run_status
      ;;
    recover)
      (( $# <= 2 )) || fail "recover accepts NODE and optional NORMAL_NODES"
      run_recover "$@"
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      fail "unknown action: $action (use: bash start.sh help)"
      ;;
  esac
}

main "$@"
