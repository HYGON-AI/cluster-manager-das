#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# 功能验证总入口：每项直接调用 hcu-cluster-run，不用 Shell 函数分派。
# 默认基础只读检测；主动/IB/容器维护均 dry-run。
# RUN_PROFILES=yes: RCCL 完整二进制测试（短小参数）。
# RCCL 用例不传 profile/--tests，验证默认十项完整二进制验收。
# 基准必须覆盖当前卡数全部十项；短小参数仅验证接口/报告，不代表生产性能达标。
# contexts/ 输入须在各节点/容器内同路径可见。不会申请资源或变更容器。
set -uo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || exit 1
if ! pwd -P >/dev/null 2>&1; then
    printf '[WARN] 当前目录已被替换；切换到工程根目录：%s\n' "$ROOT" >&2
    cd -- "$ROOT" || exit 1
fi
HCU_CLUSTER_RUN=${HCU_CLUSTER_RUN:-$ROOT/bin/hcu-cluster-run}
if [[ "${1-}" == --help || "${1-}" == -h || "${1-}" == --list ]]; then
    cat <<'HELP'
一次执行全部接口，不接受 --case 或位置参数筛选。
HOSTFILE=./hostfile ENV_SCRIPT=/share/env.sh bash scripts/test.sh
RUN_PROFILES=yes ...    RCCL 完整二进制测试
RUN_NETWORK=yes ...     IB server/client 带宽测试
变量：HOSTFILE ENV_SCRIPT SHARED_ENV_SCRIPT NODE_LOCAL_ENV_SCRIPT CONTAINER_ENV_SCRIPT
      CONTROLLER_ENV_SCRIPT CONTROLLER_PYTHON PYTHON_BIN HCU_ENVCHECK_PYTHON
      CONTAINER_NAME IMAGE CONTAINER_SSH_PORT TRANSPORT GROUP_SIZE ACTIVE_SLOTS
      RCCL_GPUS_PER_NODE TIMEOUT IB_GROUP_SIZE OUTPUT_DIR
环境BLOCKED/FAIL不作门禁；工具异常、缺报告、错节点范围、缺launch artifact返回非零。
PRECHECK_FAILED单列BLOCKED_NOT_EXECUTED，不宣称完成真实测试。
HELP
    exit 0
fi
if (( $# )); then printf '[ERROR] 不支持筛选参数；一次执行全部接口（--help 查看变量）\n' >&2; exit 2; fi

# 控制端环境仅由专用变量指定；绝不在此 source ENV_SCRIPT。
if [[ -n "${CONTROLLER_ENV_SCRIPT:-}" ]]; then
    if [[ "$CONTROLLER_ENV_SCRIPT" != /* || ! -r "$CONTROLLER_ENV_SCRIPT" ]]; then
        printf '[ERROR] CONTROLLER_ENV_SCRIPT必须是入口节点可读绝对路径\n' >&2; exit 2
    fi
    set +u
    set -e
    source "$CONTROLLER_ENV_SCRIPT"
    set +e
    set -u
fi
PYTHON_BIN=${CONTROLLER_PYTHON:-${PYTHON_BIN:-${HCU_ENVCHECK_PYTHON:-}}}
if [[ -z "$PYTHON_BIN" ]]; then
    for candidate in python3 python3.14 python3.13 python3.12 python3.11 python3.10 python; do
        if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys;sys.exit(sys.version_info < (3,10))' >/dev/null 2>&1; then
            PYTHON_BIN=$candidate; break
        fi
    done
fi
if [[ -z "$PYTHON_BIN" ]] || ! "$PYTHON_BIN" -c 'import sys;sys.exit(sys.version_info < (3,10))'; then
    printf '[ERROR] 配置CONTROLLER_ENV_SCRIPT或CONTROLLER_PYTHON（>=3.10，仅标准库）\n' >&2; exit 2
fi
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
CONTROLLER_OPTIONS=(--controller-python "$PYTHON_BIN")
HOSTFILE=${HOSTFILE:-./hostfile}
ENV_SCRIPT=${ENV_SCRIPT:-$ROOT/env.sh}
SHARED_ENV_SCRIPT=${SHARED_ENV_SCRIPT:-$ENV_SCRIPT}
NODE_LOCAL_ENV_SCRIPT=${NODE_LOCAL_ENV_SCRIPT:-$ENV_SCRIPT}
CONTAINER_ENV_SCRIPT=${CONTAINER_ENV_SCRIPT:-$ENV_SCRIPT}
CONTAINER_NAME=${CONTAINER_NAME:-hcu-worker}
IMAGE=${IMAGE:-image:tag}
CONTAINER_SSH_PORT=${CONTAINER_SSH_PORT:-25901}
TRANSPORT=${TRANSPORT:-ssh}
GROUP_SIZE=${GROUP_SIZE:-1}
IB_GROUP_SIZE=${IB_GROUP_SIZE:-2}
ACTIVE_SLOTS=${ACTIVE_SLOTS:-1}
RCCL_GPUS_PER_NODE=${RCCL_GPUS_PER_NODE:-8}  # Binary suite uses real card-count baselines.
TIMEOUT=${TIMEOUT:-120}
OUTPUT_DIR=${OUTPUT_DIR:-./manual_test_results}
mkdir -p -- "$OUTPUT_DIR" || exit 1
OUTPUT_DIR=$(cd -- "$OUTPUT_DIR" && pwd) || exit 1
RUN_DIR=$(mktemp -d "$OUTPUT_DIR/run_$(date +%Y%m%d_%H%M%S)_XXXXXX") || exit 1
mkdir -p "$RUN_DIR/logs" "$RUN_DIR/contexts"
CALLS="$RUN_DIR/calls.tsv"
printf 'case\treturncode\tlog_returncode\n' > "$CALLS"
COMMON_OPTIONS=(--transport "$TRANSPORT")
PROFILE_OPTIONS=(--dry-run); PROFILE_MODE=dry
NETWORK_OPTIONS=(--dry-run); NETWORK_MODE=dry
[[ "${RUN_PROFILES:-dry-run}" == yes || "${RUN_PROFILES:-}" == 1 ]] && { PROFILE_OPTIONS=(); PROFILE_MODE=real; }
[[ "${RUN_NETWORK:-dry-run}" == yes || "${RUN_NETWORK:-}" == 1 ]] && { NETWORK_OPTIONS=(); NETWORK_MODE=real; }

# 只生成本地输入，不访问目标节点。随机token验证目标env.sh确实执行。
"$PYTHON_BIN" - "$RUN_DIR" "$HOSTFILE" "$SHARED_ENV_SCRIPT" "$NODE_LOCAL_ENV_SCRIPT" "$CONTAINER_ENV_SCRIPT" <<'FIXTURES' || exit 1
import json, pathlib, shlex, sys, time, uuid
from cluster_run.hostfile import read_nodes
out = pathlib.Path(sys.argv[1])
nodes = read_nodes(pathlib.Path(sys.argv[2]))
token = uuid.uuid4().hex
(out / 'meta.json').write_text(json.dumps({'started': time.time(), 'nodes': nodes, 'token': token}), encoding='utf-8')
# MPI计划至少两节点；不足时使用明确的plan-only虚拟节点，不发起连接。
plan_nodes = list(nodes[:2]) if len(nodes) >= 2 else ['hcu-plan-node01', 'hcu-plan-node02']
(out / 'launcher-hostfile').write_text('\n'.join(plan_nodes) + '\n', encoding='utf-8')
for scenario, env_script in zip(('shared-conda','node-local-conda','per-node-container'), sys.argv[3:6]):
    if not env_script.startswith('/'):
        raise SystemExit('--env-script target paths must be absolute: ' + env_script)
    text = 'set -e\nsource ' + shlex.quote(env_script) + '\nexport HCU_ACCEPTANCE_TOKEN=' + token + '\n'
    (out / 'contexts' / (scenario + '.env.sh')).write_text(text, encoding='utf-8')
    # Keep RCCL's short workload in the target environment wrapper.
    (out / 'contexts' / (scenario + '.rccl-env.sh')).write_text(
        text + 'export RCCL_ARG_B=4 RCCL_ARG_E=4K RCCL_ARG_N=2 RCCL_ARG_W=0\n', encoding='utf-8')
FIXTURES
SHARED_RCCL_ENV_SCRIPT=$RUN_DIR/contexts/shared-conda.rccl-env.sh
NODE_LOCAL_RCCL_ENV_SCRIPT=$RUN_DIR/contexts/node-local-conda.rccl-env.sh
CONTAINER_RCCL_ENV_SCRIPT=$RUN_DIR/contexts/per-node-container.rccl-env.sh
LOCAL_RC=0
# 集成用例自身运行本脚本时防递归；一般用户执行始终运行本地单测。
if [[ "${HCU_ACCEPTANCE_NESTED_TEST:-}" != 1 ]]; then
    while IFS= read -r -d '' script; do bash -n "$script" || LOCAL_RC=1; done < <(find "$ROOT" -type f -name '*.sh' -not -path '*/.git/*' -print0)
    bash -n "$ROOT/bin/hcu-cluster-run" || LOCAL_RC=1
    "$PYTHON_BIN" -m unittest discover -s "$ROOT/tests" -q || LOCAL_RC=1
fi
printf '[LOCAL] rc=%s\n[RUN_DIR] %s\n' "$LOCAL_RC" "$RUN_DIR"

# 固定矩阵是预期清单，不能仅按实际产出报告反推是否齐全。
cat > "$RUN_DIR/expected.tsv" <<CASES
case	kind	scenario	operation	mode	directory	hostfile	env_script	context
shared-conda-platform	basic	shared-conda	platform	real	shared-conda/platform	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-resource	basic	shared-conda	resource	real	shared-conda/resource	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-base	basic	shared-conda	platform,resource	real	shared-conda/base	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-rccl-tests	active	shared-conda	rccl	$PROFILE_MODE	shared-conda/rccl-tests	$HOSTFILE	$SHARED_RCCL_ENV_SCRIPT	no
shared-conda-ib-write-bw	active	shared-conda	ib-write-bw	$NETWORK_MODE	shared-conda/ib-write-bw	$HOSTFILE	$SHARED_ENV_SCRIPT	no
node-local-conda-platform	basic	node-local-conda	platform	real	node-local-conda/platform	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-resource	basic	node-local-conda	resource	real	node-local-conda/resource	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-base	basic	node-local-conda	platform,resource	real	node-local-conda/base	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-rccl-tests	active	node-local-conda	rccl	$PROFILE_MODE	node-local-conda/rccl-tests	$HOSTFILE	$NODE_LOCAL_RCCL_ENV_SCRIPT	no
node-local-conda-ib-write-bw	active	node-local-conda	ib-write-bw	$NETWORK_MODE	node-local-conda/ib-write-bw	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
per-node-container-platform	basic	per-node-container	platform	real	per-node-container/platform	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-resource	basic	per-node-container	resource	real	per-node-container/resource	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-base	basic	per-node-container	platform,resource	real	per-node-container/base	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-rccl-tests	active	per-node-container	rccl	$PROFILE_MODE	per-node-container/rccl-tests	$HOSTFILE	$CONTAINER_RCCL_ENV_SCRIPT	no
per-node-container-ib-write-bw	active	per-node-container	ib-write-bw	$NETWORK_MODE	per-node-container/ib-write-bw	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-status	status	per-node-container	container-status	real	per-node-container/container-status	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-create	lifecycle	per-node-container	container-create	dry	per-node-container/container-create	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-recreate	lifecycle	per-node-container	container-recreate	dry	per-node-container/container-recreate	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-delete	lifecycle	per-node-container	container-delete	dry	per-node-container/container-delete	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
help	help	-	--help	dry	help	-	-	no
version	help	-	--version	dry	version	-	-	no
CASES

# shared-conda-platform: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] shared-conda-platform\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda platform -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/platform" 2>&1 | tee "$RUN_DIR/logs/shared-conda-platform.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-platform\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-platform rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-platform.log"; then LOCAL_RC=1; fi

# shared-conda-resource: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] shared-conda-resource\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/resource" 2>&1 | tee "$RUN_DIR/logs/shared-conda-resource.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-resource\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-resource rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-resource.log"; then LOCAL_RC=1; fi

# shared-conda-base: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] shared-conda-base\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda platform,resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/base" 2>&1 | tee "$RUN_DIR/logs/shared-conda-base.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-base\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-base rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-base.log"; then LOCAL_RC=1; fi

# shared-conda-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] shared-conda-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_RCCL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/rccl-tests" \
    --group-size "$GROUP_SIZE" --gpus-per-node "$RCCL_GPUS_PER_NODE" --timeout "$TIMEOUT" \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-rccl-tests.log"; then LOCAL_RC=1; fi

# shared-conda-ib-write-bw: ib；节点范围 $HOSTFILE；mode=$NETWORK_MODE
printf "[CALL] shared-conda-ib-write-bw\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda ib-write-bw -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --group-size "$IB_GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --ib-message-bytes 4096 --ib-iterations 10 --ib-concurrency 1 \
    --output-dir "$RUN_DIR/shared-conda/ib-write-bw" \
    --timeout "$TIMEOUT" "${NETWORK_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-ib-write-bw.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-ib-write-bw\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-ib-write-bw rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-ib-write-bw.log"; then LOCAL_RC=1; fi

# node-local-conda-platform: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] node-local-conda-platform\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda platform -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/platform" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-platform.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-platform\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-platform rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-platform.log"; then LOCAL_RC=1; fi

# node-local-conda-resource: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] node-local-conda-resource\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/resource" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-resource.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-resource\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-resource rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-resource.log"; then LOCAL_RC=1; fi

# node-local-conda-base: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] node-local-conda-base\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda platform,resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/base" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-base.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-base\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-base rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-base.log"; then LOCAL_RC=1; fi

# node-local-conda-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] node-local-conda-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_RCCL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/rccl-tests" \
    --group-size "$GROUP_SIZE" --gpus-per-node "$RCCL_GPUS_PER_NODE" --timeout "$TIMEOUT" \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-rccl-tests.log"; then LOCAL_RC=1; fi

# node-local-conda-ib-write-bw: ib；节点范围 $HOSTFILE；mode=$NETWORK_MODE
printf "[CALL] node-local-conda-ib-write-bw\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda ib-write-bw -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --group-size "$IB_GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --ib-message-bytes 4096 --ib-iterations 10 --ib-concurrency 1 \
    --output-dir "$RUN_DIR/node-local-conda/ib-write-bw" \
    --timeout "$TIMEOUT" "${NETWORK_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-ib-write-bw.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-ib-write-bw\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-ib-write-bw rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-ib-write-bw.log"; then LOCAL_RC=1; fi

# per-node-container-platform: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] per-node-container-platform\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container platform -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/platform" \
    --container "$CONTAINER_NAME" -i "$IMAGE" 2>&1 | tee "$RUN_DIR/logs/per-node-container-platform.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-platform\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-platform rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-platform.log"; then LOCAL_RC=1; fi

# per-node-container-resource: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] per-node-container-resource\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/resource" \
    --container "$CONTAINER_NAME" -i "$IMAGE" 2>&1 | tee "$RUN_DIR/logs/per-node-container-resource.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-resource\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-resource rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-resource.log"; then LOCAL_RC=1; fi

# per-node-container-base: basic；节点范围 $HOSTFILE；mode=real
printf "[CALL] per-node-container-base\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container platform,resource -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/base" \
    --container "$CONTAINER_NAME" -i "$IMAGE" 2>&1 | tee "$RUN_DIR/logs/per-node-container-base.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-base\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-base rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-base.log"; then LOCAL_RC=1; fi

# per-node-container-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] per-node-container-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_RCCL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/rccl-tests" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --group-size "$GROUP_SIZE" --gpus-per-node "$RCCL_GPUS_PER_NODE" --timeout "$TIMEOUT" \
    "${PROFILE_OPTIONS[@]}" \
    --container-ssh-port "$CONTAINER_SSH_PORT" 2>&1 | tee "$RUN_DIR/logs/per-node-container-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-rccl-tests.log"; then LOCAL_RC=1; fi

# per-node-container-ib-write-bw: ib；节点范围 $HOSTFILE；mode=$NETWORK_MODE
printf "[CALL] per-node-container-ib-write-bw\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container ib-write-bw -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --group-size "$IB_GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --ib-message-bytes 4096 --ib-iterations 10 --ib-concurrency 1 \
    --output-dir "$RUN_DIR/per-node-container/ib-write-bw" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --timeout "$TIMEOUT" "${NETWORK_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-ib-write-bw.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-ib-write-bw\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-ib-write-bw rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-ib-write-bw.log"; then LOCAL_RC=1; fi

# per-node-container-container-status: status；节点范围 $HOSTFILE；mode=real
printf "[CALL] per-node-container-container-status\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container container-status -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --container "$CONTAINER_NAME" \
    -i "$IMAGE" 2>&1 | tee "$RUN_DIR/logs/per-node-container-container-status.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-container-status\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-container-status rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-container-status.log"; then LOCAL_RC=1; fi

# per-node-container-container-create: 验证 --port 参数与维护入口；仅 dry-run，不声称完成真实免密
printf "[CALL] per-node-container-container-create\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container container-create -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --container "$CONTAINER_NAME" \
    -i "$IMAGE" \
    --port "$CONTAINER_SSH_PORT" --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-container-create.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-container-create\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-container-create rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-container-create.log"; then LOCAL_RC=1; fi

# per-node-container-container-recreate: lifecycle；节点范围 $HOSTFILE；mode=dry
printf "[CALL] per-node-container-container-recreate\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container container-recreate -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --container "$CONTAINER_NAME" \
    -i "$IMAGE" \
    --port "$CONTAINER_SSH_PORT" --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-container-recreate.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-container-recreate\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-container-recreate rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-container-recreate.log"; then LOCAL_RC=1; fi

# per-node-container-container-delete: lifecycle；节点范围 $HOSTFILE；mode=dry
printf "[CALL] per-node-container-container-delete\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container container-delete -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --container "$CONTAINER_NAME" \
    --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-container-delete.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-container-delete\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-container-delete rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-container-delete.log"; then LOCAL_RC=1; fi

# help: help；节点范围 -；mode=dry
printf "[CALL] help\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    --help 2>&1 | tee "$RUN_DIR/logs/help.log"
case_rc=("${PIPESTATUS[@]}")
printf "help\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] help rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/help.log"; then LOCAL_RC=1; fi

# version: help；节点范围 -；mode=dry
printf "[CALL] version\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    --version 2>&1 | tee "$RUN_DIR/logs/version.log"
case_rc=("${PIPESTATUS[@]}")
printf "version\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] version rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/version.log"; then LOCAL_RC=1; fi

# BEGIN_ACCEPTANCE_VALIDATOR
"$PYTHON_BIN" - "$RUN_DIR" "$LOCAL_RC" <<'VALIDATOR' || LOCAL_RC=1
import collections
import csv
import json
import pathlib
import re
import sys
from cluster_run.hostfile import read_nodes


class ContractError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ContractError(message)


def load_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def expand_nodes(groups):
    if isinstance(groups, list):
        require(all(isinstance(item, dict) and item.get('node') for item in groups), 'invalid node records')
        require(len({item['node'] for item in groups}) == len(groups), 'duplicate node records')
        groups = {item['node']: item for item in groups}
    require(isinstance(groups, dict) and bool(groups), 'missing node records')
    result = {}
    for key, item in groups.items():
        require(isinstance(item, dict), 'invalid node record')
        for node in item.get('members', [key]):
            require(node not in result, 'duplicate node: ' + node)
            result[node] = item
    return result


def check_completion(record, *, allow_cancelled=False):
    require(not record.get('interrupted'), 'interrupted execution cannot pass acceptance')
    require(record.get('status') != 'CLEANUP_UNCONFIRMED' and
            (allow_cancelled or record.get('status') != 'CANCELLED'), 'execution cancelled or cleanup unconfirmed')
    cleanup = record.get('cleanup')
    if cleanup is not None:
        require(isinstance(cleanup, dict), 'invalid cleanup evidence')
        # dataclasses.asdict(CancellationReport) has nodes/run_token, NOT the
        # computed confirmed property. Per-node script records carry one item.
        records = cleanup.get('nodes') if 'nodes' in cleanup else {'node': cleanup}
        require(isinstance(records, dict) and bool(records), 'missing cleanup node evidence')
        require(cleanup.get('confirmed') is not False and all(
            isinstance(item, dict) and item.get('status') == 'CONFIRMED' for item in records.values()),
            'cleanup node not CONFIRMED')


def check_worker_result(record, *, allow_cancelled=False):
    check_completion(record, allow_cancelled=allow_cancelled)
    status, rc = record.get('status'), record.get('returncode')
    require(type(rc) is int, 'missing worker return code')
    require((status == 'PASS' and rc == 0) or (status == 'INCOMPLETE' and rc == 2) or
            (status == 'FAIL' and rc != 0 and rc not in {130, 143}) or
            (allow_cancelled and status == 'CANCELLED' and rc in {130, 143}), 'worker status/exit-code mismatch')


def aggregate_status(records):
    return next((status for status in ('FAIL', 'CANCELLED', 'BLOCKED', 'PRECHECK_FAILED', 'SKIPPED', 'INCOMPLETE')
                 if any(record.get('status') == status for record in records)), 'PASS')


def check_active_launch(launch, members, operation, profile, requested):
    effective = 'rccl-tests'
    require(launch.get('launcher') == effective, 'effective launcher mismatch')
    require(launch.get('execution_profile') == profile, 'group profile mismatch')
    require(launch.get('requested_launcher') == requested, 'requested launcher mismatch')
    require(launch.get('leader') == members[0], 'group leader mismatch')
    dispatched = members[:1]
    commands = launch.get('commands') or []
    require(len(commands) == len(dispatched) and {item['node'] for item in commands} == set(dispatched),
            'launch command node coverage mismatch')
    require(all(item.get('command') for item in commands), 'empty node launch command')
    return dispatched


def verify_staged_case(path, case, rc, nodes, result, meta):
    from hcu_envcheck.reporting.facts import load_group, relative_source
    from hcu_envcheck.reporting.log_store import open_log
    report = load_json(path)

    def confirmed_cleanup(facts, stage, members):
        cleanup = [row for row in facts.table('cleanup') if row['stage_id'] == stage]
        source = 'normal' if any(row['source'] == 'normal' for row in cleanup) else 'fallback'
        selected = [row for row in cleanup if row['source'] == source]
        require(selected and {row['node'] for row in selected} == set(members) and
                all(row['status'] == 'CONFIRMED' for row in selected), 'missing staged cleanup node proof')
    require(type(report.get('schema_version')) is int and report['schema_version'] == 1,
            'invalid staged schema_version')
    require(path.stat().st_mtime >= float(meta['started']) - 1, 'stale report')
    require(report.get('final') is True, 'staged report is not final')
    control, state = report.get('control') or {}, path.parent
    require(control.get('state_layout') == 'compact-group-v2', 'unsupported staged layout')
    require(control.get('scenario') == case['scenario'] and control.get('operation') == case['operation'],
            'staged scenario/operation mismatch')
    require(control.get('env_script') == case['env_script'], 'report env_script mismatch')
    require(isinstance(control.get('container'), str) and
            (bool(control['container']) if case['scenario'] == 'per-node-container' else control['container'] == ''),
            'report execution scope mismatch')
    status = report.get('status')
    require(status in {'PASS', 'WARN', 'FAIL'}, 'invalid staged execution status')
    require(rc == (0 if status in {'PASS', 'WARN'} else 1), 'staged status/exit-code mismatch')
    groups, stages = report.get('groups'), report.get('stages')
    require(isinstance(groups, list) and bool(groups) and isinstance(stages, list) and bool(stages),
            'missing staged group/stage plan')
    require(len({group['group'] for group in groups}) == len(groups) and
            len({stage['stage_id'] for stage in stages}) == len(stages), 'duplicate staged plan identity')
    if case['operation'] == 'rccl':
        require([stage['item'] for stage in stages] == ['all_reduce', 'all_gather', 'broadcast', 'reduce',
                'reduce_scatter', 'gather', 'scatter', 'alltoall', 'alltoallv', 'sendrecv'], 'RCCL suite is incomplete')
    require(all(str(stage.get('iteration')) == '0' for stage in stages), 'unexpected staged repetition')
    expected = [(stage['stage_id'], group['group']) for stage in stages for group in groups]
    records = report.get('results') or []
    require([(row['stage_id'], row['group']) for row in records] == expected, 'staged result coverage/order mismatch')
    require(report.get('planned') == len(expected), 'staged planned count mismatch')
    preflight = report.get('preflight') or []
    require([row['group'] for row in preflight] == [group['group'] for group in groups], 'preflight coverage mismatch')
    grouped, facts_by_group = [], {}
    for group in groups:
        ident = group['group']
        members = group['nodes'].split(',')
        require(bool(members) and all(members) and len(set(members)) == len(members), 'invalid staged group members')
        grouped.extend(members)
        facts = load_group(state, ident)
        require(facts is not None, 'missing authoritative compact facts')
        facts_by_group[ident] = facts
        tasks = {row['stage_id']: row for row in facts.table('tasks')}
        prepare = tasks.get('preflight') or {}
        require(prepare.get('status') in {'PASS','WARN','FAIL','NA'} and prepare.get('cleanup') == 'CONFIRMED',
                'preflight not closed with confirmed cleanup')
        confirmed_cleanup(facts, 'preflight', members)
        with open_log(state / 'prepared' / ident / 'group.log') as stream:
            while stream.read(65536):
                pass
    omitted = report.get('non_participating') or {}
    omitted = omitted.get('nodes', []) if isinstance(omitted, dict) else []
    require(len(grouped) == len(set(grouped)) and grouped == [node for node in nodes if node not in omitted]
            and set(grouped).isdisjoint(omitted) and set(grouped + omitted) == set(nodes),
            'staged group coverage differs from hostfile')
    for row in records:
        group, stage = row['group'], row['stage_id']
        facts = facts_by_group[group]
        task = next((task for task in facts.table('tasks') if task['stage_id'] == stage), {})
        require(task and task.get('phase') == 'execute', 'missing staged execution task')
        members = next(group_row['nodes'].split(',') for group_row in groups if group_row['group'] == group)
        require(row.get('nodes') == ','.join(members), 'staged result member mismatch')
        if not row.get('dispatched'):
            require(task.get('status') == row.get('status') == 'BLOCKED', 'undispatched staged task is not BLOCKED')
            for node in members:
                result['node_execution'].setdefault(node, 'NOT_EXECUTED')
            continue
        require(task.get('cleanup') == row.get('cleanup') == 'CONFIRMED', 'staged cleanup unconfirmed')
        confirmed_cleanup(facts, stage, members)
        require(task.get('status') in {'PASS','WARN','FAIL'} and row.get('status') in {'PASS','WARN','FAIL'},
                'interrupted or incomplete staged task')
        require(str(task.get('exit_code', '')).lstrip('-').isdigit(), 'missing staged raw exit code')
        require(row['status'] != 'PASS' or (task['status'] == 'PASS' and str(task['exit_code']) == '0'),
                'staged PASS contradicts execution facts')
        prefix = f'stages/{stage}/groups/{group}/'
        with open_log(state / prefix / 'group.log') as stream:
            while stream.read(65536):
                pass
        node_rows = [node for node in facts.table('nodes') if node['stage_id'] == stage]
        require([node['node'] for node in node_rows] == members, 'staged node execution coverage mismatch')
        if case['operation'] == 'ib-write-bw':
            saved = [item for item in facts.table('ib') if item['stage_id'] == stage]
            require(len(saved) == 1 and saved[0]['token'] == task['token'], 'missing authoritative IB metadata')
            evidence = saved[0]['metadata']
            require(evidence.get('selected_nodes') == members and evidence.get('run_token') == task['token'], 'IB group inventory identity mismatch')
            pairs = evidence.get('pairs')
            require(isinstance(pairs, list), 'missing IB pair artifacts')
            require(evidence.get('status') != 'PASS' or bool(pairs), 'IB PASS without server/client measurements')
            for pair in pairs:
                require(pair.get('source') in members and pair.get('destination') in members, 'IB pair outside group')
                commands = pair.get('commands') or []
                require(len(commands) == 2, 'IB pair missing server/client command evidence')
                for command in commands:
                    require(type(command.get('returncode')) is int and command.get('argv'), 'IB endpoint missing command/returncode')
                    for name in ('stdout_path', 'stderr_path'):
                        source = relative_source(command.get(name))
                        require(source.startswith(prefix), 'IB endpoint log outside its task')
                        with open_log(state / source) as stream:
                            while stream.read(65536):
                                pass
            result['pairs_executed'] = result.get('pairs_executed', 0) + len(pairs)
        for node in members:
            result['node_execution'][node] = 'EXECUTION_ATTEMPTED'
    for node in omitted:
        result['node_execution'][node] = 'NOT_EXECUTED'
    result.update(report=str(path), status=status, nodes_reported=grouped,
                  execution_state='EXECUTION_ATTEMPTED' if any(row.get('dispatched') for row in records) else 'BLOCKED_NOT_EXECUTED')
    return result


def verify_case(root, case, receipt, meta):
    ident, kind, mode = case['case'], case['kind'], case['mode']
    log = root / 'logs' / (ident + '.log')
    result = dict(case=ident, kind=kind, mode=mode, scenario=case['scenario'], operation=case['operation'],
                  returncode=None, log=str(log), report=None, nodes_requested=[], nodes_reported=[],
                  execution_state='INTERFACE_ERROR', node_execution={})
    try:
        require(receipt is not None, 'case was not called')
        rc = int(receipt['returncode'])
        result['returncode'] = rc
        require(int(receipt['log_returncode']) == 0, 'failed to save command log')
        require(log.is_file(), 'missing command log')
        text = re.sub(r'\x1b\[[0-9;]*m', '', log.read_text(encoding='utf-8'))
        require(not re.search(r'RESULT\s+TOOL_ERROR|Traceback \(most recent call last\)', text), 'tool exception in log')
        if kind == 'help':
            require(rc == 0 and 'hcu-cluster-run' in text, 'help/version did not execute successfully')
            result['execution_state'] = 'EXECUTED'
            return result
        nodes = read_nodes(pathlib.Path(case['hostfile']))
        result['nodes_requested'] = list(nodes)
        # This is a terminal-only, deliberately report-free precheck contract.
        # It is NOT evidence that any compute test has executed.
        if re.search(r'^RESULT\s+PRECHECK_FAILED\s*$', text, re.M):
            require(mode == 'real' and rc == 3, 'unexpected precheck result/exit code')
            require(re.search(r'nodes?=\S+.*code=\S+.*reason=', text), 'precheck missing node/reason evidence')
            result.update(status='PRECHECK_FAILED', execution_state='BLOCKED_NOT_EXECUTED')
            result['node_execution'] = {node: 'NOT_EXECUTED' for node in nodes}
            return result
        if mode == 'real' and kind in {'basic', 'active', 'ib'}:
            staged = [path for path in (root / case['directory']).rglob('report.json') if path.parent.name == '.state']
            if staged:
                require(len(staged) == 1, 'expected exactly one current staged report')
                return verify_staged_case(staged[0], case, rc, nodes, result, meta)
        require(rc in ({0} if mode == 'dry' else {0, 2}), 'unexpected tool exit code: ' + str(rc))
        if kind in {'status', 'lifecycle'}:
            match = re.search(r'^RESULT\s+(\S+)\s*$', text, re.M)
            total = re.search(r'^NODES\s+total=(\d+)', text, re.M)
            require(match is not None and total is not None, 'missing terminal receipt')
            require(int(total[1]) == len(nodes), 'terminal receipt node count differs from hostfile')
            require(match[1] in ({'DRY_RUN'} if mode == 'dry' else {'PASS','FAIL','BLOCKED','INCOMPLETE'}), 'invalid terminal status')
            require(rc == (0 if match[1] in {'PASS','DRY_RUN'} else 2), 'terminal status/exit-code mismatch')
            result.update(status=match[1], nodes_reported=list(nodes),
                          execution_state='PLANNED_ONLY' if mode == 'dry' else 'CHECK_ATTEMPTED')
            return result
        directory = root / case['directory']
        names = {'basic': ('cluster-result.json',), 'active': ('active-result.json',),
                 'ib': ('ib-write-bw-result.json',)}[kind]
        files = [p for name in names for p in directory.rglob(name)]
        require(len(files) == 1, 'expected exactly one current report: ' + ','.join(names))
        path = files[0]
        result['report'] = str(path)
        require(path.stat().st_mtime >= float(meta['started']) - 1, 'stale report')
        report = load_json(path)
        require(isinstance(report, dict), 'report must be an object')
        require(report.get('schema_version') == ({'basic':'2.0', 'active':'1.1'}.get(kind, '1.0')), 'invalid schema_version')
        require(not report.get('error'), 'report contains tool exception: ' + str(report.get('error')))
        check_completion(report)
        execution = report.get('run', {}).get('execution') or {}
        scope = 'container' if case['scenario'] == 'per-node-container' else 'host'
        require((execution.get('env_script') if kind == 'basic' else report.get('env_script')) == case['env_script'],
                'report env_script mismatch')
        require((execution.get('scope') if kind == 'basic' else report.get('execution_scope')) == scope,
                'report execution scope mismatch')
        scenario = report.get('scenario', (report.get('run', {}).get('execution') or {}).get('scenario'))
        if kind == 'basic':
            require(rc == 0, 'basic environment outcomes must return zero after report collection')
            require(execution.get('categories') == case['operation'].split(','), 'basic categories mismatch')
            require(execution.get('status', 'PASS') == 'PASS' and not execution.get('failed_nodes'), 'basic probe execution failed')
            require('cluster' in report, 'missing cluster summary')
            check_completion(report['cluster'])
            node_records = expand_nodes(report.get('node_status', {}).get('nodes'))
            needed = ['node_status','execution_evidence']
            if 'platform' in case['operation']:
                needed += ['driver_dtk','software_components','network_rdma','network_health']
            if 'resource' in case['operation']:
                needed += ['hardware_devices','system','resource_state']
            for section in needed:
                require(set(expand_nodes(report.get(section, {}).get('nodes'))) == set(nodes), 'missing/wrong node evidence in ' + section)
            status = report['cluster'].get('status', report.get('status', 'UNKNOWN'))
            for node, record in node_records.items():
                result['node_execution'][node] = 'EXECUTED' if record.get('reachable') else 'NO_TARGET_EVIDENCE'
        elif kind == 'active':
            require(report.get('test_name') == case['operation'], 'report test_name mismatch')
            profile = 'rccl-tests' if case['operation'] == 'rccl' else None
            requested = 'mpirun' if case['operation'] == 'rccl' else 'direct'
            require(report.get('profile') == profile, 'report profile mismatch')
            require(report.get('launcher') == requested, 'report launcher mismatch')
            groups = report.get('groups')
            require(isinstance(groups, list) and bool(groups), 'missing active groups')
            require(report.get('group_count') == len(groups), 'incorrect group_count')
            grouped = []
            for group in groups:
                members = group.get('nodes') or []
                require(bool(members), 'empty group')
                grouped.extend(members)
                group_dir = path.parent / 'groups' / group['group']
                require(group_dir.is_dir(), 'missing group artifacts')
                # Fail-fast can cancel peers after a real failure. This is a
                # valid FAIL outcome only with confirmed cleanup, never PASS.
                allow_cancelled = report.get('status') == 'FAIL' and report.get('cleanup') is not None
                check_completion(group, allow_cancelled=allow_cancelled)
                if case['operation'] == 'ib-write-bw':
                    require(group.get('launcher') == 'server-client', 'IB must use server/client')
                else:
                    dispatched = check_active_launch(group, members, case['operation'], profile, requested)
                if case['operation'] == 'ib-write-bw' and mode == 'real':
                    evidence = load_json(group_dir / 'ib-write-bw-result.json')
                    require(set(evidence.get('selected_nodes') or []) == set(members), 'IB group inventory scope mismatch')
                    pairs = evidence.get('pairs') or []
                    if evidence.get('status') == 'PASS':
                        require(bool(pairs), 'IB PASS without server/client measurements')
                    for pair in pairs:
                        require(pair.get('source') in members and pair.get('destination') in members, 'IB pair outside group')
                        commands = pair.get('commands') or []
                        require(len(commands) == 2, 'IB pair missing server/client command evidence')
                        for command in commands:
                            require(isinstance(command.get('returncode'), int), 'IB endpoint missing returncode')
                            for stream in ('stdout_path','stderr_path'):
                                require(command.get(stream) and pathlib.Path(command[stream]).is_file(), 'missing IB endpoint logs')
                    result['pairs_executed'] = result.get('pairs_executed', 0) + len(pairs)
                if mode == 'dry':
                    launch = load_json(group_dir / ('ib-write-bw-result.json' if case['operation'] == 'ib-write-bw' else 'launch.json'))
                    require(launch.get('command') or launch.get('commands'), 'empty launch artifact')
                    if case['operation'] == 'ib-write-bw':
                        require(launch.get('pairing') == 'all-directions-per-discovered-HCA' and launch.get('config'), 'missing IB inventory/pair plan')
                        result['plan_stage'] = 'INVENTORY_THEN_PAIR'; result['pairs_executed'] = 0
                    else:
                        check_active_launch(launch, members, case['operation'], profile, requested)
                    persisted = load_json(group_dir / 'result.json')
                    require(persisted.get('status') == group.get('status') == 'DRY_RUN', 'group dry-run status mismatch')
                    if case['operation'] != 'ib-write-bw':
                        check_active_launch(persisted, members, case['operation'], profile, requested)
                    require(list(read_nodes(group_dir / 'hostfile')) == list(members), 'group hostfile mismatch')
                else:
                    require(isinstance(group.get('returncode'), int) or bool(group.get('node_results')) or
                            group.get('status') in {'SKIPPED','BLOCKED','PRECHECK_FAILED'}, 'missing group execution evidence')
                    if case['operation'] != 'ib-write-bw':
                        records = group.get('node_results') or []
                        require(len(records) == len(dispatched) and {item['node'] for item in records} == set(dispatched),
                                'node execution coverage mismatch')
                        for record in records:
                            check_worker_result(record, allow_cancelled=allow_cancelled)
                            for stream in ('stdout', 'stderr'):
                                require((group_dir / 'nodes' / record['node'] / (stream + '.log')).is_file(),
                                        'missing worker ' + stream + ' evidence')
                        require(group.get('status') == aggregate_status(records), 'group/worker status mismatch')
                for node in members:
                    result['node_execution'][node] = ('PLANNED_ONLY' if mode == 'dry' else
                        'NOT_EXECUTED' if group.get('status') in {'SKIPPED','BLOCKED','PRECHECK_FAILED'} else 'EXECUTION_ATTEMPTED')
            require(len(grouped) == len(set(grouped)) and set(grouped) == set(nodes), 'group coverage differs from hostfile')
            require(set(report.get('nodes') or []) == set(nodes), 'active node list mismatch')
            node_records = dict.fromkeys(grouped)
            status = report.get('status')
            if mode == 'real':
                require(status == aggregate_status(groups), 'report/group status mismatch')
        else:
            # The paired server/client adapter must account for every requested
            # node, including an unmatched last node; no MPI launch is expected.
            require(report.get('operation', report.get('test_name')) == 'ib-write-bw', 'IB operation mismatch')
            node_records = expand_nodes(report.get('nodes'))
            status = report.get('status')
            require(isinstance(report.get('pairs'), list), 'missing IB pair artifacts')
            for pair in report['pairs']:
                require(pair.get('server') and pair.get('client'), 'missing IB server/client')
                if mode == 'dry':
                    require(pair.get('server_command') and pair.get('client_command'), 'missing IB pair launch commands')
            for node, record in node_records.items():
                result['node_execution'][node] = 'PLANNED_ONLY' if mode == 'dry' else 'EXECUTION_ATTEMPTED'
        require(set(node_records) == set(nodes), 'report node scope differs from hostfile')
        allowed = {'DRY_RUN'} if mode == 'dry' else {'PASS','FAIL','READY','BLOCKED','INCOMPLETE','PRECHECK_FAILED','SKIPPED'}
        require(status in allowed, 'invalid execution status: ' + str(status))
        if kind == 'active':
            require(rc == (0 if status in {'PASS','DRY_RUN','INCOMPLETE'} else 2), 'report status/exit-code mismatch')
        result.update(status=status, nodes_reported=list(node_records), execution_state='PLANNED_ONLY' if mode == 'dry' else 'EXECUTION_ATTEMPTED')
        if mode == 'real' and all(state in {'NOT_EXECUTED','NO_TARGET_EVIDENCE'} for state in result['node_execution'].values()):
            result['execution_state'] = 'BLOCKED_NOT_EXECUTED'
        if result.get('runtime_context') and len(result['runtime_context']) == len(nodes):
            result['execution_state'] = 'EXECUTED_VERIFIED'
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError) as exc:
        result.update(execution_state='INTERFACE_ERROR', error=str(exc))
    return result


def validate_run(root, local_rc=0):
    meta = load_json(root / 'meta.json')
    with (root / 'expected.tsv').open(encoding='utf-8', newline='') as stream:
        cases = list(csv.DictReader(stream, delimiter='\t'))
    with (root / 'calls.tsv').open(encoding='utf-8', newline='') as stream:
        receipts = list(csv.DictReader(stream, delimiter='\t'))
    counts = collections.Counter(item['case'] for item in receipts)
    case_ids = [item['case'] for item in cases]
    structure_errors = []
    if len(case_ids) != len(set(case_ids)) or not case_ids:
        structure_errors.append('empty or duplicate expected case identifiers')
    if set(counts) - set(case_ids):
        structure_errors.append('unexpected case calls: ' + repr(set(counts) - set(case_ids)))
    for ident, count in counts.items():
        if count != 1:
            structure_errors.append(f'{ident}: called {count} times')
    by_id = {item['case']: item for item in receipts}
    results = [verify_case(root, case, by_id.get(case['case']), meta) for case in cases]
    totals = dict(collections.Counter(item['execution_state'] for item in results))
    report = dict(schema_version='1.0', cases=results, counts=totals, errors=structure_errors,
                  local_unit_returncode=local_rc,
                  status='FAIL' if local_rc or structure_errors or totals.get('INTERFACE_ERROR') else 'PASS')
    (root / 'acceptance-result.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    for item in results:
        print(f"[{item['execution_state']}] {item['case']} rc={item['returncode']} nodes={len(item['nodes_reported'])}/{len(item['nodes_requested'])} report={item['report'] or '-'} {item.get('error', '')}")
    for error in structure_errors:
        print('[INTERFACE_ERROR] ' + error, file=sys.stderr)
    return 1 if report['status'] == 'FAIL' else 0


if __name__ == '__main__':
    sys.exit(validate_run(pathlib.Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 0))
VALIDATOR
# END_ACCEPTANCE_VALIDATOR
[[ -s "$RUN_DIR/acceptance-result.json" ]] || LOCAL_RC=1
printf '[SUMMARY] %s/acceptance-result.json\n' "$RUN_DIR"
printf '环境异常不作门禁；实际执行、仅dry-run、预检受阻及接口错误已逐项记录。\n'
exit "$LOCAL_RC"
