#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# 功能验证总入口：每项直接调用 hcu-cluster-run，不用 Shell 函数分派。
# 默认基础只读检测；主动/NHC/IB/脚本/容器维护均 dry-run。
# RUN_ACTIVE=yes: 短时 worker；RUN_PROFILES=yes: 既有性能脚本（短小参数）。
# RCCL worker 显式 --profile worker；rccl-tests 用例不传 profile/--tests，验证默认十项完整二进制验收。
# 基准必须覆盖当前卡数全部十项；短小参数仅验证接口/报告，不代表生产性能达标。
# RUN_NETWORK=yes: IB带宽；RUN_DIAGNOSTICS=yes: nhc（IB状态由platform覆盖）。
# RUN_SCRIPTS=yes: 用户脚本；RUN_CONTEXT=yes: 无DCU的Shell/Python上下文探针。
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
RUN_CONTEXT=yes ...     真实验证 env.sh、节点/容器位置、Shell/Python；不使用DCU
RUN_ACTIVE=yes ...      小规模worker；RUN_PROFILES=yes 开启既有性能脚本
RUN_NETWORK=yes ...     IB server/client；RUN_DIAGNOSTICS=yes 开启 nhc（IB状态由platform覆盖）
RUN_SCRIPTS=yes ...     执行 DIAGNOSTIC_SCRIPT/CUSTOM_SCRIPT（需自行确认内容）
变量：HOSTFILE ENV_SCRIPT SHARED_ENV_SCRIPT NODE_LOCAL_ENV_SCRIPT CONTAINER_ENV_SCRIPT
      CONTROLLER_ENV_SCRIPT CONTROLLER_PYTHON PYTHON_BIN HCU_ENVCHECK_PYTHON
      CONTAINER_NAME IMAGE CONTAINER_SSH_PORT TRANSPORT GROUP_SIZE ACTIVE_SLOTS
      NPROC_PER_NODE RCCL_NPROC_PER_NODE NP LAUNCHER MASTER_PORT TIMEOUT IB_GROUP_SIZE OUTPUT_DIR
      CUSTOM_SCRIPT DIAGNOSTIC_SCRIPT
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
NPROC_PER_NODE=${NPROC_PER_NODE:-1}
RCCL_NPROC_PER_NODE=${RCCL_NPROC_PER_NODE:-8}  # Binary suite uses real card-count baselines, not smoke-test ranks.
NP=${NP:-}
LAUNCHER=${LAUNCHER:-mpirun-torchrun}
MASTER_PORT=${MASTER_PORT:-29500}
TIMEOUT=${TIMEOUT:-120}
DIAGNOSTIC_SCRIPT=${DIAGNOSTIC_SCRIPT:-$ROOT/cluster_run/payloads/check_deepep_env.sh}
CUSTOM_SCRIPT=${CUSTOM_SCRIPT:-$DIAGNOSTIC_SCRIPT}
OUTPUT_DIR=${OUTPUT_DIR:-./manual_test_results}
mkdir -p -- "$OUTPUT_DIR" || exit 1
OUTPUT_DIR=$(cd -- "$OUTPUT_DIR" && pwd) || exit 1
RUN_DIR=$(mktemp -d "$OUTPUT_DIR/run_$(date +%Y%m%d_%H%M%S)_XXXXXX") || exit 1
mkdir -p "$RUN_DIR/logs" "$RUN_DIR/contexts"
CALLS="$RUN_DIR/calls.tsv"
printf 'case\treturncode\tlog_returncode\n' > "$CALLS"
COMMON_OPTIONS=(--transport "$TRANSPORT")
ACTIVE_OPTIONS=(--dry-run); ACTIVE_MODE=dry
PROFILE_OPTIONS=(--dry-run); PROFILE_MODE=dry
NETWORK_OPTIONS=(--dry-run); NETWORK_MODE=dry
DIAGNOSTIC_OPTIONS=(--dry-run); DIAGNOSTIC_MODE=dry
SCRIPT_OPTIONS=(--dry-run); SCRIPT_MODE=dry
CONTEXT_OPTIONS=(--dry-run); CONTEXT_MODE=dry
[[ "${RUN_ACTIVE:-dry-run}" == yes || "${RUN_ACTIVE:-}" == 1 ]] && { ACTIVE_OPTIONS=(); ACTIVE_MODE=real; }
[[ "${RUN_PROFILES:-dry-run}" == yes || "${RUN_PROFILES:-}" == 1 ]] && { PROFILE_OPTIONS=(); PROFILE_MODE=real; }
[[ "${RUN_NETWORK:-dry-run}" == yes || "${RUN_NETWORK:-}" == 1 ]] && { NETWORK_OPTIONS=(); NETWORK_MODE=real; }
[[ "${RUN_DIAGNOSTICS:-dry-run}" == yes || "${RUN_DIAGNOSTICS:-}" == 1 ]] && { DIAGNOSTIC_OPTIONS=(); DIAGNOSTIC_MODE=real; }
[[ "${RUN_SCRIPTS:-dry-run}" == yes || "${RUN_SCRIPTS:-}" == 1 ]] && { SCRIPT_OPTIONS=(); SCRIPT_MODE=real; }
[[ "${RUN_CONTEXT:-dry-run}" == yes || "${RUN_CONTEXT:-}" == 1 ]] && { CONTEXT_OPTIONS=(); CONTEXT_MODE=real; }
NP_OPTION=(); [[ -n "$NP" ]] && NP_OPTION=(--np "$NP")

# 只生成本地输入，不访问目标节点。随机token验证目标env.sh确实执行。
"$PYTHON_BIN" - "$RUN_DIR" "$HOSTFILE" "$SHARED_ENV_SCRIPT" "$NODE_LOCAL_ENV_SCRIPT" "$CONTAINER_ENV_SCRIPT" "$LAUNCHER" <<'FIXTURES' || exit 1
import json, pathlib, shlex, sys, time, uuid
from cluster_run.hostfile import read_nodes
out = pathlib.Path(sys.argv[1])
nodes = read_nodes(pathlib.Path(sys.argv[2]))
token = uuid.uuid4().hex
(out / 'meta.json').write_text(json.dumps({'started': time.time(), 'nodes': nodes, 'token': token, 'launcher': sys.argv[6]}), encoding='utf-8')
# MPI计划至少两节点；不足时使用明确的plan-only虚拟节点，不发起连接。
plan_nodes = list(nodes[:2]) if len(nodes) >= 2 else ['hcu-plan-node01', 'hcu-plan-node02']
(out / 'launcher-hostfile').write_text('\n'.join(plan_nodes) + '\n', encoding='utf-8')
for scenario, env_script in zip(('shared-conda','node-local-conda','per-node-container'), sys.argv[3:6]):
    if not env_script.startswith('/'):
        raise SystemExit('--env-script target paths must be absolute: ' + env_script)
    text = 'set -e\nsource ' + shlex.quote(env_script) + '\nexport HCU_ACCEPTANCE_TOKEN=' + token + '\n'
    (out / 'contexts' / (scenario + '.env.sh')).write_text(text, encoding='utf-8')
(out / 'contexts' / 'context.sh').write_text('''#!/usr/bin/env bash
set -e
: "${HCU_ACCEPTANCE_TOKEN:?target env wrapper was not sourced}"
scope=host
[[ -f /.dockerenv || -f /run/.containerenv ]] && scope=container
printf 'HCU_CONTEXT\\t%s\\t%s\\t%s\\t%s\\n' "$HCU_ACCEPTANCE_TOKEN" "$(hostname -s)" "$scope" "$$"
''', encoding='utf-8')
(out / 'contexts' / 'context.py').write_text('''import json, os, pathlib, socket, sys
assert os.environ.get('HCU_ACCEPTANCE_TOKEN'), 'target env wrapper was not sourced'
print('HCU_CONTEXT_JSON ' + json.dumps({'token': os.environ['HCU_ACCEPTANCE_TOKEN'], 'node': socket.gethostname().split('.')[0], 'scope': 'container' if pathlib.Path('/.dockerenv').exists() or pathlib.Path('/run/.containerenv').exists() else 'host', 'pid': os.getpid(), 'python': sys.executable}))
''', encoding='utf-8')
FIXTURES
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
shared-conda-rccl	active	shared-conda	rccl	$ACTIVE_MODE	shared-conda/rccl	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-gemm	active	shared-conda	gemm	$ACTIVE_MODE	shared-conda/gemm	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-rccl-tests	active	shared-conda	rccl	$PROFILE_MODE	shared-conda/rccl-tests	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-rocblas	active	shared-conda	gemm	$PROFILE_MODE	shared-conda/rocblas	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-ib-write-bw	active	shared-conda	ib-write-bw	$NETWORK_MODE	shared-conda/ib-write-bw	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-nhc	diagnostic	shared-conda	nhc	$DIAGNOSTIC_MODE	shared-conda/nhc	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-script	script	shared-conda	script	$SCRIPT_MODE	shared-conda/script	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-custom	active	shared-conda	custom	$SCRIPT_MODE	shared-conda/custom	$HOSTFILE	$SHARED_ENV_SCRIPT	no
shared-conda-context-sh	script	shared-conda	script	$CONTEXT_MODE	shared-conda/context-sh	$HOSTFILE	$RUN_DIR/contexts/shared-conda.env.sh	yes
shared-conda-context-py	active	shared-conda	custom	$CONTEXT_MODE	shared-conda/context-py	$HOSTFILE	$RUN_DIR/contexts/shared-conda.env.sh	yes
node-local-conda-platform	basic	node-local-conda	platform	real	node-local-conda/platform	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-resource	basic	node-local-conda	resource	real	node-local-conda/resource	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-base	basic	node-local-conda	platform,resource	real	node-local-conda/base	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-rccl	active	node-local-conda	rccl	$ACTIVE_MODE	node-local-conda/rccl	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-gemm	active	node-local-conda	gemm	$ACTIVE_MODE	node-local-conda/gemm	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-rccl-tests	active	node-local-conda	rccl	$PROFILE_MODE	node-local-conda/rccl-tests	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-rocblas	active	node-local-conda	gemm	$PROFILE_MODE	node-local-conda/rocblas	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-ib-write-bw	active	node-local-conda	ib-write-bw	$NETWORK_MODE	node-local-conda/ib-write-bw	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-nhc	diagnostic	node-local-conda	nhc	$DIAGNOSTIC_MODE	node-local-conda/nhc	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-script	script	node-local-conda	script	$SCRIPT_MODE	node-local-conda/script	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-custom	active	node-local-conda	custom	$SCRIPT_MODE	node-local-conda/custom	$HOSTFILE	$NODE_LOCAL_ENV_SCRIPT	no
node-local-conda-context-sh	script	node-local-conda	script	$CONTEXT_MODE	node-local-conda/context-sh	$HOSTFILE	$RUN_DIR/contexts/node-local-conda.env.sh	yes
node-local-conda-context-py	active	node-local-conda	custom	$CONTEXT_MODE	node-local-conda/context-py	$HOSTFILE	$RUN_DIR/contexts/node-local-conda.env.sh	yes
per-node-container-platform	basic	per-node-container	platform	real	per-node-container/platform	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-resource	basic	per-node-container	resource	real	per-node-container/resource	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-base	basic	per-node-container	platform,resource	real	per-node-container/base	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-rccl	active	per-node-container	rccl	$ACTIVE_MODE	per-node-container/rccl	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-gemm	active	per-node-container	gemm	$ACTIVE_MODE	per-node-container/gemm	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-rccl-tests	active	per-node-container	rccl	$PROFILE_MODE	per-node-container/rccl-tests	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-rocblas	active	per-node-container	gemm	$PROFILE_MODE	per-node-container/rocblas	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-ib-write-bw	active	per-node-container	ib-write-bw	$NETWORK_MODE	per-node-container/ib-write-bw	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-nhc	diagnostic	per-node-container	nhc	$DIAGNOSTIC_MODE	per-node-container/nhc	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-script	script	per-node-container	script	$SCRIPT_MODE	per-node-container/script	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-custom	active	per-node-container	custom	$SCRIPT_MODE	per-node-container/custom	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-context-sh	script	per-node-container	script	$CONTEXT_MODE	per-node-container/context-sh	$HOSTFILE	$RUN_DIR/contexts/per-node-container.env.sh	yes
per-node-container-context-py	active	per-node-container	custom	$CONTEXT_MODE	per-node-container/context-py	$HOSTFILE	$RUN_DIR/contexts/per-node-container.env.sh	yes
per-node-container-container-status	status	per-node-container	container-status	real	per-node-container/container-status	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-create	lifecycle	per-node-container	container-create	dry	per-node-container/container-create	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-recreate	lifecycle	per-node-container	container-recreate	dry	per-node-container/container-recreate	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-container-delete	lifecycle	per-node-container	container-delete	dry	per-node-container/container-delete	$HOSTFILE	$CONTAINER_ENV_SCRIPT	no
per-node-container-launcher-mpirun-torchrun	active	per-node-container	rccl	dry	per-node-container/launcher-mpirun-torchrun	$RUN_DIR/launcher-hostfile	$CONTAINER_ENV_SCRIPT	no
per-node-container-launcher-ssh-torchrun	active	per-node-container	rccl	dry	per-node-container/launcher-ssh-torchrun	$RUN_DIR/launcher-hostfile	$CONTAINER_ENV_SCRIPT	no
per-node-container-launcher-mpirun	active	per-node-container	rccl	dry	per-node-container/launcher-mpirun	$RUN_DIR/launcher-hostfile	$CONTAINER_ENV_SCRIPT	no
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

# shared-conda-rccl: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] shared-conda-rccl\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/rccl" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --script-arg=--bytes --script-arg=4096 --script-arg=--iterations --script-arg=2 --script-arg=--warmup --script-arg=0 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-rccl.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-rccl\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-rccl rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-rccl.log"; then LOCAL_RC=1; fi

# shared-conda-gemm: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] shared-conda-gemm\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/gemm" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --script-arg=--size --script-arg=128 --script-arg=--iterations --script-arg=2 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-gemm.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-gemm\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-gemm rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-gemm.log"; then LOCAL_RC=1; fi

# shared-conda-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] shared-conda-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/rccl-tests" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --nproc-per-node "$RCCL_NPROC_PER_NODE" --timeout "$TIMEOUT" \
    --script-arg=-b --script-arg=4 --script-arg=-e --script-arg=4K --script-arg=-n --script-arg=2 --script-arg=-w --script-arg=0 \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-rccl-tests.log"; then LOCAL_RC=1; fi

# shared-conda-rocblas: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] shared-conda-rocblas\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/rocblas" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --profile rocblas --timeout "$TIMEOUT" \
    --script-arg=--shapes --script-arg=128x128x128 --script-arg=--cards --script-arg=0 --script-arg=--iters --script-arg=2 \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-rocblas.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-rocblas\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-rocblas rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-rocblas.log"; then LOCAL_RC=1; fi

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

# shared-conda-nhc: diagnostic；节点范围 $HOSTFILE；mode=$DIAGNOSTIC_MODE
printf "[CALL] shared-conda-nhc\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda nhc -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/nhc" \
    --timeout "$TIMEOUT" "${DIAGNOSTIC_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-nhc.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-nhc\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-nhc rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-nhc.log"; then LOCAL_RC=1; fi

# shared-conda-script: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] shared-conda-script\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/shared-conda/script" \
    --script "$DIAGNOSTIC_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-script.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-script\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-script rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-script.log"; then LOCAL_RC=1; fi

# shared-conda-custom: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] shared-conda-custom\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$SHARED_ENV_SCRIPT" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/shared-conda/custom" \
    --script "$CUSTOM_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-custom.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-custom\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-custom rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-custom.log"; then LOCAL_RC=1; fi

# shared-conda-context-sh: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] shared-conda-context-sh\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/shared-conda.env.sh" \
    --output-dir "$RUN_DIR/shared-conda/context-sh" \
    --script "$RUN_DIR/contexts/context.sh" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-context-sh.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-context-sh\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-context-sh rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-context-sh.log"; then LOCAL_RC=1; fi

# shared-conda-context-py: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] shared-conda-context-py\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    shared-conda custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/shared-conda.env.sh" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/shared-conda/context-py" \
    --script "$RUN_DIR/contexts/context.py" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/shared-conda-context-py.log"
case_rc=("${PIPESTATUS[@]}")
printf "shared-conda-context-py\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] shared-conda-context-py rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/shared-conda-context-py.log"; then LOCAL_RC=1; fi

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

# node-local-conda-rccl: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] node-local-conda-rccl\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/rccl" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --script-arg=--bytes --script-arg=4096 --script-arg=--iterations --script-arg=2 --script-arg=--warmup --script-arg=0 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-rccl.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-rccl\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-rccl rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-rccl.log"; then LOCAL_RC=1; fi

# node-local-conda-gemm: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] node-local-conda-gemm\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/gemm" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --script-arg=--size --script-arg=128 --script-arg=--iterations --script-arg=2 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-gemm.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-gemm\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-gemm rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-gemm.log"; then LOCAL_RC=1; fi

# node-local-conda-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] node-local-conda-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/rccl-tests" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --nproc-per-node "$RCCL_NPROC_PER_NODE" --timeout "$TIMEOUT" \
    --script-arg=-b --script-arg=4 --script-arg=-e --script-arg=4K --script-arg=-n --script-arg=2 --script-arg=-w --script-arg=0 \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-rccl-tests.log"; then LOCAL_RC=1; fi

# node-local-conda-rocblas: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] node-local-conda-rocblas\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/rocblas" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --profile rocblas --timeout "$TIMEOUT" \
    --script-arg=--shapes --script-arg=128x128x128 --script-arg=--cards --script-arg=0 --script-arg=--iters --script-arg=2 \
    "${PROFILE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-rocblas.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-rocblas\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-rocblas rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-rocblas.log"; then LOCAL_RC=1; fi

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

# node-local-conda-nhc: diagnostic；节点范围 $HOSTFILE；mode=$DIAGNOSTIC_MODE
printf "[CALL] node-local-conda-nhc\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda nhc -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/nhc" \
    --timeout "$TIMEOUT" "${DIAGNOSTIC_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-nhc.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-nhc\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-nhc rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-nhc.log"; then LOCAL_RC=1; fi

# node-local-conda-script: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] node-local-conda-script\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/node-local-conda/script" \
    --script "$DIAGNOSTIC_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-script.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-script\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-script rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-script.log"; then LOCAL_RC=1; fi

# node-local-conda-custom: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] node-local-conda-custom\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$NODE_LOCAL_ENV_SCRIPT" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/node-local-conda/custom" \
    --script "$CUSTOM_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-custom.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-custom\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-custom rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-custom.log"; then LOCAL_RC=1; fi

# node-local-conda-context-sh: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] node-local-conda-context-sh\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/node-local-conda.env.sh" \
    --output-dir "$RUN_DIR/node-local-conda/context-sh" \
    --script "$RUN_DIR/contexts/context.sh" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-context-sh.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-context-sh\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-context-sh rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-context-sh.log"; then LOCAL_RC=1; fi

# node-local-conda-context-py: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] node-local-conda-context-py\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    node-local-conda custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/node-local-conda.env.sh" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/node-local-conda/context-py" \
    --script "$RUN_DIR/contexts/context.py" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/node-local-conda-context-py.log"
case_rc=("${PIPESTATUS[@]}")
printf "node-local-conda-context-py\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] node-local-conda-context-py rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/node-local-conda-context-py.log"; then LOCAL_RC=1; fi

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

# per-node-container-rccl: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] per-node-container-rccl\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/rccl" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --container-ssh-port "$CONTAINER_SSH_PORT" \
    --script-arg=--bytes --script-arg=4096 --script-arg=--iterations --script-arg=2 --script-arg=--warmup --script-arg=0 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-rccl.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-rccl\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-rccl rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-rccl.log"; then LOCAL_RC=1; fi

# per-node-container-gemm: active；节点范围 $HOSTFILE；mode=$ACTIVE_MODE
printf "[CALL] per-node-container-gemm\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/gemm" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --launcher "$LAUNCHER" --profile worker --nproc-per-node "$NPROC_PER_NODE" \
    "${NP_OPTION[@]}" --master-port "$MASTER_PORT" --timeout "$TIMEOUT" \
    --container-ssh-port "$CONTAINER_SSH_PORT" \
    --script-arg=--size --script-arg=128 --script-arg=--iterations --script-arg=2 \
    "${ACTIVE_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-gemm.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-gemm\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-gemm rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-gemm.log"; then LOCAL_RC=1; fi

# per-node-container-rccl-tests: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] per-node-container-rccl-tests\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/rccl-tests" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --nproc-per-node "$RCCL_NPROC_PER_NODE" --timeout "$TIMEOUT" \
    --script-arg=-b --script-arg=4 --script-arg=-e --script-arg=4K --script-arg=-n --script-arg=2 --script-arg=-w --script-arg=0 \
    "${PROFILE_OPTIONS[@]}" \
    --container-ssh-port "$CONTAINER_SSH_PORT" 2>&1 | tee "$RUN_DIR/logs/per-node-container-rccl-tests.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-rccl-tests\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-rccl-tests rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-rccl-tests.log"; then LOCAL_RC=1; fi

# per-node-container-rocblas: active；节点范围 $HOSTFILE；mode=$PROFILE_MODE
printf "[CALL] per-node-container-rocblas\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container gemm -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/rocblas" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --profile rocblas --timeout "$TIMEOUT" \
    --script-arg=--shapes --script-arg=128x128x128 --script-arg=--cards --script-arg=0 --script-arg=--iters --script-arg=2 \
    "${PROFILE_OPTIONS[@]}" \
    --container-ssh-port "$CONTAINER_SSH_PORT" 2>&1 | tee "$RUN_DIR/logs/per-node-container-rocblas.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-rocblas\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-rocblas rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-rocblas.log"; then LOCAL_RC=1; fi

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

# per-node-container-nhc: diagnostic；节点范围 $HOSTFILE；mode=$DIAGNOSTIC_MODE
printf "[CALL] per-node-container-nhc\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container nhc -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/nhc" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --timeout "$TIMEOUT" "${DIAGNOSTIC_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-nhc.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-nhc\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-nhc rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-nhc.log"; then LOCAL_RC=1; fi

# per-node-container-script: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] per-node-container-script\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/script" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --script "$DIAGNOSTIC_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-script.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-script\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-script rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-script.log"; then LOCAL_RC=1; fi

# per-node-container-custom: script；节点范围 $HOSTFILE；mode=$SCRIPT_MODE
printf "[CALL] per-node-container-custom\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/per-node-container/custom" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --script "$CUSTOM_SCRIPT" --timeout "$TIMEOUT" "${SCRIPT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-custom.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-custom\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-custom rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-custom.log"; then LOCAL_RC=1; fi

# per-node-container-context-sh: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] per-node-container-context-sh\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container script -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/per-node-container.env.sh" \
    --output-dir "$RUN_DIR/per-node-container/context-sh" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --script "$RUN_DIR/contexts/context.sh" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-context-sh.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-context-sh\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-context-sh rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-context-sh.log"; then LOCAL_RC=1; fi

# per-node-container-context-py: script；节点范围 $HOSTFILE；mode=$CONTEXT_MODE
printf "[CALL] per-node-container-context-py\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container custom -f "$HOSTFILE" "${COMMON_OPTIONS[@]}" \
    --env-script "$RUN_DIR/contexts/per-node-container.env.sh" \
    --group-size "$GROUP_SIZE" --slots "$ACTIVE_SLOTS" \
    --output-dir "$RUN_DIR/per-node-container/context-py" \
    --container "$CONTAINER_NAME" -i "$IMAGE" \
    --script "$RUN_DIR/contexts/context.py" --timeout "$TIMEOUT" "${CONTEXT_OPTIONS[@]}" 2>&1 | tee "$RUN_DIR/logs/per-node-container-context-py.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-context-py\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-context-py rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-context-py.log"; then LOCAL_RC=1; fi

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

# per-node-container-launcher-mpirun-torchrun: active；节点范围 $RUN_DIR/launcher-hostfile；mode=dry
printf "[CALL] per-node-container-launcher-mpirun-torchrun\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$RUN_DIR/launcher-hostfile" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/launcher-mpirun-torchrun" \
    --container "$CONTAINER_NAME" \
    --group-size 2 --slots 1 --profile worker --launcher mpirun-torchrun \
    --container-ssh-port "$CONTAINER_SSH_PORT" --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-launcher-mpirun-torchrun.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-launcher-mpirun-torchrun\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-launcher-mpirun-torchrun rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-launcher-mpirun-torchrun.log"; then LOCAL_RC=1; fi

# per-node-container-launcher-ssh-torchrun: active；节点范围 $RUN_DIR/launcher-hostfile；mode=dry
printf "[CALL] per-node-container-launcher-ssh-torchrun\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$RUN_DIR/launcher-hostfile" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/launcher-ssh-torchrun" \
    --container "$CONTAINER_NAME" \
    --group-size 2 --slots 1 --profile worker --launcher ssh-torchrun \
    --container-ssh-port "$CONTAINER_SSH_PORT" --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-launcher-ssh-torchrun.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-launcher-ssh-torchrun\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-launcher-ssh-torchrun rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-launcher-ssh-torchrun.log"; then LOCAL_RC=1; fi

# per-node-container-launcher-mpirun: active；节点范围 $RUN_DIR/launcher-hostfile；mode=dry
printf "[CALL] per-node-container-launcher-mpirun\n"
bash "$HCU_CLUSTER_RUN" "${CONTROLLER_OPTIONS[@]}" \
    per-node-container rccl -f "$RUN_DIR/launcher-hostfile" "${COMMON_OPTIONS[@]}" \
    --env-script "$CONTAINER_ENV_SCRIPT" \
    --output-dir "$RUN_DIR/per-node-container/launcher-mpirun" \
    --container "$CONTAINER_NAME" \
    --group-size 2 --slots 1 --profile worker --launcher mpirun \
    --container-ssh-port "$CONTAINER_SSH_PORT" --dry-run 2>&1 | tee "$RUN_DIR/logs/per-node-container-launcher-mpirun.log"
case_rc=("${PIPESTATUS[@]}")
printf "per-node-container-launcher-mpirun\t%s\t%s\n" "${case_rc[0]}" "${case_rc[1]}" >> "$CALLS"
printf "[RESULT] per-node-container-launcher-mpirun rc=%s\n" "${case_rc[0]}"
if (( case_rc[1] != 0 || (case_rc[0] != 0 && case_rc[0] != 2 && case_rc[0] != 3) )); then LOCAL_RC=1; fi
if (( case_rc[0] == 3 )) && ! grep -Eq "^RESULT[[:space:]]+PRECHECK_FAILED[[:space:]]*$" "$RUN_DIR/logs/per-node-container-launcher-mpirun.log"; then LOCAL_RC=1; fi

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


def check_context(path, node, scenario, token):
    text = path.read_text(encoding='utf-8')
    records = []
    for line in text.splitlines():
        if line.startswith('HCU_CONTEXT_JSON '):
            records.append(json.loads(line.partition(' ')[2]))
        elif line.startswith('HCU_CONTEXT\t'):
            _, marker, hostname, scope, pid = line.split('\t')
            records.append(dict(token=marker, node=hostname, scope=scope, pid=int(pid)))
    require(len(records) == 1, 'missing or repeated runtime-context evidence for ' + node)
    record = records[0]
    require(record.get('token') == token, 'env.sh execution marker missing/wrong on ' + node)
    # Docker hostname is an observation, not the physical SSH target identity.
    # The caller binds this evidence to the per-node result and stdout path.
    require(isinstance(record.get('node'), str) and bool(record['node']), 'missing observed hostname')
    if scenario != 'per-node-container':
        require(record['node'] == node.split('.')[0], 'payload executed on wrong node: ' + repr(record))
    require(record.get('scope') == ('container' if scenario == 'per-node-container' else 'host'),
            'payload executed in wrong host/container scope: ' + repr(record))
    require(isinstance(record.get('pid'), int) and record['pid'] > 0, 'missing process evidence')
    return record


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
    effective = ('node-script' if operation == 'custom' else profile if profile != 'worker' else
                 'local' if len(members) == 1 else requested)
    require(launch.get('launcher') == effective, 'effective launcher mismatch')
    require(launch.get('execution_profile') == profile, 'group profile mismatch')
    require(launch.get('requested_launcher') == requested, 'requested launcher mismatch')
    require(launch.get('leader') == members[0], 'group leader mismatch')
    dispatched = members if effective in {'ssh-torchrun', 'node-script', 'rocblas'} else members[:1]
    commands = launch.get('commands') or []
    require(len(commands) == len(dispatched) and {item['node'] for item in commands} == set(dispatched),
            'launch command node coverage mismatch')
    require(all(item.get('command') for item in commands), 'empty node launch command')
    return dispatched


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
                 'script': ('script-result.json',), 'diagnostic': (case['operation'] + '-result.json',),
                 'ib': ('ib-write-bw-result.json',)}[kind]
        files = [p for name in names for p in directory.rglob(name)]
        require(len(files) == 1, 'expected exactly one current report: ' + ','.join(names))
        path = files[0]
        result['report'] = str(path)
        require(path.stat().st_mtime >= float(meta['started']) - 1, 'stale report')
        report = load_json(path)
        require(isinstance(report, dict), 'report must be an object')
        require(report.get('schema_version') == ({'basic':'2.0', 'active':'1.1', 'script':'1.1'}.get(kind, '1.0')), 'invalid schema_version')
        require(not report.get('error'), 'report contains tool exception: ' + str(report.get('error')))
        check_completion(report)
        execution = report.get('run', {}).get('execution') or {}
        scope = 'container' if case['scenario'] == 'per-node-container' else 'host'
        require((execution.get('env_script') if kind == 'basic' else report.get('env_script')) == case['env_script'],
                'report env_script mismatch')
        require((execution.get('scope') if kind == 'basic' else report.get('execution_scope')) == scope,
                'report execution scope mismatch')
        scenario = report.get('scenario', (report.get('run', {}).get('execution') or {}).get('scenario'))
        if kind == 'diagnostic':
            require(report.get('execution_scope') == ('container' if case['scenario'] == 'per-node-container' else 'host'), 'diagnostic execution scope mismatch')
            require(report.get('env_script') == case['env_script'], 'diagnostic env_script mismatch')
        else:
            require(scenario == case['scenario'], 'report scenario mismatch')
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
            # Infer fixed profiles/launcher cases from existing directories. Only
            # normal worker cases use the configurable LAUNCHER captured in meta.
            leaf = pathlib.PurePosixPath(case['directory']).name
            profile = leaf if leaf in {'rccl-tests', 'rocblas'} else 'worker'
            requested = (leaf[len('launcher-'):] if leaf.startswith('launcher-') else
                         meta.get('launcher', 'mpirun-torchrun') if case['operation'] in {'rccl', 'gemm'} and profile == 'worker'
                         else 'mpirun' if profile == 'rccl-tests' else 'mpirun-torchrun')
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
                if case['operation'] == 'custom' and mode == 'dry':
                    require(group.get('launcher') == 'node-script', 'custom must not launch MPI/torchrun')
                    require({item['node'] for item in launch.get('commands', [])} == set(members), 'custom missing node launch')
                for node in members:
                    result['node_execution'][node] = ('PLANNED_ONLY' if mode == 'dry' else
                        'NOT_EXECUTED' if group.get('status') in {'SKIPPED','BLOCKED','PRECHECK_FAILED'} else 'EXECUTION_ATTEMPTED')
                if case['context'] == 'yes' and mode == 'real':
                    by_node = {item['node']: item for item in group.get('node_results', [])}
                    require(set(by_node) == set(members), 'context missing node results')
                    for node, record in by_node.items():
                        require(record.get('returncode') == 0 and record.get('status') == 'PASS', 'runtime-context payload failed')
                        context = check_context(group_dir / 'nodes' / node / 'stdout.log', node, case['scenario'], meta['token'])
                        result.setdefault('runtime_context', {})[node] = context
                        result['node_execution'][node] = 'EXECUTED_VERIFIED'
            require(len(grouped) == len(set(grouped)) and set(grouped) == set(nodes), 'group coverage differs from hostfile')
            require(set(report.get('nodes') or []) == set(nodes), 'active node list mismatch')
            node_records = dict.fromkeys(grouped)
            status = report.get('status')
            if mode == 'real':
                require(status == aggregate_status(groups), 'report/group status mismatch')
        elif kind in {'script', 'diagnostic'}:
            require(report.get('operation') == case['operation'], 'report operation mismatch')
            node_records = expand_nodes(report.get('nodes'))
            status = report.get('status')
            for node, record in node_records.items():
                check_completion(record)
                if mode == 'dry':
                    require(record.get('status') == 'DRY_RUN' and (record.get('command') or (kind == 'diagnostic' and report.get('command'))), 'missing per-node launch artifact')
                    result['node_execution'][node] = 'PLANNED_ONLY'
                elif record.get('status') in {'SKIPPED','BLOCKED','PRECHECK_FAILED'}:
                    require(record.get('issues') or record.get('reason'), 'skipped node lacks reason')
                    result['node_execution'][node] = 'NOT_EXECUTED'
                else:
                    require(isinstance(record.get('returncode'), int), 'missing per-node return code')
                    if kind == 'script':
                        check_worker_result(record)
                    if kind == 'diagnostic':
                        require(isinstance(record.get('command'), dict) and record.get('evidence_dir'), 'missing diagnostic execution evidence')
                        require(pathlib.Path(record['evidence_dir']).is_dir(), 'missing diagnostic evidence directory')
                    else:
                        for stream in ('stdout','stderr'):
                            require(record.get(stream) and pathlib.Path(record[stream]).is_file(), 'missing ' + stream + ' evidence')
                            require(pathlib.Path(record[stream]).resolve() == (path.parent / 'nodes' / node / (stream + '.log')).resolve(),
                                    'script log is bound to wrong node: ' + node)
                    result['node_execution'][node] = 'EXECUTION_ATTEMPTED'
                    if case['context'] == 'yes':
                        require(record.get('status') == 'PASS' and record['returncode'] == 0, 'runtime-context payload failed')
                        context = check_context(pathlib.Path(record['stdout']), node, case['scenario'], meta['token'])
                        result.setdefault('runtime_context', {})[node] = context
                        result['node_execution'][node] = 'EXECUTED_VERIFIED'
            if kind == 'script' and mode == 'real':
                expected = 'PASS' if all(item.get('status') == 'PASS' for item in node_records.values()) else 'FAIL'
                require(status == expected, 'report/node status mismatch')
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
        if kind in {'active','script','diagnostic'}:
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
