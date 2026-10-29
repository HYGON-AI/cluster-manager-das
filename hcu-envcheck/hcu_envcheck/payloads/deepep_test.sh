#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# DeepEP entrypoint. "intranode" runs the single-node test directly;
# "group" launches the in-project run_deepep_test.sh once per node via mpirun
# (slotsfile slots=1: one control rank per node), with MASTER_ADDR pinned to the
# group's first node and a per-group master port.
set -euo pipefail

usage() {
    printf 'usage: deepep_test.sh intranode\n' >&2
    printf '       deepep_test.sh group <slotsfile> <lowl|norm> <master-port> <ssh-port> <env-script-or-empty>\n' >&2
    exit 2
}
ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/hcu_envcheck/logging.sh"
TESTS=${HCU_DEEPEP_TEST_DIR:-$ROOT/third-party/DeepEP/tests}
[[ ${HCU_GPUS_PER_NODE:-} =~ ^[1-9][0-9]*$ ]] || { printf 'explicit HCU_GPUS_PER_NODE required\n' >&2; exit 2; }
require_file() {
    [[ -r $1 ]] || { printf 'DeepEP file missing: %s\n' "$1" >&2; exit 69; }
}

intranode() {
    (( $# == 0 )) || usage
    require_file "$TESTS/test_intranode.py"
    RUNTEST=$ROOT/hcu_envcheck/payloads/run_deepep_test.sh
    require_file "$RUNTEST"
    [[ -n ${HCU_DEVICE_SELECTION_FILE:-} ]] || { printf 'device selection record required\n' >&2; exit 2; }
    cd "$TESTS"
    printf '测试文件：test_intranode.py\n'
    exec bash "$RUNTEST" intranode
}

group() {
    (( $# == 5 )) || usage
    HOSTFILE=$1 MODE=$2 MASTER_PORT=$3 SSH_PORT=$4 ENV_SCRIPT=$5
    [[ -r $HOSTFILE ]] || { printf 'hostfile missing: %s\n' "$HOSTFILE" >&2; exit 2; }
    [[ $MODE == lowl || $MODE == norm ]] || exit 2
    [[ $MASTER_PORT =~ ^[1-9][0-9]*$ && $MASTER_PORT -le 65535 && $SSH_PORT =~ ^[1-9][0-9]*$ ]] || exit 2
    awk 'NF && $1 !~ /^#/ {
        if (NF != 2 || $1 !~ /^[A-Za-z0-9][A-Za-z0-9_.-]*$/ || $2 != "slots=1" || seen[$1]++) exit 1
        count++
    } END {if (!count) exit 1}' "$HOSTFILE" || {
        printf 'invalid DeepEP slotsfile (expected unique nodes with slots=1): %s\n' "$HOSTFILE" >&2; exit 2;
    }
    mapfile -t NODES < <(awk 'NF && $1 !~ /^#/ {print $1}' "$HOSTFILE")
    (( ${#NODES[@]} >= 2 )) || { printf 'multi-node DeepEP needs at least two nodes\n' >&2; exit 2; }
    (( HCU_GPUS_PER_NODE == 8 )) || { printf 'multi-node norm requires 8 GPUs per node\n' >&2; exit 2; }
    (( 256 % (${#NODES[@]} * HCU_GPUS_PER_NODE) == 0 && 288 % (${#NODES[@]} * HCU_GPUS_PER_NODE) == 0 )) || {
        printf 'DeepEP total ranks must divide norm experts=256 and lowl experts=288\n' >&2; exit 2;
    }
    [[ -n ${HCU_DEVICE_SELECTION_DIR:-} && -n ${HCU_GROUP_HOSTFILE:-} ]] || { printf 'per-node device selection and member list required\n' >&2; exit 2; }
    command -v mpirun >/dev/null || { printf 'mpirun missing in selected environment\n' >&2; exit 69; }
    case $MODE in
        lowl) TEST=test_low_latency.py ;;
        norm) TEST=test_internode.py ;;
    esac
    require_file "$TESTS/$TEST"
    RUNTEST=$ROOT/hcu_envcheck/payloads/run_deepep_test.sh
    require_file "$RUNTEST"
    export HCU_DEEPEP_TESTS=$TESTS HCU_DEEPEP_RUNTEST=$RUNTEST HCU_DEEPEP_MODE=$MODE
    export HCU_CLUSTER_ENV_SCRIPT=$ENV_SCRIPT
    export MASTER_ADDR=${NODES[0]} MASTER_PORT=$MASTER_PORT
    # T11: the master already runs inside task_guard (worker.sh run_node), so
    # HCU_TASK_TOKEN is present here. Ship token + guard path to every member
    # node via mpirun -x; each rank re-enters the guard before sourcing any
    # environment, so the token reaches torchrun/GPU worker environ there and
    # the per-node `task_guard stop` scan has real identities to match.
    if [[ -n "${HCU_TASK_TOKEN:-}" ]]; then
        export HCU_DEEPEP_GUARD=${HCU_TASK_GUARD_FILE:-$ROOT/hcu_envcheck/payloads/task_guard.sh}
        require_file "$HCU_DEEPEP_GUARD"
        [[ -f $HCU_DEEPEP_GUARD && ! -L $HCU_DEEPEP_GUARD ]] || {
            printf 'DeepEP guard must be a regular immutable file: %s\n' "$HCU_DEEPEP_GUARD" >&2; exit 69;
        }
    fi

    # lowl/norm share an immutable rank body; mode, token and node identity
    # still arrive through this launch's environment, never through the cache.
    source "$ROOT/hcu_envcheck/state_runtime.sh"
    rank_body=$(cat <<'RANK'
#!/usr/bin/env bash
set -Eeuo pipefail
deepep_rank_body() {
    local _hcu_gpus=$HCU_GPUS_PER_NODE _hcu_devices=$HCU_DEVICE_SELECTION_DIR _hcu_hosts=$HCU_GROUP_HOSTFILE
    if [[ -n "${HCU_CLUSTER_ENV_SCRIPT:-}" ]]; then
        source "$HCU_CLUSTER_ENV_SCRIPT"
    elif [[ -r ~/.bashrc ]]; then
        set +u; source ~/.bashrc; set -u
    fi
    export HCU_GPUS_PER_NODE=$_hcu_gpus HCU_DEVICE_SELECTION_DIR=$_hcu_devices HCU_GROUP_HOSTFILE=$_hcu_hosts
    cd "$HCU_DEEPEP_TESTS"
    node=$(awk -v row="$((OMPI_COMM_WORLD_RANK + 1))" 'NF&&$1!~/^#/ {if(++n==row){print $1;exit}}' "$HCU_GROUP_HOSTFILE")
    [[ -n $node ]] || { printf 'DeepEP control rank has no planned node\n' >&2; exit 2; }
    export HCU_DEVICE_SELECTION_FILE=$HCU_DEVICE_SELECTION_DIR/$node.json
    exec bash "$HCU_DEEPEP_RUNTEST" "$OMPI_COMM_WORLD_SIZE" "$OMPI_COMM_WORLD_RANK" "$HCU_DEEPEP_MODE"
}
# T11: enter this node's task guard before touching any environment. Without
# this, member-node ranks and their torchrun workers carry no token, and a
# per-node stop there is vacuously confirmed while the workers may live.
if [[ -n "${HCU_TASK_TOKEN:-}" && -r "${HCU_DEEPEP_GUARD:-}" ]]; then
    exec bash "$HCU_DEEPEP_GUARD" run "$HCU_TASK_TOKEN" -- \
        bash -c "$(declare -f deepep_rank_body); deepep_rank_body"
fi
deepep_rank_body
RANK
    )
    rank_script=$(hcu_cache_script "${HOSTFILE}.deepep-rank.sh" "$rank_body"$'\n')
    MPI=(mpirun -np "${#NODES[@]}" --hostfile "$HOSTFILE" --map-by ppr:1:node --rank-by slot --bind-to none
         --mca plm_rsh_no_tree_spawn 1 --wdir "$TESTS")
    if (( EUID == 0 )); then MPI+=(--allow-run-as-root); fi
    (( SSH_PORT == 22 )) || MPI+=(--mca plm_rsh_args "-p $SSH_PORT")
    for name in PATH MASTER_ADDR MASTER_PORT HCU_DEEPEP_TESTS HCU_DEEPEP_RUNTEST HCU_DEEPEP_MODE HCU_CLUSTER_ENV_SCRIPT HCU_GPUS_PER_NODE HCU_DEVICE_SELECTION_DIR HCU_GROUP_HOSTFILE; do
        MPI+=(-x "$name")
    done
    for name in HCU_TASK_TOKEN HCU_DEEPEP_GUARD; do
        [[ -z ${!name:-} ]] || MPI+=(-x "$name")
    done
    for name in LD_LIBRARY_PATH ROCM_PATH; do
        [[ -z ${!name:-} ]] || MPI+=(-x "$name")
    done
    printf 'DeepEP 模式：%s · 测试文件：%s · 节点数：%s · 主节点：%s:%s\n' \
           "$MODE" "$TEST" "${#NODES[@]}" "${NODES[0]}" "$MASTER_PORT"
    hcu_log_command MPI-COMMAND "${NODES[0]}" "${MPI[@]}" bash "$rank_script"
    "${MPI[@]}" bash "$rank_script"
}

(( $# >= 1 )) || usage
case $1 in
    intranode) shift; intranode "$@" ;;
    group) shift; group "$@" ;;
    *) usage ;;
esac
