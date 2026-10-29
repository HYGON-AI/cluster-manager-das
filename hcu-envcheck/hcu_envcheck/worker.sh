#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Internal compute-node worker. Scheduling stays on the login node.
set -uo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || exit 70
die() { printf '[ERROR] %s\n' "$*" >&2; exit 2; }
quote() { printf -v QUOTED '%q ' "$@"; }
(( $# >= 2 )) || die 'internal worker requires scenario and operation'
SCENARIO=$1 OPERATION=$2; shift 2
case $SCENARIO in per-node-container|shared-conda|node-local-conda) ;; *) die 'invalid scenario' ;; esac
case $OPERATION in platform|resource|platform,resource|rccl|deepep|byte-flux|check_deep|ib-write-bw) ;; model) exit 3 ;; *) die 'invalid operation' ;; esac
HOSTFILE='' CONTAINER='' IMAGE='' ENV_SCRIPT='' CONTAINER_WORKDIR=''
PHASE='' ITEM='' ITERATION=1 STAGE_DIR='' PREPARED_DIR='' TOKEN=''
SLOTS=8 GPUS_PER_NODE='' BASELINE_FILE='' TIMEOUT_SECONDS=300 CONTAINER_SSH_PORT=25901 SKIP_IDLE_CHECK=0 KILL_AFTER=15
SSH_USER=${HCU_SSH_USER:-${USER:-root}}
declare -a NODES=() IB_ARGS=() ACTIVE_LOCAL_PIDS=()
while (( $# )); do
    case $1 in
        --skip-idle-check) SKIP_IDLE_CHECK=1; shift; continue ;;
        --*=*) option=${1%%=*}; value=${1#*=}; shift ;;
        -f|--hostfile|--container|-i|--image|--env-script|--container-workdir|--timeout|--slots|--gpus-per-node|--baseline-file|--ib-arg|--container-ssh-port|--phase|--stage-item|--stage-iteration|--stage-dir|--prepared-dir|--run-token)
            (( $# >= 2 )) || die "$1 needs a value"
            option=$1 value=$2; shift 2 ;;
        *) die "unsupported internal argument: $1" ;;
    esac
    [[ -n $value ]] || die "$option needs a nonempty value"
    case $option in
        -f|--hostfile) HOSTFILE=$value ;; --container) CONTAINER=$value ;;
        -i|--image) IMAGE=$value ;; --env-script) ENV_SCRIPT=$value ;;
        --container-workdir) CONTAINER_WORKDIR=$value ;; --timeout) TIMEOUT_SECONDS=$value ;;
        --slots) [[ $OPERATION != rccl && $OPERATION != deepep ]] || die 'unsupported parameter: --slots'; SLOTS=$value ;;
        --gpus-per-node) [[ $OPERATION == rccl || $OPERATION == deepep ]] || die 'GPU count only applies to RCCL/DeepEP'; GPUS_PER_NODE=$value ;; --baseline-file) BASELINE_FILE=$value ;;
        --container-ssh-port) CONTAINER_SSH_PORT=$value ;;
        --ib-arg) IB_ARGS+=("$value") ;;
        --phase) PHASE=$value ;; --stage-item) ITEM=$value ;; --stage-iteration) ITERATION=$value ;;
        --stage-dir) STAGE_DIR=$value ;; --prepared-dir) PREPARED_DIR=$value ;;
        --run-token) TOKEN=$value ;; *) die "unsupported internal argument: $option" ;;
    esac
done
[[ -r $HOSTFILE ]] || die "unreadable hostfile: $HOSTFILE"
HOSTFILE=$(CDPATH= cd -- "$(dirname -- "$HOSTFILE")" && printf '%s/%s' "$PWD" "$(basename -- "$HOSTFILE")") || exit 70
case $PHASE in prepare|execute|cleanup) ;; *) die 'phase must be prepare, execute or cleanup' ;; esac
[[ $TOKEN =~ ^[A-Za-z0-9][A-Za-z0-9_-]{15,95}$ ]] || die 'invalid run token'
[[ $SSH_USER =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]] || die 'unsafe HCU_SSH_USER'
for value in "$TIMEOUT_SECONDS" "$SLOTS" "$CONTAINER_SSH_PORT"; do
    [[ $value =~ ^[1-9][0-9]*$ ]] || die 'timeout, slots and port must be positive integers'
done
if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
    [[ $GPUS_PER_NODE =~ ^[1-9][0-9]*$ ]] || die 'explicit --gpus-per-node is required by the group worker'
fi
[[ $ITERATION =~ ^[0-9]+$ ]] || die 'iteration must be a non-negative integer'
(( CONTAINER_SSH_PORT <= 65535 )) || die 'invalid container SSH port'
for value in "$STAGE_DIR" "$PREPARED_DIR"; do [[ $value == /* ]] || die 'stage and prepared directories must be absolute'; done
for value in "$ENV_SCRIPT" "$BASELINE_FILE" "$CONTAINER_WORKDIR"; do [[ -z $value || $value == /* ]] || die 'target paths must be absolute'; done
if [[ $SCENARIO == per-node-container ]]; then
    [[ $CONTAINER =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && -n $IMAGE ]] || die 'container and image required'
fi
[[ $OPERATION != check_deep || $SCENARIO == per-node-container ]] || die 'checkdeepep requires per-node-container'
declare -A seen_nodes=()
while read -r node _ || [[ -n ${node:-} ]]; do
    node=${node%$'\r'}
    [[ -z $node || $node == \#* ]] && continue
    [[ $node =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "invalid node: $node"
    # T26: set-indexed dedup; order and rejection semantics unchanged.
    [[ -z ${seen_nodes[$node]:-} ]] || die "duplicate node: $node"
    seen_nodes[$node]=1
    NODES+=("$node")
done < "$HOSTFILE"
(( ${#NODES[@]} )) || die 'hostfile has no nodes'
if [[ $OPERATION == platform || $OPERATION == resource || $OPERATION == platform,resource || $OPERATION == byte-flux || $OPERATION == check_deep ]]; then
    (( ${#NODES[@]} == 1 )) || die "$OPERATION requires one node per group"
fi
COMPACT=0 TASK_PHASE=$PHASE TASK_GROUP='' TASK_STAGE='' IDENTITY_BODY=''
WORKER_MAIN_BASHPID=$BASHPID WORKER_RESULT_WRITTEN=0 WORKER_FINALIZING=0
CLEANUP_STARTED=0 CLEANUP_FINISHED=0 HANDOFF_FAILED=0 REMOTE_PID=''
EXECUTION_COMPLETED=0
if [[ ${HCU_STATE_LAYOUT:-} == compact-group-v2 ]]; then
    COMPACT=1
    [[ -r $STAGE_DIR/identity.tsv && ! -L $STAGE_DIR/identity.tsv ]] || die 'missing runtime identity'
    IDENTITY_BODY=$(cat -- "$STAGE_DIR/identity.tsv") || die 'cannot read runtime identity'
    declare -A identity=()
    while IFS=$'\t' read -r key value; do
        case $key in schema_version|group|stage_id|phase|item|iteration|token) ;; *) die 'invalid runtime identity key' ;; esac
        [[ -z ${identity[$key]+set} ]] || die 'duplicate runtime identity key'
        identity[$key]=$value
    done < "$STAGE_DIR/identity.tsv"
    [[ ${identity[schema_version]:-} == 2 && ${identity[token]:-} == "$TOKEN" ]] || die 'runtime token/schema mismatch'
    TASK_GROUP=${identity[group]:-} TASK_STAGE=${identity[stage_id]:-} TASK_PHASE=${identity[phase]:-}
    [[ $TASK_GROUP =~ ^group-[0-9]+$ && $TASK_STAGE =~ ^[A-Za-z0-9][A-Za-z0-9_.,-]*$ ]] || die 'invalid runtime group/stage'
    [[ $TASK_PHASE == prepare || $TASK_PHASE == execute ]] || die 'invalid runtime task phase'
    [[ $PHASE == cleanup || $PHASE == "$TASK_PHASE" ]] || die 'runtime phase mismatch'
    [[ ${identity[item]:-} == "$ITEM" && ${identity[iteration]:-} == "$ITERATION" ]] || die 'runtime item/iteration mismatch'
    [[ -z ${HCU_GROUP_NAME:-} || $HCU_GROUP_NAME == "$TASK_GROUP" ]] || die 'runtime group mismatch'
fi
identity_current() {
    [[ -r $STAGE_DIR/identity.tsv && ! -L $STAGE_DIR/identity.tsv &&
       $(cat -- "$STAGE_DIR/identity.tsv") == "$IDENTITY_BODY" ]]
}
identity_columns() { printf '%s\t%s\t%s\t%s' "$TASK_GROUP" "$TASK_STAGE" "$TASK_PHASE" "$TOKEN"; }
publish_handoff() {
    local destination=$1 header=$2 row=$3 temporary
    identity_current || return 73
    [[ ! -L $destination ]] || return 73
    temporary=$(mktemp "$destination.tmp.$BASHPID.XXXXXX") || return 73
    printf '%s\n%s\n' "$header" "$row" > "$temporary" || return 73
    identity_current || return 73
    mv -f -- "$temporary" "$destination" || return 73
}
write_node_exit() {
    local node=$1 code=$2 row
    if (( ! COMPACT )); then printf '%s\n' "$code" > "$STAGE_DIR/$node.exit-code"; return; fi
    printf -v row '%s\t%s\t%s\t%s' "$(identity_columns)" "$node" "$code" "$BASHPID"
    publish_handoff "$STAGE_DIR/node-exit.$node.tsv" \
        $'group\tstage_id\tphase\ttoken\tnode\texit_code\twriter_pid' "$row"
}
read_node_exit() {
    local node=$1 record=$STAGE_DIR/node-exit.$1.tsv group stage phase token member code writer header extra
    NODE_EXIT=${INTERRUPTED_RC:-125}
    [[ -r $record && ! -L $record ]] || return 73
    {
        IFS= read -r header || return 73
        IFS=$'\t' read -r group stage phase token member code writer || return 73
        if IFS= read -r extra; then return 73; fi
    } < "$record"
    [[ $header == $'group\tstage_id\tphase\ttoken\tnode\texit_code\twriter_pid' ]] || return 73
    [[ $group == "$TASK_GROUP" && $stage == "$TASK_STAGE" && $phase == "$TASK_PHASE" &&
       $token == "$TOKEN" && $member == "$node" && $code =~ ^[0-9]+$ && $writer =~ ^[1-9][0-9]*$ ]] || return 73
    NODE_EXIT=$code
}
TARGET_ROOT=$ROOT
[[ $SCENARIO != per-node-container ]] || TARGET_ROOT=${CONTAINER_WORKDIR:-$ROOT}
map_path() {
    MAPPED=$1
    if [[ $SCENARIO == per-node-container && $1 == "$ROOT"/* ]]; then MAPPED=$TARGET_ROOT/${1#"$ROOT"/}; fi
}
map_path "$STAGE_DIR"; TARGET_STAGE=$MAPPED
map_path "$PREPARED_DIR"; TARGET_PREPARED=$MAPPED
map_path "$HOSTFILE"; TARGET_HOSTFILE=$MAPPED
map_path "$ENV_SCRIPT"; ENV_SCRIPT=$MAPPED
map_path "$BASELINE_FILE"; BASELINE_FILE=$MAPPED
map_path "${HCU_PUBLIC_GROUP_LOG:-}"; TARGET_PUBLIC_GROUP_LOG=$MAPPED
GUARD=$TARGET_ROOT/hcu_envcheck/payloads/task_guard.sh
(( ! COMPACT )) || GUARD=${TARGET_PREPARED%/*}/task-guard.sh
RCCL=$TARGET_ROOT/hcu_envcheck/payloads/rccl_perf_test.sh
DEEPEP=$TARGET_ROOT/hcu_envcheck/payloads/deepep_test.sh
FLUX=$TARGET_ROOT/hcu_envcheck/payloads/flux_70b_test.sh
CHECK_DEEP=$TARGET_ROOT/hcu_envcheck/payloads/check_deepep_env.sh
IDLE=$TARGET_ROOT/hcu_envcheck/payloads/dcu_idle_check.sh
DEVICES=$TARGET_ROOT/hcu_envcheck/payloads/device_selection.py
DEVICE_RECORD_DIR=$PREPARED_DIR/devices
TARGET_DEVICE_RECORD_DIR=$TARGET_PREPARED/devices
TASK_LOG_DIR=$STAGE_DIR
if (( COMPACT )); then
    DEVICE_RECORD_DIR=$STAGE_DIR/devices TARGET_DEVICE_RECORD_DIR=$TARGET_STAGE/devices
    TASK_LOG_DIR=$PREPARED_DIR
    [[ $TASK_PHASE == prepare ]] || TASK_LOG_DIR=${PREPARED_DIR%/prepared/*}/stages/$TASK_STAGE/groups/$TASK_GROUP
fi
if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
    if (( ! COMPACT )) || [[ $PHASE == prepare ]]; then mkdir -p -- "$DEVICE_RECORD_DIR" || exit 73; fi
fi
mkdir -p -- "$STAGE_DIR" "$PREPARED_DIR" || exit 73
DIRECT_LOG=${HCU_GROUP_LOG:-}
CLEANUP_EXT=log
if [[ -n $DIRECT_LOG ]]; then
    if (( COMPACT )) && [[ ${HCU_LOG_CHANNEL:-} == 1 ]]; then
        [[ $DIRECT_LOG == /* && -p $DIRECT_LOG ]] || die 'invalid group log channel'
        # The inherited descriptor is already connected. Reopening a FIFO after
        # collector failure could block instead of reporting the failed write.
        DIRECT_LOG=/dev/fd/1
    else
        [[ $DIRECT_LOG == /* && -f $DIRECT_LOG ]] || die 'invalid direct group log'
    fi
    CLEANUP_EXT=txt
fi
# Keep log staging parents owned by the login user before container payloads
# create files underneath; the controller archives and unlinks them later.
case $OPERATION in
    rccl) mkdir -p -- "$STAGE_DIR/rccl" || exit 73 ;;
    ib-write-bw) mkdir -p -- "$STAGE_DIR/ib" || exit 73 ;;
esac
run_transport() {
    local rc=0
    "$@" & REMOTE_PID=$!
    wait "$REMOTE_PID" || rc=$?
    REMOTE_PID=''
    return "$rc"
}
# T20: cross-worker budget on ordinary node SSH. One flock slot wraps exactly
# one ssh (no permit is held while requesting another, so no self-lock). The
# cleanup lane may also take the single global reserved slot when every
# ordinary slot is busy, so stop/cleanup is never starved at N=1. When the
# controller did not export a permit dir this worker is unlimited (legacy
# callers keep their old behavior).
node_permit_exec() {
    local lane=$1; shift
    local -a slots=()
    local slot fd rc i
    for ((i=1; i<=HCU_NODE_CONCURRENCY; i++)); do slots+=("$HCU_NODE_PERMIT_DIR/permit.$i"); done
    while :; do
        for slot in "${slots[@]}"; do
            { exec {fd}>>"$slot"; } 2>/dev/null || continue
            if flock -n "$fd" 2>/dev/null; then
                run_transport "$@"; rc=$?
                flock -u "$fd" 2>/dev/null; { exec {fd}>&-; } 2>/dev/null
                return "$rc"
            fi
            { exec {fd}>&-; } 2>/dev/null
        done
        if [[ $lane == cleanup ]]; then
            { exec {fd}>>"$HCU_NODE_PERMIT_DIR/reserved.cleanup"; } 2>/dev/null || { sleep 0.2; continue; }
            if flock -w 25 "$fd" 2>/dev/null; then
                run_transport "$@"; rc=$?
                flock -u "$fd" 2>/dev/null; { exec {fd}>&-; } 2>/dev/null
                return "$rc"
            fi
            { exec {fd}>&-; } 2>/dev/null
        fi
        printf '[NODE-PERMIT] lane=%s waited: all %s ordinary node permits busy\n' "$lane" "${#slots[@]}" >&2
        sleep 0.2
    done
}
remote() {
    local node=$1; shift
    quote "$@"
    if [[ -z ${HCU_NODE_PERMIT_DIR:-} ]]; then
        run_transport ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=2 "${SSH_USER}@${node}" "$QUOTED"
        return $?
    fi
    node_permit_exec "${SSH_LANE:-ordinary}" \
        ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=2 "${SSH_USER}@${node}" "$QUOTED"
}
capture_remote() {
    local node=$1 rc=0 capture=$STAGE_DIR/control-capture.$1.txt; shift
    # Keep the SSH as a child of the worker, rather than hiding it inside a
    # command substitution which signal cleanup cannot join.
    remote "$node" "$@" > "$capture" 2>&1 || rc=$?
    CAPTURED=$(cat -- "$capture") || return 73
    return "$rc"
}
target() {
    local node=$1 body=$2
    if [[ $SCENARIO == per-node-container ]]; then
        remote "$node" docker exec -i "$CONTAINER" bash -c "$body"
    else
        remote "$node" bash -c "$body"
    fi
}
environment_body() {
    BODY="cd $(printf '%q' "$TARGET_ROOT") && "
    if (( COMPACT )); then BODY+='exec 2>&1 && '; fi
    BODY+="export HCU_LOG_SCENARIO=$(printf '%q' "$SCENARIO") HCU_LOG_CONTAINER=$(printf '%q' "$CONTAINER") HCU_LOG_NODES=$(printf '%q' "$(IFS=,; printf '%s' "${NODES[*]}")") && "
    if (( COMPACT )); then
        BODY+="export HCU_STATE_LAYOUT=compact-group-v2 HCU_LOG_CHANNEL=1 HCU_LOG_TOKEN=$(printf '%q' "$TOKEN") && "
        BODY+="export HCU_TASK_GROUP=$(printf '%q' "$TASK_GROUP") HCU_TASK_STAGE_ID=$(printf '%q' "$TASK_STAGE") HCU_TASK_PHASE=$(printf '%q' "$TASK_PHASE") && "
        BODY+="export HCU_GROUP_LOG=$(printf '%q' "$TARGET_PUBLIC_GROUP_LOG") && "
    fi
    if [[ -n $ENV_SCRIPT ]]; then
        quote printf '【环境加载】\nsource %q\n\n' "$ENV_SCRIPT"
        BODY+="$QUOTED && source $(printf '%q' "$ENV_SCRIPT") && "
    fi
    if (( COMPACT )); then
        BODY+="export HCU_TASK_GUARD_FILE=$(printf '%q' "${TARGET_PREPARED%/*}/task-guard.sh") && "
    fi
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        BODY+="export HCU_GPUS_PER_NODE=$GPUS_PER_NODE HCU_DEVICE_SELECTION_DIR=$(printf '%q' "$TARGET_PREPARED/devices") HCU_GROUP_HOSTFILE=$(printf '%q' "$TARGET_HOSTFILE") && "
        BODY+="export HCU_DEVICE_SELECTION_FILE=$(printf '%q' "$TARGET_PREPARED/devices/${NODES[0]}.json") && "
        BODY+="export HCU_STATE_CACHE_LOCK=$(printf '%q' "${TARGET_PREPARED%/*}/.script-cache.lock") && "
        if (( COMPACT )); then
            BODY+="export HCU_TASK_GUARD_FILE=$(printf '%q' "$GUARD") && "
        fi
        if [[ $OPERATION == rccl ]]; then
            BODY+="export HCU_RCCL_SCRIPT_DIR=$(printf '%q' "$TARGET_PREPARED/rccl") HCU_TASK_GUARD_FILE=$(printf '%q' "${TARGET_PREPARED%/*}/task-guard.sh") && "
        fi
    fi
}
emit_task_log() {
    local destination=$1; shift
    if (( COMPACT )) && [[ ${HCU_LOG_CHANNEL:-} == 1 ]]; then
        # Duplicate the inherited descriptor. Opening /dev/fd/1 as a named
        # FIFO would block after a failed collector lost its last read end.
        "$@" >&1 2>&1
    else
        "$@" >> "$destination" 2>&1
    fi
}
run_node() {
    local node=$1 body=$2 flags=-lc rc=0 log marker started
    if (( COMPACT )); then
        trap - EXIT
        node_interrupted() {
            local interrupted_code=$1
            trap '' INT TERM
            if [[ -n $REMOTE_PID ]]; then
                kill -TERM "$REMOTE_PID" 2>/dev/null || true
                wait "$REMOTE_PID" 2>/dev/null || true
                REMOTE_PID=''
            fi
            write_node_exit "$node" "$interrupted_code" || true
            exit "$interrupted_code"
        }
        trap 'node_interrupted 130' INT
        trap 'node_interrupted 143' TERM
    fi
    log=$STAGE_DIR/$node.log
    [[ -z $DIRECT_LOG ]] || log=$DIRECT_LOG
    marker=$TARGET_STAGE/$node.expired
    [[ $SCENARIO != per-node-container ]] || flags=-ic
    emit_task_log "$log" printf '节点执行：%s · %s · 开始\n' "$node" "${ITEM:-环境预检}" || return 73
    if [[ -n $DIRECT_LOG && $PHASE != prepare && $OPERATION != rccl && $OPERATION != deepep && $OPERATION != byte-flux ]]; then
        emit_task_log "$log" printf '【执行命令｜在 %s 执行】\n%s\n【命令结束】\n\n【测试原始输出】\n' "$node" "$body"
    fi
    # Target-side expiry evidence, produced by a deadline witness: a child of
    # this wrapper that sleeps exactly the command budget N beside timeout(1)
    # and writes the sentinel only when the deadline actually fires -- never
    # before, and only if the wrapper (its parent) is still alive. GNU timeout
    # runs the payload in its own process group, so a kill-after KILL never
    # reaches this wrapper: a real force-kill timeout still lets the wrapper
    # observe the exit code, and the witness proves the deadline fired. When
    # the payload itself dies to OOM/external SIGKILL before the deadline the
    # witness is killed mid-sleep and writes nothing. The sentinel carries the
    # run token in this stage/group directory, isolated from payload output;
    # the login-side elapsed clock logged below is diagnostic only and never
    # classifies anything.
    if (( COMPACT )); then
        marker=$TARGET_STAGE/timeout.deadline.$node.tsv
        # Preserve the single-line remote shell contract. The owner EXIT trap
        # removes its own temporary; after joining, the parent also removes
        # that exact path because SIGKILL cannot run the owner's EXIT trap.
        quote bash "$GUARD" run "$TOKEN" -- bash -c '( temporary="$1.tmp.$BASHPID"; cleanup_deadline_temporary() { rm -f -- "$temporary"; }; trap cleanup_deadline_temporary EXIT; sleep "$2"; kill -0 "$$" 2>/dev/null || exit 0; printf "group\tstage_id\tphase\ttoken\tnode\tsource\texit_code\texpired\n%s\t%s\t%s\t%s\t%s\tdeadline\t137\t1\n" "$7" "$8" "$9" "$4" "${10}" > "$temporary" && mv -f -- "$temporary" "$1" ) & witness=$!; timeout --signal=TERM --kill-after="${3}s" "$2" bash "$5" "$6"; rc=$?; kill "$witness" 2>/dev/null; wait "$witness" 2>/dev/null; rm -f -- "$1.tmp.$witness"; [ "$rc" -eq 137 ] || rm -f -- "$1" 2>/dev/null; exit "$rc"' \
            hcu-expiry-watch "$marker" "$TIMEOUT_SECONDS" "$KILL_AFTER" "$TOKEN" "$flags" "$body" "$TASK_GROUP" "$TASK_STAGE" "$TASK_PHASE" "$node"
    else
        quote bash "$GUARD" run "$TOKEN" -- bash -c '( sleep "$2"; kill -0 "$$" 2>/dev/null || exit 0; printf "%s\n" "$4" > "$1" 2>/dev/null ) & witness=$!; timeout --signal=TERM --kill-after="${3}s" "$2" bash "$5" "$6"; rc=$?; kill "$witness" 2>/dev/null; wait "$witness" 2>/dev/null; [ "$rc" -eq 137 ] || rm -f -- "$1" 2>/dev/null; exit "$rc"' \
            hcu-expiry-watch "$marker" "$TIMEOUT_SECONDS" "$KILL_AFTER" "$TOKEN" "$flags" "$body"
    fi
    started=$SECONDS
    emit_task_log "$log" target "$node" "$QUOTED" || rc=$?
    emit_task_log "$log" printf '\n节点执行：%s · %s · 退出码 %s · 耗时 %s 秒\n' "$node" "${ITEM:-环境预检}" "$rc" "$((SECONDS - started))"
    write_node_exit "$node" "$rc" || return 73
    # stdout/stderr here are the group's durable log, not the foreground panel.
    if [[ -z $DIRECT_LOG ]]; then
        printf '[TASK] node=%s phase=%s item=%s exit_code=%s log=%s\n' "$node" "$PHASE" "${ITEM:-preflight}" "$rc" "$log"
    fi
    if (( rc != 0 )); then
        printf '[ERROR] node=%s phase=%s exit_code=%s\n' "$node" "$PHASE" "$rc" >&2
        if [[ -z $DIRECT_LOG ]]; then
            awk 'tolower($0) ~ /error|failed|exception|refused|timed out|traceback/ {print; if (++n == 2) exit}' "$log" >&2
        fi
    fi
    return "$rc"
}
CLEANUP_FAILED=0
# T12 bounded-cleanup budget: each node stop is capped by timeout(1) at
# CLEANUP_NODE_BUDGET (the guard's own grace 3 + verify 10 plus scan/ssh
# slack), and at most CLEANUP_CONCURRENCY stops run at once. The worst-case
# group budget is ceil(N/CONCURRENCY) * NODE_BUDGET, which stays inside the
# controller watchdog's cleanup grace (TIMEOUT + 90) for group sizes up to
# 12 nodes, so a normal cleanup is never preempted by the supervisor.
CLEANUP_CONCURRENCY=4
CLEANUP_NODE_BUDGET=25
wait_joined() {
    local pid=$1 rc
    while :; do
        CHILD_WAIT_INTERRUPTED=0 rc=0
        wait "$pid" || rc=$?
        # A trapped signal can return from wait while the child still writes.
        # Only a subsequent uninterrupted wait is a completion receipt.
        (( CHILD_WAIT_INTERRUPTED != 0 )) || return "$rc"
    done
}
stop_tasks_legacy() {
    local node pid
    CLEANUP_FAILED=0
    printf 'node\tstatus\tevidence\n' > "$STAGE_DIR/cleanup.tsv.tmp"
    local -a stop_pids=() stop_nodes=()
    for node in "${NODES[@]}"; do
        while (( $(jobs -rp | wc -l) >= CLEANUP_CONCURRENCY )); do wait -n || true; done
            (
                # T20: stop/cleanup runs on the reserved lane when ordinary
                # permits are busy, so payload sshs can never starve cleanup.
                SSH_LANE=cleanup
                evidence=$STAGE_DIR/$node.cleanup.$CLEANUP_EXT
            status=UNCONFIRMED
            quote timeout --signal=TERM --kill-after=3s "$CLEANUP_NODE_BUDGET" bash "$GUARD" stop "$TOKEN" 3 10
            if target "$node" "$QUOTED" > "$evidence" 2>&1 && grep -q '"status":"STOPPED"' "$evidence"; then status=CONFIRMED; fi
            printf '%s\t%s\t%s\n' "$node" "$status" "$evidence" > "$STAGE_DIR/cleanup.$node.row"
        ) & stop_pids+=("$!") stop_nodes+=("$node")
    done
    # Every stop job is reaped and its row consumed exactly once, so no
    # result is lost and cleanup.tsv is assembled by a single writer in
    # hostfile order (atomic rename), never raced by parallel appends.
    for pid in "${stop_pids[@]}"; do wait "$pid" || true; done
    for node in "${stop_nodes[@]}"; do
        if [[ -f $STAGE_DIR/cleanup.$node.row ]]; then
            cat "$STAGE_DIR/cleanup.$node.row" >> "$STAGE_DIR/cleanup.tsv.tmp"
            rm -f -- "$STAGE_DIR/cleanup.$node.row"
        else
            printf '%s\tUNCONFIRMED\t%s\n' "$node" "$STAGE_DIR/$node.cleanup.$CLEANUP_EXT" >> "$STAGE_DIR/cleanup.tsv.tmp"
        fi
    done
    grep -q $'\tUNCONFIRMED\t' "$STAGE_DIR/cleanup.tsv.tmp" && CLEANUP_FAILED=1
    mv -f "$STAGE_DIR/cleanup.tsv.tmp" "$STAGE_DIR/cleanup.tsv"
    (( CLEANUP_FAILED == 0 )) || touch "$STAGE_DIR/cleanup-unconfirmed"
    touch "$STAGE_DIR/cleanup-complete"
}
stop_tasks() {
    if (( ! COMPACT )); then stop_tasks_legacy; return; fi
    local node pid status evidence row stop_rc temporary count=0
    local -a stop_pids=()
    (( CLEANUP_STARTED == 0 )) || return 0
    CLEANUP_STARTED=1 CLEANUP_FAILED=0
    CLEANUP_SOURCE=normal
    [[ $PHASE != cleanup ]] || CLEANUP_SOURCE=fallback
    CLEANUP_ATTEMPT=1 CLEANUP_DIR=$STAGE_DIR/cleanup-$CLEANUP_SOURCE-1
    identity_current && mkdir -- "$CLEANUP_DIR" || { HANDOFF_FAILED=1; CLEANUP_FAILED=1; return 73; }
    for node in "${NODES[@]}"; do
        (
            trap - EXIT INT TERM
            SSH_LANE=cleanup
            evidence=$CLEANUP_DIR/$node.guard.log status=UNCONFIRMED stop_rc=0
            quote timeout --signal=TERM --kill-after=3s "$CLEANUP_NODE_BUDGET" bash "$GUARD" stop "$TOKEN" 3 10
            target "$node" "$QUOTED" > "$evidence" 2>&1 || stop_rc=$?
            # The guard emits canonical compact JSON. Authenticate this task's
            # stop record; unrelated or stale STOPPED output is not a receipt.
            if (( stop_rc == 0 )) && grep '^__HCU_TASK_STOP__=' "$evidence" \
                | grep -F '"token":"'"$TOKEN"'"' | grep -q '"status":"STOPPED"'; then status=CONFIRMED; fi
            cat -- "$evidence" || exit 73
            printf -v row '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$(identity_columns)" \
                "$CLEANUP_SOURCE" "$CLEANUP_ATTEMPT" "$node" "$status" "$stop_rc" "$evidence"
            publish_handoff "$CLEANUP_DIR/$node.row.tsv" \
                $'group\tstage_id\tphase\ttoken\tsource\tattempt\tnode\tstatus\texit_code\tevidence' "$row"
        ) & stop_pids+=("$!")
        ((count += 1))
        # Count only this cleanup attempt's stop jobs. Execution jobs may still
        # be alive during cancellation and must not starve the stop pool.
        if (( count >= CLEANUP_CONCURRENCY )); then
            for pid in "${stop_pids[@]}"; do wait_joined "$pid" || HANDOFF_FAILED=1; done
            stop_pids=() count=0
        fi
    done
    for pid in "${stop_pids[@]}"; do wait_joined "$pid" || HANDOFF_FAILED=1; done
    temporary=$(mktemp "$CLEANUP_DIR/nodes.tsv.tmp.$BASHPID.XXXXXX") || { HANDOFF_FAILED=1; CLEANUP_FAILED=1; return 73; }
    printf 'group\tstage_id\tphase\ttoken\tsource\tattempt\tnode\tstatus\texit_code\tevidence\n' > "$temporary" || HANDOFF_FAILED=1
    for node in "${NODES[@]}"; do
        if [[ -r $CLEANUP_DIR/$node.row.tsv ]]; then
            IFS= read -r row < <(tail -n 1 -- "$CLEANUP_DIR/$node.row.tsv") || { row=''; HANDOFF_FAILED=1; }
            [[ $row != *$'\tUNCONFIRMED\t'* ]] || CLEANUP_FAILED=1
            printf '%s\n' "$row" >> "$temporary" || HANDOFF_FAILED=1
        else
            CLEANUP_FAILED=1
            printf '%s\t%s\t%s\t%s\tUNCONFIRMED\t125\t%s\n' "$(identity_columns)" \
                "$CLEANUP_SOURCE" "$CLEANUP_ATTEMPT" "$node" "$CLEANUP_DIR/$node.guard.log" >> "$temporary" || HANDOFF_FAILED=1
        fi
    done
    identity_current && mv -f -- "$temporary" "$CLEANUP_DIR/nodes.tsv" || HANDOFF_FAILED=1
    stop_rc=0; (( CLEANUP_FAILED == 0 )) || stop_rc=70
    printf -v row '%s\t%s\t%s\t%s\t%s\t1' "$(identity_columns)" "$CLEANUP_SOURCE" "$CLEANUP_ATTEMPT" "$BASHPID" "$stop_rc"
    publish_handoff "$CLEANUP_DIR/result.tsv" \
        $'group\tstage_id\tphase\ttoken\tsource\tattempt\twriter_pid\texit_code\tcompleted' "$row" || HANDOFF_FAILED=1
    CLEANUP_FINISHED=1
}
write_results() {
    local node rc cleanup log
    if (( COMPACT )); then
        local temporary
        temporary=$(mktemp "$STAGE_DIR/node-results.tsv.tmp.$BASHPID.XXXXXX") || return 73
        printf 'group\tstage_id\tphase\ttoken\tnode\texit_code\tcleanup\tlog\n' > "$temporary" || return 73
        for node in "${NODES[@]}"; do
            read_node_exit "$node" || return 73
            rc=$NODE_EXIT
            cleanup=$(awk -F '\t' -v n="$node" 'NR>1&&$7==n{print $8}' "$CLEANUP_DIR/nodes.tsv") || return 73
            log=$TASK_LOG_DIR/$node.log
            [[ $OPERATION != rccl && $OPERATION != deepep && $OPERATION != ib-write-bw || $TASK_PHASE != execute ]] || log=$TASK_LOG_DIR/${NODES[0]}.log
            printf '%s\t%s\t%s\t%s\t%s\n' "$(identity_columns)" "$node" "$rc" "${cleanup:-UNCONFIRMED}" "$log" >> "$temporary" || return 73
        done
        identity_current && mv -f -- "$temporary" "$STAGE_DIR/node-results.tsv" || return 73
        # A bare return inside a signal trap inherits the interrupted command's
        # status, even after successful writes. Report the publication itself.
        return 0
    fi
    printf 'node\texit_code\tcleanup\tlog\n' > "$STAGE_DIR/node-results.tsv.tmp"
    for node in "${NODES[@]}"; do
        rc=${INTERRUPTED_RC:-125}
        [[ ! -r $STAGE_DIR/$node.exit-code ]] || read -r rc < "$STAGE_DIR/$node.exit-code"
        cleanup=$(awk -F '\t' -v n="$node" 'NR>1&&$1==n{print $2}' "$STAGE_DIR/cleanup.tsv")
        log=$STAGE_DIR/$node.log
        [[ $OPERATION != rccl && $OPERATION != deepep && $OPERATION != ib-write-bw || $PHASE != execute ]] || log=$STAGE_DIR/${NODES[0]}.log
        printf '%s\t%s\t%s\t%s\n' "$node" "$rc" "${cleanup:-UNCONFIRMED}" "$log" >> "$STAGE_DIR/node-results.tsv.tmp"
    done
    mv -f -- "$STAGE_DIR/node-results.tsv.tmp" "$STAGE_DIR/node-results.tsv"
}
annotate_execution() {
    local execution=$STAGE_DIR/rccl/execution.tsv temporary
    [[ $OPERATION == rccl && $TASK_PHASE == execute && -r $execution ]] || return 0
    temporary=$(mktemp "$execution.tmp.$BASHPID.XXXXXX") || return 73
    awk -F '\t' -v OFS='\t' -v group="$TASK_GROUP" -v stage="$TASK_STAGE" -v phase="$TASK_PHASE" \
        -v token="$TOKEN" -v runtime="$STAGE_DIR" -v logical="$TASK_LOG_DIR" \
        'NR==1{print "group","stage_id","phase","token",$0;next}
         {if(index($4,runtime "/")==1)$4=logical substr($4,length(runtime)+1);print group,stage,phase,token,$0}' \
        "$execution" > "$temporary" || return 73
    identity_current && mv -f -- "$temporary" "$execution"
}
publish_worker_result() {
    local execution_rc=$1 worker_rc=$2 row
    [[ $BASHPID == "$WORKER_MAIN_BASHPID" && $PHASE != cleanup ]] || return 73
    (( WORKER_RESULT_WRITTEN == 0 )) || return 0
    printf -v row '%s\t%s\t%s\t%s\t%s\t%s\t%s\t1' "$(identity_columns)" "$BASHPID" \
        "$execution_rc" "$worker_rc" "${INTERRUPTED_RC:-0}" "${CLEANUP_SOURCE:-normal}" "${CLEANUP_ATTEMPT:-1}"
    publish_handoff "$STAGE_DIR/worker-result.tsv" \
        $'group\tstage_id\tphase\ttoken\tworker_pid\texecution_exit_code\texit_code\tinterrupted_exit_code\tcleanup_source\tcleanup_attempt\tcompleted' "$row" || return 73
    WORKER_RESULT_WRITTEN=1
}
finish_compact_worker() {
    local execution_rc=$1 worker_rc=$1 pid child_rc collected_rc=0 had_children=0 node
    [[ $BASHPID == "$WORKER_MAIN_BASHPID" ]] || return 73
    WORKER_FINALIZING=1
    stop_tasks || HANDOFF_FAILED=1
    if [[ -n $REMOTE_PID ]]; then
        kill -TERM "$REMOTE_PID" 2>/dev/null || true
        wait "$REMOTE_PID" 2>/dev/null || true
        REMOTE_PID=''
    fi
    if [[ -n ${INTERRUPTED_RC:-} ]] && (( CLEANUP_FAILED != 0 )); then
        for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
    fi
    # A confirmed token stop allows SSH and run_node to return their actual
    # result and publish node handoffs. Do not kill that writer before joining.
    for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do
        had_children=1 child_rc=0
        wait_joined "$pid" || child_rc=$?
        (( child_rc == 0 )) || collected_rc=$child_rc
    done
    ACTIVE_LOCAL_PIDS=()
    if (( EXECUTION_COMPLETED == 0 && had_children )); then execution_rc=$collected_rc; fi
    if [[ $TASK_PHASE == execute && ( $OPERATION == rccl || $OPERATION == deepep || $OPERATION == ib-write-bw ) ]]; then
        for node in "${NODES[@]:1}"; do
            [[ -e $STAGE_DIR/node-exit.$node.tsv ]] || write_node_exit "$node" "$execution_rc" || HANDOFF_FAILED=1
        done
    fi
    write_results || HANDOFF_FAILED=1
    annotate_execution || HANDOFF_FAILED=1
    worker_rc=$execution_rc
    [[ -z ${INTERRUPTED_RC:-} ]] || worker_rc=$INTERRUPTED_RC
    (( CLEANUP_FAILED == 0 )) || worker_rc=70
    (( HANDOFF_FAILED == 0 )) || worker_rc=73
    publish_worker_result "$execution_rc" "$worker_rc" || worker_rc=73
    return "$worker_rc"
}
compact_exit() {
    local exit_rc=$1
    [[ $BASHPID == "$WORKER_MAIN_BASHPID" ]] || return
    trap - EXIT
    trap '' INT TERM
    if [[ $PHASE != cleanup ]] && (( WORKER_RESULT_WRITTEN == 0 && WORKER_FINALIZING == 0 )); then
        INTERRUPTED_RC=${INTERRUPTED_RC:-$exit_rc}
        finish_compact_worker "$exit_rc"; exit_rc=$?
    fi
    exit "$exit_rc"
}
interrupted() {
    INTERRUPTED_RC=$1
    CHILD_WAIT_INTERRUPTED=1
    trap '' INT TERM
    if (( COMPACT )); then
        # A signal during normal cleanup is recorded, but must not recursively
        # start a second normal attempt or publish while its node writers live.
        (( CLEANUP_STARTED == 0 )) || return
        if [[ $PHASE == cleanup ]]; then
            stop_tasks
            (( CLEANUP_FAILED == 0 )) || exit 70
            (( HANDOFF_FAILED == 0 )) || exit 73
            exit "$INTERRUPTED_RC"
        fi
        finish_compact_worker "$INTERRUPTED_RC"
        exit $?
    fi
    stop_tasks
    for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    write_results
    printf '%s\n' "$INTERRUPTED_RC" > "$STAGE_DIR/execution-exit-code"
    (( CLEANUP_FAILED == 0 )) || exit 70
    exit "$INTERRUPTED_RC"
}
trap 'interrupted 130' INT
trap 'interrupted 143' TERM
(( ! COMPACT )) || trap 'compact_exit "$?"' EXIT
if [[ $PHASE == cleanup ]]; then
    stop_tasks
    (( CLEANUP_FAILED == 0 )) || exit 70
    (( HANDOFF_FAILED == 0 )) || exit 73
    exit 0
fi
if (( ! COMPACT )); then
    printf 'node\texit_code\tcleanup\tlog\n' > "$STAGE_DIR/node-results.tsv"
    printf 'node\tstatus\tevidence\n' > "$STAGE_DIR/cleanup.tsv"
fi

if [[ $PHASE == prepare ]]; then
    # Passive checks run once per group before any active stage is dispatched.
    prepare_failed=0
    for node in "${NODES[@]}"; do
        if [[ $SCENARIO == per-node-container ]]; then
            if (( COMPACT )); then
                if capture_remote "$node" docker inspect --type container -f '{{.State.Running}}|{{.Config.Image}}|{{.Id}}' "$CONTAINER"; then actual=$CAPTURED
                else actual="inspect failed: ${CAPTURED:-}"; fi
            else
                actual=$(remote "$node" docker inspect --type container -f '{{.State.Running}}|{{.Config.Image}}|{{.Id}}' "$CONTAINER" 2>&1) || actual="inspect failed: $actual"
            fi
            # T07 identity evidence: the host-side container facts land in the
            # node log before any target command. The gate below still only
            # compares running state and image tag.
            ident_log=${DIRECT_LOG:-$STAGE_DIR/$node.log}
            emit_task_log "$ident_log" printf '[IDENT] node=%s scenario=%s container=%s image=%s container_id=%s\n' \
                "$node" "$SCENARIO" "$CONTAINER" "$IMAGE" "$(printf '%s' "$actual" | awk -F'|' '{print $3}')"
            if [[ "$(printf '%s' "$actual" | awk -F'|' '{print $1"|"$2}')" != "true|$IMAGE" ]]; then
                if [[ -n $DIRECT_LOG ]]; then
                    emit_task_log "$DIRECT_LOG" printf '[ERROR] node=%s container/image mismatch: %s\n' "$node" "$actual"
                else
                    printf '[ERROR] node=%s container/image mismatch: %s\n' "$node" "$actual" > "$STAGE_DIR/$node.log"
                    cat "$STAGE_DIR/$node.log" >&2
                fi
                write_node_exit "$node" 69 || HANDOFF_FAILED=1; prepare_failed=1; continue
            fi
        fi
        environment_body
        # T07 identity evidence: actual python interpreter/prefix and target
        # path facts inside the execution scope. Python is probed, never
        # required here; env.sh is only sourced by environment_body.
        BODY+='ident_python=""; for ident_p in python3 python; do command -v "$ident_p" >/dev/null 2>&1 && ident_python=$ident_p && break; done; '
        BODY+='printf "[IDENT] scenario='"$SCENARIO"' pwd=%s writable=%s python=%s prefix=%s\n" "$PWD" "$([ -w . ] && echo yes || echo no)" "${ident_python:-none}" "$([ -n "$ident_python" ] && "$ident_python" -c "import sys; print(sys.prefix)" 2>/dev/null || echo none)" && '
        BODY+="test -r $(printf '%q' "$GUARD") && test -r $(printf '%q' "$TARGET_HOSTFILE") && test -d $(printf '%q' "$TARGET_STAGE") && test -w $(printf '%q' "$TARGET_STAGE") && test -d $(printf '%q' "$TARGET_PREPARED") && "
        BODY+='command -v timeout >/dev/null && command -v setsid >/dev/null && command -v flock >/dev/null && '
        case $OPERATION in
            platform|resource|platform,resource)
                BODY+='python_bin=""; for p in python3 python; do command -v "$p" >/dev/null 2>&1 && python_bin=$p && break; done; test -n "$python_bin"'
                ;;
            rccl) BODY+="test -r $(printf '%q' "$RCCL")" ;;
            deepep)
                BODY+="test -r $(printf '%q' "$DEEPEP") && test -r $(printf '%q' "$TARGET_ROOT/hcu_envcheck/payloads/run_deepep_test.sh") && command -v torchrun >/dev/null && "
                BODY+="deepep_dir=$(printf '%q' "$TARGET_ROOT/third-party/DeepEP/tests") && "
                BODY+='deepep_dir=${HCU_DEEPEP_TEST_DIR:-$deepep_dir} && test -r "$deepep_dir/test_low_latency.py"'
                if (( ${#NODES[@]} < 2 )); then BODY+=' && test -r "$deepep_dir/test_intranode.py"'; else BODY+=' && command -v mpirun >/dev/null && test -r "$deepep_dir/test_internode.py"'; fi
                ;;
            byte-flux)
                BODY+="test -r $(printf '%q' "$FLUX") && command -v torchrun >/dev/null && "
                BODY+="flux_dir=$(printf '%q' "$TARGET_ROOT/third-party/flux/model_tests/training") && "
                BODY+='flux_dir=${HCU_FLUX_TEST_DIR:-$flux_dir} && test -r "$flux_dir/../../scripts/launch_tp4.sh" && test -r "$flux_dir/../../test/python/test_ag_kernel.py" && test -r "$flux_dir/../../test/python/test_gemm_rs.py"'
                ;;
            check_deep) BODY+="test -r $(printf '%q' "$CHECK_DEEP")" ;;
            ib-write-bw) BODY+='command -v python3 >/dev/null && command -v ssh >/dev/null && command -v ib_write_bw >/dev/null' ;;
        esac
        if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
            quote bash -c 'p=; for candidate in python3 python; do command -v "$candidate" >/dev/null && { p=$candidate; break; }; done; test -n "$p" && exec "$p" -B "$@"' hcu-select-devices "$DEVICES" select --count "$GPUS_PER_NODE" --record "$TARGET_DEVICE_RECORD_DIR/$node.json" --node "$node"
            BODY+=" && $QUOTED"
        fi
        # T10: rccl joins deepep/byte-flux in the unified per-node idle gate —
        # every group member must pass dcu_idle_check during prepare before
        # the group's payload is dispatched (the rccl payload itself only
        # re-checks the group master, so multi-node groups relied on this
        # loop alone). Execute keeps its explicit --skip-idle-check because
        # this same-run prepare already holds the evidence.
        if [[ $OPERATION == deepep || $OPERATION == byte-flux || $OPERATION == rccl ]] && (( SKIP_IDLE_CHECK == 0 )); then BODY+=" && bash $(printf '%q' "$IDLE") --quiet"; fi
        run_node "$node" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
    done
    for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do wait "$pid" || prepare_failed=1; done
    ACTIVE_LOCAL_PIDS=()
    if (( prepare_failed == 0 )) && [[ $OPERATION == rccl ]]; then
        environment_body
        BODY+="export HCU_CLUSTER_TIMEOUT_SECONDS=$TIMEOUT_SECONDS && "
        [[ -z $ENV_SCRIPT ]] || BODY+="export HCU_CLUSTER_ENV_SCRIPT=$(printf '%q' "$ENV_SCRIPT") && "
        [[ $SCENARIO != per-node-container ]] || BODY+='export HCU_ALLOW_ROOT_MPI=1 && '
        port=22; [[ $SCENARIO != per-node-container ]] || port=$CONTAINER_SSH_PORT
        quote bash "$RCCL" "$TARGET_HOSTFILE" --port "$port" --gpus-per-node "$GPUS_PER_NODE"
        BODY+=$QUOTED
        [[ -z $BASELINE_FILE ]] || BODY+=" --baseline-file $(printf '%q' "$BASELINE_FILE")"
        (( SKIP_IDLE_CHECK == 0 )) || BODY+=' --skip-idle-check'
        plan_file=$TARGET_PREPARED/rccl-plan.tsv
        (( ! COMPACT )) || plan_file=$TARGET_STAGE/rccl-plan.tsv
        BODY+=" --prepare-only --plan-file $(printf '%q' "$plan_file") --log-dir $(printf '%q' "$TARGET_STAGE/rccl")"
        run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
        wait "${ACTIVE_LOCAL_PIDS[0]}" || prepare_failed=1
        ACTIVE_LOCAL_PIDS=()
    fi
    result=$prepare_failed
else
    [[ -n $ITEM ]] || die 'execute needs stage-item'
    environment_body
    case $OPERATION in
        platform|resource|platform,resource)
            if [[ $OPERATION == resource ]]; then
                BODY+='python3 -c '\''from hcu_envcheck.pod_probe import main; main(("resource",))'\'''
            elif [[ $OPERATION == platform ]]; then
                BODY+='python3 -c '\''from hcu_envcheck.pod_probe import main; main(("platform",))'\'''
            else
                BODY+='python3 -m hcu_envcheck.pod_probe'
            fi
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
        rccl)
            port=22; [[ $SCENARIO != per-node-container ]] || port=$CONTAINER_SSH_PORT
            BODY+="export HCU_CLUSTER_TIMEOUT_SECONDS=$TIMEOUT_SECONDS && "
            [[ -z $DIRECT_LOG ]] || BODY+='export HCU_DIRECT_GROUP_LOG=1 && '
            [[ -z $ENV_SCRIPT ]] || BODY+="export HCU_CLUSTER_ENV_SCRIPT=$(printf '%q' "$ENV_SCRIPT") && "
            [[ $SCENARIO != per-node-container ]] || BODY+='export HCU_ALLOW_ROOT_MPI=1 && '
            quote bash "$RCCL" "$TARGET_HOSTFILE" --port "$port" --gpus-per-node "$GPUS_PER_NODE"
            BODY+=$QUOTED
            [[ -z $BASELINE_FILE ]] || BODY+=" --baseline-file $(printf '%q' "$BASELINE_FILE")"
            BODY+=" --tests $(printf '%q' "$ITEM") --raw-only --skip-idle-check --log-dir $(printf '%q' "$TARGET_STAGE/rccl")"
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
        deepep)
            [[ $ITEM == intranode || $ITEM == lowl || $ITEM == norm ]] || die 'invalid DeepEP mode'
            group_index=${HCU_GROUP_INDEX:-1}
            [[ $group_index =~ ^[0-9]+$ ]] || die 'invalid group index'
            port=${HCU_GROUP_PORT:-$((29521 + group_index))}
            [[ $port =~ ^[1-9][0-9]*$ && $port -le 65535 ]] || die 'invalid DeepEP port'
            if (( ${#NODES[@]} == 1 )); then
                [[ $ITEM == intranode ]] || die 'single-node DeepEP only runs intranode'
                quote bash "$DEEPEP" intranode
            else
                [[ $ITEM != intranode ]] || die 'intranode requires one-node groups'
                peer_port=22; [[ $SCENARIO != per-node-container ]] || peer_port=$CONTAINER_SSH_PORT
                quote bash "$DEEPEP" group "$TARGET_HOSTFILE" "$ITEM" "$port" "$peer_port" "$ENV_SCRIPT"
            fi
            BODY+=$QUOTED
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
        byte-flux)
            [[ $ITEM == 70b ]] || die 'invalid byte-flux item'
            quote bash "$FLUX"; BODY+=$QUOTED
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
        check_deep)
            quote bash "$CHECK_DEEP"; BODY+=$QUOTED
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
        ib-write-bw)
            peer_port=22; peer_user=$SSH_USER
            if [[ $SCENARIO == per-node-container ]]; then
                peer_user=root
                if (( COMPACT )); then
                    capture_remote "${NODES[0]}" docker exec "$CONTAINER" bash -lc \
                        "ps -eo args | awk '/[s]shd/ {for(i=1;i<=NF;i++) if(\$i==\"-p\") {print \$(i+1); exit}}'" || die 'cannot inspect container sshd port'
                    peer_port=$CAPTURED
                else
                    peer_port=$(remote "${NODES[0]}" docker exec "$CONTAINER" bash -lc \
                        "ps -eo args | awk '/[s]shd/ {for(i=1;i<=NF;i++) if(\$i==\"-p\") {print \$(i+1); exit}}'") || die 'cannot inspect container sshd port'
                fi
                [[ $peer_port =~ ^[1-9][0-9]*$ && $peer_port -le 65535 ]] || die 'container sshd port not found for ib-write-bw'
            fi
            if [[ -n $DIRECT_LOG ]] && (( ! COMPACT )); then
                map_path "$DIRECT_LOG"
                BODY+="export HCU_GROUP_LOG=$(printf '%q' "$MAPPED") && "
            fi
            quote python3 -m hcu_envcheck.target_ib --hostfile "$TARGET_HOSTFILE" --output-dir "$TARGET_STAGE/ib" --ssh-port "$peer_port" --ssh-user "$peer_user" --run-token "$TOKEN" --timeout "$TIMEOUT_SECONDS"
            [[ -z $ENV_SCRIPT ]] || QUOTED+=" --env-script $(printf '%q' "$ENV_SCRIPT")"
            for arg in "${IB_ARGS[@]}"; do QUOTED+=" $(printf '%q' "$arg")"; done
            BODY+=$QUOTED
            run_node "${NODES[0]}" "$BODY" & ACTIVE_LOCAL_PIDS+=("$!")
            ;;
    esac
    result=0
    for pid in "${ACTIVE_LOCAL_PIDS[@]}"; do
        rc=0; wait "$pid" || rc=$?
        (( rc == 0 )) || result=$rc
    done
    ACTIVE_LOCAL_PIDS=()
    if [[ $OPERATION == rccl || $OPERATION == deepep || $OPERATION == ib-write-bw ]]; then
        # Group launchers provide one collective exit code; retain it for every node.
        for node in "${NODES[@]:1}"; do write_node_exit "$node" "$result" || HANDOFF_FAILED=1; done
    fi
fi
if (( COMPACT )); then
    EXECUTION_COMPLETED=1
    finish_compact_worker "$result"
    exit $?
fi
printf '%s\n' "$result" > "$STAGE_DIR/execution-exit-code"
stop_tasks
write_results
(( CLEANUP_FAILED == 0 )) || exit 70
exit "$result"
