#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Container lifecycle operations for hcu-cluster-run.
set -uo pipefail

usage() {
    cat <<'HELP'
Usage: hcu-cluster-run per-node-container <container-operation> -f HOSTFILE [options]
Operations: container-status | container-create | container-recreate | container-delete
Required:   --container NAME
            -i/--image TAG (except container-delete)
            --port N (container-create/container-recreate)
Options:    --image-tar TAR
            --concurrency N (default 32)
            -o/--output-dir DIR (default: project verify_results)
            --log-detail milestone|every|quiet
              create/recreate/delete: grouped results (default), full detail,
              or exceptions and summary; per-node evidence is always saved
            -v/--volume HOST:CONTAINER[:ro] (repeatable)
            --docker-arg=VALUE (repeatable)
            --yes (container-recreate/container-delete)
            --dry-run

container-create/container-recreate use these default mounts:
  /opt/hyhal:/opt/hyhal:ro
  /root/.ssh:/root/.ssh:ro
HELP
}

die() { printf '[ERROR] %s\n' "$*" >&2; exit 2; }
quote() { printf -v QUOTED '%q ' "$@"; }

(( $# >= 2 )) || { usage; exit 2; }
SCENARIO=$1 OPERATION=$2; shift 2
[[ $SCENARIO == per-node-container ]] || die 'container operations require per-node-container'
case $OPERATION in
    container-status|container-create|container-recreate|container-delete) ;;
    *) die "unknown container operation: $OPERATION" ;;
esac

HOSTFILE='' CONTAINER='' IMAGE='' IMAGE_TAR='' PORT='' CONCURRENCY=32 OUTPUT_DIR='' LOG_DETAIL=milestone
YES=0 DRY_RUN=0
SSH_USER=${HCU_SSH_USER:-${USER:-root}}
declare -a VOLUMES=() DOCKER_ARGS=() NODES=()
declare -a DEFAULT_VOLUMES=(
    /opt/hyhal:/opt/hyhal:ro
    /root/.ssh:/root/.ssh:ro
)

while (( $# )); do
    case $1 in
        -f|--hostfile|--container|-i|--image|--image-tar|--port|-v|--volume|--docker-arg|--concurrency|-o|--output-dir|--log-detail)
            (( $# >= 2 )) || die "$1 requires a value"
            option=$1 value=$2; shift 2 ;;
        --*=*) option=${1%%=*}; value=${1#*=}; shift ;;
        --yes) YES=1; shift; continue ;;
        --dry-run) DRY_RUN=1; shift; continue ;;
        -h|--help) usage; exit 0 ;;
        # Parsed by the group supervisor or irrelevant to a container operation.
        --group-size|--slots|--timeout|--env-script|--container-ssh-port)
            (( $# >= 2 )) || die "$1 requires a value"
            shift 2; continue ;;
        *) die "unsupported argument: $1" ;;
    esac
    [[ -n $value ]] || die "$option requires a nonempty value"
    case $option in
        -f|--hostfile) HOSTFILE=$value ;;
        --container) CONTAINER=$value ;;
        -i|--image) IMAGE=$value ;;
        --image-tar) IMAGE_TAR=$value ;;
        --port) PORT=$value ;;
        --concurrency) CONCURRENCY=$value ;;
        -o|--output-dir) OUTPUT_DIR=$value ;;
        --log-detail) LOG_DETAIL=$value ;;
        -v|--volume) VOLUMES+=("$value") ;;
        --docker-arg) DOCKER_ARGS+=("$value") ;;
        --group-size|--slots|--timeout|--env-script|--container-ssh-port) : ;;
        *) die "unsupported argument: $option" ;;
    esac
done

[[ -n $HOSTFILE && -r $HOSTFILE ]] || die "hostfile is missing or unreadable: $HOSTFILE"
[[ $CONCURRENCY =~ ^[1-9][0-9]*$ && $CONCURRENCY -le 1024 ]] || die 'invalid concurrency'
case $LOG_DETAIL in milestone|every|quiet) ;; *) die 'invalid log-detail' ;; esac
[[ $SSH_USER =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]] || die 'unsafe HCU_SSH_USER'
[[ $CONTAINER =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die 'invalid container name'
if [[ $OPERATION != container-delete ]]; then
    [[ -n $IMAGE ]] || die 'image is required'
fi
if [[ $OPERATION == container-create || $OPERATION == container-recreate ]]; then
    [[ $PORT =~ ^[1-9][0-9]*$ && $PORT -le 65535 ]] || die "$OPERATION requires a valid --port"
fi
if [[ $OPERATION == container-recreate || $OPERATION == container-delete ]]; then
    (( YES || DRY_RUN )) || die "$OPERATION requires --yes"
fi
[[ -z $IMAGE_TAR || $IMAGE_TAR == /* ]] || die '--image-tar needs a target-visible absolute path'
for value in "${DOCKER_ARGS[@]}"; do
    case $value in
        --network*|--net=*|--entrypoint*|--user*|--pid*|--mount*|--volume*|-v*|--name*|--publish*|-p*|--privileged=false)
            die "conflicting --docker-arg: $value" ;;
    esac
done

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

remote() {
    local node=$1; shift
    quote "$@"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_USER}@${node}" "$QUOTED"
}

wait_wave() {
    local pid
    WAVE_FAILED=0
    for pid in "${PIDS[@]}"; do wait "$pid" || WAVE_FAILED=1; done
    PIDS=()
}

# T18: per-node structured results for all four operations. status uses the
# reporting protocol vocabulary (model.py TERMINAL: PASS/FAIL/WARN); new
# reason codes below are appended to that enumeration, never renamed. reason
# is always the final field. The last record per node wins in RESULTS_DIR so
# the summary counts final outcomes; nodes never reached stay unknown.
RESULTS_DIR=''
LIFECYCLE_OUTPUT_ENABLED=0
source "$(dirname -- "${BASH_SOURCE[0]}")/lifecycle_output.sh" || die 'cannot load lifecycle output adapter'
node_result() {
    local node=$1 stage=$2 status=$3 code=$4 rc=$5 cid=${6:--} img=${7:--} text=$8
    # docker error text can embed newlines; one record must stay one line
    text=${text//$'\n'/ }
    text=${text//$'\r'/ }
    printf '[NODE-RESULT] scenario=per-node-container op=%s node=%s stage=%s status=%s rc=%s container_id=%s image=%s reason_code=%s reason=%s\n' \
        "$OPERATION" "$node" "$stage" "$status" "$rc" "$cid" "$img" "$code" "$text"
    if [[ $LIFECYCLE_OUTPUT_ENABLED == 1 ]]; then
        lifecycle_output_record "$node" "$stage" "$status" "$code" "$rc" "$cid" "$img" "$text"
    fi
    [[ -n $RESULTS_DIR && -n $node ]] && printf '%s\n' "$status" > "$RESULTS_DIR/$node"
}
summarize() {
    lifecycle_output_flush || printf '[ERROR] cannot finish grouped output; see lifecycle evidence\n' >&2
    local total=$1 pass=0 warn=0 fail=0 outcome file
    if [[ -n $RESULTS_DIR ]]; then
        for file in "$RESULTS_DIR"/*; do
            [[ -f $file ]] || continue
            outcome=$(cat "$file" 2>/dev/null)
            case $outcome in
                PASS) (( pass += 1 )) ;;
                WARN) (( warn += 1 )) ;;
                *)   (( fail += 1 )) ;;
            esac
        done
    fi
    printf '[SUMMARY] op=%s planned=%s pass=%s warn=%s fail=%s unknown=%s\n' \
        "$OPERATION" "$total" "$pass" "$warn" "$fail" "$(( total - pass - warn - fail ))"
}
trap 'lifecycle_output_exit "$?"' EXIT

load_or_pull_image() {
    local node=$1
    if [[ -n $IMAGE_TAR ]]; then
        remote "$node" docker load -i "$IMAGE_TAR" >/dev/null
    else
        local inspected inspect_rc
        if inspected=$(remote "$node" docker image inspect -f '{{.Id}}' "$IMAGE" 2>&1); then
            printf '[IMAGE] node=%s source=local image=%s\n' "$node" "$IMAGE"
            return 0
        else
            inspect_rc=$?
        fi
        # Only Docker's explicit missing-image response allows a pull.
        # SSH, daemon and permission failures must remain preflight errors.
        if (( inspect_rc != 1 )) || [[ $inspected != *'No such image:'* ]]; then
            printf '[ERROR] node=%s image inspection failed (rc=%s): %s\n' \
                "$node" "$inspect_rc" "$inspected" >&2
            return "$inspect_rc"
        fi
        printf '[IMAGE] node=%s source=pull image=%s\n' "$node" "$IMAGE"
        remote "$node" docker pull "$IMAGE" >/dev/null
    fi
}

check_port() {
    local node=$1 listeners
    listeners=$(remote "$node" bash -c "ss -H -ltn 'sport = :$PORT'" 2>&1) || {
        printf '[ERROR] cannot inspect port %s on %s: %s\n' "$PORT" "$node" "$listeners" >&2
        return 1
    }
    [[ -z $listeners ]] || {
        printf '[ERROR] port %s busy on %s\n' "$PORT" "$node" >&2
        return 1
    }
}

build_run_args() {
    local volume
    RUN_ARGS=(docker run -dit --name "$CONTAINER" --restart unless-stopped
              --shm-size=64G --device=/dev/kfd --device=/dev/mkfd --device=/dev/dri
              --cap-add=SYS_PTRACE --security-opt seccomp=unconfined --ulimit memlock=-1:-1
              --ipc=host --network=host --group-add video --privileged)
    for volume in "${DEFAULT_VOLUMES[@]}"; do RUN_ARGS+=(-v "$volume"); done
    for volume in "${VOLUMES[@]}"; do RUN_ARGS+=(-v "$volume"); done
    RUN_ARGS+=("${DOCKER_ARGS[@]}" "$IMAGE" /bin/bash)
}

if (( DRY_RUN )); then
    if [[ $OPERATION == container-create || $OPERATION == container-recreate ]]; then
        build_run_args
        printf 'DRY_RUN nodes=%s operation=%s command=' "${NODES[*]}" "$OPERATION"
        printf '%q ' "${RUN_ARGS[@]}"
        printf '\n'
    else
        printf 'DRY_RUN nodes=%s operation=%s container=%s\n' "${NODES[*]}" "$OPERATION" "$CONTAINER"
    fi
    exit 0
fi

if [[ $OPERATION != container-status ]]; then
    lifecycle_output_init || die 'cannot create lifecycle evidence directory'
fi

if [[ $OPERATION == container-status ]]; then
    RESULTS_DIR=$(mktemp -d /tmp/hcu-lifecycle.XXXXXX) || RESULTS_DIR=''
    failed=0
    PIDS=()
    for node in "${NODES[@]}"; do
        (
            state=$(remote "$node" docker inspect --type container -f '{{.State.Running}}|{{.Config.Image}}|{{.Id}}' "$CONTAINER" 2>&1)
            rc=$?
            if (( rc != 0 )); then
                if remote "$node" true >/dev/null 2>&1; then
                    node_result "$node" inspect FAIL CONTAINER_ABSENT "$rc" - "$IMAGE" "container $CONTAINER not present: $state"
                else
                    node_result "$node" inspect FAIL NODE_UNREACHABLE "$rc" - "$IMAGE" "node unreachable via ssh"
                fi
                printf '[ERROR] %s: %s\n' "$node" "$state" >&2
                exit 1
            fi
            running=$(printf '%s' "$state" | awk -F'|' '{print $1}')
            image=$(printf '%s' "$state" | awk -F'|' '{print $2}')
            cid=$(printf '%s' "$state" | awk -F'|' '{print $3}')
            if [[ $image != "$IMAGE" ]]; then
                node_result "$node" inspect FAIL IMAGE_MISMATCH "$rc" "$cid" "$IMAGE" "container runs image $image, expected $IMAGE (running=$running)"
                printf '[ERROR] %s: unexpected container image: %s\n' "$node" "$state" >&2
                exit 1
            fi
            if [[ $running != true ]]; then
                # observable state fact, not a failed read-only query
                node_result "$node" inspect WARN CONTAINER_STOPPED 0 "$cid" "$IMAGE" "expected image present but State.Running=$running"
            else
                node_result "$node" inspect PASS OK 0 "$cid" "$IMAGE" "running with expected image"
                # T07 identity evidence: traceable container ID next to the gate;
                # the pass/fail gate itself only compares state and image tag.
                printf '[IDENT] node=%s container=%s image=%s container_id=%s\n' "$node" "$CONTAINER" "$IMAGE" "$cid"
            fi
        ) & PIDS+=("$!")
        if (( ${#PIDS[@]} >= CONCURRENCY )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    done
    if (( ${#PIDS[@]} )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    summarize "${#NODES[@]}"
    (( failed == 0 )) && echo 'RESULT        PASS' || echo 'RESULT        FAIL'
    exit "$failed"
fi

if [[ $OPERATION == container-delete ]]; then
    lifecycle_output_stage delete || die 'cannot prepare lifecycle output'
    RESULTS_DIR=$(mktemp -d /tmp/hcu-lifecycle.XXXXXX) || RESULTS_DIR=''
    failed=0
    PIDS=()
    for node in "${NODES[@]}"; do
        (
            cid=$(remote "$node" docker inspect --type container -f '{{.Id}}' "$CONTAINER" 2>/dev/null) || cid=''
            if [[ -z $cid ]]; then
                if ! remote "$node" true >/dev/null 2>&1; then
                    node_result "$node" delete FAIL NODE_UNREACHABLE 255 - "${IMAGE:--}" "node unreachable via ssh"
                else
                    node_result "$node" delete FAIL CONTAINER_ABSENT 1 - "${IMAGE:--}" "container $CONTAINER not present; nothing removed"
                fi
                exit 1
            fi
            # Stop by the inspected ID for both running and stopped containers.
            out=$(remote "$node" docker stop "$cid" 2>&1)
            rc=$?
            if (( rc != 0 )); then
                node_result "$node" delete FAIL EXECUTION_FAILED "$rc" "$cid" "${IMAGE:--}" "docker stop failed: $out"
                exit 1
            fi
            out=$(remote "$node" docker rm -f "$cid" 2>&1)
            rc=$?
            if (( rc == 0 )); then
                node_result "$node" delete PASS OK 0 "$cid" "${IMAGE:--}" "removed container $CONTAINER"
            else
                node_result "$node" delete FAIL EXECUTION_FAILED "$rc" "$cid" "${IMAGE:--}" "docker rm failed: $out"
                exit 1
            fi
        ) > "$LIFECYCLE_WORK_DIR/delete/$node.log" 2>&1 & PIDS+=("$!")
        if (( ${#PIDS[@]} >= CONCURRENCY )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    done
    if (( ${#PIDS[@]} )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    summarize "${#NODES[@]}"
    (( failed == 0 )) && echo 'RESULT        PASS' || echo 'RESULT        FAIL'
    exit "$failed"
fi

failed=0
image_id=''
if [[ $OPERATION == container-recreate ]]; then
    # T16: verify Docker, image IDs and image SSH tools on every node before
    # replacing any old container. Record its original ID without depending
    # on running state; check the port only after stopping and removing it.
    # A preflight failure still leaves all old containers untouched.
    declare -A OLD_IDS=()
    RESULTS_DIR=$(mktemp -d /tmp/hcu-lifecycle.XXXXXX) || RESULTS_DIR=''
    lifecycle_output_stage preflight || die 'cannot prepare lifecycle output'
    for node in "${NODES[@]}"; do
        {
        remote "$node" docker info >/dev/null; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "docker info failed on $node"; die "Docker unavailable on $node"; }
        load_or_pull_image "$node"; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "image preparation failed"; die "image preparation failed on $node"; }
        current=$(remote "$node" docker image inspect -f '{{.Id}}' "$IMAGE"); rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "image inspect failed"; die "image inspect failed on $node"; }
        [[ $current == sha256:* ]] || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "invalid image ID: $current"; die "invalid image ID on $node"; }
        [[ -z $image_id || $image_id == "$current" ]] || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "image IDs differ between nodes: $current vs $image_id"; die 'image IDs differ between nodes'; }
        image_id=$current
        remote "$node" docker run --rm --network=none --entrypoint=/bin/bash "$IMAGE" \
            -lc 'command -v ssh >/dev/null && test -x /usr/sbin/sshd' >/dev/null; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" "$image_id" "$IMAGE" "image SSH tools missing"; die "image SSH tools missing on $node"; }
        old_id=$(remote "$node" docker inspect --type container -f '{{.Id}}' "$CONTAINER" 2>/dev/null) || old_id=''
        if [[ -n $old_id ]]; then
            [[ $old_id =~ ^(sha256:)?[0-9a-f]{64}$ ]] || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "invalid old container ID: $old_id"; die "invalid old container ID on $node: $old_id"; }
        fi
        OLD_IDS[$node]=$old_id
        node_result "$node" preflight PASS OK 0 "${old_id:-$image_id}" "$IMAGE" "docker/image/tools checks passed"
        } > "$LIFECYCLE_WORK_DIR/preflight/$node.log" 2>&1
    done
    (( failed == 0 )) || { summarize "${#NODES[@]}"; echo 'RESULT        FAIL'; exit 2; }
    lifecycle_output_stage replace || die 'cannot prepare lifecycle output'
    # Every node preflighted green: replace now. The FULL old ID is
    # re-verified immediately before each removal so a same-name newcomer
    # can never be mistaken for the old container, and the port must be
    # free again after removal and before the new run.
    PIDS=()
    for node in "${NODES[@]}"; do
        (
            current_id=$(remote "$node" docker inspect --type container -f '{{.Id}}' "$CONTAINER" 2>/dev/null) || current_id=''
            if [[ -n $current_id ]]; then
                [[ $current_id == "${OLD_IDS[$node]}" ]] || { node_result "$node" replace FAIL EXECUTION_FAILED 1 "$current_id" "$IMAGE" "container ID changed since preflight; not deleting"; printf '[ERROR] %s: container ID changed since preflight; not deleting\n' "$node" >&2; exit 1; }
                out=$(remote "$node" docker stop "$current_id" 2>&1); rc=$?
                (( rc == 0 )) || { node_result "$node" replace FAIL EXECUTION_FAILED "$rc" "$current_id" "$IMAGE" "docker stop failed: $out"; exit 1; }
                out=$(remote "$node" docker rm -f "$current_id" 2>&1); rc=$?
                (( rc == 0 )) || { node_result "$node" replace FAIL EXECUTION_FAILED "$rc" "$current_id" "$IMAGE" "docker rm failed: $out"; exit 1; }
                printf '[REPLACE] %s: removed old container %s\n' "$node" "${current_id:0:12}"
            fi
            listeners=$(remote "$node" bash -c "ss -H -ltn 'sport = :$PORT'" 2>&1); rc=$?
            if (( rc != 0 )); then node_result "$node" replace FAIL EXECUTION_FAILED "$rc" "${current_id:--}" "$IMAGE" "cannot inspect port: $listeners"; printf '[ERROR] %s: %s\n' "$node" "$listeners" >&2; exit 1; fi
            [[ -z $listeners ]] || { node_result "$node" replace FAIL EXECUTION_FAILED 1 "${current_id:--}" "$IMAGE" "port $PORT still busy after removal"; printf '[ERROR] %s: port %s still busy after removal\n' "$node" "$PORT" >&2; exit 1; }
            node_result "$node" replace PASS OK 0 "${current_id:--}" "$IMAGE" "old container removed and port free"
        ) > "$LIFECYCLE_WORK_DIR/replace/$node.log" 2>&1 & PIDS+=("$!")
        if (( ${#PIDS[@]} >= CONCURRENCY )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    done
    if (( ${#PIDS[@]} )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
    (( failed == 0 )) || { summarize "${#NODES[@]}"; echo 'RESULT        FAIL'; exit 2; }
else
    # container-create: finish all checks before creating any container in this group.
    RESULTS_DIR=$(mktemp -d /tmp/hcu-lifecycle.XXXXXX) || RESULTS_DIR=''
    lifecycle_output_stage preflight || die 'cannot prepare lifecycle output'
    for node in "${NODES[@]}"; do
        {
        node_ok=1
        remote "$node" docker info >/dev/null; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "docker info failed on $node"; die "Docker unavailable on $node"; }
        load_or_pull_image "$node"; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "image preparation failed"; die "image preparation failed on $node"; }
        current=$(remote "$node" docker image inspect -f '{{.Id}}' "$IMAGE"); rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "image inspect failed"; die "image inspect failed on $node"; }
        [[ $current == sha256:* ]] || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "invalid image ID: $current"; die "invalid image ID on $node"; }
        [[ -z $image_id || $image_id == "$current" ]] || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "image IDs differ between nodes: $current vs $image_id"; die 'image IDs differ between nodes'; }
        image_id=$current
        if remote "$node" docker inspect --type container "$CONTAINER" >/dev/null 2>&1; then
            node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "container $CONTAINER exists; use container-recreate --yes"
            die "container $CONTAINER exists on $node; use container-recreate --yes"
        fi
        remote "$node" docker run --rm --network=none --entrypoint=/bin/bash "$IMAGE" \
            -lc 'command -v ssh >/dev/null && test -x /usr/sbin/sshd' >/dev/null; rc=$?
        (( rc == 0 )) || { node_result "$node" preflight FAIL EXECUTION_FAILED "$rc" "$image_id" "$IMAGE" "image SSH tools missing"; die "image SSH tools missing on $node"; }
        check_port "$node" || { node_result "$node" preflight FAIL EXECUTION_FAILED 1 - "$IMAGE" "port $PORT busy"; failed=1; node_ok=0; }
        (( node_ok )) && node_result "$node" preflight PASS OK 0 "$image_id" "$IMAGE" "docker/image/tools/port checks passed"
        } > "$LIFECYCLE_WORK_DIR/preflight/$node.log" 2>&1
    done
    (( failed == 0 )) || { summarize "${#NODES[@]}"; echo 'RESULT        FAIL'; exit 2; }
fi

lifecycle_output_stage create || die 'cannot prepare lifecycle output'
build_run_args
PIDS=()
for node in "${NODES[@]}"; do
    (
        remote "$node" "${RUN_ARGS[@]}" >/dev/null; rc=$?
        if (( rc != 0 )); then
            node_result "$node" create FAIL EXECUTION_FAILED "$rc" - "$IMAGE" "docker run failed"
            printf '[ERROR] docker run failed on %s\n' "$node" >&2
            exit 1
        fi
    ) > "$LIFECYCLE_WORK_DIR/create/$node.log" 2>&1 & PIDS+=("$!")
    if (( ${#PIDS[@]} >= CONCURRENCY )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
done
if (( ${#PIDS[@]} )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
(( failed == 0 )) || { summarize "${#NODES[@]}"; echo 'RESULT        FAIL'; exit 2; }

lifecycle_output_stage ssh-identity || die 'cannot prepare lifecycle output'
# T17: initialization result, a bounded SSH-readiness wait and a provable
# target identity. A listening port, a bare `ssh true` or the container name
# alone prove nothing: each node must actually enter ITS container over SSH.
# On this site the SSH client cannot be the dispatching user (no key of that
# user is in the container's authorized_keys, which are the host-mounted
# read-only /root/.ssh); the client is the container itself, riding the same
# host root keypair the RCCL/DeepEP interconnect uses. Identity cannot come
# from /proc/self/cgroup (docker's private cgroup namespace masks it inside
# the container) nor from hostname (--network=host shares the host UTS ns),
# so a one-time token equal to the container's full ID is staged in the
# container-private /run via docker exec and must be read back through a real
# SSH connection on $PORT: only the target container's filesystem can return
# it. No keys are exchanged, host checking is not disabled and no passwords
# are enabled.
PIDS=()
for node in "${NODES[@]}"; do
    (
        if ! remote "$node" docker exec "$CONTAINER" bash -c "mkdir -p /var/run/sshd && /usr/sbin/sshd -p $PORT" >/dev/null 2>&1; then
            node_result "$node" ssh-init FAIL EXECUTION_FAILED 1 - "$IMAGE" "sshd initialization failed in $CONTAINER"
            printf '[ERROR] %s: sshd initialization failed in %s\n' "$node" "$CONTAINER" >&2
            exit 1
        fi
        container_id=$(remote "$node" docker inspect --type container -f '{{.Id}}' "$CONTAINER" 2>/dev/null) || { node_result "$node" ssh-init FAIL EXECUTION_FAILED 1 - "$IMAGE" "container ID unobtainable after create"; exit 1; }
        remote "$node" docker exec "$CONTAINER" bash -c "printf '%s\n' '$container_id' > /run/hcu-container-identity && chmod 644 /run/hcu-container-identity" >/dev/null 2>&1 || {
            node_result "$node" ssh-init FAIL EXECUTION_FAILED 1 "$container_id" "$IMAGE" "cannot stage identity token inside $CONTAINER"
            printf '[ERROR] %s: cannot stage identity token inside %s\n' "$node" "$CONTAINER" >&2
            exit 1
        }
        identity=''
        for attempt in 1 2 3 4 5 6 7 8 9 10; do
            identity=$(remote "$node" docker exec "$CONTAINER" ssh -o BatchMode=yes -o ConnectTimeout=3 -p "$PORT" root@127.0.0.1 \
                "cat /run/hcu-container-identity" 2>/dev/null) || { sleep 2; continue; }
            [[ "$identity" == "$container_id" ]] && break
            sleep 2
        done
        if [[ "$identity" != "$container_id" ]]; then
            node_result "$node" ssh-identity FAIL EXECUTION_FAILED 1 "$container_id" "$IMAGE" "SSH into $CONTAINER on port $PORT not ready or identity unprovable after bounded wait (last=${identity:-none})"
            printf '[ERROR] %s: SSH into %s on port %s not ready or identity unprovable after bounded wait (last=%s)\n' \
                "$node" "$CONTAINER" "$PORT" "${identity:-none}" >&2
            exit 1
        fi
        node_result "$node" ssh-identity PASS OK 0 "$container_id" "$IMAGE" "SSH entered target container; token roundtrip proven"
        printf '[SSH-IDENTITY] node=%s container=%s port=%s evidence=token-roundtrip/%s\n' "$node" "$CONTAINER" "$PORT" "${container_id:0:12}"
    ) > "$LIFECYCLE_WORK_DIR/ssh-identity/$node.log" 2>&1 & PIDS+=("$!")
    if (( ${#PIDS[@]} >= CONCURRENCY )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
done
if (( ${#PIDS[@]} )); then wait_wave; (( WAVE_FAILED == 0 )) || failed=1; fi
(( failed == 0 )) || { summarize "${#NODES[@]}"; echo 'RESULT        FAIL'; exit 2; }

summarize "${#NODES[@]}"
echo 'RESULT        PASS'
