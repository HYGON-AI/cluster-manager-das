#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Test item -> concurrent groups -> barrier. Bash owns execution and cleanup.
set -uo pipefail
ROOT=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || exit 70
die() { printf '[ERROR] %s\n' "$*" >&2; exit 2; }
source "$ROOT/hcu_envcheck/logging.sh" || die 'cannot load logging helpers'
source "$ROOT/hcu_envcheck/state_runtime.sh" || die 'cannot load state runtime helpers'
usage() {
    cat <<'HELP'
Usage: hcu-cluster-run <scenario> <operation> -f HOSTFILE [options]
Operations: platform | resource | platform,resource | checkdeepep
            rccl | deepep | byte-flux | ib-write-bw
  --group-size N          Nodes per RCCL/DeepEP group (default 1); IB retains its group default.
                          RCCL/DeepEP test complete groups in hostfile order only; the trailing
                          remainder is not scheduled and never counts as failure; zero complete
                          groups fails before dispatch
  --gpus-per-node N       RCCL/DeepEP cards per node (default 8), independent of group-size.
                          DeepEP single: 2/4/8; multi: 8, total ranks must divide 256 and 288.
  --slots N               Retained for other operations; unsupported by RCCL/DeepEP.
  --concurrency N         Concurrent node/group tasks (default 32)
  --node-concurrency N    Cross-worker budget on ordinary node SSH (default 32);
                          --concurrency stays the active-group limit. Reporting
                          keeps 1 separate connection and stop/cleanup keeps a
                          global reserved one, so control connections <= N+2
  --timeout SECONDS       One test item x group deadline (default 300)
  --container NAME -i IMAGE  Existing per-node container
  --env-script PATH       Optional target environment setup, also report default
  --container-workdir PATH   Target project root if its path differs
  --report-env-script PATH   Optional first-node reporting environment setup
  --report-python PATH    Python >=3.10 inside that environment
  --report-workdir PATH   Reporting project root (default target project root)
  --report-only RUN_DIR   Rebuild reports without running tests
  --export-plan DIR       With --report-only: export failed/unfinished plan
                          files (failed-nodes.hostfile, failed-groups.json,
                          unfinished-groups.json, cleanup-unconfirmed.json)
                          into a new empty directory; no tests are run
  --rerun-plan FILE       Rerun a T28 failed/unfinished export list with the
                          original members, scale, modes and rounds; new run
                          directory and tokens, old evidence untouched; host
                          file only validates members; dry-run previews
  --report-host HOST      Reporting host (default: first tested node). Decoupled
                          from the tested list; never added to the hostfile.
                          report-only refuses a host switch unless the original
                          host is selected explicitly
  --log-detail milestone|every|quiet  Panel / detailed snapshots / errors only
  --baseline-file PATH    RCCL baseline
  --container-ssh-port N  RCCL/DeepEP group peer port (default 25901)
  --skip-idle-check --dry-run -o OUTPUT_DIR
Each item's groups finish and cleanup is verified before the next item starts.
The entry host needs Bash/SSH; reporting Python runs on the first hostfile node.
HELP
}
(( $# >= 2 )) || { usage; exit 2; }
SCENARIO=$1 OPERATION=$2; shift 2
case $SCENARIO in shared-conda|node-local-conda|per-node-container) ;; *) die 'unknown scenario' ;; esac
case $OPERATION in platform|resource|platform,resource|rccl|deepep|byte-flux|check_deep|ib-write-bw) ;; *) die 'unknown operation' ;; esac
HOSTFILE='' OUTPUT_DIR='' CONTAINER='' IMAGE='' ENV_SCRIPT='' CONTAINER_WORKDIR=''
REPORT_ENV_SCRIPT='' REPORT_PYTHON='' REPORT_WORKDIR='' REPORT_ONLY='' REPORT_HOST='' EXPORT_PLAN=''
GROUP_SIZE='' SLOTS=8 SLOTS_GIVEN=0 GPUS_PER_NODE=8 GPUS_GIVEN=0 GROUP_SIZE_GIVEN=0 CONCURRENCY=32 NODE_CONCURRENCY=32 NODE_CONCURRENCY_GIVEN=0 TIMEOUT_SECONDS=300 TIMEOUT_GIVEN=0 BASELINE='' RERUN_PLAN=''
LOG_DETAIL=milestone DRY_RUN=0 SKIP_IDLE=0 CONTAINER_PORT=25901
SSH_USER=${HCU_SSH_USER:-${USER:-root}}
declare -a IB_ARGS=() NODES=() HOST_LINES=() GROUPS_LIST=() GROUP_NODES=() GROUP_FILES=()
declare -a STAGE_IDS=() STAGE_ITEMS=() STAGE_ROUNDS=() STAGE_LABELS=() ACTIVE_PIDS=()
declare -A BLOCKED=() BLOCKED_REASON=()
while (( $# )); do
    case $1 in
        -h|--help) usage; exit 0 ;;
        --dry-run) DRY_RUN=1; shift; continue ;;
        --skip-idle-check) SKIP_IDLE=1; shift; continue ;;
        --*=*) option=${1%%=*}; value=${1#*=}; shift ;;
        -f|--hostfile|-o|--output-dir|--container|-i|--image|--env-script|--container-workdir|--report-env-script|--report-python|--report-workdir|--report-only|--report-host|--export-plan|--rerun-plan|--group-size|--gpus-per-node|--slots|--concurrency|--node-concurrency|--timeout|--baseline-file|--container-ssh-port|--log-detail|--ibstat-command|--ib-tool|--ib-protocol|--ib-device|--ib-port|--ib-gid-index|--ib-control-port|--ib-message-bytes|--ib-iterations|--ib-minimum-gbps|--ib-concurrency|--ib-max-tests)
            (( $# >= 2 )) || die "$1 requires a value"
            option=$1 value=$2; shift 2 ;;
        *) die "unsupported staged argument: $1" ;;
    esac
    [[ -n $value ]] || die "$option requires a nonempty value"
    case $option in
        -f|--hostfile) HOSTFILE=$value ;; -o|--output-dir) OUTPUT_DIR=$value ;;
        --container) CONTAINER=$value ;; -i|--image) IMAGE=$value ;;
        --env-script) ENV_SCRIPT=$value ;; --container-workdir) CONTAINER_WORKDIR=$value ;;
        --report-env-script) REPORT_ENV_SCRIPT=$value ;; --report-python) REPORT_PYTHON=$value ;;
        --report-workdir) REPORT_WORKDIR=$value ;; --report-only) REPORT_ONLY=$value ;; --export-plan) EXPORT_PLAN=$value ;; --rerun-plan) RERUN_PLAN=$value ;;
        --report-host) REPORT_HOST=$value ;;
        --group-size) GROUP_SIZE=$value; GROUP_SIZE_GIVEN=1 ;; --timeout) TIMEOUT_SECONDS=$value; TIMEOUT_GIVEN=1 ;;
        --gpus-per-node)
            [[ $OPERATION == rccl || $OPERATION == deepep ]] || die 'gpus-per-node only applies to RCCL/DeepEP'
            GPUS_PER_NODE=$value; GPUS_GIVEN=1 ;;
        --slots)
            [[ $OPERATION != rccl && $OPERATION != deepep ]] || die 'unsupported parameter: --slots'
            SLOTS=$value; SLOTS_GIVEN=1 ;; --concurrency) CONCURRENCY=$value ;;
        --node-concurrency) NODE_CONCURRENCY=$value; NODE_CONCURRENCY_GIVEN=1 ;;
        --baseline-file) [[ $OPERATION == rccl ]] || die 'baseline-file only applies to RCCL'; BASELINE=$value ;;
        --container-ssh-port)
            [[ $OPERATION == rccl || $OPERATION == deepep ]] || die 'container-ssh-port only applies to RCCL/DeepEP'
            CONTAINER_PORT=$value ;;
        --log-detail) LOG_DETAIL=$value ;;
        --ibstat-command|--ib-tool|--ib-protocol|--ib-device|--ib-port|--ib-gid-index|--ib-control-port|--ib-message-bytes|--ib-iterations|--ib-minimum-gbps|--ib-concurrency|--ib-max-tests)
            [[ $OPERATION == ib-write-bw ]] || die "$option only applies to ib-write-bw"
            IB_ARGS+=("$option=$value") ;;
        *) die "unsupported staged argument: $option" ;;
    esac
done
# T29: an explicit rerun plan is a separate mode and never combines with the
# rebuild/export modes.
[[ -z $RERUN_PLAN || -z $REPORT_ONLY ]] || die '--rerun-plan cannot combine with --report-only'
[[ -z $RERUN_PLAN || -z $EXPORT_PLAN ]] || die '--rerun-plan cannot combine with --export-plan'
# T14: byte-flux is a full 64-case TP4 sweep per node (16 hidden sizes x
# ag/gemmrs variants) and routinely runs for hours; the generic 300s item
# budget would truncate a normal sweep mid-matrix. This operation-specific
# timeout applies only when the caller did not pass it explicitly; every
# other operation keeps its timeout default unchanged.
if [[ $OPERATION == byte-flux ]]; then
    (( TIMEOUT_GIVEN )) || TIMEOUT_SECONDS=4800
fi
for value in "$GPUS_PER_NODE" "$SLOTS" "$CONCURRENCY" "$NODE_CONCURRENCY" "$TIMEOUT_SECONDS"; do
    [[ $value =~ ^[1-9][0-9]*$ ]] || die 'GPU count, slots, concurrency, node-concurrency and timeout must be positive integers'
done
[[ -z $GROUP_SIZE || $GROUP_SIZE =~ ^[1-9][0-9]*$ ]] || die 'group-size must be a positive integer'
(( SLOTS <= 128 && CONCURRENCY <= 1024 )) || die 'slots/concurrency is unreasonably large'
# T20: cross-worker budget on ordinary node SSH. Rejected here, before any
# dispatch: 0/negative/non-integer never starts a run.
(( NODE_CONCURRENCY <= 1024 )) || die 'node-concurrency is unreasonably large'
if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
    [[ $CONTAINER_PORT =~ ^[1-9][0-9]*$ ]] && (( CONTAINER_PORT <= 65535 )) || die 'invalid container port'
fi
case $LOG_DETAIL in milestone|every|quiet) ;; *) die 'invalid log-detail' ;; esac
[[ $SSH_USER =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]] || die 'unsafe SSH user'
[[ -n $HOSTFILE && -r $HOSTFILE ]] || die 'readable hostfile required'
for value in "$ENV_SCRIPT" "$REPORT_ENV_SCRIPT" "$CONTAINER_WORKDIR" "$REPORT_WORKDIR" "$BASELINE" "$EXPORT_PLAN"; do
    [[ -z $value || $value == /* ]] || die 'environment, workdir and baseline paths must be absolute target paths'
    [[ $value != *$'\t'* && $value != *$'\n'* && $value != *$'\r'* ]] || die 'control characters in path'
done
if [[ $SCENARIO == per-node-container ]]; then
    [[ $CONTAINER =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && -n $IMAGE ]] || die 'container and image are required'
fi
[[ $OPERATION != check_deep || $SCENARIO == per-node-container ]] || die 'checkdeepep requires per-node-container'
declare -A seen_nodes=()
while IFS= read -r line || [[ -n ${line:-} ]]; do
    line=${line%$'\r'}; read -r node _ <<< "$line"
    [[ -z ${node:-} || $node == \#* ]] && continue
    [[ $node =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "invalid node: $node"
    # T26: set-indexed dedup (O(1) per line) instead of a scan over NODES;
    # first-seen order, duplicate/invalid/empty rejection semantics unchanged.
    [[ -z ${seen_nodes[$node]:-} ]] || die "duplicate node: $node"
    seen_nodes[$node]=1
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        read -r _node extra <<< "$line"
        [[ -z ${extra:-} || $extra == \#* ]] || die 'RCCL/DeepEP hostfile must contain node names only; remove slots= and use --gpus-per-node'
    fi
    NODES+=("$node"); HOST_LINES+=("$line")
done < "$HOSTFILE"
(( ${#NODES[@]} )) || die 'empty hostfile'
# T29: explicit rerun of an exported failed/unfinished list. The plan file is
# authoritative: original members, scale, modes and rounds are inherited from
# it (no regrouping, no node substitution, no plan-shape overrides); the
# current hostfile only validates that every original member is still an
# allowed node. A new run gets a fresh directory, fresh per-task tokens and
# rerun linkage fields in control.tsv; the original evidence is untouched.
# Real dispatch is blocked while any entry's cleanup is unconfirmed.
if [[ -n $RERUN_PLAN ]]; then
    [[ $RERUN_PLAN == /* ]] || die 'rerun plan must be an absolute path'
    [[ -r $RERUN_PLAN ]] || die "rerun plan not readable: $RERUN_PLAN"
    command -v python3 >/dev/null 2>&1 || die '--rerun-plan requires python3 on the entry host'
    if (( GROUP_SIZE_GIVEN || GPUS_GIVEN )); then
        die '--rerun-plan forbids --group-size/--gpus-per-node (plan shape comes from the export list)'
    fi
    (( ! SLOTS_GIVEN )) || die '--rerun-plan forbids --slots (plan shape comes from the export list)'
    rerun_err=$(mktemp /tmp/hcu-rerun.XXXXXX) || die 'cannot create rerun scratch file'
    if ! rerun_raw=$(python3 "$ROOT/cluster_run/rerun_plan.py" --plan "$RERUN_PLAN" --operation "$OPERATION" 2>"$rerun_err"); then
        reason=$(tr '\n' ' ' < "$rerun_err"); rm -f -- "$rerun_err"
        die "rerun plan rejected: ${reason:-python3 failed}"
    fi
    rm -f -- "$rerun_err"
    RERUN_SOURCE_RUN='' RERUN_FINGERPRINT='' RERUN_DEPLOYMENT=''
    RERUN_UNCONFIRMED=0 RERUN_CTX_NODES='' RERUN_CTX_REASON=''
    RERUN_GROUP_LINES=()
    while IFS=$'\t' read -r tag key value rest; do
        case $tag in
            ENV)
                case $key in
                    scenario) [[ $value == "$SCENARIO" ]] || die "rerun plan scenario mismatch: plan=$value invocation=$SCENARIO" ;;
                    operation) [[ $value == "$OPERATION" ]] || die "rerun plan operation mismatch: plan=$value invocation=$OPERATION" ;;
                    group_size) GROUP_SIZE=$value ;;
                    gpus_per_node) GPUS_PER_NODE=$value ;;
                    slots) SLOTS=$value ;;
                    source_run) RERUN_SOURCE_RUN=$value ;;
                    fingerprint) RERUN_FINGERPRINT=$value ;;
                    deployment_md5) RERUN_DEPLOYMENT=$value ;;
                esac ;;
            GROUP)
                group=$key
                IFS=, read -r -a gnodes <<< "$rest"
                collected=''
                for member in "${gnodes[@]}"; do
                    member_line=''
                    for ni in "${!NODES[@]}"; do
                        if [[ ${NODES[ni]} == "$member" ]]; then member_line=${HOST_LINES[ni]}; break; fi
                    done
                    [[ -n $member_line ]] || die "rerun member $member of $group is not in the current hostfile; refusing node substitution or shrunk groups"
                    if [[ $OPERATION == rccl ]]; then member_line="$member slots=$GPUS_PER_NODE"
                    elif [[ $OPERATION == deepep ]]; then member_line="$member slots=1"
                    else [[ $member_line =~ (^|[[:space:]])slots= ]] || member_line="$member_line slots=$SLOTS"; fi
                    collected+="$member_line"$'\n'
                done
                GROUPS_LIST+=("$group"); GROUP_NODES+=("$rest"); RERUN_GROUP_LINES+=("$collected") ;;
            STAGE)
                STAGE_IDS+=("$key"); STAGE_ITEMS+=("$value"); STAGE_ROUNDS+=("$rest")
                if [[ $rest == 0 ]]; then label=$value; else label="第${rest}轮 · $value"; fi
                STAGE_LABELS+=("$label") ;;
            CTX) RERUN_CTX_NODES=$key; RERUN_CTX_REASON=$value ;;
            UNCONFIRMED) RERUN_UNCONFIRMED=$key ;;
        esac
    done <<< "$rerun_raw"
    (( ${#GROUPS_LIST[@]} >= 1 )) || die 'rerun plan has no groups'
    (( ${#STAGE_IDS[@]} >= 1 )) || die 'rerun plan has no stage items'
    declare -A rerun_member_seen=()
    for members in "${GROUP_NODES[@]}"; do
        IFS=, read -r -a ms <<< "$members"
        for m in "${ms[@]}"; do rerun_member_seen[$m]=1; done
    done
    PARTICIPATING_NODES=() REMAINDER_NODES=()
    for node in "${NODES[@]}"; do
        if [[ -n ${rerun_member_seen[$node]:-} ]]; then PARTICIPATING_NODES+=("$node"); else REMAINDER_NODES+=("$node"); fi
    done
    (( ${#REMAINDER_NODES[@]} == 0 )) || printf '[RERUN] hostfile nodes outside the plan (context only, never tasks): %s\n' "$(IFS=,; printf '%s' "${REMAINDER_NODES[*]}")" >&2
    printf '[RERUN] plan=%s\n[RERUN] source_run=%s (original evidence untouched; new run gets a fresh directory and fresh per-task tokens)\n[RERUN] config fingerprint=%s matches source run; deployment=%s matches export\n[RERUN] groups=%s stages=%s; cleanup gate: %s entries with unconfirmed cleanup\n' \
        "$RERUN_PLAN" "$RERUN_SOURCE_RUN" "$RERUN_FINGERPRINT" "$RERUN_DEPLOYMENT" "${#GROUPS_LIST[@]}" "${#STAGE_IDS[@]}" "$RERUN_UNCONFIRMED" >&2
    if (( ! DRY_RUN )) && (( RERUN_UNCONFIRMED > 0 )); then
        die "rerun blocked: $RERUN_UNCONFIRMED plan entries have unconfirmed cleanup; resolve the original run first (dry-run previews this gate)"
    fi
fi
if [[ -z $RERUN_PLAN ]]; then
if [[ -z $GROUP_SIZE ]]; then
    if [[ $OPERATION == ib-write-bw ]]; then GROUP_SIZE=${#NODES[@]}; else GROUP_SIZE=1; fi
fi
if [[ $OPERATION == platform || $OPERATION == resource || $OPERATION == platform,resource || $OPERATION == check_deep || $OPERATION == byte-flux ]]; then
    (( GROUP_SIZE == 1 )) || die "$OPERATION is a per-node operation and requires --group-size 1"
fi
if [[ $OPERATION == ib-write-bw ]]; then
    (( GROUP_SIZE >= 2 && ${#NODES[@]} % GROUP_SIZE != 1 )) || die 'ib-write-bw needs at least two nodes in every group'
fi
# Shared complete-group rule (T02) for the grouped tests: RCCL and DeepEP
# schedule complete groups in hostfile order only. floor(N/G) groups
# participate; the trailing remainder is never scheduled (no preflight,
# dispatch, or cleanup, never a task denominator) and is recorded below as
# non-participating metadata. Zero complete groups fails before dispatch,
# dry-run included; groups are never shrunk and no filler tasks are created.
PARTICIPATING_NODES=() REMAINDER_NODES=()
if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
    complete_groups=$(( ${#NODES[@]} / GROUP_SIZE ))
    (( complete_groups >= 1 )) || die "$OPERATION: ${#NODES[@]} nodes cannot form one complete group of $GROUP_SIZE nodes; refusing to shrink groups or run zero tasks"
    boundary=$(( complete_groups * GROUP_SIZE ))
    PARTICIPATING_NODES=("${NODES[@]:0:boundary}")
    (( boundary >= ${#NODES[@]} )) || REMAINDER_NODES=("${NODES[@]:boundary}")
else
    PARTICIPATING_NODES=("${NODES[@]}")
fi
fi
# Validate the pinned upstream DeepEP modes before any remote preparation.
if [[ $OPERATION == deepep && -z $REPORT_ONLY ]]; then
    if (( GROUP_SIZE == 1 )); then
        case $GPUS_PER_NODE in 2|4|8) ;; *) die 'single-node DeepEP currently supports 2, 4 or 8 GPUs' ;; esac
    else
        (( GPUS_PER_NODE == 8 )) || die 'multi-node DeepEP norm requires exactly 8 GPUs per node; no stages launched'
        total_gpus=$(( GROUP_SIZE * GPUS_PER_NODE ))
        (( 256 % total_gpus == 0 && 288 % total_gpus == 0 )) || die 'DeepEP total GPU ranks must divide norm experts=256 and lowl experts=288'
    fi
fi
# T22: the reporting host is selectable and decoupled from the tested list.
# Default keeps the old first-node behavior; an explicit --report-host is
# validated before any SSH and never enters NODES/GROUPS. report-only refuses
# a host switch unless the caller explicitly selects the original one.
[[ -z $REPORT_HOST || $REPORT_HOST =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die 'invalid --report-host'
[[ -n $REPORT_HOST ]] || REPORT_HOST=${NODES[0]}
source "$ROOT/cluster_run/remote.sh"
source "$ROOT/cluster_run/log_archive.sh" || die 'cannot load group log archive'
if [[ -n $REPORT_ONLY ]]; then
    [[ $REPORT_ONLY == /* ]] || REPORT_ONLY=$PWD/$REPORT_ONLY
    # T28: plan export is report-only only, must target a brand-new absolute
    # directory and never overwrite existing evidence; no payload is started.
    [[ -z $EXPORT_PLAN ]] || [[ $EXPORT_PLAN == /* ]] || die 'export plan directory must be an absolute target path'
    [[ -z $EXPORT_PLAN ]] || [[ ! -e $EXPORT_PLAN ]] || die "export plan directory already exists: $EXPORT_PLAN"
    RUN_DIR=$REPORT_ONLY
    [[ ! -r $RUN_DIR/.state/control.tsv ]] || RUN_DIR=$REPORT_ONLY/.state
    [[ -r $RUN_DIR/control.tsv ]] || die 'report-only requires an existing staged run'
    prior_report_host=$(awk -F '\t' '$1=="report_host"{print $2; exit}' "$RUN_DIR/control.tsv" 2>/dev/null)
    if [[ -n $prior_report_host && $prior_report_host != "$REPORT_HOST" ]]; then
        die "report-only host mismatch: this run was reported from $prior_report_host; pass --report-host $prior_report_host to rebuild in the original report environment"
    fi
    report_command rebuild plain 80 "$EXPORT_PLAN"
    exit $?
fi
[[ -z $EXPORT_PLAN ]] || die '--export-plan requires --report-only'

ALL_TESTS=(all_reduce all_gather broadcast reduce reduce_scatter gather scatter alltoall alltoallv sendrecv)
TESTS=("${ALL_TESTS[@]}"); SCOPE=full
add_stage() {
    local item=$1 round=$2 label=$3 sid
    printf -v sid '%02d-%s' "$((${#STAGE_IDS[@]}+1))" "$([[ $round == 0 ]] || printf 'r%s-' "$round")$item"
    STAGE_IDS+=("$sid"); STAGE_ITEMS+=("$item"); STAGE_ROUNDS+=("$round"); STAGE_LABELS+=("$label")
}
if [[ -z $RERUN_PLAN ]]; then
case $OPERATION in
    platform|resource|platform,resource) add_stage "$OPERATION" 0 '环境检查' ;;
    rccl) for item in "${TESTS[@]}"; do add_stage "$item" 0 "$item"; done ;;
    deepep)
        if (( GROUP_SIZE == 1 )); then
            add_stage intranode 0 intranode
        else
            add_stage lowl 0 lowl
            add_stage norm 0 norm
        fi ;;
    byte-flux) add_stage 70b 0 '70B矩阵' ;;
    check_deep) add_stage check_deep 0 '环境检查' ;;
    ib-write-bw) add_stage ib_write_bw 0 '跨节点 IB 带宽' ;;
esac
fi
if (( DRY_RUN )); then
    if [[ -n $RERUN_PLAN ]]; then
        planned_groups=${#GROUPS_LIST[@]}
        printf 'RERUN plan=%s\nRERUN source_run=%s (old evidence preserved; new run = fresh directory + fresh tokens, control.tsv gains rerun_source_run/rerun_plan)\nRERUN fingerprint=%s deployment_md5=%s (config/version checks passed)\n' \
            "$RERUN_PLAN" "$RERUN_SOURCE_RUN" "$RERUN_FINGERPRINT" "$RERUN_DEPLOYMENT"
        for gi in "${!GROUPS_LIST[@]}"; do
            printf 'RERUN GROUP %s members=%s\n' "${GROUPS_LIST[gi]}" "${GROUP_NODES[gi]}"
        done
        printf 'RERUN cleanup gate: %s entries with unconfirmed cleanup (real dispatch blocked while >0)\n' "$RERUN_UNCONFIRMED"
        (( ${#REMAINDER_NODES[@]} == 0 )) || printf 'RERUN SKIPPED_OUTSIDE_PLAN %s (context only, not scheduled, not a failure)\n' "$(IFS=,; printf '%s' "${REMAINDER_NODES[*]}")"
    elif [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        planned_groups=$(( ${#NODES[@]} / GROUP_SIZE ))
    else
        planned_groups=$(( (${#NODES[@]}+GROUP_SIZE-1)/GROUP_SIZE ))
    fi
    scale_label=slots scale_value=$SLOTS
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then scale_label=gpus-per-node; scale_value=$GPUS_PER_NODE; fi
    printf 'DRY_RUN scenario=%s operation=%s nodes=%s groups=%s group-size=%s %s=%s concurrency=%s node-concurrency=%s report-host=%s timeout=%ss/item/group\n' "$SCENARIO" "$OPERATION" "${#NODES[@]}" "$planned_groups" "$GROUP_SIZE" "$scale_label" "$scale_value" "$CONCURRENCY" "$NODE_CONCURRENCY" "$REPORT_HOST" "$TIMEOUT_SECONDS"
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        printf 'PARTICIPATING %s\n' "$(IFS=,; printf '%s' "${PARTICIPATING_NODES[*]}")"
        (( ${#REMAINDER_NODES[@]} == 0 )) || printf 'SKIPPED_REMAINDER %s (remainder of complete groups; not scheduled, not a failure)\n' "$(IFS=,; printf '%s' "${REMAINDER_NODES[*]}")"
    fi
    for i in "${!STAGE_IDS[@]}"; do printf 'STAGE %s %s (concurrent groups, then barrier)\n' "${STAGE_IDS[i]}" "${STAGE_LABELS[i]}"; done
    exit 0
fi
command -v setsid >/dev/null 2>&1 || die 'setsid required'
OUTPUT_DIR=${OUTPUT_DIR:-$ROOT/verify_results}
[[ $OUTPUT_DIR == /* ]] || OUTPUT_DIR=$PWD/$OUTPUT_DIR
[[ $OUTPUT_DIR != *$'\t'* && $OUTPUT_DIR != *$'\n'* && $OUTPUT_DIR != *$'\r'* ]] || die 'control characters in output path'
PUBLIC_RUN_DIR=$OUTPUT_DIR/${SCENARIO}_${OPERATION}_stages_$(date +%Y%m%d_%H%M%S)_$$
RUN_DIR=$PUBLIC_RUN_DIR/.state
mkdir -p -- "$PUBLIC_RUN_DIR" || die 'cannot create log directory'
mkdir -p -- "$RUN_DIR/prepared" "$RUN_DIR/runtime" || die 'cannot create run directory'
# Published once by C before dispatch. Commands referencing it remain replayable.
cp -- "$ROOT/hcu_envcheck/payloads/task_guard.sh" "$RUN_DIR/prepared/task-guard.sh" || die 'cannot publish task guard'
chmod 644 -- "$RUN_DIR/prepared/task-guard.sh" || die 'cannot read task guard'
: >> "$RUN_DIR/report.lock" || die 'cannot create report lock'
: >> "$RUN_DIR/prepared/.script-cache.lock" || die 'cannot create script cache lock'
STARTED=$(date +%s); RUN_STATUS=RUNNING; CURRENT_STAGE=0
write_control() {
    {
        printf 'schema_version\t1\noperation\t%s\nscenario\t%s\nstarted_epoch\t%s\n' "$OPERATION" "$SCENARIO" "$STARTED"
        printf 'env_script\t%s\ncontainer\t%s\n' "$ENV_SCRIPT" "$CONTAINER"
        printf 'node_count\t%s\ngroup_count\t%s\ngroup_size\t%s\nconcurrency\t%s\nnode_concurrency\t%s\nreport_host\t%s\n' "${#NODES[@]}" "${#GROUPS_LIST[@]}" "$GROUP_SIZE" "$CONCURRENCY" "$NODE_CONCURRENCY" "$REPORT_HOST"
        [[ $OPERATION == rccl || $OPERATION == deepep ]] || printf 'slots\t%s\n' "$SLOTS"
        if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
            printf 'scale_semantics\tgroup-nodes-gpus-v1\ngpus_per_node\t%s\n' "$GPUS_PER_NODE"
            mpi_ranks=0; (( GROUP_SIZE <= 1 )) || mpi_ranks=$(( GROUP_SIZE * GPUS_PER_NODE ))
            if [[ $OPERATION == deepep ]]; then mpi_ranks=0; (( GROUP_SIZE <= 1 )) || mpi_ranks=$GROUP_SIZE; fi
            printf 'mpi_ranks_per_group\t%s\nplanned_gpus_per_group\t%s\n' "$mpi_ranks" "$(( GROUP_SIZE * GPUS_PER_NODE ))"
            [[ $OPERATION != deepep ]] || printf 'planned_deepep_gpu_workers_per_node\t%s\n' "$GPUS_PER_NODE"
        fi
        # Complete-group partition bookkeeping (T02). The reporter renders
        # these as non-participating nodes: traceability only, never in the
        # planned/executed/valid denominators, never a failure.
        printf 'source_node_count\t%s\nparticipating_node_count\t%s\n' "${#NODES[@]}" "${#PARTICIPATING_NODES[@]}"
        if [[ -n $RERUN_PLAN ]]; then
            printf 'rerun_source_run\t%s\nrerun_plan\t%s\n' "$RERUN_SOURCE_RUN" "$RERUN_PLAN"
        fi
        if (( ${#REMAINDER_NODES[@]} )); then
            if [[ -n $RERUN_PLAN ]]; then
                unparticipated_reason='不在重跑计划内（仅上下文，不派发不清理）'
            else
                unparticipated_reason="不足一个完整组(group-size=$GROUP_SIZE)，仅测试完整组"
            fi
            printf 'unparticipated_nodes\t%s\nunparticipated_reason\t%s\n' "$(IFS=,; printf '%s' "${REMAINDER_NODES[*]}")" "$unparticipated_reason"
        fi
        printf 'scope\t%s\nstatus\t%s\ncurrent_stage\t%s\nentry_run_dir\t%s\n' "$SCOPE" "$RUN_STATUS" "$CURRENT_STAGE" "$RUN_DIR"
        printf 'log_format\tdirect-group-v3\nstate_layout\tcompact-group-v2\n'
        printf 'public_run_dir\t%s\n' "$PUBLIC_RUN_DIR"
    } > "$RUN_DIR/control.tsv.tmp.$$"
    mv -f -- "$RUN_DIR/control.tsv.tmp.$$" "$RUN_DIR/control.tsv"
}
# Keep the historical TSV column name; its value is the actual group file path.
GROUP_FILE_NAME=hostfile
GROUP_MPI_SLOTS=$GPUS_PER_NODE
if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then GROUP_FILE_NAME=slotsfile; fi
if [[ $OPERATION == deepep ]]; then GROUP_MPI_SLOTS=1; fi
printf 'group\tnodes\thostfile\n' > "$RUN_DIR/groups.tsv"
if [[ -n $RERUN_PLAN ]]; then
    # T29: materialize the plan's own groups verbatim (members, order and
    # hostfile lines were validated at plan load); never re-partition.
    for gi in "${!GROUPS_LIST[@]}"; do
        group=${GROUPS_LIST[gi]}
        group_root=$RUN_DIR/prepared/$group
        mkdir -p -- "$group_root"
        group_file=$group_root/$GROUP_FILE_NAME
        printf '%s' "${RERUN_GROUP_LINES[gi]}" > "$group_file"
        GROUP_FILES+=("$group_file")
        printf '%s\t%s\t%s\n' "$group" "${GROUP_NODES[gi]}" "$group_file" >> "$RUN_DIR/groups.tsv"
    done
else
for ((offset=0;offset<${#NODES[@]};offset+=GROUP_SIZE)); do
    printf -v group 'group-%03d' "$((${#GROUPS_LIST[@]}+1))"
    group_root=$RUN_DIR/prepared/$group
    mkdir -p -- "$group_root"
    group_nodes=("${NODES[@]:offset:GROUP_SIZE}")
    # Complete-group rule: the trailing remainder of an RCCL/DeepEP hostfile
    # never forms a shrunk group and stays fully unscheduled.
    if [[ $OPERATION == rccl || $OPERATION == deepep ]] && (( ${#group_nodes[@]} < GROUP_SIZE )); then
        break
    fi
    node_list=$(IFS=,; printf '%s' "${group_nodes[*]}")
    group_file=$group_root/$GROUP_FILE_NAME
    # GPU groups persist the allocation used directly by mpirun.
    for line in "${HOST_LINES[@]:offset:GROUP_SIZE}"; do
        if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then read -r member _ <<< "$line"; printf '%s slots=%s\n' "$member" "$GROUP_MPI_SLOTS"
        elif [[ $line =~ (^|[[:space:]])slots=[0-9]+($|[[:space:]]) ]]; then printf '%s\n' "$line"; else printf '%s slots=%s\n' "$line" "$SLOTS"; fi
    done > "$group_file"
    GROUPS_LIST+=("$group"); GROUP_NODES+=("$node_list"); GROUP_FILES+=("$group_file")
    printf '%s\t%s\t%s\n' "$group" "$node_list" "$group_file" >> "$RUN_DIR/groups.tsv"
done
fi
printf 'stage_id\titem\titeration\tlabel\n' > "$RUN_DIR/stages.tsv"
for i in "${!STAGE_IDS[@]}"; do
    printf '%s\t%s\t%s\t%s\n' "${STAGE_IDS[i]}" "${STAGE_ITEMS[i]}" "${STAGE_ROUNDS[i]}" "${STAGE_LABELS[i]}" >> "$RUN_DIR/stages.tsv"
done
# T27: execution directories are created on demand, one stage at a time (see
# the stage loop below), instead of initializing every stage x group upfront.
# The login user still creates each stage's tree BEFORE any dispatch in that
# stage, so the root container reporter never races to create a stage/group
# directory the entry user could not extend afterwards.
write_control
write_task() {
    local dir=$1 status=$2 rc=$3 cleanup=$4 started=$5 ended=$6 reason=$7 execution_rc='' key value
    local -A identity=()
    while IFS=$'\t' read -r key value; do identity[$key]=$value; done < "$dir/identity.tsv"
    if [[ -r $dir/execution-exit-code ]]; then IFS= read -r execution_rc < "$dir/execution-exit-code" || true; fi
    if [[ -s $dir/worker-result.tsv ]]; then execution_rc=$(awk -F '\t' 'NR==1{for(i=1;i<=NF;i++)if($i=="execution_exit_code")c=i}NR==2&&c{print $c}' "$dir/worker-result.tsv"); fi
    {
        printf 'group\tstage_id\tphase\ttoken\titem\titeration\tstatus\texit_code\tcleanup\tstarted_epoch\tended_epoch\treason_code\tlog\texecution_exit_code\n'
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "${identity[group]}" "${identity[stage_id]}" "${identity[phase]}" "${identity[token]}" "${identity[item]}" "${identity[iteration]}" \
            "$status" "$rc" "$cleanup" "$started" "$ended" "$reason" "$dir/group.log" "$execution_rc"
    } > "$dir/task.tsv.tmp.$BASHPID"
    mv -f -- "$dir/task.tsv.tmp.$BASHPID" "$dir/task.tsv"
}
worker_args() {
    local gi=$1 phase=$2 dir=$3 token=$4 item=$5 round=$6
    WORKER_ARGS=("$SCENARIO" "$OPERATION" -f "${GROUP_FILES[gi]}" --phase "$phase" --stage-dir "$dir" --prepared-dir "$RUN_DIR/prepared/${GROUPS_LIST[gi]}" --run-token "$token" --timeout "$TIMEOUT_SECONDS")
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then WORKER_ARGS+=(--gpus-per-node "$GPUS_PER_NODE")
    else WORKER_ARGS+=(--slots "$SLOTS"); fi
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        WORKER_ARGS+=(--container-ssh-port "$CONTAINER_PORT")
    fi
    WORKER_ARGS+=(--stage-iteration "$round")
    [[ -z $item ]] || WORKER_ARGS+=(--stage-item "$item")
    [[ -z $ENV_SCRIPT ]] || WORKER_ARGS+=(--env-script "$ENV_SCRIPT")
    [[ -z $CONTAINER_WORKDIR ]] || WORKER_ARGS+=(--container-workdir "$CONTAINER_WORKDIR")
    [[ $SCENARIO != per-node-container ]] || WORKER_ARGS+=(--container "$CONTAINER" --image "$IMAGE")
    if [[ $OPERATION == rccl ]]; then
        [[ -z $BASELINE ]] || WORKER_ARGS+=(--baseline-file "$BASELINE")
    fi
    if [[ $OPERATION == ib-write-bw ]]; then
        for value in "${IB_ARGS[@]}"; do WORKER_ARGS+=("--ib-arg=$value"); done
    fi
    (( SKIP_IDLE == 0 )) || WORKER_ARGS+=(--skip-idle-check)
}
process_alive() { kill -0 -- "-$1" 2>/dev/null || kill -0 "$1" 2>/dev/null; }
session_identity_current() {
    local pid=$1 expected=$2 actual
    [[ $pid =~ ^[1-9][0-9]*$ && $expected =~ ^[0-9]+$ ]] || return 1
    [[ -r /proc/$pid/stat ]] || return 1
    actual=$(awk '{print $22}' "/proc/$pid/stat") || return 1
    [[ $actual == "$expected" ]]
}
stop_session() {
    local pid=$1 n expected=${session_tick:-}
    # The group leader is still our child until wait reaps it. Refuse to act
    # on a reused PID. A surviving orphaned group is retained as unconfirmed.
    session_identity_current "$pid" "$expected" || return 0
    kill -TERM "$pid" 2>/dev/null || true
    for ((n=0;n<25;n++)); do process_alive "$pid" || return 0; sleep 1; done
    session_identity_current "$pid" "$expected" || return 0
    kill -TERM -- "-$pid" 2>/dev/null || true
    sleep 2
    session_identity_current "$pid" "$expected" || return 0
    kill -KILL -- "-$pid" 2>/dev/null || true
}
# C is the only process allowed to publish a group's facts.json. Helpers in
# the target Python environment close exclusive candidates and return hashes.
CONTROLLER_MAIN_PID=$BASHPID
C_CRITICAL=0 CANCEL_REQUESTED=0 CANCELLING=0
declare -A FACT_REV=() LAST_STATUS=() LAST_CLEANUP=() LAST_REASON=()
declare -A PID_GROUP=() PID_PHASE=() PID_STAGE=() PID_TOKEN=() PID_ITEM=() PID_ROUND=()
commit_facts_inner() {
    local gi=$1 phase=$2 stage=$3 token=$4 item=$5 round=$6 kind=$7 reason=${8:-OK}
    local group=${GROUPS_LIST[gi]} dir candidate receipt key value digest previous
    local -A got=()
    [[ $BASHPID == "$CONTROLLER_MAIN_PID" ]] || return 73
    dir=$RUN_DIR/runtime/$group
    previous=${FACT_REV[$group]:-0}
    candidate=$RUN_DIR/prepared/$group/.facts-candidate.$token.$kind.$previous.json
    receipt=$RUN_DIR/.candidate-receipt.$CONTROLLER_MAIN_PID
    if ! facts_command --group "$group" --stage-id "$stage" --phase "$phase" \
        --token "$token" --item "$item" --iteration "$round" --runtime-dir "$dir" \
        --candidate "$candidate" --expected-revision "$previous" --kind "$kind" \
        --started-epoch "${task_started:-0}" --reason "$reason" > "$receipt"; then
        printf '[ERROR] facts candidate failed group=%s stage=%s; workspace retained: %s\n' "$group" "$stage" "$dir" >&2
        return 73
    fi
    while IFS=$'\t' read -r key value; do
        [[ -n $key && -z ${got[$key]+present} ]] || return 73
        got[$key]=$value
    done < "$receipt"
    [[ ${got[group]:-} == "$group" && ${got[stage_id]:-} == "$stage" && ${got[phase]:-} == "$phase" && ${got[token]:-} == "$token" && \
       ${got[expected_revision]:-} == "$previous" && ${got[revision]:-} == "$((previous+1))" && ${got[sha256]:-} =~ ^[0-9a-f]{64}$ ]] || return 73
    digest=$(sha256sum -- "$candidate") || return 73
    [[ ${digest%% *} == "${got[sha256]}" ]] || return 73
    sync -d -- "$candidate" || return 73
    # Do not move this publication into command substitution or a group job.
    mv -f -- "$candidate" "$RUN_DIR/prepared/$group/facts.json" || return 73
    FACT_REV[$group]=${got[revision]}
    sync -- "$RUN_DIR/prepared/$group" || return 73
    LAST_STATUS[$group]=${got[status]}
    LAST_CLEANUP[$group]=${got[cleanup]}
    LAST_REASON[$group]=${got[reason_code]}
    rm -f -- "$receipt"
    if [[ $kind == finish && ${got[recyclable]:-0} == 1 ]]; then
        if ! hcu_recycle_workspace "$RUN_DIR" "$group" "$stage" "$phase" "$token"; then
            printf '[ERROR] committed facts, but workspace recycling failed group=%s stage=%s; retained: %s\n' "$group" "$stage" "$dir" >&2
            return 73
        fi
    fi
    return 0
}
commit_facts() {
    local commit_rc=0
    ((C_CRITICAL += 1))
    commit_facts_inner "$@" || commit_rc=$?
    ((C_CRITICAL -= 1))
    if (( CANCEL_REQUESTED && ! CANCELLING && C_CRITICAL == 0 )); then interrupt_run; fi
    return "$commit_rc"
}
append_group_log() {
    local gi=$1 phase=$2 dir=$3 item=$4 stage=$5 token=$6 offset=$7 size=$8
    local group=${GROUPS_LIST[gi]} prefix digest node
    if [[ $phase == prepare ]]; then prefix=prepared/$group
    else prefix=stages/$stage/groups/$group; fi
    digest=$(archive_log_digest "$PUBLIC_RUN_DIR/$group.log" "$offset" "$((size-offset))") || return 73
    {
        printf 'group\tstage_id\tphase\ttoken\tsource\toffset\tlength\tsha256\n'
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$group" "$stage" "$phase" "$token" "$prefix/group.log" "$offset" "$((size-offset))" "$digest"
        while read -r node _; do
            [[ -z $node || $node == \#* ]] || printf '%s\t%s\t%s\t%s\t%s/%s.log\t%s\t%s\t%s\n' \
                "$group" "$stage" "$phase" "$token" "$prefix" "$node" "$offset" "$((size-offset))" "$digest"
        done < "${GROUP_FILES[gi]}"
        [[ $OPERATION != rccl || $phase != execute ]] || printf '%s\t%s\t%s\t%s\t%s/rccl/%s.log\t%s\t%s\t%s\n' \
            "$group" "$stage" "$phase" "$token" "$prefix" "$item" "$offset" "$((size-offset))" "$digest"
    } > "$dir/logs.tsv.tmp.$BASHPID" || return 73
    mv -f -- "$dir/logs.tsv.tmp.$BASHPID" "$dir/logs.tsv"
}
run_task() (
    gi=$1 phase=$2 dir=$3 item=$4 round=$5 stage=$6 token=$7
    started=$(date +%s); rc=0; canceled=0; child='' timer='' session='' session_tick=''; log_failed=0
    cancel_task() { canceled=1; [[ -z $session ]] || stop_session "$session"; }
    trap cancel_task INT TERM
    group=${GROUPS_LIST[gi]}; group_log=$PUBLIC_RUN_DIR/$group.log
    fifo=$dir/log.pipe
    # The FIFO provides bounded backpressure. cat is the sole archive writer;
    # controller labels, workers, ranks and IB chunks all use this one channel.
    (( canceled == 0 )) || exit 130
    mkfifo -m 600 -- "$fifo" || exit 73
    log_offset=$(stat -c '%s' -- "$group_log") || exit 73
    # Open the destination before opening the FIFO: a failed archive open
    # must not leave the parent blocked waiting for a reader that exited.
    exec {archive_fd}>>"$group_log" || exit 73
    (( canceled == 0 )) || { exec {archive_fd}>&-; exit 130; }
    cat -- "$fifo" >&"$archive_fd" & collector=$!
    exec {archive_fd}>&-
    # A broken collector must reach the normal token-cleanup path. Record
    # write errors rather than allowing SIGPIPE to kill the wrapper outright.
    trap '' PIPE
    exec {log_fd}>"$fifo" || { wait "$collector"; exit 73; }
    display_suite=$OPERATION
    case $OPERATION in rccl) display_suite=RCCL ;; deepep) display_suite=DeepEP ;; check_deep) display_suite=checkdeepep ;; esac
    display_item=${item:-环境预检}; display_order=环境预检
    if [[ $phase != prepare ]]; then printf -v display_order '%02d/%02d' "$CURRENT_STAGE" "${#STAGE_IDS[@]}"; fi
    environment=$SCENARIO; [[ $SCENARIO != per-node-container ]] || environment+=" / $CONTAINER"
    node_count=$(awk 'NF&&$1!~/^#/ {n++} END{print n+0}' "${GROUP_FILES[gi]}")
    scale="$node_count 个节点"
    if [[ $OPERATION == rccl || $OPERATION == deepep ]]; then
        scale+=" × 每节点 $GPUS_PER_NODE 张卡，共 $((node_count * GPUS_PER_NODE)) 张卡"
    elif [[ $OPERATION == byte-flux ]]; then scale+=' / 单机 TP4'; fi
    if (( log_offset == 0 )); then
        printf '【运行信息】\n分组：%s\n镜像：%s\n工程目录：%s\n环境脚本：%s\n分组节点文件：%s\n日志文件：%s\n' \
            "$group" "${IMAGE:--}" "${CONTAINER_WORKDIR:-$ROOT}" "${ENV_SCRIPT:-使用目标环境当前配置}" "${GROUP_FILES[gi]}" "$group_log" >&"$log_fd" || log_failed=1
    fi
    hcu_log_start "$display_order" "$display_suite" "$display_item" "$environment" \
        "${GROUP_NODES[gi]}" "$(head -n 1 "${GROUP_FILES[gi]}" | awk '{print $1}')" "$scale" >&"$log_fd" || log_failed=1
    join_task_child() {
        local target_pid=$1 joined_rc=0
        while :; do
            joined_rc=0; wait "$target_pid" || joined_rc=$?
            # A trapped signal interrupts wait before the child has closed
            # its writers. Repeat the wait instead of treating it as a join.
            kill -0 "$target_pid" 2>/dev/null || break
        done
        return "$joined_rc"
    }
    if (( canceled )); then
        exec {log_fd}>&-
        join_task_child "$collector" || true
        exit 130
    fi
    worker_args "$gi" "$phase" "$dir" "$token" "$item" "$round"
    setsid --wait bash -c 'file=$1; group=$2; stage=$3; phase=$4; token=$5; shift 5
        tick=$(awk "{print \$22}" /proc/$$/stat) || exit 70
        printf "group\tstage_id\tphase\ttoken\tpid\tstart_tick\n%s\t%s\t%s\t%s\t%s\t%s\n" "$group" "$stage" "$phase" "$token" "$$" "$tick" > "$file.tmp.$$" && mv -f -- "$file.tmp.$$" "$file" || exit 73
        exec "$@"' hcu-stage-session "$dir/session.tsv" "$group" "$stage" "$phase" "$token" \
        env HCU_STATE_LAYOUT=compact-group-v2 HCU_GROUP_NAME="$group" HCU_GROUP_INDEX="$((gi+1))" \
            HCU_NODE_PERMIT_DIR="$NODE_PERMIT_DIR" HCU_NODE_CONCURRENCY="$NODE_CONCURRENCY" \
            HCU_GROUP_LOG="$fifo" HCU_PUBLIC_GROUP_LOG="$group_log" HCU_LOG_CHANNEL=1 HCU_LOG_TOKEN="$token" \
        bash "$ROOT/hcu_envcheck/worker.sh" "${WORKER_ARGS[@]}" >&"$log_fd" 2>&1 & child=$!
    for ((n=0;n<50;n++)); do
        if [[ -s $dir/session.tsv ]]; then
            session=$(awk -F '\t' -v g="$group" -v s="$stage" -v p="$phase" -v t="$token" 'NR==2&&$1==g&&$2==s&&$3==p&&$4==t{print $5}' "$dir/session.tsv")
            session_tick=$(awk -F '\t' 'NR==2{print $6}' "$dir/session.tsv")
            if [[ $session =~ ^[1-9][0-9]*$ ]]; then
                (( canceled == 0 )) || stop_session "$session"
                break
            fi
        fi
        kill -0 "$child" 2>/dev/null || break
        sleep 0.1
    done
    if [[ ! $session =~ ^[1-9][0-9]*$ ]]; then
        join_task_child "$child" || rc=$?; (( rc != 0 )) || rc=70
    else
        (
            exec {log_fd}>&-
            timer_sleep=''
            trap '[[ -z $timer_sleep ]] || { kill "$timer_sleep" 2>/dev/null; wait "$timer_sleep" 2>/dev/null; }; exit 0' INT TERM
            sleep "$((TIMEOUT_SECONDS+90))" & timer_sleep=$!
            wait "$timer_sleep" || exit 0; timer_sleep=''
            printf 'group\tstage_id\tphase\ttoken\tnode\tsource\texit_code\texpired\n%s\t%s\t%s\t%s\t-\twatchdog\t124\t1\n' \
                "$group" "$stage" "$phase" "$token" > "$dir/timeout.watchdog.tsv.tmp.$BASHPID"
            mv -f -- "$dir/timeout.watchdog.tsv.tmp.$BASHPID" "$dir/timeout.watchdog.tsv"
            stop_session "$session"
        ) & timer=$!
        join_task_child "$child" || rc=$?
        [[ -z $timer ]] || { kill "$timer" 2>/dev/null || true; join_task_child "$timer" 2>/dev/null || true; }
    fi
    # A child exit is not proof that remote ranks stopped. The independent
    # fallback attempt may run only after the normal worker was joined.
    if [[ ! -s $dir/cleanup-normal-1/result.tsv ]]; then
        worker_args "$gi" cleanup "$dir" "$token" "$item" "$round"
        HCU_STATE_LAYOUT=compact-group-v2 HCU_GROUP_LOG="$fifo" HCU_PUBLIC_GROUP_LOG="$group_log" \
            HCU_LOG_CHANNEL=1 HCU_LOG_TOKEN="$token" bash "$ROOT/hcu_envcheck/worker.sh" "${WORKER_ARGS[@]}" >&"$log_fd" 2>&1 || true
    fi
    cleanup=UNCONFIRMED
    attempt=$dir/cleanup-normal-1
    [[ -s $attempt/result.tsv ]] || attempt=$dir/cleanup-fallback-1
    if [[ -s $attempt/nodes.tsv ]] && awk -F '\t' -v g="$group" -v s="$stage" -v p="$phase" -v t="$token" -v need="$node_count" '
        NR==1{for(i=1;i<=NF;i++)col[$i]=i;next}
        {n++;if($(col["group"])!=g||$(col["stage_id"])!=s||$(col["phase"])!=p||$(col["token"])!=t||$(col["status"])!="CONFIRMED")bad=1}
        END{exit (bad||n!=need)}' "$attempt/nodes.tsv"; then cleanup=CONFIRMED; fi
    status=PASS; reason=OK
    (( rc == 0 )) || { status=FAIL; reason=EXECUTION_FAILED; }
    if [[ $OPERATION == check_deep && $rc == 2 ]]; then status=WARN; reason=CHECK_WARN; fi
    watchdog_expired=0
    if [[ -f $dir/timeout.watchdog.tsv ]] && awk -F '\t' -v g="$group" -v s="$stage" -v p="$phase" -v t="$token" \
        'NR==2{ok=($1==g&&$2==s&&$3==p&&$4==t&&$6=="watchdog"&&$8==1)}END{exit !(NR==2&&ok)}' "$dir/timeout.watchdog.tsv"; then watchdog_expired=1; fi
    if (( watchdog_expired || rc == 124 )); then status=TIMEOUT; rc=124; reason=TIMEOUT
    elif (( rc == 137 )); then
        for marker in "$dir"/timeout.deadline.*.tsv; do
            [[ -f $marker ]] || continue
            if awk -F '\t' -v g="$group" -v s="$stage" -v p="$phase" -v t="$token" \
                'NR==2{ok=($1==g&&$2==s&&$3==p&&$4==t&&$6=="deadline"&&$8==1)}END{exit !(NR==2&&ok)}' "$marker"; then status=TIMEOUT; reason=TIMEOUT; break; fi
        done
    fi
    if (( canceled )); then status=CANCELLED; rc=130; reason=CANCELLED; fi
    if [[ $cleanup == UNCONFIRMED ]]; then reason=CLEANUP_UNCONFIRMED; status=FAIL; (( rc != 0 )) || rc=70; fi
    hcu_log_end "$display_order" "$display_suite" "$display_item" "$status" "$rc" "$(( $(date +%s)-started ))" "$cleanup" >&"$log_fd" || log_failed=1
    # No inherited output FD may outlive its owner. Waiting for the session
    # and watchdog above is required in addition to the per-file token checks.
    writers_joined=1
    if [[ -n $session ]] && process_alive "$session"; then writers_joined=0; fi
    exec {log_fd}>&-
    collector_rc=0; join_task_child "$collector" || collector_rc=$?
    size=$(stat -c '%s' -- "$group_log") || { size=$log_offset; log_failed=1; }
    (( collector_rc == 0 )) || log_failed=1
    {
        printf 'group\tstage_id\tphase\ttoken\tcollector_pid\texit_code\tdrained\tstart_offset\tend_offset\n'
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$group" "$stage" "$phase" "$token" "$collector" "$collector_rc" "$((collector_rc==0))" "$log_offset" "$size"
    } > "$dir/collector-result.tsv.tmp.$BASHPID"
    mv -f -- "$dir/collector-result.tsv.tmp.$BASHPID" "$dir/collector-result.tsv"
    append_group_log "$gi" "$phase" "$dir" "$item" "$stage" "$token" "$log_offset" "$size" || log_failed=1
    if (( log_failed )); then status=FAIL; reason=LOG_WRITE_FAILED; rc=73; fi
    write_task "$dir" "$status" "$rc" "$cleanup" "$started" "$(date +%s)" "$reason"
    {
        printf 'group\tstage_id\tphase\ttoken\tworker_joined\twatchdog_joined\tcleanup_joined\tcollector_joined\n'
        printf '%s\t%s\t%s\t%s\t%s\t1\t1\t1\n' "$group" "$stage" "$phase" "$token" "$writers_joined"
    } > "$dir/delivery.tsv.tmp.$BASHPID"
    mv -f -- "$dir/delivery.tsv.tmp.$BASHPID" "$dir/delivery.tsv"
    trap - INT TERM
    (( rc == 0 ))
)
launch_task_inner() {
    local gi=$1 phase=$2 item=$3 round=$4 stage=$5 group dir token task_started pid
    group=${GROUPS_LIST[gi]}; dir=$RUN_DIR/runtime/$group
    token=hcu${STARTED}p$$s${CURRENT_STAGE}g${gi}x${RANDOM}${RANDOM}
    task_started=$(date +%s)
    if [[ -n ${BLOCKED[$group]:-} ]]; then
        commit_facts "$gi" "$phase" "$stage" "$token" "$item" "$round" blocked "${BLOCKED_REASON[$group]:-CLEANUP_UNCONFIRMED}" || return 73
        return 0
    fi
    # A retained workspace is never overwritten by the next stage.
    [[ ! -e $dir ]] || { BLOCKED[$group]=1; BLOCKED_REASON[$group]=INCOMPLETE; return 73; }
    mkdir -p -- "$dir" || return 73
    printf 'schema_version\t2\ngroup\t%s\nstage_id\t%s\nphase\t%s\nitem\t%s\niteration\t%s\ntoken\t%s\n' \
        "$group" "$stage" "$phase" "$item" "$round" "$token" > "$dir/identity.tsv.tmp.$$" || return 73
    mv -f -- "$dir/identity.tsv.tmp.$$" "$dir/identity.tsv" || return 73
    touch -- "$PUBLIC_RUN_DIR/$group.log" || return 73
    if ! commit_facts "$gi" "$phase" "$stage" "$token" "$item" "$round" start; then
        BLOCKED[$group]=1; BLOCKED_REASON[$group]=INCOMPLETE; return 73
    fi
    run_task "$gi" "$phase" "$dir" "$item" "$round" "$stage" "$token" & pid=$!
    ACTIVE_PIDS+=("$pid")
    PID_GROUP[$pid]=$gi; PID_PHASE[$pid]=$phase; PID_STAGE[$pid]=$stage; PID_TOKEN[$pid]=$token
    PID_ITEM[$pid]=$item; PID_ROUND[$pid]=$round
}
launch_task() {
    local launch_rc=0
    ((C_CRITICAL += 1))
    launch_task_inner "$@" || launch_rc=$?
    ((C_CRITICAL -= 1))
    if (( CANCEL_REQUESTED && ! CANCELLING && C_CRITICAL == 0 )); then interrupt_run; fi
    return "$launch_rc"
}
collect_task_inner() {
    local pid=$1 gi group collect_rc=0
    [[ -n ${PID_GROUP[$pid]+present} ]] || return 0
    gi=${PID_GROUP[$pid]}; group=${GROUPS_LIST[gi]}
    if ! commit_facts "$gi" "${PID_PHASE[$pid]}" "${PID_STAGE[$pid]}" "${PID_TOKEN[$pid]}" "${PID_ITEM[$pid]}" "${PID_ROUND[$pid]}" finish; then
        BLOCKED[$group]=1; BLOCKED_REASON[$group]=INCOMPLETE
        collect_rc=73
    fi
    if (( collect_rc == 0 )) && [[ ${LAST_CLEANUP[$group]:-} != CONFIRMED ]]; then
        BLOCKED[$group]=1; BLOCKED_REASON[$group]=CLEANUP_UNCONFIRMED
    fi
    unset 'PID_GROUP[$pid]' 'PID_PHASE[$pid]' 'PID_STAGE[$pid]' 'PID_TOKEN[$pid]' 'PID_ITEM[$pid]' 'PID_ROUND[$pid]'
    return "$collect_rc"
}
collect_task() {
    local collect_rc=0
    ((C_CRITICAL += 1))
    collect_task_inner "$@" || collect_rc=$?
    ((C_CRITICAL -= 1))
    if (( CANCEL_REQUESTED && ! CANCELLING && C_CRITICAL == 0 )); then interrupt_run; fi
    return "$collect_rc"
}
wait_active() {
    local pid
    WAVE_FAILED=0
    for pid in "${ACTIVE_PIDS[@]}"; do
        wait "$pid" || WAVE_FAILED=1
        collect_task "$pid" || WAVE_FAILED=1
    done
    ACTIVE_PIDS=()
}
ROLLING=0
if (( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) )); then ROLLING=1; fi
stage_dispatch() {
    local phase=$1 item=$2 round=$3 mode=$4 gi rc done_pid i stage
    STAGE_FAILED=0
    stage=${sid:-preflight}; [[ $phase != prepare ]] || stage=preflight
    if (( ! ROLLING )); then
        ACTIVE_PIDS=()
        for gi in "${!GROUPS_LIST[@]}"; do
            launch_task "$gi" "$phase" "$item" "$round" "$stage" || STAGE_FAILED=1
            if (( ${#ACTIVE_PIDS[@]} >= CONCURRENCY )); then wait_active; (( WAVE_FAILED == 0 )) || STAGE_FAILED=1; fi
        done
        if (( ${#ACTIVE_PIDS[@]} )); then wait_active; (( WAVE_FAILED == 0 )) || STAGE_FAILED=1; fi
        return "$STAGE_FAILED"
    fi
    local -i next=0
    ACTIVE_PIDS=()
    while (( next < ${#GROUPS_LIST[@]} || ${#ACTIVE_PIDS[@]} > 0 )); do
        while (( next < ${#GROUPS_LIST[@]} && ${#ACTIVE_PIDS[@]} < CONCURRENCY )); do
            gi=$next; (( next += 1 ))
            launch_task "$gi" "$phase" "$item" "$round" "$stage" || STAGE_FAILED=1
        done
        (( ${#ACTIVE_PIDS[@]} > 0 )) || break
        rc=0; done_pid=''
        wait -n -p done_pid || rc=$?
        if [[ -n $done_pid ]]; then
            for i in "${!ACTIVE_PIDS[@]}"; do
                [[ ${ACTIVE_PIDS[i]} == "$done_pid" ]] || continue
                unset 'ACTIVE_PIDS[i]'; ACTIVE_PIDS=("${ACTIVE_PIDS[@]}")
                (( rc == 0 )) || STAGE_FAILED=1
                collect_task "$done_pid" || STAGE_FAILED=1
                break
            done
        else sleep 0.1; fi
    done
    return "$STAGE_FAILED"
}

REPORT_PID='' REPORT_FAILED=0
finalize_runtime_state() {
    # All workers and the reporting SSH have been joined by the caller.
    # Failed tests may compact; missing facts or UNCONFIRMED cleanup may not.
    hcu_prune_state_runtime "$RUN_DIR" compact ||
        printf '[WARN] temporary state cleanup incomplete; execution evidence retained: %s\n' "$RUN_DIR" >&2
    return 0
}
finish_reporter() {
    local n report_state=''
    : > "$RUN_DIR/done"
    for ((n=0;n<45;n++)); do
        [[ ! -e $RUN_DIR/reporter.finished ]] || break
        [[ -z $REPORT_PID ]] || kill -0 "$REPORT_PID" 2>/dev/null || break
        sleep 1
    done
    if [[ ! -e $RUN_DIR/reporter.finished && -n $REPORT_PID ]]; then kill -TERM "$REPORT_PID" 2>/dev/null || true; fi
    [[ -z $REPORT_PID ]] || wait "$REPORT_PID" || REPORT_FAILED=1
    if [[ -s $RUN_DIR/report-status.tsv ]]; then
        report_state=$(awk -F '\t' 'NR>1{print $1;exit}' "$RUN_DIR/report-status.tsv")
    fi
    case $report_state in PASS) return 0 ;; WARN) return 2 ;; FAIL) return 1 ;; *)
        printf '[ERROR] 报告未正常完成，原始证据：%s\n' "$RUN_DIR" >&2; return 70 ;;
    esac
}
interrupt_run() {
    trap - INT TERM
    CANCELLING=1
    RUN_STATUS=CANCELLED; write_control
    for pid in "${ACTIVE_PIDS[@]}"; do
        [[ -z ${PID_GROUP[$pid]+present} ]] || kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${ACTIVE_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
        collect_task "$pid" || true
    done
    finish_reporter || true
    finalize_runtime_state
    printf 'RESULT        CANCELLED\nREPORT        %s/summary.md\n' "$PUBLIC_RUN_DIR"
    exit 130
}
request_interrupt() {
    CANCEL_REQUESTED=1
    (( C_CRITICAL > 0 || CANCELLING )) || interrupt_run
}
trap request_interrupt INT TERM
display=plain; width=80
if [[ -t 2 && ${TERM:-dumb} != dumb ]]; then display=tty; width=$(tput cols 2>/dev/null || printf 80); fi
[[ $LOG_DETAIL != quiet ]] || display=quiet
[[ $LOG_DETAIL != every ]] || display=every
report_command watch "$display" "$width" 2> >(cat >&2) & REPORT_PID=$!
for ((n=0;n<30;n++)); do
    [[ ! -e $RUN_DIR/reporter.ready ]] || break
    kill -0 "$REPORT_PID" 2>/dev/null || break
    sleep 1
done
if [[ ! -e $RUN_DIR/reporter.ready ]]; then
    RUN_STATUS=PRECHECK_FAILED; write_control; : > "$RUN_DIR/done"
    kill -TERM "$REPORT_PID" 2>/dev/null || true; wait "$REPORT_PID" 2>/dev/null || true
    die "reporting Python unavailable on report host $REPORT_HOST; no tests dispatched. Evidence: $RUN_DIR"
fi

# T20: shared cross-worker SSH budget. N ordinary flock slots (permit.N) plus
# one global reserved slot (reserved.cleanup) used only when ordinary slots
# are busy, so stop/cleanup is never starved by N occupied ordinary permits.
# The reporter holds its own single connection, bounding control connections
# at N+2. Each permit wraps exactly one ssh inside remote(); nothing holds a
# permit while requesting another, so no self-lock.
NODE_PERMIT_DIR=$RUN_DIR/.node-permits
mkdir -p -- "$NODE_PERMIT_DIR" || die 'cannot create node permit directory'
for ((permit_i=1; permit_i<=NODE_CONCURRENCY; permit_i++)); do
    : > "$NODE_PERMIT_DIR/permit.$permit_i"
done
: > "$NODE_PERMIT_DIR/reserved.cleanup"

# Preflight all planned groups and all requested RCCL baselines before stage 1.
# T21: preflight faults are node-local. Plan/parameter/baseline damage dies
# before any dispatch (global); whatever fails here is a per-group fact
# (unreachable node, missing container, bad environment, busy accelerator).
# The failing group is quarantined like any mid-run failure — every later
# stage keeps its planned task rows as BLOCKED with the prepare reason —
# while healthy groups keep running. The run still ends nonzero.
# (awk exits nonzero when the final prepare row is anything but PASS/WARN/NA,
# including a header-only file, so those groups take the isolation branch.)
execution_failed=0
prepare_failed=0
stage_dispatch prepare '' 0 prepare || prepare_failed=1
if (( prepare_failed )); then
    for group in "${GROUPS_LIST[@]}"; do
        if [[ ${LAST_STATUS[$group]:-INCOMPLETE} != PASS && ${LAST_STATUS[$group]:-INCOMPLETE} != WARN && ${LAST_STATUS[$group]:-INCOMPLETE} != NA ]]; then
            BLOCKED[$group]=1
            BLOCKED_REASON[$group]=${BLOCKED_REASON[$group]:-${LAST_REASON[$group]:-INCOMPLETE}}
            [[ -n ${BLOCKED_REASON[$group]:-} ]] || BLOCKED_REASON[$group]=EXECUTION_FAILED
            printf '[ISOLATED] group=%s preflight failed (%s); group tasks stay planned as BLOCKED\n' \
                "$group" "${BLOCKED_REASON[$group]}" >&2
        fi
    done
    execution_failed=1
fi

for si in "${!STAGE_IDS[@]}"; do
    CURRENT_STAGE=$((si+1)); write_control
    sid=${STAGE_IDS[si]}; item=${STAGE_ITEMS[si]}; round=${STAGE_ROUNDS[si]}
    # Each active group gets the same runtime path for every stage. Persistent
    # stage paths are logical addresses projected from the group's facts.
    stage_dispatch execute "$item" "$round" execute || execution_failed=1
    for group in "${GROUPS_LIST[@]}"; do
        if [[ ${LAST_CLEANUP[$group]:-UNCONFIRMED} != CONFIRMED ]]; then BLOCKED[$group]=1; execution_failed=1; fi
    done
    printf '%s\n' "$sid" > "$RUN_DIR/stage-request.tmp.$$"
    mv -f -- "$RUN_DIR/stage-request.tmp.$$" "$RUN_DIR/stage-request"
    if ! kill -0 "$REPORT_PID" 2>/dev/null; then
        if (( REPORT_FAILED == 0 )); then printf '[ERROR] 观察进程已退出，继续保留测试证据：%s\n' "$RUN_DIR" >&2; fi
        REPORT_FAILED=1
    fi
done
RUN_STATUS=FINISHED; write_control
report_rc=0; finish_reporter || report_rc=$?
trap - INT TERM
finalize_runtime_state
if (( REPORT_FAILED && report_rc == 0 )); then report_rc=70; fi
if (( execution_failed && report_rc == 0 )); then report_rc=1; fi
result=PASS; (( report_rc == 0 )) || result=FAIL
(( report_rc != 2 )) || result=WARN
printf 'RESULT        %s\nREPORT        %s/summary.md\n' "$result" "$PUBLIC_RUN_DIR"
exit "$report_rc"
