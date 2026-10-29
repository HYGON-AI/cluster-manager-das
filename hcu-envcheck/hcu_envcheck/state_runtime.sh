#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Shared script publication and post-run housekeeping. Never prune live runs.

hcu_recycle_workspace() (
    # C invokes this only after publishing the validated finish candidate.
    local requested=$1 group=$2 stage=$3 phase=$4 token=$5 state dir key value file name
    local -A identity=()
    [[ $group =~ ^group-[0-9]+$ && $stage =~ ^[A-Za-z0-9_,-]+$ && $phase =~ ^(prepare|execute)$ && $token =~ ^[A-Za-z0-9][A-Za-z0-9_-]{15,95}$ ]] || return 64
    [[ -d $requested && ! -L $requested && ! -L $requested/runtime ]] || return 73
    state=$(CDPATH= cd -- "$requested" && pwd -P) || return 73
    [[ ${state##*/} == .state ]] || return 64
    dir=$state/runtime/$group
    [[ -d $dir && ! -L $dir ]] || return 73
    while IFS=$'\t' read -r key value; do identity[$key]=$value; done < "$dir/identity.tsv"
    [[ ${identity[group]:-} == "$group" && ${identity[stage_id]:-} == "$stage" && ${identity[phase]:-} == "$phase" && ${identity[token]:-} == "$token" ]] || return 73
    # Unknown files or links are an exceptional scene, never a reason to
    # expand deletion. Both .timeout and witnesses are deleted, not truncated.
    [[ -z $(find "$dir" -type l -print -quit) ]] || return 73
    shopt -s nullglob dotglob
    for file in "$dir"/*; do
        name=${file##*/}
        case $name in
            identity.tsv|session.tsv|task.tsv|worker-result.tsv|node-results.tsv|collector-result.tsv|delivery.tsv|logs.tsv|log.pipe|execution-exit-code|rccl-plan.tsv|node-exit.*.tsv|timeout.*.tsv|control-capture.*.txt|cleanup-normal-[1-9]*|cleanup-fallback-[1-9]*|devices|rccl|ib|.timeout|cleanup-complete|cleanup-unconfirmed) ;;
            *) return 73 ;;
        esac
    done
    # All these trees belong to this exact task, and all writing processes
    # have been joined before C atomically published the persistent facts.
    find "$dir" -depth \( -type f -o -type p \) -exec rm -f -- {} + || return 73
    find "$dir" -depth -type d -exec rmdir -- {} \; || return 73
    [[ ! -e $dir ]]
)

hcu_cache_script() (
    local preferred=$1 body=$2 directory target digest existing fd temporary='' mask mode lock
    directory=$(dirname -- "$preferred")
    mkdir -p -- "$directory" || return 73
    # The controller supplies a lock in its own prepared directory so that
    # post-run unlink does not require ownership of container-created rccl/.
    lock=${HCU_STATE_CACHE_LOCK:-$directory/.script-cache.lock}
    exec {fd}>"$lock" || return 73
    flock -x "$fd" || return 73
    digest=$(printf '%s' "$body" | sha256sum) || return 73
    digest=${digest%% *}
    target=$preferred
    if [[ -e $target ]]; then
        [[ -f $target && ! -L $target ]] || return 73
        existing=$(sha256sum -- "$target") || return 73
        if [[ ${existing%% *} != "$digest" ]]; then target=${preferred%.sh}.$digest.sh; fi
    fi
    if [[ -e $target ]]; then
        [[ -f $target && ! -L $target ]] || return 73
        existing=$(sha256sum -- "$target") || return 73
        [[ ${existing%% *} == "$digest" ]] || return 73
    else
        temporary=$(mktemp "$directory/.script.XXXXXX") || return 73
        trap '[[ -z $temporary ]] || rm -f -- "$temporary"' EXIT
        printf '%s' "$body" > "$temporary" || return 73
        mask=$(umask) || return 73
        printf -v mode '%03o' "$((0666 & ~(8#$mask)))"
        chmod "$mode" -- "$temporary" || return 73
        mv -f -- "$temporary" "$target" || return 73
        temporary=''
    fi
    # Lock is released by the subshell before any payload starts.
    printf '%s\n' "$target"
)

hcu_staged_state_closed() {
    local state=$1 key value status='' header row rest cleanup dir group nodes allocation
    local groups=0 stages=0 tasks=0
    [[ -f $state/done && -s $state/report-status.tsv && -s $state/reporter.finished ]] || return 1
    while IFS=$'\t' read -r key value; do
        [[ $key != status ]] || status=$value
    done < "$state/control.tsv"
    [[ $status == FINISHED || $status == CANCELLED ]] || return 1
    IFS= read -r header < "$state/report-status.tsv" || return 1
    [[ $header == status ]] || return 1
    { IFS= read -r header; IFS= read -r status; } < "$state/report-status.tsv" || return 1
    case $status in PASS|WARN|FAIL) ;; *) return 1 ;; esac
    IFS= read -r value < "$state/reporter.finished" || return 1
    [[ $value == "$status" ]] || return 1
    while IFS=$'\t' read -r group nodes allocation; do
        [[ $group != group ]] || continue
        [[ $group =~ ^group-[0-9]+$ && -n $nodes ]] || return 1
        [[ -s $state/log-archive/$group.tsv && -f $state/../$group.log ]] || return 1
        IFS= read -r header < "$state/log-archive/$group.tsv" || return 1
        [[ $header == $'source\tarchive\toffset\tlength\tsha256' ]] || return 1
        ((groups += 1))
    done < "$state/groups.tsv"
    while IFS=$'\t' read -r key value; do
        [[ $key == stage_id ]] || ((stages += 1))
    done < "$state/stages.tsv"
    (( groups > 0 && stages > 0 )) || return 1
    for dir in "$state"/prepared/group-* "$state"/stages/*/groups/group-*; do
        [[ -d $dir && ! -L $dir ]] || continue
        [[ -s $dir/task.tsv && -s $dir/node-results.tsv && -s $dir/cleanup.tsv ]] || return 1
        { IFS= read -r header; IFS= read -r row; } < "$dir/task.tsv" || return 1
        [[ $header == $'status\texit_code\tcleanup\t'* ]] || return 1
        status=${row%%$'\t'*}
        case $status in PASS|WARN|FAIL|NA|TIMEOUT|CANCELLED) ;; *) return 1 ;; esac
        rest=${row#*$'\t'}; rest=${rest#*$'\t'}; cleanup=${rest%%$'\t'*}
        [[ $cleanup == CONFIRMED && $row != *$'\tLOG_WRITE_FAILED\t'* ]] || return 1
        ((tasks += 1))
    done
    (( tasks == groups * (stages + 1) ))
}

hcu_prune_state_runtime() (
    # Call only after worker pools and the reporter have been joined. The
    # Python owner additionally supplies positive cleanup/completion evidence.
    local requested=$1 protocol=$2 state dir file node rc cleanup log saved suffix
    local -a pending=()
    [[ -d $requested && ! -L $requested ]] || return 0
    state=$(CDPATH= cd -- "$requested" && pwd -P) || return 1
    [[ ${state##*/} == .state ]] || return 64
    case $protocol in
        staged) hcu_staged_state_closed "$state" || return 0 ;;
        compact)
            # A retained group means unconfirmed cleanup, a live writer, or
            # failed persistence. Keep every common runtime aid in that case.
            [[ -f $state/done && -s $state/reporter.finished && -s $state/report-status.tsv ]] || return 0
            [[ -z $(find "$state/runtime" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null) ]] || return 0
            for file in "$state"/reporter.ready "$state"/reporter.finished "$state"/stage-request \
                        "$state"/.node-permits/permit.* "$state"/.node-permits/reserved.cleanup \
                        "$state"/prepared/.script-cache.lock; do
                [[ ! -L $file ]] || return 73
                [[ ! -f $file ]] || rm -f -- "$file" || return 73
            done
            rmdir -- "$state/.node-permits" "$state/runtime" 2>/dev/null || true
            # report.lock is stable across normal reporting and in-place
            # rebuilds. Never unlink or replace its inode, even after a run.
            return 0
            ;;
        python-closed) ;;  # Only the Python run owner calls this after joining.
        *) return 64 ;;
    esac
    queue_remove() {
        local candidate=$1
        [[ -f $candidate && ! -L $candidate ]] || return 0
        pending+=("$candidate")
        if (( ${#pending[@]} >= 128 )); then
            rm -f -- "${pending[@]}" || return 1
            pending=()
        fi
    }
    if [[ $protocol == staged ]]; then
        for file in "$state"/reporter.ready "$state"/reporter.finished "$state"/stage-request; do
            queue_remove "$file" || return 1
        done
        for dir in "$state"/prepared/group-* "$state"/stages/*/groups/group-*; do
            [[ -d $dir && ! -L $dir ]] || continue
            for file in "$dir/session.pid" "$dir/cleanup-complete"; do queue_remove "$file" || return 1; done
            # New tasks persist the original payload result in the existing
            # task table before removing its cross-process handoff file.
            file=$dir/execution-exit-code
            if [[ -f $file && ! -L $file ]]; then
                saved=$(awk -F '\t' 'NR==1 {for(i=1;i<=NF;i++)if($i=="execution_exit_code")c=i} NR==2&&c {print $c;exit}' "$dir/task.tsv") || return 1
                IFS= read -r rc < "$file" || rc=''
                if [[ $saved =~ ^-?[0-9]+$ && $saved == "$rc" ]]; then queue_remove "$file" || return 1; fi
            fi
            # Per-node exit codes are removed only after the exact value has
            # reached node-results.tsv. Old task tables have no raw-exit
            # column, so their original execution-exit-code remains intact.
            [[ -s $dir/node-results.tsv ]] || continue
            while IFS=$'\t' read -r node rc cleanup log; do
                [[ $node =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && $node != node && $rc =~ ^-?[0-9]+$ ]] || continue
                file=$dir/$node.exit-code
                [[ -f $file && ! -L $file ]] || continue
                IFS= read -r saved < "$file" || continue
                [[ $saved != "$rc" ]] || queue_remove "$file" || return 1
            done < "$dir/node-results.tsv"
        done
        for file in "$state"/.node-permits/permit.* "$state"/.node-permits/reserved.cleanup; do
            suffix=${file##*/}
            [[ $suffix == reserved.cleanup || $suffix =~ ^permit\.[1-9][0-9]*$ ]] || continue
            queue_remove "$file" || return 1
        done
    fi
    # These are locks, never log indexes or direct-log pointers. Every writer
    # is closed before unlinking: removing a live flock path would split locks.
    for file in "$state"/log-archive/group-*.lock \
                "$state"/prepared/.script-cache.lock \
                "$state"/prepared/group-*/.script-cache.lock \
                "$state"/prepared/group-*/rccl/.script-cache.lock \
                "$state"/groups/.script-cache.lock \
                "$state"/groups/group-*/rccl/.script-cache.lock; do
        queue_remove "$file" || return 1
    done
    (( ${#pending[@]} == 0 )) || rm -f -- "${pending[@]}" || return 1
    if [[ $protocol == staged && -d $state/.node-permits ]]; then rmdir -- "$state/.node-permits" 2>/dev/null || true; fi
    # Keep token, log-format, timeout witnesses, cleanup evidence, fact tables,
    # saved scripts, devices, baseline plans and all log indexes unchanged.
    return 0
)

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
    [[ $# == 3 && $1 == finalize ]] || exit 64
    hcu_prune_state_runtime "$2" "$3"
fi
