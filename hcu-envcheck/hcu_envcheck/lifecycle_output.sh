#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Login-side output adapter. Execution, node statuses and exit codes belong
# to lifecycle.sh; this file only stores evidence and renders completed stages.

lifecycle_output_emit() {
    printf "$@" >&"$LIFECYCLE_TERMINAL_FD"
    printf "$@" >> "$LIFECYCLE_DETAIL_LOG"
}

lifecycle_output_init() {
    local root parent
    root=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || return 1
    parent=${OUTPUT_DIR:-$root/verify_results}
    [[ $parent == /* ]] || parent=$PWD/$parent
    [[ $parent != *$'\t'* && $parent != *$'\n'* && $parent != *$'\r'* ]] || return 1
    mkdir -p -- "$parent" || return 1
    LIFECYCLE_OUTPUT_DIR=$(mktemp -d "$parent/${SCENARIO}_${OPERATION}_lifecycle_$(date +%Y%m%d_%H%M%S)_XXXXXX") || return 1
    LIFECYCLE_DETAIL_LOG=$LIFECYCLE_OUTPUT_DIR/lifecycle-details.log
    LIFECYCLE_WORK_DIR=$LIFECYCLE_OUTPUT_DIR/.work
    mkdir -- "$LIFECYCLE_WORK_DIR" || return 1
    : > "$LIFECYCLE_DETAIL_LOG" || return 1
    exec {LIFECYCLE_TERMINAL_FD}>&1 || return 1
    LIFECYCLE_OUTPUT_OWNER=$BASHPID
    LIFECYCLE_CURRENT_STAGE=''
    LIFECYCLE_OUTPUT_ENABLED=1
    if [[ $LOG_DETAIL == quiet ]]; then
        printf '[CONTAINER] operation=%s container=%s planned=%s image=%s\n' \
            "$OPERATION" "$CONTAINER" "${#NODES[@]}" "${IMAGE:--}" >> "$LIFECYCLE_DETAIL_LOG"
    else
        lifecycle_output_emit '[CONTAINER] operation=%s container=%s planned=%s image=%s\n' \
            "$OPERATION" "$CONTAINER" "${#NODES[@]}" "${IMAGE:--}"
    fi
}

lifecycle_output_record() {
    local node=$1 stage=$2 status=$3 code=$4 rc=$5 cid=$6 image=$7 message=$8 normalized file
    # Preserve real differences in error text. Only the known node-identity
    # suffix is normalized; do not strip arbitrary paths, ports or image IDs.
    message=${message//$'\t'/ }
    image=${image//$'\t'/\\t}
    image=${image//$'\n'/\\n}
    image=${image//$'\r'/\\r}
    normalized=$message
    [[ $message != "docker info failed on $node" ]] || normalized='docker info failed on node'
    file=$LIFECYCLE_WORK_DIR/$LIFECYCLE_CURRENT_STAGE/$node.row
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$node" "$stage" "$status" "$code" "$rc" "$cid" "$image" "$message" "$normalized" \
        > "$file.tmp.$BASHPID" && mv -f -- "$file.tmp.$BASHPID" "$file"
}

lifecycle_output_flush() {
    [[ ${LIFECYCLE_OUTPUT_ENABLED:-0} == 1 && -n ${LIFECYCLE_CURRENT_STAGE:-} ]] || return 0
    [[ $BASHPID == "$LIFECYCLE_OUTPUT_OWNER" ]] || return 0
    local stage=$LIFECYCLE_CURRENT_STAGE directory node record
    directory=$LIFECYCLE_WORK_DIR/$stage
    : > "$directory/ordered.tsv" || return 1
    # Workers own separate files. Only the parent appends to the public log
    # and renders the terminal, always in hostfile order after the barrier.
    for node in "${NODES[@]}"; do
        if [[ -f $directory/$node.row ]]; then
            IFS= read -r record < "$directory/$node.row" || return 1
            printf '%s\n' "$record" >> "$directory/ordered.tsv" || return 1
        fi
        if [[ -f $directory/$node.log ]]; then
            printf '\n[DETAIL] node=%s stage=%s\n' "$node" "$stage" >> "$LIFECYCLE_DETAIL_LOG" || return 1
            cat -- "$directory/$node.log" >> "$LIFECYCLE_DETAIL_LOG" || return 1
            if [[ $LOG_DETAIL == every ]]; then
                printf '[DETAIL] node=%s stage=%s\n' "$node" "$stage" >&"$LIFECYCLE_TERMINAL_FD"
                cat -- "$directory/$node.log" >&"$LIFECYCLE_TERMINAL_FD"
            fi
        fi
    done
    if [[ $LOG_DETAIL != every ]]; then
        awk -F '\t' -v total="${#NODES[@]}" -v stage="$stage" -v mode="$LOG_DETAIL" '
        function piece(prefix, first, last) {
            return first == last ? prefix first : prefix "[" first "-" last "]"
        }
        function folded(key, n, i, node, prefix, digits, numeric, active, oldprefix, first, last, width, previous, output) {
            n=count[key]; output=""; active=0
            for(i=1;i<=n;i++) {
                node=member[key,i]; prefix=node; sub(/[0-9]+$/,"",prefix)
                digits=substr(node,length(prefix)+1)
                numeric=(prefix!="" && prefix!=node && length(digits)<=9 && node!~/^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/)
                if(i>1 && numeric && active && prefix==oldprefix && length(digits)==width && (digits+0)==previous+1) {
                    last=digits
                } else {
                    if(i>1) output=output (output=="" ? "" : ",") piece(oldprefix,first,last)
                    oldprefix=numeric ? prefix : node; first=numeric ? digits : ""; last=first
                }
                active=numeric; width=length(digits); previous=digits+0
            }
            if(n>0) output=output (output=="" ? "" : ",") piece(oldprefix,first,last)
            return output
        }
        NF>=9 {
            key=$2 SUBSEP $3 SUBSEP $4 SUBSEP $5 SUBSEP $7 SUBSEP $9
            if(!(key in seen)) { seen[key]=++groups; order[groups]=key; step[key]=$2; status[key]=$3; code[key]=$4; rc[key]=$5; reason[key]=$9 }
            count[key]++; member[key,count[key]]=$1
        }
        END {
            split("PASS WARN FAIL", severity, " ")
            for(s=1;s<=3;s++) for(i=1;i<=groups;i++) {
                key=order[i]
                if(status[key]!=severity[s] || (mode=="quiet" && status[key]=="PASS")) continue
                members=count[key]==total ? "ALL" : folded(key); message=reason[key]
                if(step[key]=="delete" && status[key]=="PASS" && code[key]=="OK") message="容器已删除"
                else if(step[key]=="delete" && code[key]=="NODE_UNREACHABLE") message="SSH连接失败"
                else if(step[key]=="delete" && code[key]=="CONTAINER_ABSENT") message="容器不存在，未执行删除"
                printf "[%s] nodes=%s count=%d stage=%s",status[key],members,count[key],step[key]
                if(status[key]!="PASS") printf " code=%s rc=%s",code[key],rc[key]
                printf " reason=%s\n",message
            }
        }' "$directory/ordered.tsv" > "$directory/groups.txt" || return 1
        while IFS= read -r record; do lifecycle_output_emit '%s\n' "$record" || return 1; done < "$directory/groups.txt"
    fi
    LIFECYCLE_CURRENT_STAGE=''
    rm -rf -- "$directory" || return 1
    return 0
}

lifecycle_output_stage() {
    lifecycle_output_flush || return 1
    LIFECYCLE_CURRENT_STAGE=$1
    mkdir -- "$LIFECYCLE_WORK_DIR/$1" || return 1
    if [[ $LOG_DETAIL != quiet && $1 != delete ]]; then
        lifecycle_output_emit '[STAGE] stage=%s planned=%s\n' "$1" "${#NODES[@]}"
    fi
}

lifecycle_output_exit() {
    local rc=$1
    if [[ ${LIFECYCLE_OUTPUT_ENABLED:-0} == 1 ]]; then
        # Never flush/remove the parents files from a worker subshell.
        [[ $BASHPID == "$LIFECYCLE_OUTPUT_OWNER" ]] || return "$rc"
        # An early exit inside redirected preflight still holds its log open.
        # Release it before removal: NFS cannot unlink an open log cleanly.
        exec >&"$LIFECYCLE_TERMINAL_FD" 2>&1
        if lifecycle_output_flush; then
            rm -rf -- "$LIFECYCLE_WORK_DIR"
        else
            printf '[ERROR] lifecycle output incomplete; raw evidence: %s\n' "$LIFECYCLE_OUTPUT_DIR" >&"$LIFECYCLE_TERMINAL_FD"
        fi
        printf 'LOG %s\n' "$LIFECYCLE_DETAIL_LOG" >&"$LIFECYCLE_TERMINAL_FD"
    fi
    [[ -z ${RESULTS_DIR:-} ]] || rm -rf -- "$RESULTS_DIR"
    return "$rc"
}
