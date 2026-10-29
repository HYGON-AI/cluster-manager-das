#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# One durable raw log per group. Task facts keep their original logical paths;
# the reporting reader resolves archived paths through the committed TSV index.

archive_log_digest() {
    local archive=$1 offset=$2 length=$3
    dd if="$archive" iflag=skip_bytes,count_bytes bs=1048576 \
        skip="$offset" count="$length" status=none | sha256sum | awk '{print $1}'
}

# Register bytes already written to the public log; never copy a task log.
index_group_range() (
    local state=$1 public=$2 group=$3 offset=$4 length=$5
    shift 5
    local directory=$state/log-archive index pending source digest fd
    [[ $group =~ ^group-[0-9]+$ && $offset =~ ^[0-9]+$ && $length =~ ^[0-9]+$ ]] || return 1
    mkdir -p -- "$directory" || return 1
    index=$directory/$group.tsv
    pending=$directory/.$group.tsv.$BASHPID
    exec {fd}> "$directory/$group.lock" || return 1
    flock -x "$fd" || return 1
    digest=$(archive_log_digest "$public/$group.log" "$offset" "$length") || return 1
    if [[ -f $index ]]; then cp -- "$index" "$pending" || return 1
    else printf 'source\tarchive\toffset\tlength\tsha256\n' > "$pending" || return 1; fi
    for source in "$@"; do
        [[ $source == "$state"/* && $source != *$'\t'* && $source != *$'\n'* ]] || return 1
        printf '%s\t%s.log\t%s\t%s\t%s\n' "${source#"$state"/}" "$group" "$offset" "$length" "$digest" >> "$pending" || return 1
    done
    sync -d -- "$public/$group.log" "$pending" || return 1
    mv -f -- "$pending" "$index" || return 1
    sync -- "$directory" "$state" "$public" || return 1
)

archive_group_logs() (
    local state=$1 public=$2 group=$3 directory=$4 phase=$5 item=$6 round=$7 nodes=$8
    local outcome=$9 code=${10} cleanup=${11}
    local index_dir index pending listing archive archive_fd lock_fd
    local file source size digest offset copied record old_source old_archive old_offset old_length old_digest
    local failed=0 header legacy_iteration=''
    local -a sources=()
    local -A known=()
    [[ $group =~ ^group-[0-9]+$ && $directory == "$state"/* ]] || return 1
    index_dir=$state/log-archive
    index=$index_dir/$group.tsv
    pending=$index_dir/.$group.tsv.$BASHPID
    listing=$index_dir/.$group.files.$BASHPID
    archive=$public/$group.log
    mkdir -p -- "$index_dir" || return 1
    exec {lock_fd}> "$index_dir/$group.lock" || return 1
    flock -x "$lock_fd" || return 1
    header=$'source\tarchive\toffset\tlength\tsha256'
    if [[ -f $index ]]; then
        IFS= read -r record < "$index" || return 1
        [[ $record == "$header" ]] || return 1
        while IFS=$'\t' read -r old_source old_archive old_offset old_length old_digest; do
            [[ $old_source != source ]] || continue
            [[ -n $old_source && $old_archive == "$group.log" && $old_offset =~ ^[0-9]+$ && $old_length =~ ^[0-9]+$ && $old_digest =~ ^[0-9a-f]{64}$ ]] || return 1
            known[$old_source]=$old_offset$'\t'$old_length$'\t'$old_digest
        done < "$index"
        cp -- "$index" "$pending" || return 1
    else
        printf '%s\n' "$header" > "$pending" || return 1
    fi
    find "$directory" -type f \( -name '*.log' -o -name stdout.txt -o -name stderr.txt \
        -o -name controller.stdout -o -name controller.stderr \
        -o -path '*/.clush/stdout/*' -o -path '*/.clush/stderr/*' \) -print0 \
        | LC_ALL=C sort -z > "$listing" || return 1
    exec {archive_fd}>> "$archive" || return 1
    [[ $round == 0 ]] || legacy_iteration=" 历史轮次=$round"
    printf '\n========== 分组 %s | 节点 %s ==========\n日志=%s\n阶段=%s 项目=%s%s\n' \
        "$group" "$nodes" "$archive" "$phase" "$item" "$legacy_iteration" >&"$archive_fd" || return 1
    while IFS= read -r -d '' file; do
        source=${file#"$state"/}
        [[ $source != *$'\t'* && $source != *$'\n'* && $source != *$'\r'* ]] || return 1
        size=$(stat -c '%s' -- "$file") || return 1
        digest=$(sha256sum -- "$file" | awk '{print $1}') || return 1
        if [[ -n ${known[$source]:-} ]]; then
            IFS=$'\t' read -r old_offset old_length old_digest <<< "${known[$source]}"
            [[ $size == "$old_length" && $digest == "$old_digest" ]] || return 1
            copied=$(archive_log_digest "$archive" "$old_offset" "$old_length") || return 1
            [[ $copied == "$digest" ]] || return 1
        else
            printf '\n----- 原始输出（来源路径，已合并）：%s -----\n' "$file" >&"$archive_fd" || return 1
            offset=$(stat -c '%s' -- "$archive") || return 1
            cat -- "$file" >&"$archive_fd" || return 1
            copied=$(stat -c '%s' -- "$archive") || return 1
            (( copied - offset == size )) || return 1
            copied=$(archive_log_digest "$archive" "$offset" "$size") || return 1
            [[ $copied == "$digest" ]] || return 1
            [[ $(stat -c '%s' -- "$file") == "$size" ]] || return 1
            printf '%s\t%s\t%s\t%s\t%s\n' "$source" "$group.log" "$offset" "$size" "$digest" >> "$pending" || return 1
            known[$source]=$offset$'\t'$size$'\t'$digest
            printf '\n----- 输出结束 -----\n' >&"$archive_fd" || return 1
        fi
        sources+=("$file")
    done < "$listing"
    printf '\n[阶段执行] status=%s rc=%s cleanup=%s\n健康、正确性和性能结论见 summary.md。\n' \
        "$outcome" "$code" "$cleanup" >&"$archive_fd" || return 1
    exec {archive_fd}>&-
    # Commit data before publishing references. On failure all source files stay.
    sync -d -- "$archive" "$pending" || return 1
    mv -f -- "$pending" "$index" || return 1
    # With operands, GNU sync fsyncs these directories rather than flushing
    # the entire filesystem. Persist the new log/index names before unlink.
    sync -- "$index_dir" "$state" "$public" "${public%/*}" || return 1
    rm -f -- "$listing"
    # A still-unconfirmed remote process may hold a source open. Preserve that
    # evidence rather than unlinking a file which could still receive output.
    [[ $cleanup == CONFIRMED ]] || return 0
    for file in "${sources[@]}"; do
        source=${file#"$state"/}
        IFS=$'\t' read -r old_offset size digest <<< "${known[$source]}"
        if [[ $(stat -c '%s' -- "$file") == "$size" && $(sha256sum -- "$file" | awk '{print $1}') == "$digest" ]]; then
            rm -f -- "$file" || failed=1
        else
            failed=1
        fi
    done
    (( failed == 0 ))
)
