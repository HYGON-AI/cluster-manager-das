#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Inlined by task_control.py, before env.sh. No remote Python or shared files.
# Persistent token tombstones intentionally prevent delayed MPI ranks starting.
# Requires Linux /proc, Bash 4+, setsid and flock (util-linux).

# Do not inherit a caller's errexit/nounset/job-control policy. All critical
# operations below check their status explicitly, including wait and stop.
set +e +u
set +m
set +o pipefail

hcu_stat() {
    local line rest
    IFS= read -r line 2>/dev/null < "/proc/$1/stat" || return 1
    rest=${line##*) }
    local -a fields
    read -r -a fields <<< "$rest"
    hcu_state=${fields[0]} hcu_ppid=${fields[1]} hcu_sid=${fields[3]} hcu_flags=${fields[6]} hcu_start=${fields[19]}
    [[ $hcu_start =~ ^[0-9]+$ && $hcu_sid =~ ^[0-9]+$ ]]
}

hcu_env_matches() {
    local entry env_fd token_ok=0 member_ok=0
    # Distinguish permission denial from a readable environment without token.
    if ! { exec {env_fd}<"/proc/$1/environ"; } 2>/dev/null; then return 2; fi
    [[ -z $hcu_subset ]] && member_ok=1
    while IFS= read -r -d '' entry; do
        [[ $entry == "HCU_TASK_TOKEN=$hcu_token" ]] && token_ok=1
        [[ -n $hcu_subset && $entry == "HCU_TASK_MEMBER=$hcu_subset" ]] && member_ok=1
        ((token_ok && member_ok)) && break
    done <&"$env_fd"
    exec {env_fd}<&-
    ((token_ok && member_ok))
}

hcu_candidate_uid() {
    local key real effective rest
    [[ -O /proc/$1 || $EUID -eq 0 ]] && return 0
    # A non-dumpable process can have root-owned proc files even when its real
    # UID is ours. File ownership alone must not hide such a detached worker.
    while read -r key real effective rest; do
        if [[ $key == Uid: ]]; then
            [[ $real == "$EUID" || $effective == "$EUID" ]]
            return $?
        fi
    done 2>/dev/null < "/proc/$1/status"
    return 1
}

hcu_init() {
    hcu_token=$1
    [[ $hcu_token =~ ^[A-Za-z0-9][A-Za-z0-9_-]{15,95}$ ]] || return 64
    [[ -d /proc/self && ${BASH_VERSINFO[0]} -ge 4 ]] || return 69
    local boot ns pid parent
    IFS= read -r boot < /proc/sys/kernel/random/boot_id || return 69
    ns=$(readlink /proc/self/ns/pid) || return 69
    ns=${ns//[^0-9]/}
    [[ -n $boot && -n $ns ]] || return 69
    # PID namespace + boot ID also isolate a bind-mounted /tmp across containers.
    hcu_dir="/tmp/hcu-task-$boot-$ns-$hcu_token"
    umask 077
    if ! mkdir -m 700 -- "$hcu_dir" 2>/dev/null; then
        [[ -d $hcu_dir && ! -L $hcu_dir ]] || return 73
        [[ -O $hcu_dir || $EUID -eq 0 ]] || return 77
    fi
    [[ ! -L $hcu_dir/lock && (! -e $hcu_dir/lock || -f $hcu_dir/lock) ]] || return 73
    command -v flock >/dev/null && command -v setsid >/dev/null || return 69
    hcu_self=$BASHPID
    hcu_subset=''
    declare -gA hcu_known=()
    declare -gA hcu_ancestors=()
    # The independent SSH/Docker stop connection has new, often non-dumpable
    # sshd/PAM ancestors. Prove ancestry by PID AND start time, not comm names.
    pid=$hcu_self
    while hcu_stat "$pid"; do
        parent=$hcu_ppid
        [[ $pid != "$hcu_self" ]] && hcu_ancestors[$pid]=$hcu_start
        [[ $parent =~ ^[0-9]+$ && $parent -gt 0 && $parent != "$pid" ]] || break
        [[ -z ${hcu_ancestors[$parent]+seen} ]] || break
        pid=$parent
    done
}

hcu_lock() {
    exec {hcu_lock_fd}>"$hcu_dir/lock" || return 73
    flock -x -w 10 "$hcu_lock_fd" || { exec {hcu_lock_fd}>&-; return 75; }
}

hcu_unlock() {
    flock -u "$hcu_lock_fd"
    exec {hcu_lock_fd}>&-
}

hcu_scan() {
    local f pid start sid flags match_rc epoch='' registrations=0
    hcu_uncertain=0
    local -A sessions=()
    # Keep a session leader alive until the payload and its children finish.
    # A reused numeric PID never authenticates a stale session registration.
    for f in "$hcu_dir"/member.*; do
        [[ -f $f && ! -L $f ]] || continue
        ((registrations += 1))
        if ! read -r pid start < "$f"; then hcu_uncertain=1; continue; fi
        [[ $pid =~ ^[0-9]+$ && $start =~ ^[0-9]+$ ]] || continue
        [[ -z $hcu_subset || $hcu_subset == "$pid:$start" ]] || continue
        if hcu_stat "$pid"; then
            if [[ $hcu_start == "$start" && $hcu_sid == "$pid" && $hcu_state != Z ]]; then
                sessions[$pid]=$start
            fi
        elif [[ -d /proc/$pid ]]; then
            hcu_uncertain=1
        fi
    done
    if [[ -f $hcu_dir/epoch && ! -L $hcu_dir/epoch ]]; then
        IFS= read -r epoch < "$hcu_dir/epoch" || epoch=''
        [[ $epoch =~ ^[0-9]+$ ]] || { epoch=''; hcu_uncertain=1; }
    elif ((registrations > 0)); then
        # Old/corrupt bookkeeping cannot prove when an unreadable orphan was
        # born. Retain uncertainty rather than inventing a late epoch at stop.
        hcu_uncertain=1
    fi
    for f in /proc/[0-9]*/stat; do
        pid=${f#/proc/}; pid=${pid%/stat}
        [[ $pid != "$hcu_self" ]] || continue
        if ! hcu_stat "$pid"; then
            [[ -d /proc/$pid ]] && hcu_candidate_uid "$pid" && hcu_uncertain=1
            continue
        fi
        [[ $hcu_state != Z && $hcu_state != X ]] || continue
        start=$hcu_start sid=$hcu_sid flags=$hcu_flags
        # PF_KTHREAD identifies a kernel thread with no userspace payload or
        # inherited task environment. Never infer this from a process name.
        if [[ $flags =~ ^[0-9]+$ ]] && ((flags & 0x00200000)); then continue; fi
        if [[ -n ${sessions[$sid]+yes} ]]; then
            hcu_known[$pid]=$start
        elif hcu_candidate_uid "$pid"; then
            hcu_env_matches "$pid"; match_rc=$?
            if ((match_rc == 0)); then
                hcu_known[$pid]=$start
            elif ((match_rc == 2)) && hcu_stat "$pid" && [[ $hcu_start == "$start" && $hcu_state != Z && $hcu_state != X ]]; then
                # Positive token/session identity above always takes priority.
                # Only unknown, unreadable candidates use provenance exclusion.
                if [[ -n $epoch ]] && ((start < epoch)); then continue; fi
                if [[ ${hcu_ancestors[$pid]:-} == "$start" ]]; then continue; fi
                # An unreadable same-UID candidate might be a detached worker.
                # Do not kill it without identity, and do not claim it is gone.
                # Without epoch or registrations, STOP was installed before
                # any payload could be admitted. Late owners will reject STOP.
                if [[ -n $epoch ]] || ((registrations > 0)); then hcu_uncertain=1; fi
            fi
        fi
    done
    # Retain known live identities if /proc becomes unreadable: fail closed.
    for pid in "${!hcu_known[@]}"; do
        if [[ ! -e /proc/$pid ]]; then
            unset 'hcu_known[$pid]'
        elif hcu_stat "$pid"; then
            if [[ $hcu_start != "${hcu_known[$pid]}" || $hcu_state == Z || $hcu_state == X ]]; then
                unset 'hcu_known[$pid]'
            fi
        fi
    done
}

hcu_signal() {
    local pid sig=$1
    for pid in "${!hcu_known[@]}"; do
        # Revalidate start time immediately before every signal. Never pkill by
        # program name, never signal a stale PID/process group from a pidfile.
        if hcu_stat "$pid" && [[ $hcu_start == "${hcu_known[$pid]}" && $hcu_state != Z ]]; then
            kill -s "$sig" -- "$pid" 2>/dev/null || :
        fi
    done
}

hcu_quiet() {
    local grace=$1 verify=$2 end stable=0
    hcu_scan
    hcu_signal TERM
    end=$((SECONDS + grace))
    while ((${#hcu_known[@]} && SECONDS < end)); do
        sleep 0.1
        hcu_scan
    done
    end=$((SECONDS + verify))
    while ((SECONDS <= end)); do
        hcu_scan
        if ((${#hcu_known[@]} == 0 && hcu_uncertain == 0)); then
            ((stable += 1))
            ((stable >= 2)) && return 0
        else
            stable=0
            # Freeze before killing, then rescan forks born during the first
            # snapshot. Detached/setsid children retain the token environment.
            hcu_signal STOP
            hcu_scan
            hcu_signal STOP
            hcu_signal KILL
        fi
        sleep 0.1
    done
    hcu_scan
    ((${#hcu_known[@]} == 0 && hcu_uncertain == 0))
}

hcu_owner() {
    local payload_umask=$1 child rc=0 owner_start child_start=''
    shift
    hcu_stat "$hcu_self" || return 70
    [[ $hcu_sid == "$hcu_self" ]] || { echo 'task guard: setsid failed' >&2; return 70; }
    owner_start=$hcu_start
    hcu_lock || return $?
    if [[ -e $hcu_dir/STOP ]]; then
        hcu_unlock
        echo "task guard: CANCELLED before start token=$hcu_token" >&2
        return 125
    fi
    printf '%s %s\n' "$hcu_self" "$owner_start" > "$hcu_dir/member.$hcu_self.$owner_start" || return 73
    # Catch (rather than ignore) signals so exec'ed programs get normal signal
    # dispositions. The owner remains a reaper while remote stop terminates it.
    trap ':' TERM INT HUP
    export HCU_TASK_TOKEN="$hcu_token" HCU_TASK_MEMBER="$hcu_self:$owner_start"
    # Guard bookkeeping remains private (077), but it must not change the
    # caller's file-creation policy. Restore only in the payload child; this
    # also preserves explicit umask changes subsequently made by env.sh.
    (
        umask "$payload_umask" || exit 70
        exec "$@"
    ) {hcu_lock_fd}>&- &
    child=$!
    if hcu_stat "$child"; then child_start=$hcu_start; fi
    hcu_unlock
    while :; do
        wait "$child"; rc=$?
        # wait is interrupted by a trapped signal; don't abandon the live child.
        if ! hcu_stat "$child" || [[ $hcu_state == Z || $hcu_state == X ||
            (-n $child_start && $hcu_start != "$child_start") ]]; then
            break
        fi
    done
    # A script can return leaving background/forked children. Clean only this
    # member, not concurrently running ranks sharing the global run token.
    hcu_subset="$hcu_self:$owner_start"
    hcu_quiet 1 5 || { echo 'task guard: child cleanup UNCONFIRMED' >&2; ((rc == 0)) && rc=70; }
    return "$rc"
}

hcu_main() {
    local mode=${1:-} token=${2:-} rc=0 grace verify definition payload_umask=''
    shift 2 || return 64
    if [[ $mode == run ]]; then
        # Capture before hcu_init sets 077. _owner must receive the original
        # value via argv because its inherited process umask is already 077.
        payload_umask=$(umask) || return 70
    elif [[ $mode == _owner ]]; then
        payload_umask=${1:-}
        shift || return 64
    fi
    if [[ $mode == run || $mode == _owner ]]; then
        [[ $payload_umask =~ ^[0-7]{3,4}$ ]] || return 64
    fi
    hcu_init "$token" || { rc=$?; echo "task guard: initialization failed rc=$rc" >&2; return "$rc"; }
    case "$mode" in
        run)
            [[ ${1:-} == -- && $# -ge 2 ]] || return 64
            shift
            # Admission provenance is recorded before setsid or any payload.
            # All ranks share this minimum start tick in this boot/PID namespace.
            # The same lock serializes STOP, epoch updates and owner admission.
            hcu_lock || return $?
            if [[ -e $hcu_dir/STOP ]]; then
                hcu_unlock
                echo "task guard: CANCELLED before start token=$hcu_token" >&2
                return 125
            fi
            hcu_stat "$hcu_self" || { hcu_unlock; return 70; }
            local run_start=$hcu_start epoch=''
            if [[ -e $hcu_dir/epoch ]]; then
                if [[ -L $hcu_dir/epoch ]] || ! IFS= read -r epoch < "$hcu_dir/epoch" || [[ ! $epoch =~ ^[0-9]+$ ]]; then
                    hcu_unlock
                    return 73
                fi
            fi
            if [[ -z $epoch ]] || ((run_start < epoch)); then
                printf '%s\n' "$run_start" > "$hcu_dir/epoch" || { hcu_unlock; return 73; }
            fi
            hcu_unlock
            # Do not let the caller's shell job control choose the worker group.
            # Inline the functions so rank hosts do not need this file installed.
            definition=$(declare -f hcu_stat hcu_env_matches hcu_candidate_uid hcu_init hcu_lock hcu_unlock hcu_scan hcu_signal hcu_quiet hcu_owner hcu_main)
            env -u BASH_ENV -u ENV HCU_TASK_TOKEN="$token" setsid --wait bash --noprofile --norc -c "set +e +u; set +m; set +o pipefail; $definition"$'\nhcu_main "$@"' hcu-task-guard _owner "$token" "$payload_umask" -- "$@"
            return $?
            ;;
        _owner)
            [[ ${1:-} == -- && $# -ge 2 ]] || return 64
            shift
            hcu_owner "$payload_umask" "$@"
            return $?
            ;;
        stop)
            grace=${1:-3} verify=${2:-10}
            [[ $grace =~ ^[0-9]+$ && $verify =~ ^[0-9]+$ ]] || return 64
            ((grace <= 60 && verify >= 1 && verify <= 120)) || return 64
            hcu_lock || return $?
            [[ ! -L $hcu_dir/STOP && (! -e $hcu_dir/STOP || -f $hcu_dir/STOP) ]] || { hcu_unlock; return 73; }
            : > "$hcu_dir/STOP" || { hcu_unlock; return 73; }
            hcu_unlock
            hcu_quiet "$grace" "$verify" || rc=1
            if ((rc == 0)); then
                printf '__HCU_TASK_STOP__={"token":"%s","status":"STOPPED","remaining":0,"tombstone":true}\n' "$token"
            else
                printf '__HCU_TASK_STOP__={"token":"%s","status":"FAILED","remaining":%s,"tombstone":true,"uncertain":%s}\n' "$token" "${#hcu_known[@]}" "$([[ $hcu_uncertain == 1 ]] && printf true || printf false)"
                printf 'task guard: cleanup UNCONFIRMED; unreadable_proc=%s; surviving PIDs: %s\n' "$hcu_uncertain" "${!hcu_known[*]}" >&2
            fi
            return "$rc"
            ;;
        *) return 64 ;;
    esac
}

hcu_main "$@"
