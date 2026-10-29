#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Host-only probe. Exit 0=idle, 1=busy/unknown/unconfirmed. Never delete containers.
set -uo pipefail
RELEASE=${1:-0}
LIMIT=${2:-60}
emit() { printf '[%s] %s\n' "$1" "$2"; }
fail() { emit ERROR "$*"; exit 1; }
HSMI=''
for candidate in hy-smi Hy-smi /opt/hyhal/bin/hy-smi /opt/dtk/.hyhal/bin/hy-smi /opt/dtk/bin/hy-smi; do
    if command -v "$candidate" >/dev/null 2>&1; then HSMI=$candidate; break; fi
done
[[ -n $HSMI ]] || fail 'hy-smi unavailable on host'
command -v timeout >/dev/null || fail 'timeout unavailable'
trap 'emit ERROR "interrupted; release state unconfirmed"; exit 130' INT TERM HUP
# DTK host binaries can be installed without their libraries in the login path.
# Apply discovered vendor library directories only to SMI, never Docker/SSH.
smi_lib=${LD_LIBRARY_PATH:-}
for directory in /opt/hyhal/lib /opt/dtk/lib; do
    [[ ! -r $directory/librocm_smi64.so ]] || smi_lib="$directory${smi_lib:+:$smi_lib}"
done
SMI=(env "LD_LIBRARY_PATH=$smi_lib" "$HSMI")
sample() {
    local output parsed
    output=$(timeout "$LIMIT" "${SMI[@]}" 2>&1) || { emit ERROR "hy-smi failed: $output"; return 1; }
    printf '%s\n' "$output"
    parsed=$(awk '
      /VRAM%/ && /HCU%/ {header=1}
      $1 ~ /^[0-9]+$/ {
        count++
        if ($6 !~ /^[0-9]+([.][0-9]+)?%$/ || $7 !~ /^[0-9]+([.][0-9]+)?%$/) bad=1
        v=$6; u=$7; sub(/%$/, "", v); sub(/%$/, "", u)
        if (v+0>100 || u+0>100) bad=1
        if (v+0!=0 || u+0!=0) {busy++; printf "[CARD] card=%s vram=%s util=%s\n",$1,$6,$7}
      }
      END {if (!header || !count || bad) exit 2; print busy+0}
    ' <<< "$output") || { emit ERROR 'invalid or missing HCU readings'; return 1; }
    BUSY_COUNT=${parsed##*$'\n'}
    [[ $parsed != *$'\n'* ]] || printf '%s\n' "${parsed%$'\n'*}"
}
sample || exit 1
if (( BUSY_COUNT == 0 )); then emit IDLE 'all cards VRAM=0% HCU=0%'; exit 0; fi
emit BUSY "$BUSY_COUNT cards occupied; resolving host PIDs and containers"
pid_output=''
# Probe supported flags by successful execution and recognizable process output.
for flag in --showpids --showpid; do
    if pid_output=$(timeout "$LIMIT" "${SMI[@]}" "$flag" 2>&1) &&
       grep -qE 'PID:|KFD|PID[[:space:]]' <<< "$pid_output" &&
       ! grep -qiE 'unrecognized|unknown argument|invalid argument|Traceback' <<< "$pid_output"; then break; fi
    pid_output=''
done
[[ -n $pid_output ]] || fail 'cannot query HCU processes'
printf '%s\n' "$pid_output"
gpu_pids() {
    # Hy-smi can return valid KFD PIDs with blank HCU Index/VRAM fields.
    # Node-level PID ownership must not depend on those optional fields.
    awk '/^[[:space:]]*PID:[[:space:]]*[0-9]+/ {gsub(/[^0-9]/,"",$0); print $0}' <<< "$1"
}
mapfile -t GPU_PIDS < <(gpu_pids "$pid_output" | sort -un)
((${#GPU_PIDS[@]})) || fail 'busy readings but no identifiable host PIDs'
command -v docker >/dev/null || fail 'Docker unavailable; ownership unknown'
containers=$(timeout "$LIMIT" docker ps -q --no-trunc) || fail 'cannot list Docker containers'
declare -A OWNER=() NAME=() STARTED=() PID_START=() TARGETS=()
identity() {
    local value
    [[ -r /proc/$1/stat ]] || return 1
    value=$(cat "/proc/$1/stat") || return 1
    value=${value##*) }
    local -a fields
    read -r -a fields <<< "$value"
    [[ ${fields[19]:-} =~ ^[0-9]+$ ]] || return 1
    printf '%s' "${fields[19]}"
}
unknown=0
for cid in $containers; do
    [[ $cid =~ ^[a-f0-9]{64}$ ]] || { unknown=1; continue; }
    info=$(timeout "$LIMIT" docker inspect -f '{{.Name}}|{{.State.StartedAt}}|{{.State.Running}}' "$cid") || { unknown=1; continue; }
    IFS='|' read -r name started running <<< "$info"
    [[ $running == true ]] || continue
    members=$(timeout "$LIMIT" docker top "$cid" -eo pid) || { unknown=1; continue; }
    NAME[$cid]=${name#/}; STARTED[$cid]=$started
    while read -r member _; do
        [[ $member =~ ^[0-9]+$ ]] || continue
        OWNER[$member]=$cid
    done <<< "$members"
done
for pid in "${GPU_PIDS[@]}"; do
    start=$(identity "$pid") || continue # Process exited during snapshot.
    cid=${OWNER[$pid]:-}
    if [[ -z $cid ]]; then
        emit UNKNOWN "pid=$pid host process or container ownership unavailable"
        unknown=1; continue
    fi
    # Revalidate membership after enumerating all containers, including PID reuse.
    members=$(timeout "$LIMIT" docker top "$cid" -eo pid) || { unknown=1; continue; }
    if ! awk -v p="$pid" '$1==p {found=1} END {exit !found}' <<< "$members" ||
       [[ $(identity "$pid") != "$start" ]]; then unknown=1; continue; fi
    PID_START[$pid]=$start; TARGETS[$cid]=1
    emit OWNER "pid=$pid container=${NAME[$cid]} id=$cid"
done
(( RELEASE )) || exit 1
((${#TARGETS[@]})) || fail 'no confirmed occupying containers to release'
for cid in "${!TARGETS[@]}"; do
    sample || exit 1
    if (( BUSY_COUNT == 0 )); then emit IDLE 'all cards now zero; no further containers stopped'; exit 0; fi
    current=$(timeout "$LIMIT" docker inspect -f '{{.State.StartedAt}}|{{.State.Running}}' "$cid") || { unknown=1; continue; }
    [[ $current == "${STARTED[$cid]}|true" ]] || { emit ERROR "container identity changed: $cid"; unknown=1; continue; }
    members=$(timeout "$LIMIT" docker top "$cid" -eo pid) || { unknown=1; continue; }
    fresh=$(timeout "$LIMIT" "${SMI[@]}" "$flag" 2>&1) || { emit ERROR 'cannot refresh HCU process list'; unknown=1; continue; }
    fresh_pids=$(gpu_pids "$fresh")
    valid=0; changed=0
    for pid in "${!PID_START[@]}"; do
        [[ ${OWNER[$pid]} == "$cid" ]] || continue
        now=$(identity "$pid") || continue
        if [[ $now != "${PID_START[$pid]}" ]] || ! grep -qx "$pid" <<< "$fresh_pids" || ! awk -v p="$pid" '$1==p {f=1} END {exit !f}' <<< "$members"; then
            changed=1
        else valid=1; fi
    done
    if (( changed || !valid )); then emit ERROR "occupying process changed: $cid"; unknown=1; continue; fi
    emit RELEASE "stopping container=${NAME[$cid]} id=$cid (all container tasks; grace=30s)"
    if ! timeout "$LIMIT" docker stop --time 30 "$cid"; then emit ERROR "stop failed: $cid"; unknown=1; continue; fi
    running=$(timeout "$LIMIT" docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)
    # AutoRemove containers disappear after a successful stop.
    if [[ $running == false ]] || { [[ -z $running ]] && timeout "$LIMIT" docker ps -aq --no-trunc | awk -v c="$cid" '$1==c{f=1} END{exit f}'; }; then
        emit STOPPED "container=${NAME[$cid]} id=$cid"
    else emit ERROR "stop unconfirmed: $cid"; unknown=1; fi
done
for attempt in 1 2 3 4 5; do
    sleep 2
    sample || exit 1
    if (( BUSY_COUNT == 0 )); then
        emit IDLE 'release verified: all cards VRAM=0% HCU=0%'
        (( unknown == 0 )) || fail 'resources idle, but some ownership/release actions unconfirmed'
        exit 0
    fi
done
fail 'release attempted; HCU resources still busy'
