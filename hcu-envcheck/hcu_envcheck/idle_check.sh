#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Host-side, read-only by default. --release explicitly authorizes container stop.
set -uo pipefail
ROOT=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || exit 70
die() { printf '[ERROR] %s\n' "$*" >&2; exit 2; }
usage() {
 cat <<'HELP'
Usage: hcu-cluster-run per-node-container idle-check -f HOSTFILE [--release]
  --release            Stop all confirmed occupying Docker containers, then recheck.
                       Stops every task inside those containers; never runs docker rm.
  --concurrency N      Concurrent nodes (default 32)
  --timeout SECONDS    Per-node overall deadline (default 300)
  -o, --output-dir DIR Evidence parent directory
  --dry-run            Preview nodes only, no connections or live idle judgment
Idle means every card has VRAM%=0 AND HCU%=0. Any nonzero reading queries PIDs.
Missing/unparseable readings fail. Host processes/unmapped PIDs are never killed.
HELP
}
[[ ${1:-} == per-node-container && ${2:-} == idle-check ]] || die 'idle-check requires per-node-container'
shift 2
HOSTFILE='' OUTPUT_DIR="$ROOT/verify_results" RELEASE=0 CONCURRENCY=32 LIMIT=300 DRY=0
while (($#)); do
 case $1 in
   -h|--help) usage; exit 0;;
   --release) RELEASE=1; shift; continue;;
   --dry-run) DRY=1; shift; continue;;
   --*=*) option=${1%%=*}; value=${1#*=}; shift;;
   -f|--hostfile|-o|--output-dir|--concurrency|--timeout)
      (($#>=2)) || die "$1 requires a value"; option=$1; value=$2; shift 2;;
   *) die "unsupported option: $1";;
 esac
 [[ -n $value ]] || die "$option requires a nonempty value"
 case $option in
   -f|--hostfile) HOSTFILE=$value;; -o|--output-dir) OUTPUT_DIR=$value;;
   --concurrency) CONCURRENCY=$value;; --timeout) LIMIT=$value;;
   *) die "unsupported option: $option";;
 esac
done
[[ -r $HOSTFILE ]] || die 'hostfile missing or unreadable'
[[ $CONCURRENCY =~ ^[1-9][0-9]*$ && ${#CONCURRENCY} -le 4 ]] && ((CONCURRENCY<=1024)) || die 'invalid concurrency'
[[ $LIMIT =~ ^[1-9][0-9]*$ && ${#LIMIT} -le 6 ]] && ((LIMIT>=40 && LIMIT<=86400)) || die 'timeout must be 40..86400 seconds'
SSH_USER=${HCU_SSH_USER:-${USER:-root}}
[[ $SSH_USER =~ ^[a-zA-Z_][a-zA-Z0-9_-]*$ ]] || die 'invalid SSH user'
declare -a NODES=() PIDS=()
declare -A SEEN=()
while read -r node rest || [[ -n ${node:-} ]]; do
 node=${node%$'\r'}; [[ -z $node || $node == \#* ]] && continue
 [[ $node =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && -z ${rest:-} ]] || die "invalid hostfile node: $node"
 [[ -z ${SEEN[$node]:-} ]] || die "duplicate node: $node"
 SEEN[$node]=1; NODES+=("$node")
done < "$HOSTFILE"
((${#NODES[@]})) || die 'empty hostfile'
if ((DRY)); then printf 'DRY_RUN idle-check release=%s nodes=%s\n' "$RELEASE" "${NODES[*]}"; exit 0; fi
mkdir -p -- "$OUTPUT_DIR" || exit 70
RUN=$(mktemp -d "$OUTPUT_DIR/idle-check_$(date +%Y%m%d_%H%M%S)_XXXXXX") || exit 70
printf 'node\texit_code\n' > "$RUN/results.tsv"
printf '[IDLE-CHECK] release=%s nodes=%s evidence=%s\n' "$RELEASE" "${#NODES[@]}" "$RUN"
cancel() {
 trap - INT TERM
 for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || :; done
 for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || :; done
 printf '[CANCELLED] remote release may be unconfirmed; inspect %s\n' "$RUN" >&2
 exit 130
}
trap cancel INT TERM
wait_wave() { local pid; for pid in "${PIDS[@]}"; do wait "$pid" || :; done; PIDS=(); }
for node in "${NODES[@]}"; do
 (
   rc=0
   # stdin sends the exact probe, so hosts need neither a project mount nor Python.
   transport=''
   trap '[[ -z $transport ]] || kill "$transport" 2>/dev/null; [[ -z $transport ]] || wait "$transport" 2>/dev/null; exit 130' TERM INT
   ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
     "$SSH_USER@$node" "timeout --signal=TERM --kill-after=10 $LIMIT bash -s -- $RELEASE 60" \
     < "$ROOT/hcu_envcheck/payloads/idle_check_node.sh" > "$RUN/$node.log" 2>&1 & transport=$!
   wait "$transport" || rc=$?
   printf '%s\n' "$rc" > "$RUN/$node.rc"
 ) & PIDS+=("$!")
 ((${#PIDS[@]}<CONCURRENCY)) || wait_wave
done
wait_wave
failed=0
for node in "${NODES[@]}"; do
 rc=70; [[ ! -r $RUN/$node.rc ]] || read -r rc < "$RUN/$node.rc"
 printf '%s\t%s\n' "$node" "$rc" >> "$RUN/results.tsv"
 printf '\n[NODE] %s rc=%s\n' "$node" "$rc"
 # Full raw SMI output remains in evidence; keep the default terminal concise.
 awk '/^\[(IDLE|BUSY|CARD|OWNER|UNKNOWN|RELEASE|STOPPED|ERROR)\]/' "$RUN/$node.log"
 if ((rc!=0)) && ! grep -qE '^\[(ERROR|BUSY|UNKNOWN)\]' "$RUN/$node.log"; then
   tail -5 -- "$RUN/$node.log"
 fi
 ((rc==0)) || failed=$((failed+1))
done
printf '[SUMMARY] total=%s idle=%s busy_or_failed=%s evidence=%s\n' "${#NODES[@]}" "$((${#NODES[@]}-failed))" "$failed" "$RUN"
((failed==0))
