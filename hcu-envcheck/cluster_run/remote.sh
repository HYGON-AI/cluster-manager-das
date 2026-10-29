#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Launch only the reporting process in a selectable host's environment
# (default: the first tested node; --report-host decouples it). Sourced by
# controller.sh; the entry host never needs Python.
report_command() {
    local mode=$1 display=$2 width=$3 export_plan=${4:-} body quoted remote_command python_setup
    local target_root=${REPORT_WORKDIR:-${CONTAINER_WORKDIR:-$ROOT}}
    local target_run=$RUN_DIR report_env=${REPORT_ENV_SCRIPT:-$ENV_SCRIPT}
    if [[ $RUN_DIR == "$ROOT"/* ]]; then target_run=$target_root/${RUN_DIR#"$ROOT"/}; fi
    local export_args=''
    if [[ -n $export_plan ]]; then printf -v export_args ' --export-plan %q' "$export_plan"; fi
    printf -v body 'set -Ee; cd -- %q; ' "$target_root"
    if [[ -n $report_env ]]; then
        printf -v quoted '%q' "$report_env"
        body+="source $quoted; "
    fi
    if [[ -n $REPORT_PYTHON ]]; then
        printf -v quoted '%q' "$REPORT_PYTHON"
        body+="report_python=$quoted; "
    else
        body+='report_python=""; for candidate in python3 python; do if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c '\''import sys; sys.exit(sys.version_info < (3,10))'\''; then report_python=$candidate; break; fi; done; test -n "$report_python"; '
    fi
    # Report provenance is written by Python inside the stable report.lock,
    # together with all other derived files, never before taking that lock.
    printf -v quoted '%q ' "$target_run"
    printf -v python_setup 'export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=%q; exec "$report_python" -u -m hcu_envcheck.reporting %q --run-dir %q%s' "$target_root" "$mode" "$target_run" "$export_args"
    body+=$python_setup
    if [[ $mode == watch ]]; then
        printf -v quoted ' --display %q --width %q' "$display" "$width"
        body+=$quoted
    fi
    if [[ $SCENARIO == per-node-container ]]; then
        printf -v remote_command '%q ' docker exec "$CONTAINER" bash -lc "$body"
    else
        printf -v remote_command '%q ' bash -lc "$body"
    fi
    ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=5 \
        -o ServerAliveCountMax=3 "$SSH_USER@${REPORT_HOST}" "$remote_command" </dev/null
}

# Serialization runs where Python already exists. It uses one ordinary SSH
# permit, never the cleanup reservation, and never publishes facts or reports.
facts_command() {
    local target_root=${REPORT_WORKDIR:-${CONTAINER_WORKDIR:-$ROOT}} target_run=$RUN_DIR
    local report_env=${REPORT_ENV_SCRIPT:-$ENV_SCRIPT} body quoted remote_command value mapped
    local slot fd rc candidate_python permit_i
    local -a arguments=()
    if [[ $RUN_DIR == "$ROOT"/* ]]; then target_run=$target_root/${RUN_DIR#"$ROOT"/}; fi
    for value in "$@"; do
        mapped=$value
        if [[ $value == "$ROOT"/* ]]; then mapped=$target_root/${value#"$ROOT"/}; fi
        arguments+=("$mapped")
    done
    printf -v body 'set -Ee; cd -- %q; ' "$target_root"
    if [[ -n $report_env ]]; then printf -v quoted '%q' "$report_env"; body+="source $quoted >&2; "; fi
    if [[ -n $REPORT_PYTHON ]]; then
        printf -v quoted '%q' "$REPORT_PYTHON"; body+="candidate_python=$quoted; "
    else
        body+='candidate_python=""; for p in python3 python; do if command -v "$p" >/dev/null 2>&1 && "$p" -c '\''import sys; sys.exit(sys.version_info < (3,10))'\''; then candidate_python=$p; break; fi; done; test -n "$candidate_python"; '
    fi
    printf -v quoted '%q ' "${arguments[@]}"
    body+='export PYTHONDONTWRITEBYTECODE=1; exec "$candidate_python" -B -m hcu_envcheck.state_facts '
    printf -v value -- '--run-dir %q ' "$target_run"; body+="$value$quoted"
    if [[ $SCENARIO == per-node-container ]]; then printf -v remote_command '%q ' docker exec "$CONTAINER" bash -lc "$body"
    else printf -v remote_command '%q ' bash -lc "$body"; fi
    while :; do
        for ((permit_i=1;permit_i<=NODE_CONCURRENCY;permit_i++)); do
            slot=$NODE_PERMIT_DIR/permit.$permit_i
            exec {fd}>>"$slot" || return 73
            if flock -n "$fd"; then
                ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 \
                    "$SSH_USER@$REPORT_HOST" "$remote_command" </dev/null
                rc=$?
                flock -u "$fd"; exec {fd}>&-
                return "$rc"
            fi
            exec {fd}>&-
        done
        sleep 0.2
    done
}
