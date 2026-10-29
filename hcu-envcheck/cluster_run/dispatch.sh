#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Unique public dispatch source for hcu-cluster-run. The source-tree launcher
# (bin/hcu-cluster-run) and the installed console script
# (cluster_run.__main__:entrypoint) both forward here, so the operation table,
# parameter boundary, help, and exit-code contract live in exactly one place.
# --env-script is NEVER sourced on this node. Python is selected only after
# this dispatch, so the login node does not need Python for the core paths.
set -u

# Public operation manifest, shared with cluster_run/cli.py (which parses
# these lines for its argparse choices). Groups must stay disjoint and their
# union is the complete public operation list. Empty optional manifest groups
# declare no public operations.
HCU_SCENARIOS='shared-conda|node-local-conda|per-node-container'
HCU_LIFECYCLE_OPS='container-status|container-create|container-recreate|container-delete'
HCU_CONTROLLER_OPS='idle-check|platform|resource|platform,resource|checkdeepep|rccl|deepep|byte-flux|ib-write-bw'
HCU_PYTHON_OPS=''
HCU_ALL_OPERATIONS="$HCU_LIFECYCLE_OPS|$HCU_CONTROLLER_OPS|$HCU_PYTHON_OPS"

# Exact membership test: literal string comparison only, no glob or eval
# semantics, so an unknown token can never match by accident.
hcu_list_has() {
    local entry token=$2
    local IFS='|'
    for entry in $1; do
        [[ $entry == "$token" ]] && return 0
    done
    return 1
}

resolve_script_dir() {
    local target=$1 target_dir link
    while [[ -h "$target" ]]; do
        target_dir=$(CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd) || return 1
        link=$(readlink "$target") || return 1
        case "$link" in
            /*) target=$link ;;
            *) target=$target_dir/$link ;;
        esac
    done
    CDPATH= cd -P -- "$(dirname -- "$target")" 2>/dev/null && pwd
}

SCRIPT_DIR=$(resolve_script_dir "${BASH_SOURCE[0]}") || {
    printf 'hcu-cluster-run: cannot resolve launcher path: %s\n' "$0" >&2
    exit 70
}
ROOT=$(CDPATH= cd -P -- "$SCRIPT_DIR/.." 2>/dev/null && pwd) || exit 70
if ! pwd -P >/dev/null 2>&1; then
    printf 'hcu-cluster-run: current directory no longer exists; switching to package root: %s; relative paths now use this directory\n' "$ROOT" >&2
    CDPATH= cd -P -- "$ROOT" || exit 70
fi

# Consume controller options, retaining exact argument boundaries for Python.
# Values of repeatable payload options are opaque, including --help.
hcu_controller_env=''
hcu_controller_python=''
hcu_forward=()
while (( $# )); do
    case "$1" in
        --controller-env-script|--controller-python)
            if (( $# < 2 )) || [[ -z "$2" || "$2" == --* ]]; then
                printf 'hcu-cluster-run: %s requires a value\n' "$1" >&2
                exit 3
            fi
            if [[ "$1" == --controller-env-script ]]; then
                hcu_controller_env=$2
            else
                hcu_controller_python=$2
            fi
            shift 2 ;;
        --controller-env-script=*) hcu_controller_env=${1#*=}; [[ -n "$hcu_controller_env" ]] || exit 3; shift ;;
        --controller-python=*) hcu_controller_python=${1#*=}; [[ -n "$hcu_controller_python" ]] || exit 3; shift ;;
        --) hcu_forward+=("$@"); break ;;
        --docker-arg|--container-command)
            hcu_forward+=("$1"); shift
            if (( $# )); then hcu_forward+=("$1"); shift; fi ;;
        *) hcu_forward+=("$1"); shift ;;
    esac
done

hcu_usage_status=0
if (( ${#hcu_forward[@]} == 1 )); then
    case "${hcu_forward[0]}" in
        shared-conda|node-local-conda|per-node-container)
            printf 'hcu-cluster-run: missing operation after %s\n' "${hcu_forward[0]}" >&2
            hcu_forward=(--help)
            hcu_usage_status=2 ;;
    esac
fi
if (( ${#hcu_forward[@]} == 2 )); then
    case "${hcu_forward[0]}:${hcu_forward[1]}" in
        shared-conda:--help|shared-conda:-h|node-local-conda:--help|node-local-conda:-h|per-node-container:--help|per-node-container:-h)
            hcu_forward=(--help) ;;
    esac
fi
if (( ${#hcu_forward[@]} == 0 )) ||
   { (( ${#hcu_forward[@]} == 1 )) && [[ "${hcu_forward[0]}" == --help || "${hcu_forward[0]}" == -h ]]; }; then
    cat <<'HELP'
Usage: hcu-cluster-run <scenario> <operation> -f <hostfile> [options]

Scenarios: shared-conda | node-local-conda | per-node-container
Checks:    idle-check | platform | resource | platform,resource | checkdeepep
Tests:     rccl | deepep | byte-flux
Container: container-status | container-create | container-recreate | container-delete
Extension: ib-write-bw (IB bandwidth)

  --release                      With idle-check: stop occupying containers and recheck
  --env-script /path/env.sh       Optional target-node/container environment setup
  --controller-env-script /path/controller-env.sh
                                 Source on entry node before selecting Python
  --controller-python /path/python3
                                 Entry-node Python (overrides HCU_ENVCHECK_PYTHON)
  --container NAME               Existing container for per-node-container
  --transport ssh|clush          Default: ssh
  --log-detail milestone|every|quiet
                                 Foreground log granularity (default: milestone
                                 folds same-cause nodes; every = per node/group)
  --group-size G                 RCCL/DeepEP nodes per group (default: 1).
  --gpus-per-node C              RCCL/DeepEP GPUs per node (default: 8).
                                 Independent settings; hostfile lists node names only.
                                 Complete groups only; trailing nodes are skipped.
                                 DeepEP intranode: 2/4/8 GPUs; multi-node norm: 8.
  --slots N                      Retained IB setting; unsupported by RCCL/DeepEP.
  --concurrency N                Concurrent node/group tasks (default: 32)
  --node-concurrency N           Cross-worker budget on ordinary node SSH
                                 (default: 32); reporting keeps 1 separate
                                 connection and stop/cleanup a global reserved
                                 one, so control connections stay <= N+2
  --timeout SECONDS              rccl/deepep/byte-flux: deadline per item and group;
                                 checkdeepep: per-node deadline. Defaults: 300s;
                                 byte-flux 4800s (full 64-case TP4 sweep per node)
  --container-ssh-port N         RCCL/DeepEP grouped multi-node peer port (default: 25901)
  --port N                       Required for create/recreate: start container SSH
                                 Use the same N with --container-ssh-port for MPI
  --script /path/test.sh          Target-visible Shell or Python payload
  --output-dir PATH              Reports on the entry node
  --report-env-script PATH       Reporting environment on first hostfile node
  --report-python PATH           Python >=3.10 in that container/Conda environment
  --report-workdir PATH          Reporting project root (default target root)
  --report-only RUN_DIR          Rebuild staged reports without executing tests
  --export-plan DIR              With --report-only: export failed/unfinished
                                 plan files (failed-nodes.hostfile,
                                 failed-groups.json, unfinished-groups.json,
                                 cleanup-unconfirmed.json) into a new empty
                                 directory; no tests are run
  --rerun-plan FILE              Rerun a T28 failed/unfinished export list
                                 with the original members/scale/modes/rounds;
                                 new run directory and tokens, old evidence
                                 untouched; dry-run previews the plan
  --report-host HOST             Select the reporting host (default: first
                                 tested node); decoupled from the tested list.
                                 report-only requires the original host to be
                                 selected explicitly on any switch
  --help-full                    Full Python CLI help (requires controller Python)
  --version                      Print release version without Python
HELP
    exit "$hcu_usage_status"
fi
if (( ${#hcu_forward[@]} == 1 )) && [[ "${hcu_forward[0]}" == --version ]]; then
    if IFS= read -r hcu_version < "$ROOT/VERSION" 2>/dev/null && [[ -n "$hcu_version" ]]; then
        printf 'hcu-cluster-run %s\n' "${hcu_version%$'\r'}"
        exit 0
    fi
    # Installed layout: no repository VERSION file next to the packages. A
    # pip-installed environment always has Python, so ask the package itself;
    # the source tree keeps the Python-free VERSION read above.
    for hcu_candidate in "${HCU_ENVCHECK_CONSOLE_PYTHON:-}" python3 python; do
        [[ -n "$hcu_candidate" ]] || continue
        command -v "$hcu_candidate" >/dev/null 2>&1 || continue
        exec "$hcu_candidate" -m cluster_run --version
    done
    printf 'hcu-cluster-run: cannot determine version: %s/VERSION is unreadable and no python3 is available\n' "$ROOT" >&2
    exit 70
fi
hcu_full_help=0
if (( ${#hcu_forward[@]} == 1 )) && [[ "${hcu_forward[0]}" == --help-full ]]; then
    hcu_forward=(--help)
    hcu_full_help=1
fi

# Preserve the previous public spelling without adding a separate operation.
if (( ${#hcu_forward[@]} >= 2 )) && [[ ${hcu_forward[1]} == check_deep ]]; then
    hcu_forward[1]=checkdeepep
fi

# Reject invalid commands before probing the controller Python. Complete
# argparse help and explicitly selected internal compatibility paths need it.
if (( ! hcu_full_help )); then
    if ! hcu_list_has "$HCU_SCENARIOS" "${hcu_forward[0]:-}"; then
        printf 'hcu-cluster-run: unknown scenario: %s\n' "${hcu_forward[0]}" >&2; exit 2
    fi
    if ! hcu_list_has "$HCU_ALL_OPERATIONS" "${hcu_forward[1]:-}"; then
        printf 'hcu-cluster-run: unknown operation: %s\n' "${hcu_forward[1]:-}" >&2; exit 2
    fi
fi

# idle-check observes and stops containers from the host; never enter its targets.
if [[ ${hcu_forward[1]:-} == idle-check ]]; then
    exec bash "$ROOT/hcu_envcheck/idle_check.sh" "${hcu_forward[@]}"
fi

# The Bash controller owns all high-scale operations.  Python is selected only
# after this dispatch, so the login node does not need Python for these paths.
# HCU_GROUP_WORKER=1 keeps the escape hatch to the legacy Python path; the old
# group_runner.sh supervisor it once described had no production caller and was
# retired by T31 (controller.sh grouping/dispatch is the accepted replacement).
if (( ${#hcu_forward[@]} >= 2 )) && [[ ${HCU_GROUP_WORKER:-0} != 1 ]]; then
    if hcu_list_has "$HCU_LIFECYCLE_OPS" "${hcu_forward[1]}"; then
        exec bash "$ROOT/hcu_envcheck/lifecycle.sh" "${hcu_forward[@]}"
    elif hcu_list_has "$HCU_CONTROLLER_OPS" "${hcu_forward[1]}"; then
        # Keep the existing internal stage/report identifier and payload path.
        [[ ${hcu_forward[1]} != checkdeepep ]] || hcu_forward[1]=check_deep
        exec bash "$ROOT/cluster_run/controller.sh" "${hcu_forward[@]}"
    fi
fi

# Environment acceptance commands have a Bash controller and do not use the
# controller Python/Conda environment. The former worker-op route (model
# placeholder) is retired by T35; unknown or retired operations were already
# rejected above before any dispatch.

if [[ -n "$hcu_controller_env" ]]; then
    if [[ "$hcu_controller_env" != /* || ! -f "$hcu_controller_env" || ! -r "$hcu_controller_env" ]]; then
        printf 'hcu-cluster-run: --controller-env-script requires a readable absolute entry-node path: %s\n' "$hcu_controller_env" >&2
        exit 3
    fi
    # module/Conda scripts often read unset optional variables. Fail a genuine
    # setup command error, but do not impose nounset on third-party setup code.
    set +u
    set -Ee
    trap 'hcu_bootstrap_rc=$?; printf "hcu-cluster-run: controller environment setup failed (rc=%s): %s\n" "$hcu_bootstrap_rc" "$hcu_controller_env" >&2; exit "$hcu_bootstrap_rc"' ERR
    source "$hcu_controller_env"
    trap - ERR
    set +Ee
    set -u
fi

hcu_python=${hcu_controller_python:-${HCU_ENVCHECK_PYTHON:-${HCU_ENVCHECK_CONSOLE_PYTHON:-}}}
if [[ -z "$hcu_python" ]]; then
    for hcu_candidate in python3 python3.14 python3.13 python3.12 python3.11 python3.10 python; do
        if command -v "$hcu_candidate" >/dev/null 2>&1 &&
           "$hcu_candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
            hcu_python=$hcu_candidate
            break
        fi
    done
fi
if [[ -z "$hcu_python" ]] || ! "$hcu_python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
    printf 'hcu-cluster-run: entry node requires Python >=3.10 (stdlib only); use --controller-env-script /absolute/controller-env.sh or --controller-python /absolute/python3 (selected: %s)\n' "${hcu_python:-none}" >&2
    exit 69
fi
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
exec "$hcu_python" -m cluster_run "${hcu_forward[@]}"
