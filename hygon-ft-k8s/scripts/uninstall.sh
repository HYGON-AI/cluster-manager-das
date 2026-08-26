#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -uo pipefail

NAMESPACE="${HYGON_FT_NAMESPACE:-hygon-ft}"
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-300}"
SYSTEM_LABEL="node-role.hygon.io/system"
TRAINING_LABEL="node-role.hygon.io/training"
ACCELERATOR_LABEL="accelerator.hygon.io/enabled"
SYSTEM_TAINT="node-role.hygon.io/system"
UNHEALTHY_TAINT="ft.hygon.io/node-unhealthy"
REMOVE_ACCELERATOR_LABEL="${REMOVE_ACCELERATOR_LABEL:-true}"
FAILURES=0

info() {
  printf '[uninstall] %s\n' "$*"
}

warn() {
  printf '[uninstall] WARN: %s\n' "$*" >&2
}

fail() {
  printf '[uninstall] ERROR: %s\n' "$*" >&2
  exit 1
}

run_kubectl() {
  info "kubectl $*"
  if ! kubectl "$@"; then
    warn "command failed: kubectl $*"
    FAILURES=$((FAILURES + 1))
  fi
}

append_nonempty_lines() {
  local array_name="$1"
  local content="$2"
  local -n output_array="$array_name"
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    output_array+=("$line")
  done <<<"$content"
}

collect_labeled_nodes() {
  local selector="$1"
  local output
  if ! output="$(kubectl get nodes -l "$selector" -o name)"; then
    warn "cannot list nodes matching label selector: $selector"
    return 1
  fi
  printf '%s\n' "$output"
}

collect_taint_rows() {
  kubectl get nodes -o go-template='{{range .items}}{{$node := .metadata.name}}{{range .spec.taints}}{{$node}}{{"\t"}}{{.key}}{{"\n"}}{{end}}{{end}}'
}

[[ "${CONFIRM_HYGON_FT_CLEANUP:-}" == "yes" ]] ||
  fail "refusing destructive cleanup; run via 'bash start.sh cleanup' or set CONFIRM_HYGON_FT_CLEANUP=yes"
[[ "$CLEANUP_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
  fail "CLEANUP_TIMEOUT_SECONDS must be a positive integer"
[[ "$REMOVE_ACCELERATOR_LABEL" == "true" || "$REMOVE_ACCELERATOR_LABEL" == "false" ]] ||
  fail "REMOVE_ACCELERATOR_LABEL must be true or false"
command -v kubectl >/dev/null 2>&1 || fail "required command not found: kubectl"
command -v awk >/dev/null 2>&1 || fail "required command not found: awk"

info "Kubernetes context: $(kubectl config current-context)"
info "discovering Hygon FT node labels and taints before deleting controllers"

declare -a system_label_nodes=()
declare -a training_label_nodes=()
declare -a accelerator_label_nodes=()
declare -a system_taint_nodes=()
declare -a unhealthy_taint_nodes=()

if ! system_label_output="$(collect_labeled_nodes "${SYSTEM_LABEL}=true")"; then
  system_label_output=""
  FAILURES=$((FAILURES + 1))
fi
if ! training_label_output="$(collect_labeled_nodes "${TRAINING_LABEL}=true")"; then
  training_label_output=""
  FAILURES=$((FAILURES + 1))
fi
append_nonempty_lines system_label_nodes "$system_label_output"
append_nonempty_lines training_label_nodes "$training_label_output"

if [[ "$REMOVE_ACCELERATOR_LABEL" == "true" ]]; then
  if ! accelerator_label_output="$(
    collect_labeled_nodes "${TRAINING_LABEL}=true,${ACCELERATOR_LABEL}=true"
  )"; then
    accelerator_label_output=""
    FAILURES=$((FAILURES + 1))
  fi
  append_nonempty_lines accelerator_label_nodes "$accelerator_label_output"
fi

if taint_rows="$(collect_taint_rows)"; then
  system_taint_output="$(awk -v key="$SYSTEM_TAINT" '$2 == key {print $1}' <<<"$taint_rows")"
  unhealthy_taint_output="$(awk -v key="$UNHEALTHY_TAINT" '$2 == key {print $1}' <<<"$taint_rows")"
  append_nonempty_lines system_taint_nodes "$system_taint_output"
  append_nonempty_lines unhealthy_taint_nodes "$unhealthy_taint_output"
else
  warn "cannot list node taints"
  FAILURES=$((FAILURES + 1))
fi

info "disabling admission mutation and stopping Hygon FT workloads"
run_kubectl delete mutatingwebhookconfiguration hygon-ft-pytorchjob-mutator \
  --ignore-not-found=true
run_kubectl -n "$NAMESPACE" delete deployment ft-operator ft-webhook \
  --ignore-not-found=true --wait=false
run_kubectl -n "$NAMESPACE" delete daemonset nodehealth-agent \
  --ignore-not-found=true --wait=false

info "deleting namespace and every namespaced Hygon FT resource"
run_kubectl delete namespace "$NAMESPACE" --ignore-not-found=true --wait=false

info "deleting cluster-scoped Hygon FT resources"
run_kubectl delete clusterrolebinding \
  hygon-ft-operator hygon-ft-nodehealth --ignore-not-found=true
run_kubectl delete clusterrole \
  hygon-ft-operator hygon-ft-nodehealth --ignore-not-found=true
run_kubectl delete customresourcedefinition \
  faultevents.ft.hygon.io --ignore-not-found=true

info "removing node taints installed or managed by Hygon FT"
for node in "${system_taint_nodes[@]}"; do
  run_kubectl taint node "${node#node/}" "${SYSTEM_TAINT}-"
done
for node in "${unhealthy_taint_nodes[@]}"; do
  run_kubectl taint node "${node#node/}" "${UNHEALTHY_TAINT}-"
done

info "removing node labels installed by Hygon FT"
for node in "${system_label_nodes[@]}"; do
  run_kubectl label "$node" "${SYSTEM_LABEL}-"
done
for node in "${training_label_nodes[@]}"; do
  run_kubectl label "$node" "${TRAINING_LABEL}-"
done
if [[ "$REMOVE_ACCELERATOR_LABEL" == "true" ]]; then
  info "removing accelerator label from Hygon FT training nodes"
  for node in "${accelerator_label_nodes[@]}"; do
    run_kubectl label "$node" "${ACCELERATOR_LABEL}-"
  done
else
  info "preserving accelerator label because REMOVE_ACCELERATOR_LABEL=false"
fi

if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  info "waiting for namespace deletion"
  run_kubectl wait --for=delete "namespace/$NAMESPACE" \
    --timeout="${CLEANUP_TIMEOUT_SECONDS}s"
fi

if (( FAILURES > 0 )); then
  fail "cleanup finished with $FAILURES failed operation(s); inspect the warnings above"
fi

info "cleanup completed"
info "preserved external log directory and user-submitted training Job/Pod resources"
