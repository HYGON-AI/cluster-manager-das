#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

TRAIN_YAML="${1:-${TRAIN_YAML:-}}"
NAMESPACE="${FT_NAMESPACE:-hygon-ft}"
KEEP_SELFTEST_EVENT="${KEEP_SELFTEST_EVENT:-false}"

fail() {
  echo "[verify-volcano] ERROR: $*" >&2
  exit 1
}

[[ -n "$TRAIN_YAML" ]] || fail "usage: bash scripts/verify_volcano.sh /path/to/train.yaml"
[[ -f "$TRAIN_YAML" ]] || fail "train yaml not found: $TRAIN_YAML"

kubectl get crd jobs.batch.volcano.sh >/dev/null 2>&1 \
  || fail "Volcano is not installed: CRD jobs.batch.volcano.sh not found"

echo "[verify-volcano] checking rollout"
kubectl -n "$NAMESPACE" rollout status deployment/ft-operator --timeout=60s
kubectl -n "$NAMESPACE" rollout status deployment/ft-webhook --timeout=60s
kubectl -n "$NAMESPACE" rollout status daemonset/nodehealth-agent --timeout=60s

kubectl get mutatingwebhookconfiguration hygon-ft-pytorchjob-mutator >/dev/null
echo "[verify-volcano] checking Volcano admission mutation with server-side dry-run"
mutated_yaml="$(kubectl apply --dry-run=server -o yaml -f "$TRAIN_YAML")"
grep -q 'ft.hygon.io/injected:' <<<"$mutated_yaml" || fail "Volcano Job was not marked as injected"
grep -q 'ft-launcher' <<<"$mutated_yaml" || fail "Volcano Job command was not wrapped by ft-launcher"

endpoint_count="$(kubectl -n "$NAMESPACE" get endpoints ft-operator -o jsonpath='{.subsets[*].addresses[*].ip}' | wc -w)"
[[ "$endpoint_count" -ge 1 ]] || fail "ft-operator service has no ready endpoint"

leader="$(kubectl -n "$NAMESPACE" get lease ft-operator-leader -o jsonpath='{.spec.holderIdentity}')"
[[ -n "$leader" ]] || fail "ft-operator leader lease has no holder"

grep -q 'ft.hygon.io/enabled:' "$TRAIN_YAML" || fail "train yaml has no enabled fault-tolerance annotation"
labelled_pods="$(kubectl get pods -A -l ft.hygon.io/enabled=true --no-headers 2>/dev/null | wc -l)"
if [[ "$labelled_pods" -eq 0 ]]; then
  echo "[verify-volcano] no active training pod; validating the configured train yaml and control plane only"
fi

operator_pod="$(kubectl -n "$NAMESPACE" get pods -l app=ft-operator -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$operator_pod" ]] || fail "ft-operator pod not found"

echo "[verify-volcano] reporting a non-destructive self-test FaultEvent"
event_name="$({ kubectl -n "$NAMESPACE" exec -i "$operator_pod" -- python -; } <<'PY'
import json
import urllib.request

payload = {
    "type": "DeploymentSelfTest",
    "severity": "Info",
    "source": "deployment-self-test",
    "reason": "verify_volcano",
    "message": "non-destructive hygon-ft deployment verification",
    "action": {"taintNode": False, "deletePod": False, "deletePods": False},
}
request = urllib.request.Request(
    "http://127.0.0.1:8080/report",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    print(json.loads(response.read().decode("utf-8"))["faultEvent"])
PY
)"
event_name="$(printf '%s\n' "$event_name" | tail -n 1 | tr -d '\r')"
[[ -n "$event_name" ]] || fail "self-test reporter did not return a FaultEvent name"

processed=""
for _ in $(seq 1 20); do
  processed="$(kubectl -n "$NAMESPACE" get faultevent "$event_name" -o jsonpath='{.status.processed}' 2>/dev/null || true)"
  [[ "$processed" == "true" ]] && break
  sleep 1
done
[[ "$processed" == "true" ]] || fail "self-test FaultEvent was not processed: $event_name"

ack_status="$({ kubectl -n "$NAMESPACE" exec -i "$operator_pod" -- python - "$event_name"; } <<'PY'
import json
import sys
import urllib.parse
import urllib.request

event_name = sys.argv[1]
url = "http://127.0.0.1:8080/report/" + urllib.parse.quote(event_name, safe="")
with urllib.request.urlopen(url, timeout=10) as response:
    status = json.loads(response.read().decode("utf-8"))
print(json.dumps({
    "processed": status.get("processed"),
    "readyToRestart": status.get("readyToRestart"),
}))
PY
)"
ack_status="$(printf '%s\n' "$ack_status" | tail -n 1 | tr -d '\r')"
grep -q '"processed": true' <<<"$ack_status" || fail "report acknowledgement is not processed: $ack_status"
grep -q '"readyToRestart": true' <<<"$ack_status" || fail "report acknowledgement is not restart-ready: $ack_status"

action_type="$(kubectl -n "$NAMESPACE" get faultevent "$event_name" -o jsonpath='{.status.actions[0].type}')"
[[ "$action_type" == "Skip" ]] || fail "self-test expected Skip action, got: $action_type"

if [[ "$KEEP_SELFTEST_EVENT" != "true" ]]; then
  kubectl -n "$NAMESPACE" delete faultevent "$event_name" --wait=false >/dev/null
fi

echo "[verify-volcano] PASS"
echo "[verify-volcano] leader: $leader"
echo "[verify-volcano] service endpoints: $endpoint_count"
echo "[verify-volcano] labelled training pods: $labelled_pods"
