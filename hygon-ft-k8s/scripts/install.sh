#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="hygon-ft"
TRAIN_YAML_PATH="${TRAIN_YAML:-${1:-}}"
CA_SECRET_NAME="${FT_WEBHOOK_CA_SECRET_NAME:-ft-webhook-ca}"
TLS_SECRET_NAME="${FT_WEBHOOK_TLS_SECRET_NAME:-ft-webhook-tls}"
WEBHOOK_CONFIG_NAME="hygon-ft-pytorchjob-mutator"
CERT_RENEW_BEFORE_SECONDS="${FT_WEBHOOK_CERT_RENEW_BEFORE_SECONDS:-2592000}"
FT_CONTROLLER_IMAGE="${FT_CONTROLLER_IMAGE:-${RUNTIME_IMAGE:-hygon/ft-controller:latest}}"
SYSTEM_NODES="${SYSTEM_NODES:-}"
TRAINING_NODES="${TRAINING_NODES:-}"
APPLY_SYSTEM_TAINT="${APPLY_SYSTEM_TAINT:-true}"
TRAINING_OPERATOR_NAMESPACE="${TRAINING_OPERATOR_NAMESPACE:-kubeflow}"
TRAINING_OPERATOR_DEPLOYMENT="${TRAINING_OPERATOR_DEPLOYMENT:-training-operator}"
AUTO_CONFIGURE_TRAINING_OPERATOR_GANG_SCHEDULER="${AUTO_CONFIGURE_TRAINING_OPERATOR_GANG_SCHEDULER:-true}"

SYSTEM_NODES="${SYSTEM_NODES//,/ }"
TRAINING_NODES="${TRAINING_NODES//,/ }"

fail() {
  echo "[install] ERROR: $*" >&2
  exit 1
}

ensure_training_operator_gang_scheduler() {
  local operator_args patch
  operator_args="$(
    kubectl -n "$TRAINING_OPERATOR_NAMESPACE" \
      get deployment "$TRAINING_OPERATOR_DEPLOYMENT" \
      -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null
  )" || fail "Training Operator deployment not found: $TRAINING_OPERATOR_NAMESPACE/$TRAINING_OPERATOR_DEPLOYMENT"

  if [[ "$operator_args" == *"gang-scheduler-name=volcano"* ]]; then
    return 0
  fi
  [[ "$AUTO_CONFIGURE_TRAINING_OPERATOR_GANG_SCHEDULER" == "true" ]] \
    || fail "Training Operator must start with --gang-scheduler-name=volcano"

  if [[ -z "$operator_args" || "$operator_args" == "null" ]]; then
    patch='[{"op":"add","path":"/spec/template/spec/containers/0/args","value":["--gang-scheduler-name=volcano"]}]'
  else
    patch='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--gang-scheduler-name=volcano"}]'
  fi

  echo "[install] configuring Training Operator with Volcano gang scheduler"
  kubectl -n "$TRAINING_OPERATOR_NAMESPACE" \
    patch deployment "$TRAINING_OPERATOR_DEPLOYMENT" --type=json -p "$patch"
  kubectl -n "$TRAINING_OPERATOR_NAMESPACE" \
    rollout status deployment/"$TRAINING_OPERATOR_DEPLOYMENT" --timeout=180s

  operator_args="$(
    kubectl -n "$TRAINING_OPERATOR_NAMESPACE" \
      get deployment "$TRAINING_OPERATOR_DEPLOYMENT" \
      -o jsonpath='{.spec.template.spec.containers[0].args}'
  )"
  [[ "$operator_args" == *"gang-scheduler-name=volcano"* ]] \
    || fail "Training Operator gang scheduler configuration did not take effect"
}

for command in kubectl openssl base64 grep sed mktemp tr; do
  command -v "$command" >/dev/null 2>&1 || fail "required command not found: $command"
done

[[ -n "$FT_CONTROLLER_IMAGE" ]] || fail "FT_CONTROLLER_IMAGE is empty"

TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

validate_workload_prerequisite() {
  [[ -n "$TRAIN_YAML_PATH" ]] || return 0
  [[ -f "$TRAIN_YAML_PATH" ]] || fail "train yaml not found: $TRAIN_YAML_PATH"
  grep -q "ft.hygon.io/enabled" "$TRAIN_YAML_PATH" \
    || fail "train yaml has no ft.hygon.io/enabled label or annotation"

  if grep -q "apiVersion: batch.volcano.sh/" "$TRAIN_YAML_PATH"; then
    kubectl get crd jobs.batch.volcano.sh >/dev/null 2>&1 \
      || fail "Volcano is not installed: CRD jobs.batch.volcano.sh not found"
    kubectl get --raw /apis/batch.volcano.sh/v1alpha1 >/dev/null 2>&1 \
      || fail "Volcano API batch.volcano.sh/v1alpha1 is unavailable"
    grep -q "schedulerName:[[:space:]]*volcano" "$TRAIN_YAML_PATH" \
      || fail "Volcano Job must set schedulerName: volcano"
    grep -q "PodFailed" "$TRAIN_YAML_PATH" \
      || fail "Volcano Job must define a PodFailed policy"
    grep -q "RestartJob" "$TRAIN_YAML_PATH" \
      || fail "Volcano Job must define PodFailed -> RestartJob"
    echo "[install] external Volcano prerequisite is ready"
  elif grep -q "apiVersion: kubeflow.org/" "$TRAIN_YAML_PATH" \
      && grep -q "kind:[[:space:]]*PyTorchJob" "$TRAIN_YAML_PATH"; then
    kubectl get crd pytorchjobs.kubeflow.org >/dev/null 2>&1 \
      || fail "Kubeflow Training Operator is not installed: CRD pytorchjobs.kubeflow.org not found"
    kubectl get --raw /apis/kubeflow.org/v1 >/dev/null 2>&1 \
      || fail "Kubeflow API kubeflow.org/v1 is unavailable"
    grep -q "schedulingPolicy:" "$TRAIN_YAML_PATH" \
      && grep -q "minAvailable:" "$TRAIN_YAML_PATH" \
      || fail "PyTorchJob must set spec.runPolicy.schedulingPolicy.minAvailable for gang scheduling"
    kubectl get crd podgroups.scheduling.volcano.sh >/dev/null 2>&1 \
      || fail "Volcano PodGroup CRD is required for PyTorchJob gang scheduling"
    ensure_training_operator_gang_scheduler
    echo "[install] external Kubeflow Training Operator and Volcano gang scheduling are ready"
  else
    fail "unsupported training workload in $TRAIN_YAML_PATH; expected Volcano Job or PyTorchJob"
  fi
}

configure_node_roles() {
  local node
  for node in $SYSTEM_NODES; do
    kubectl label node "$node" node-role.hygon.io/system=true --overwrite
    if [[ "$APPLY_SYSTEM_TAINT" == "true" ]]; then
      kubectl taint node "$node" node-role.hygon.io/system=true:NoSchedule --overwrite
    fi
  done

  for node in $TRAINING_NODES; do
    kubectl label node "$node" node-role.hygon.io/training=true --overwrite
    kubectl label node "$node" accelerator.hygon.io/enabled=true --overwrite
  done

  [[ -n "$(kubectl get nodes -l node-role.hygon.io/system=true -o name)" ]] \
    || fail "no system nodes labelled; set SYSTEM_NODES or run scripts/label_nodes_example.sh"
  [[ -n "$(kubectl get nodes -l node-role.hygon.io/training=true -o name)" ]] \
    || fail "no training nodes labelled; set TRAINING_NODES or run scripts/label_nodes_example.sh"
}

render_controller_manifest() {
  local source_file="$1"
  local output_file="$2"
  local escaped_image
  escaped_image="$(printf '%s' "$FT_CONTROLLER_IMAGE" | sed 's/[&|\\]/\\&/g')"
  sed "s|hygon/ft-controller:latest|${escaped_image}|g" "$source_file" > "$output_file"
}

validate_workload_prerequisite
configure_node_roles

OPERATOR_MANIFEST="$TMP_DIR/operator-deployment.yaml"
WEBHOOK_DEPLOYMENT_MANIFEST="$TMP_DIR/webhook-deployment.yaml"
NODEHEALTH_MANIFEST="$TMP_DIR/nodehealth-daemonset.yaml"

render_controller_manifest \
  "$ROOT_DIR/manifests/base/02-operator-deployment.yaml" \
  "$OPERATOR_MANIFEST"
render_controller_manifest \
  "$ROOT_DIR/manifests/base/03-webhook-deployment.yaml" \
  "$WEBHOOK_DEPLOYMENT_MANIFEST"
render_controller_manifest \
  "$ROOT_DIR/manifests/base/06-nodehealth-daemonset.yaml" \
  "$NODEHEALTH_MANIFEST"

kubectl apply --dry-run=client -f "$OPERATOR_MANIFEST" >/dev/null
kubectl apply --dry-run=client -f "$WEBHOOK_DEPLOYMENT_MANIFEST" >/dev/null
kubectl apply --dry-run=client -f "$NODEHEALTH_MANIFEST" >/dev/null

echo "[install] applying FaultEvent CRD"
kubectl apply -f "$ROOT_DIR/manifests/crds/faultevents.yaml"

echo "[install] applying namespace and RBAC"
kubectl apply -f "$ROOT_DIR/manifests/base/00-namespace.yaml"
kubectl apply -f "$ROOT_DIR/manifests/base/01-rbac.yaml"

cat > "$TMP_DIR/ca.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[req_distinguished_name]
CN = ft-webhook.${NAMESPACE}.svc-ca

[v3_ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EOF

cat > "$TMP_DIR/server.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = ft-webhook.${NAMESPACE}.svc

[v3_req]
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ft-webhook
DNS.2 = ft-webhook.${NAMESPACE}
DNS.3 = ft-webhook.${NAMESPACE}.svc
DNS.4 = ft-webhook.${NAMESPACE}.svc.cluster.local
EOF

encode_base64() {
  if base64 --help 2>&1 | grep -q -- "-w"; then
    base64 -w0 "$1"
  else
    base64 "$1" | tr -d '\n'
  fi
}

if kubectl -n "$NAMESPACE" get secret "$CA_SECRET_NAME" >/dev/null 2>&1; then
  echo "[install] reusing webhook CA from secret/$CA_SECRET_NAME"
  kubectl -n "$NAMESPACE" get secret "$CA_SECRET_NAME" \
    -o jsonpath='{.data.ca\.crt}' | base64 -d > "$TMP_DIR/ca.crt"
  kubectl -n "$NAMESPACE" get secret "$CA_SECRET_NAME" \
    -o jsonpath='{.data.ca\.key}' | base64 -d > "$TMP_DIR/ca.key"
  if ! openssl x509 -in "$TMP_DIR/ca.crt" -noout -text | grep -q "CA:TRUE"; then
    echo "[install] secret/$CA_SECRET_NAME does not contain a valid CA certificate" >&2
    exit 1
  fi
  if [[ "$(openssl x509 -in "$TMP_DIR/ca.crt" -noout -modulus)" != \
        "$(openssl rsa -in "$TMP_DIR/ca.key" -noout -modulus 2>/dev/null)" ]]; then
    echo "[install] certificate and key in secret/$CA_SECRET_NAME do not match" >&2
    exit 1
  fi
else
  echo "[install] generating persistent webhook CA"
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$TMP_DIR/ca.key" \
    -out "$TMP_DIR/ca.crt" \
    -days 3650 \
    -config "$TMP_DIR/ca.cnf" >/dev/null 2>&1
  kubectl -n "$NAMESPACE" create secret generic "$CA_SECRET_NAME" \
    --from-file=ca.crt="$TMP_DIR/ca.crt" \
    --from-file=ca.key="$TMP_DIR/ca.key" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

TLS_VALID=false
if kubectl -n "$NAMESPACE" get secret "$TLS_SECRET_NAME" >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" get secret "$TLS_SECRET_NAME" \
    -o jsonpath='{.data.tls\.crt}' | base64 -d > "$TMP_DIR/tls.crt"
  kubectl -n "$NAMESPACE" get secret "$TLS_SECRET_NAME" \
    -o jsonpath='{.data.tls\.key}' | base64 -d > "$TMP_DIR/tls.key"
  if openssl verify -CAfile "$TMP_DIR/ca.crt" "$TMP_DIR/tls.crt" >/dev/null 2>&1 \
      && openssl x509 -in "$TMP_DIR/tls.crt" -noout -checkend "$CERT_RENEW_BEFORE_SECONDS" \
        >/dev/null 2>&1 \
      && openssl x509 -in "$TMP_DIR/tls.crt" -noout -text \
        | grep -q "DNS:ft-webhook.${NAMESPACE}.svc" \
      && [[ "$(openssl x509 -in "$TMP_DIR/tls.crt" -noout -modulus)" == \
            "$(openssl rsa -in "$TMP_DIR/tls.key" -noout -modulus 2>/dev/null)" ]]; then
    TLS_VALID=true
    echo "[install] existing webhook server certificate is valid; skip rotation"
  fi
fi

if [[ "$TLS_VALID" != "true" ]]; then
  # During migration from the legacy self-signed certificate, trust both the
  # currently served certificate and the new persistent CA until rollout ends.
  if kubectl get mutatingwebhookconfiguration "$WEBHOOK_CONFIG_NAME" >/dev/null 2>&1; then
    CURRENT_CA_BUNDLE="$(
      kubectl get mutatingwebhookconfiguration "$WEBHOOK_CONFIG_NAME" \
        -o jsonpath='{.webhooks[0].clientConfig.caBundle}'
    )"
    NEW_CA_BUNDLE="$(encode_base64 "$TMP_DIR/ca.crt")"
    if [[ -n "$CURRENT_CA_BUNDLE" && "$CURRENT_CA_BUNDLE" != "$NEW_CA_BUNDLE" ]] \
        && printf '%s' "$CURRENT_CA_BUNDLE" | base64 -d > "$TMP_DIR/old-ca.crt" 2>/dev/null; then
      cat "$TMP_DIR/old-ca.crt" "$TMP_DIR/ca.crt" > "$TMP_DIR/transition-ca.crt"
      TRANSITION_CA_BUNDLE="$(encode_base64 "$TMP_DIR/transition-ca.crt")"
      sed "s|__CA_BUNDLE__|${TRANSITION_CA_BUNDLE}|g" \
        "$ROOT_DIR/manifests/base/04-mutatingwebhookconfiguration.yaml" \
        > "$TMP_DIR/transition-mutatingwebhookconfiguration.yaml"
      echo "[install] installing transitional webhook CA bundle"
      kubectl apply -f "$TMP_DIR/transition-mutatingwebhookconfiguration.yaml"
    fi
  fi

  echo "[install] generating webhook server certificate"
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$TMP_DIR/tls.key" \
    -out "$TMP_DIR/tls.csr" \
    -config "$TMP_DIR/server.cnf" >/dev/null 2>&1

  openssl x509 -req \
    -in "$TMP_DIR/tls.csr" \
    -CA "$TMP_DIR/ca.crt" \
    -CAkey "$TMP_DIR/ca.key" \
    -CAcreateserial \
    -out "$TMP_DIR/tls.crt" \
    -days 365 \
    -extensions v3_req \
    -extfile "$TMP_DIR/server.cnf" >/dev/null 2>&1

  kubectl -n "$NAMESPACE" create secret tls "$TLS_SECRET_NAME" \
    --cert="$TMP_DIR/tls.crt" \
    --key="$TMP_DIR/tls.key" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

echo "[install] deploying operator and webhook service"
kubectl apply -f "$OPERATOR_MANIFEST"
kubectl apply -f "$WEBHOOK_DEPLOYMENT_MANIFEST"

echo "[install] restarting operator and webhook to pick up controller image updates"
kubectl -n "$NAMESPACE" rollout restart deployment/ft-operator deployment/ft-webhook

echo "[install] waiting for webhook deployment"
kubectl -n "$NAMESPACE" rollout status deployment/ft-webhook --timeout=180s

CA_BUNDLE="$(encode_base64 "$TMP_DIR/ca.crt")"

sed "s|__CA_BUNDLE__|${CA_BUNDLE}|g" "$ROOT_DIR/manifests/base/04-mutatingwebhookconfiguration.yaml" \
  > "$TMP_DIR/mutatingwebhookconfiguration.yaml"

echo "[install] installing MutatingWebhookConfiguration"
kubectl apply -f "$TMP_DIR/mutatingwebhookconfiguration.yaml"

echo "[install] deploying nodehealth DaemonSet"
kubectl apply -f "$ROOT_DIR/manifests/base/05-nodehealth-config.yaml"
kubectl apply -f "$NODEHEALTH_MANIFEST"

echo "[install] restarting nodehealth agent to pick up controller image updates"
kubectl -n "$NAMESPACE" rollout restart daemonset/nodehealth-agent

echo "[install] waiting for operator and nodehealth"
kubectl -n "$NAMESPACE" rollout status deployment/ft-operator --timeout=180s
kubectl -n "$NAMESPACE" rollout status daemonset/nodehealth-agent --timeout=180s

echo "[install] hygon-ft installed"
echo "[install] controller image: $FT_CONTROLLER_IMAGE"
if [[ -n "$TRAIN_YAML_PATH" ]]; then
  echo "[install] validated workload: $TRAIN_YAML_PATH"
fi
kubectl -n "$NAMESPACE" get pods -o wide
