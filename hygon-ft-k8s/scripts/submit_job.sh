#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

NAMESPACE="${1:-${NAMESPACE:-default}}"
JOB_FILE="${2:-${TRAIN_YAML:-}}"

fail() {
  echo "[submit-job] ERROR: $*" >&2
  exit 1
}

if [[ -z "$JOB_FILE" ]]; then
  echo "usage: bash scripts/submit_job.sh <namespace> <train-yaml>" >&2
  echo "   or: NAMESPACE=default TRAIN_YAML=/path/to/train.yaml bash scripts/submit_job.sh" >&2
  exit 1
fi

if [[ ! -f "$JOB_FILE" ]]; then
  fail "train yaml not found: $JOB_FILE"
fi

kubectl -n "$NAMESPACE" apply -f "$JOB_FILE"

echo
echo "Submitted job yaml: $JOB_FILE"
echo "Namespace: $NAMESPACE"
echo
echo "Watch FT-enabled Pods:"
echo "kubectl -n $NAMESPACE get pods -l ft.hygon.io/enabled=true -o wide -w"
