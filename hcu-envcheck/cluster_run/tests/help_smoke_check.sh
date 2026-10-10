#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

"$ROOT/bin/hcu-cluster-run" --help >/dev/null
for script in \
    "$ROOT/cluster_run/payloads/check_deepep_env.sh" \
    "$ROOT/cluster_run/payloads/dcu_idle_check.sh" \
    "$ROOT/cluster_run/payloads/gemm_perf_test.sh" \
    "$ROOT/cluster_run/payloads/rccl_perf_test.sh"; do
    bash "$script" --help >/dev/null
done

printf '%s\n' 'cluster_run help smoke test passed.'