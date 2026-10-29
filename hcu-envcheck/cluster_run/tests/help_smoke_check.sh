#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

"$ROOT/bin/hcu-cluster-run" --help >/dev/null
for script in \
    "$ROOT/hcu_envcheck/payloads/check_deepep_env.sh" \
    "$ROOT/hcu_envcheck/payloads/dcu_idle_check.sh" \
    "$ROOT/hcu_envcheck/payloads/rccl_perf_test.sh"; do
    bash "$script" --help >/dev/null
done

printf '%s\n' 'cluster_run help smoke test passed.'
