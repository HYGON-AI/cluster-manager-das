#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Per-node sweep of the flux 70B shape matrix, mirroring the upstream
# model_tests/inference/run_test_ds_r1_llama-70b-tp4.sh: one single-node TP4
# launch_tp4.sh run per case, test_ag_kernel.py for all hidden sizes and
# test_gemm_rs.py for the GemmRS shapes. Emits "flux-70b case <name>
# start|PASS|FAIL" markers so results stay greppable in node logs.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/hcu_envcheck/logging.sh"
TEST_DIR=${HCU_FLUX_TEST_DIR:-$ROOT/third-party/flux/model_tests/training}
[[ -r $TEST_DIR/../../scripts/launch_tp4.sh && -r $TEST_DIR/../../test/python/test_ag_kernel.py \
   && -r $TEST_DIR/../../test/python/test_gemm_rs.py ]] || {
    printf 'byte-flux 70B launch script or test files missing under %s; initialize third-party/flux\n' "$TEST_DIR" >&2
    exit 69
}
command -v torchrun >/dev/null || { printf 'torchrun missing in selected environment\n' >&2; exit 69; }
cd "$TEST_DIR"
export GPU_MAX_HW_QUEUES=10 GLOG_minloglevel=3
export HIP_VISIBLE_DEVICES=${HIP_VISIBLE_DEVICES:-0,1,2,3,4,5,6,7}

# Hidden sizes exercised by the upstream 70B sweep.
SIZES=(2048 2176 2304 2560 3072 4096 4160 4352 4608 5120 6144 8192 16384 32768 65536 131072)
printf 'flux-70b sweep start cases=%s\n' "$(( ${#SIZES[@]} * 4 ))"
failed=0
case_index=0
run_case() {
    local name=$1
    shift
    case_index=$((case_index + 1))
    local order started=$SECONDS
    printf -v order '%02d/64' "$case_index"
    hcu_log_start "$order" byte-flux "$name" "${HCU_LOG_SCENARIO:-目标环境} / ${HCU_LOG_CONTAINER:--}" \
        "${HCU_LOG_NODES:-$(hostname -s)}" "$(hostname -s)" '单机 TP4'
    printf 'flux-70b case %s start\n' "$name"
    hcu_log_command TEST-COMMAND "$(hostname -s)" bash ../../scripts/launch_tp4.sh "$@"
    hcu_log_raw '测试程序输出'
    # launch_tp4.sh can carry CRLF from a Windows checkout; stream it through
    # sed instead of touching the shared file.
    local rc=0
    bash <(sed 's/\r$//' ../../scripts/launch_tp4.sh) "$@" || rc=$?
    if (( rc != 0 )); then
        # A lingering rendezvous socket from the previous case can leave the
        # fixed port 23456 briefly unbindable; one retry absorbs that.
        printf 'flux-70b case %s retry\n' "$name" >&2
        sleep 5
        local retried=0
        bash <(sed 's/\r$//' ../../scripts/launch_tp4.sh) "$@" || retried=$?
        rc=$retried
    fi
    if (( rc == 0 )); then
        printf 'flux-70b case %s PASS\n' "$name"
    else
        printf 'flux-70b case %s FAIL rc=%s\n' "$name" "$rc" >&2
        failed=$rc
    fi
    local status=PASS; (( rc == 0 )) || status=FAIL
    hcu_log_end "$order" byte-flux "$name" "$status" "$rc" "$((SECONDS - started))" PENDING
}
for m in "${SIZES[@]}"; do
    run_case "ag-${m}x10240" ../../test/python/test_ag_kernel.py \
        "$m" 10240 8192 --dtype=float16 --iters=50 --warmup=5 --transpose_weight
    run_case "ag-${m}x57344" ../../test/python/test_ag_kernel.py \
        "$m" 57344 8192 --dtype=float16 --iters=50 --warmup=5 --transpose_weight
done
for m in "${SIZES[@]}"; do
    run_case "gemmrs-${m}x8192x8192" ../../test/python/test_gemm_rs.py \
        "$m" 8192 8192 --dtype=float16 --iters=50 --warmup=5 --transpose_weight
    run_case "gemmrs-${m}x8192x28672" ../../test/python/test_gemm_rs.py \
        "$m" 8192 28672 --dtype=float16 --iters=50 --warmup=5 --transpose_weight
done
if (( failed == 0 )); then
    printf 'flux-70b sweep PASS\n'
else
    printf 'flux-70b sweep FAIL\n' >&2
fi
exit "$failed"
