#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# DeepEP 调用链离线契约检查（无 SSH、无真实载荷）：
#   1. controller --dry-run：单节点/两节点/多组、slots 与 group-size 显式冲突、
#      不足整组、非法正整数；
#   2. stub mpirun/torchrun 验证构造命令：组内每节点恰好一次
#      torchrun --nproc-per-node=1 --nnodes=<组内节点数>，master 为组首节点，
#      不出现重复八卡启动（--nproc-per-node=8 / --nnodes=8 / 8 个 rank）；
#   3. 第三方 third-party/DeepEP/tests/run_test.sh 保持原样未被修改。
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN=$ROOT/bin/hcu-cluster-run
DEEPEP=$ROOT/hcu_envcheck/payloads/deepep_test.sh
WORK=$(mktemp -d)
export TORCH_STUB_LOG=$WORK/torchrun.log MPI_STUB_LOG=$WORK/mpirun.log
trap 'rm -rf "$WORK"' EXIT
fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }
ok() { printf '[PASS] %s\n' "$*"; }

mkdir -p "$WORK/stub"
cat > "$WORK/stub/mpirun" <<'STUB'
#!/usr/bin/env bash
log=$MPI_STUB_LOG
np=1 hostfile=''
args=("$@")
rank_script=${args[${#args[@]}-1]}
prev=''
for a in "$@"; do
    case $prev in -np) np=$a ;; --hostfile) hostfile=$a ;; esac
    prev=$a
done
{ printf 'ARGV: %s\n' "$*"
  if [[ -n $hostfile ]]; then printf 'HOSTFILE:\n'; cat "$hostfile"; fi
} >> "$log"
for ((rank=0; rank<np; rank++)); do
    OMPI_COMM_WORLD_SIZE=$np OMPI_COMM_WORLD_RANK=$rank bash "$rank_script" >> "$log" 2>&1
done
STUB
cat > "$WORK/stub/torchrun" <<'STUB'
#!/usr/bin/env bash
printf 'TORCHRUN: %s | HIP_VISIBLE_DEVICES=%s\n' "$*" "${HIP_VISIBLE_DEVICES:-}" >> "$TORCH_STUB_LOG"
exit 0
STUB
for name in python3 python; do
    cp "$WORK/stub/torchrun" "$WORK/stub/$name"
    sed -i 's/TORCHRUN:/PY:/' "$WORK/stub/$name"
done
chmod +x "$WORK/stub/"*
: > "$WORK/env.sh"
printf 'dn001\ndn002\ndn003\ndn004\ndn005\ndn006\n' > "$WORK/hostfile6"
printf 'dn001\ndn002\n' > "$WORK/hostfile2"
printf 'dn001\n' > "$WORK/hostfile1"

dry() { bash "$RUN" per-node-container deepep -f "$1" --container c -i img "${@:2}" --dry-run 2>&1; }

# --- 1. controller --dry-run 契约 ---
out=$(dry "$WORK/hostfile1" --slots 1)
[[ $out == *"nodes=1 groups=1 group-size=1 slots=8"* && $out == *"intranode"* ]] || fail "单节点计划: $out"
ok "单节点 --slots 1 -> 1 组 intranode（slots=8 为每节点卡数）"

out=$(dry "$WORK/hostfile2" --slots 2)
[[ $out == *"nodes=2 groups=1 group-size=2 slots=8"* && $out == *"lowl"* && $out == *"norm"* ]] || fail "两节点计划: $out"
ok "两节点 --slots 2 -> 1 组 lowl/norm"

out=$(dry "$WORK/hostfile6" --slots 2)
[[ $out == *"nodes=6 groups=3 group-size=2 slots=8"* ]] || fail "多组计划: $out"
ok "六节点 --slots 2 -> 3 组"

out=$(dry "$WORK/hostfile2" --group-size 2 --slots 2)
[[ $out == *"groups=1 group-size=2 slots=8"* ]] || fail "别名一致: $out"
ok "group-size 与 slots 一致时通过"

if out=$(dry "$WORK/hostfile2" --group-size 2 --slots 8); then fail "冲突参数应报错"; fi
[[ $out == *"must agree"* ]] || fail "冲突参数报错文案: $out"
ok "slots=8 与 group-size=2 显式冲突被拒绝"

if out=$(dry "$WORK/hostfile6" --slots 4); then fail "不足整组应报错"; fi
[[ $out == *"cannot form complete groups"* ]] || fail "整组校验文案: $out"
ok "6 节点 --slots 4 无法完整分组被拒绝"

for bad in 0 x -1 2.5; do
    if out=$(dry "$WORK/hostfile2" --slots "$bad"); then fail "非法 slots=$bad 应报错"; fi
    [[ $out == *"positive integers"* ]] || fail "slots=$bad 报错文案: $out"
done
ok "slots 非正整数（0/x/-1/2.5）全部被拒绝"

if out=$(bash "$RUN" per-node-container rccl -f "$WORK/hostfile6" --container c -i img --group-size 2 --slots 8 --dry-run 2>&1); then
    [[ $out == *"groups=3 group-size=2 slots=8"* ]] || fail "RCCL 计划: $out"
    ok "RCCL --group-size 2 --slots 8 语义不受影响"
else
    fail "RCCL dry-run 失败: $out"
fi

# --- 2. 构造命令验证（stub mpirun/torchrun，不跑真实载荷）---
if out=$(PATH="$WORK/stub:$PATH" bash "$DEEPEP" group "$WORK/hostfile1" lowl 12345 22 "$WORK/env.sh" 2>&1); then
    fail "单节点 hostfile 走 group 模式应报错"
fi
[[ $out == *"at least two nodes"* ]] || fail "节点数不足文案: $out"
ok "group 模式节点数不足（1 节点）被拒绝"

PATH="$WORK/stub:$PATH" bash "$DEEPEP" group "$WORK/hostfile2" lowl 23456 22 "$WORK/env.sh" >/dev/null 2>&1
lines=$(grep -c '^TORCHRUN:' "$TORCH_STUB_LOG") && [[ $lines -eq 2 ]] || fail "应恰好 2 次 torchrun（每节点一次），实际 $lines"
grep -q -- '--nproc-per-node=1 --nnodes=2 --node-rank=0 --master-addr=dn001 --master-port=23456' "$TORCH_STUB_LOG" \
    || fail "rank0 命令: $(cat "$TORCH_STUB_LOG")"
grep -q -- '--nproc-per-node=1 --nnodes=2 --node-rank=1 --master-addr=dn001 --master-port=23456' "$TORCH_STUB_LOG" \
    || fail "rank1 命令: $(cat "$TORCH_STUB_LOG")"
grep -q 'HIP_VISIBLE_DEVICES=0,1,2,3,4,5,6,7' "$TORCH_STUB_LOG" || fail "八卡列表未传递"
! grep -Eq -- '--nproc-per-node=8|--nnodes=8|node-rank=8' "$TORCH_STUB_LOG" || fail "出现重复八卡启动"
grep -q 'dn001 slots=1' "$MPI_STUB_LOG" && grep -q 'dn002 slots=1' "$MPI_STUB_LOG" \
    || fail "MPI hostfile 应为每节点 slots=1: $(cat "$MPI_STUB_LOG")"
grep -q -- '-np 2' "$MPI_STUB_LOG" || fail "mpirun -np 应为节点数 2"
ok "两节点构造命令：每节点一次 torchrun nproc=1 nnodes=2，MPI hostfile slots=1，无重复八卡"

: > "$TORCH_STUB_LOG"
PATH="$WORK/stub:$PATH" bash "$DEEPEP" intranode >/dev/null 2>&1
pylines=$(grep -c '^PY:' "$TORCH_STUB_LOG") && [[ $pylines -eq 1 ]] || fail "intranode 应只启动一次 python，实际 $pylines"
grep -q '^PY: ./test_intranode.py' "$TORCH_STUB_LOG" || fail "intranode 入口: $(cat "$TORCH_STUB_LOG")"
ok "intranode 单进程直接运行 test_intranode.py"

# --- 3. 第三方原始文件未修改 ---
UPSTREAM=$ROOT/third-party/DeepEP/tests/run_test.sh
grep -q -- '--master-addr="10.16.1.37"' "$UPSTREAM" || fail "第三方 run_test.sh 上游字面 master 地址丢失（疑似被改动）"
! grep -q 'MASTER_ADDR' "$UPSTREAM" || fail "第三方 run_test.sh 被注入环境变量逻辑"
execs=$(grep -rn 'bash.*tests/run_test\.sh' "$ROOT/hcu_envcheck" 2>/dev/null | grep -cv '^[^:]*:[0-9]*:#' || true)
[[ ${execs:-0} -eq 0 ]] || fail "工程脚本仍存在执行第三方 run_test.sh 的引用"
ok "third-party/DeepEP/tests/run_test.sh 未被修改，工程内仅引用 run_deepep_test.sh"

printf '%s\n' 'deepep contract check passed.'
