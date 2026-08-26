<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# stack-analyzer 采集与分析操作手册

## 前置条件

- Python 3.10 及以上。
- 采集目标安装并可执行 `py-spy`。
- 本地进程发现依赖 `psutil`。
- Ansible 采集需要控制节点安装 Ansible 并具备目标节点 SSH 权限。
- Kubernetes 采集需要可用的 `kubectl` 上下文和目标 Pod 的 `pods/exec` 权限。
- `py-spy` attach 通常需要 `SYS_PTRACE`，并受 seccomp、Yama、容器用户和宿主机内核策略限制。

默认使用 nonblocking 模式，避免为了抓栈暂停已经异常的训练进程。只有明确接受暂停风险时才使用 `--blocking`。

## Hostfile

Hostfile 按节点顺序映射全局 rank，支持常见 MPI 格式：

```text
node01 slots=8
node02 slots=8
```

节点顺序和 `slots` 必须与训练实际 rank 分配一致，否则异常 rank 会映射到错误机器。若实际映射不能从 hostfile 推导，应提供显式 topology JSON。

## 采集方式

### Kubernetes

```bash
python3 main.py collect-k8s \
  --namespace default \
  --selector app=megatron-train \
  --container trainer \
  --output diagnosis_out/stacks.json \
  --raw-output diagnosis_out/collect_errors.txt
```

使用 `--all-containers` 会检查每个普通容器；否则应通过 `--container` 指定训练容器。`--parallelism` 控制并发 Pod 数，`--command-timeout` 控制单次 kubectl 命令超时。

### Ansible

```bash
python3 main.py collect \
  --hostfile hostfile \
  --ansible-user train \
  --output diagnosis_out/stacks.json \
  --raw-output diagnosis_out/ansible_raw.txt
```

默认从 hostfile 生成临时 inventory，并使用 Ansible script 模块执行包内的 `stack_analyzer/scripts/remote_capture.py`。仅当目标环境不支持 script 模块时使用 `--use-shell`。

### Docker SSH

该模式适用于每个目标容器都运行 sshd，并且 hostfile 中的主机名可直接通过 SSH 访问的环境：

```bash
python3 main.py collect-docker \
  --hostfile docker-hostfile \
  --ssh-user train \
  --identity-file /path/to/id_ed25519 \
  --output diagnosis_out/stacks.json
```

### 本机

```bash
python3 main.py capture-local --output diagnosis_out/stacks.json
```

本机模式通过进程命令行和环境变量发现训练进程，适合验证权限和输出格式，不代表多机 rank 映射已经正确。

## 离线分析

使用 hostfile 推导 Megatron 并行组：

```bash
python3 main.py analyze \
  --input diagnosis_out/stacks.json \
  --hostfile hostfile \
  --pp-size 2 \
  --tp-size 2 \
  --dp-size 8 \
  --method auto \
  --json
```

如果训练启用了不同 rank 排列，使用 `--parallel-order`；如果对应 Megatron 的 `--use-tp-pp-dp-mapping`，同时传入同名分析选项。PP、TP、DP 和 rank order 必须与训练一致。

也可以提供 topology JSON：

```json
{
  "pp_groups": [[0, 8], [1, 9]],
  "tp_groups": [[0, 1], [8, 9]],
  "dp_groups": [[0, 2, 4, 6], [1, 3, 5, 7]]
}
```

```bash
python3 main.py analyze \
  --input diagnosis_out/stacks.json \
  --topology topology.json \
  --method auto \
  --json
```

## 一步采集并诊断

Ansible 环境可以使用 `diagnose`：

```bash
python3 main.py diagnose \
  --hostfile hostfile \
  --pp-size 2 --tp-size 2 --dp-size 8 \
  --output-dir diagnosis_out \
  --method auto --json
```

该命令保存采集结果和诊断结果，但不会自动驱逐节点。

## 输出解释

- `outlier_ranks`：调用栈与主要群体不同的 rank。
- `machines_to_evict`：结合 rank 到机器映射及并行组推导的建议隔离机器。
- `method`：实际使用的 `trie` 或 `signature` 聚合方式。
- `patterns`：从聚合结果推断的 hang 模式提示。

结果是诊断建议，不是硬件故障证明。执行驱逐或重启前，应结合训练日志、通信日志、NHC 和 HCU 状态复核。

## 常见问题

- `No usable py-spy frames`：检查 py-spy 版本、ptrace 权限、目标 PID 和 JSON 输出兼容性。
- rank 为 `-1`：采集脚本无法从环境变量或命令行解析全局 rank；需要修正启动环境或映射。
- 所有 rank 堆栈相同：可能确实在共同等待，也可能采集到了父 launcher 或 DataLoader；检查 PID、role 和 raw output。
- 建议机器不正确：优先检查 hostfile 顺序、slots、PP/TP/DP 和 parallel order。
- Kubernetes 无目标：检查 namespace、label selector、container 名和 RBAC。

内部分析原理见[分析处理链路](ANALYSIS_PIPELINE_CN.md)。
