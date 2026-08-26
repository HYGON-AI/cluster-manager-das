<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# stack-analyzer 分析处理链路

## 总览

```text
进程发现
  → py-spy 采集
  → StackSnapshot 标准化
  → 每个 rank 选择主快照
  → Trie 聚合（auto 首选）
  → signature 聚合（置信度不足时回退）
  → 异常 rank
  → PP/TP/DP 并行组映射
  → 建议隔离机器
```

## 1. 进程发现

包内唯一的远程采集实现是 `stack_analyzer/scripts/remote_capture.py`。Ansible、Kubernetes 和 Docker collector 都把该脚本发送到目标环境执行。

采集脚本优先通过 psutil 枚举进程，必要时读取 `/proc`，并根据命令行、父子关系及 rank 环境变量识别 trainer、DataLoader、checkpoint 等角色。它会尽量选择每个 rank 的训练 worker 根进程，减少重复抓取父 launcher 和子进程。

## 2. py-spy 采集与标准化

每个目标 PID 由 py-spy 输出 JSON 调用栈。解析器选择可用线程，将 frame 转换为统一的 `StackFrame(function, file, line)`，最后形成：

```text
StackSnapshot(machine_id, rank, pid, role, frames, raw_text)
```

分析匹配时保留函数和文件路径，但忽略行号，降低不同构建或小版本间行号变化造成的误差。

## 3. 主快照选择

同一 rank 可能采集到多个相关进程。`snapshot_prep.select_primary_snapshots()` 默认每个 rank 只保留一个最合适的快照，优先使用 trainer、有效 rank 和包含栈帧的结果。

选中的快照没有任何 frame 时，分析会直接失败，而不是把空栈错误地当成一种共同调用模式。

## 4. Trie 聚合

`auto` 模式首先使用 Trie：

1. 把标准化调用栈插入 `StackTrie`。
2. 叶节点记录到达该调用路径的 rank 集合。
3. 根据主要 rank 群体与较小叶群体识别异常 rank。
4. 当主要群体比例和异常覆盖率达到阈值时，结果被视为有足够置信度。

Trie 适合处理多数 rank 共享较长调用路径、少数 rank 在某个分支停住的情况。

## 5. Signature 回退

Trie 无法形成可靠分组时，`auto` 模式回退到 signature 聚合。每个快照取顶部若干 frame 形成签名；默认允许模糊匹配，减少路径或非关键 frame 差异导致的过度分组。

可以通过 CLI 使用：

- `--method trie`：强制 Trie。
- `--method signature`：强制签名聚合。
- `--depth`：调整签名深度。
- `--exact`：关闭模糊匹配。

## 6. 并行拓扑映射

分析器需要知道全局 rank 所在的 PP、TP 和 DP group。拓扑可以从 hostfile、world size 和 Megatron rank order 推导，也可以通过 JSON 显式提供。

发现异常 rank 后，分析器会寻找覆盖这些 rank 的最小共享并行组。在候选大小相同时，当前实现按 PP、TP、DP 顺序选择，以便覆盖可能被同一通信等待连带阻塞的 rank。

随后通过 `rank_to_machine` 将需要覆盖的 rank 转换成机器集合，形成 `machines_to_evict`。

## 7. 结果边界

以下情况可能产生误判：

- 采集时刻过早，rank 暂时处于不同训练阶段。
- 采集到 launcher、DataLoader 或 checkpoint 线程而不是 trainer 主线程。
- hostfile 顺序或 slots 与真实 rank 映射不同。
- PP/TP/DP 或 parallel order 配置错误。
- 所有 rank 因同一外部依赖阻塞，调用栈没有明显离群点。
- Python 栈相同，但故障发生在 HCU kernel、驱动或网络层。

因此建议在确认训练无进展后进行多次间隔采集，并结合训练、RCCL、系统和节点健康日志判断。工具只输出建议，不执行自动驱逐。

具体命令和输出格式见[采集与分析操作手册](COLLECTION_AND_ANALYSIS_CN.md)。
