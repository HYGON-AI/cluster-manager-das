<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# stack-analyzer

`stack-analyzer` 用于分布式训练 hang 诊断：通过 `py-spy` 采集各 rank 的 Python 调用栈，聚合异常调用栈，并结合 Megatron 的 PP/TP/DP 拓扑给出异常 rank 和建议驱逐节点。

## 适用场景

- 训练任务仍在运行但长时间无进展，需要批量抓取 rank 堆栈。
- 需要区分单 rank 异常、通信组等待和连带阻塞。
- 需要将异常 rank 映射到机器或容器，形成重调度建议。

## 安装

```bash
cd stack-analyzer
pip install -r requirements.txt
```

按 Python 包安装：

```bash
pip install .

# Ansible 采集支持
pip install ".[ansible]"

# 开发和测试依赖
pip install ".[all]"
```

采集端必须能执行 `python3` 和 `py-spy`。容器场景还需要允许 `py-spy` attach 训练进程，通常要求 `SYS_PTRACE`，并确认 seccomp/Yama 配置允许附加进程。

## 最小示例

Kubernetes 采集：

```bash
python3 main.py collect-k8s \
  -n default \
  -l app=megatron-train \
  -c trainer \
  -o diagnosis_out/stacks.json
```

离线分析：

```bash
python3 main.py analyze \
  -i diagnosis_out/stacks.json \
  -H k8s-hostfile \
  --pp-size 2 \
  --tp-size 2 \
  --dp-size 8 \
  --method auto \
  --json
```

无需真实集群的内置演示：

```bash
python3 main.py demo --scenario pp --method trie
```

完整的采集方式、参数、Hostfile、输出解释和分析处理链路见下方文档。

## 文档

| 文档 | 内容 |
| --- | --- |
| [采集与分析操作手册](docs/COLLECTION_AND_ANALYSIS_CN.md) | Kubernetes、Docker SSH、Ansible、本地采集，Hostfile、离线分析、输出与排障 |
| [分析处理链路](docs/ANALYSIS_PIPELINE_CN.md) | 进程发现、堆栈采集、Trie/signature 聚合、拓扑映射和驱逐判定原理 |

## 主要文件

```text
stack_analyzer/
├── runtime_analyzer.py       # 聚合入口
├── trie_aggregator.py        # Trie rank 分布聚合
├── aggregator.py             # signature 聚合和 hang 标签
├── megatron_topology.py      # Megatron 并行拓扑推断
├── hostfile.py               # rank 与机器映射
├── kubernetes_collector.py   # Kubernetes 采集
├── docker_collector.py       # Docker SSH 采集
├── ansible_collector.py      # Ansible 采集
└── stack_capture.py          # py-spy JSON 解析
```

## 测试

```bash
python3 -m pytest tests/ -v
```

工具输出是基于调用栈和拓扑的诊断建议，不能直接证明硬件故障。驱逐或重启前应结合训练日志、通信日志和节点健康信息复核。

## License

This subproject is licensed under the Apache License, Version 2.0. See the
repository-level [LICENSE](../LICENSE), [NOTICE](../NOTICE), and
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
