<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster-manager-das

HCU 大模型训练可靠性工具集：面向大规模 HCU分布式训练，提供训练前环境检查、训练启动与长稳保障、Kubernetes 运行期容错、训练 hang 根因诊断，以及 PyTorch 训练韧性库的海光 HCU 适配。

![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)

## ✨ 发布信息

- [2026-08-26]: 🚀 首次开源发布 v0.1.0

## ℹ️ 简介

`cluster-manager-das` 覆盖大模型训练可靠性的完整生命周期：

```text
训练前环境验收          训练中长稳保障                hang 根因诊断
   hcu-envcheck    →    hcu-training-reliability    →   stack-analyzer
   （集群环境检查）      （裸金属/Slurm/Docker）         （Megatron 框架）
                        hygon-ft-k8s
                        （Kubernetes）
                        ＋ hcu_resiliency_ext（可选，PyTorch 训练韧性库 HCU 适配）
```

面向在 HCU 集群开展大模型训练的算法、基础设施与集群运维工程师，也可供深度学习平台开发厂商集成，用于构建开箱即用的训练可靠性能力。

本仓库不是统一安装的单体软件：每个子项目有独立的依赖、入口和部署方式，请先按训练阶段选择子项目，再阅读对应 README 与 docs。

## ⚙️ 功能介绍

### 训练前环境检查：hcu-envcheck

从登录节点对已分配 HCU 节点执行环境验收，支持共享 Conda、节点本地 Conda 和各节点独立 Docker 容器三种执行场景。

| 特性 | 介绍 | Released |
| --- | --- | --- |
| 平台/资源检测 | 系统/驱动/DTK/CPU/内存/网络健康检查与节点配置一致性 | ✅ |
| 容器管理 | 容器状态查询、创建、重建、删除（显式维护操作） | ✅ |
| RCCL 测试 | 单机/多机集合通信测试，对比所选性能基准 | ✅ |
| DeepEP 测试 | 单机 intranode、多机 norm/lowl 测试 | ✅ |
| byte-flux 测试 | FP16、TPN 的 AG 用例，检查全部 rank 输出 | ✅ |
| IB 写带宽 | IB 配对带宽测试 | ✅ |

### 训练长稳保障（裸金属/Slurm/Docker）：hcu-training-reliability

训练的启动入口与守护组件：通过它启动训练并持续守护，故障后自动隔离异常节点、选择备用节点重新拉起训练。

| 特性 | 介绍 | Released |
| --- | --- | --- |
| 训练启动与监控 | 启动 MPI/Docker 训练，监控训练日志、NHC 与硬件状态 | ✅ |
| hang 检测与故障定位 | 解析训练日志，识别 hang 与异常 | ✅ |
| 故障自动拉起 | 隔离异常节点、选择备用节点并重启训练 | ✅ |
| 节点池管理 | 运行/备用/异常节点池维护与状态持久化 | ✅ |
| 训练进度通知 | 训练进度与故障事件飞书通知 | ✅ |
| 训练前筛机 | 两阶段性能筛机（node_check），剔除性能劣化节点 | ✅ |

### 训练容错插件（Kubernetes）：hygon-ft-k8s

为 PyTorchJob 与 Volcano Job 提供运行期容错，不安装或替代 Volcano、Training Operator、kube-scheduler 和设备插件。

| 特性 | 介绍 | Released |
| --- | --- | --- |
| 训练任务支持 | PyTorchJob / Volcano Job 注入容错环境 | ✅ |
| 进程故障检测 | LogMonitor 日志监控 + ft-launcher 故障上报 + FaultEvent 聚合 | ✅ |
| hang 检测 | 训练无进展检测与故障事件上报 | ✅ |
| 节点隔离 | Operator 执行节点 taint、Pod 删除与任务重建 | ✅ |
| 节点健康检查 | nodehealth-agent DaemonSet 周期执行 NHC | ✅ |
| 节点恢复 | 多轮 IB 状态/带宽、NHC 与检查 Pod 验证通过后移除隔离 taint | ✅ |

### hang 根因诊断：stack-analyzer

面向 Megatron 训练框架的 hang 诊断工具，区分单 rank 异常、通信组等待和连带阻塞。

| 特性 | 介绍 | Released |
| --- | --- | --- |
| 堆栈采集 | Kubernetes / Docker SSH / Ansible / 本地四种方式批量 py-spy 采集 | ✅ |
| 堆栈聚合 | Trie / signature 聚合识别异常调用栈 | ✅ |
| 拓扑分析 | 按 Megatron PP/TP/DP 并行拓扑定位异常 rank | ✅ |
| 隔离建议 | 输出建议驱逐机器列表（只建议，不自动驱逐） | ✅ |

### PyTorch 训练韧性库（可选）：hcu_resiliency_ext

基于 NVIDIA `nvidia-resiliency-ext`，功能与上游保持一致，针对海光 HCU 完成适配。

| 特性 | 介绍 | Released |
| --- | --- | --- |
| 故障检测与恢复 | hang rank 检测与作业内恢复 | ✅ |
| In-process 重启 | 故障后进程内快速恢复 | ✅ |
| 异步/本地 checkpoint | 高效异步与本地 checkpoint 保存 | ✅ |
| 掉队者检测 | 按 rank 监测 GPU/CPU 性能，标记拖慢训练的节点 | ✅ |

本目录是上游库的海光 HCU 适配补丁：在固定 commit 的上游源码上注入适配实现，不能独立 `pip install`。

## 🗂️ 目录结构

```text
cluster-manager-das/
├── docs/                       # 套件级端到端流程
├── hcu-envcheck/               # 训练前环境检查
│   └── third-party/            #   子模块：flux、DeepEP
├── hcu-training-reliability/   # 长稳训练监控与故障恢复
├── hygon-ft-k8s/               # Kubernetes 训练容错
├── stack-analyzer/             # hang 堆栈诊断
└── hcu_resiliency_ext/         # PyTorch 训练韧性库 HCU 适配
    └── nvidia_resiliency_ext/  #   子模块：NVIDIA 上游源码（固定 commit）
```

## 🚀 快速入门

克隆仓库后先初始化子模块（hcu-envcheck 主动测试与韧性库适配依赖）：

```bash
git clone https://github.com/HYGON-AI/cluster-manager-das.git
cd cluster-manager-das
git submodule update --init --recursive
```

训练前对两台已分配节点做环境检查（完整参数见 [hcu-envcheck 文档](hcu-envcheck/docs/USER_GUIDE.md)）：

```bash
cd hcu-envcheck
cat > hostfile <<'EOF'
node001
node002
EOF
bash bin/hcu-cluster-run shared-conda platform,resource -f hostfile --env-script /path/to/env.sh
```

各场景的最短路径：

- [hcu-envcheck 快速开始](hcu-envcheck/README.md)
- [hcu-training-reliability 源码启动](hcu-training-reliability/README.md)
- [hygon-ft-k8s 快速开始](hygon-ft-k8s/README.md)
- [stack-analyzer 内置演示](stack-analyzer/README.md)
- [端到端使用流程](docs/END_TO_END_WORKFLOW_CN.md)

## 📦 安装指南

| 子项目 | 安装方式 |
| --- | --- |
| hcu-envcheck | 源码运行，或使用自带独立 Python 的 `.run` 安装包 |
| hcu-training-reliability | 源码直接运行（需具备 `setup.py` 声明的依赖），可选 `pip install -e .` |
| hygon-ft-k8s | 构建控制器镜像后在管理节点 `bash start.sh install` |
| stack-analyzer | `pip install .` |
| hcu_resiliency_ext | 在固定 commit 的上游源码上注入适配，见[安装说明](hcu_resiliency_ext/docs/INSTALLATION_CN.md) |

## 📘 使用指南

各子项目的命令、配置、恢复流程和排障说明统一维护在子项目 `docs/` 中：

| 子项目 | 主要文档 |
| --- | --- |
| hcu-envcheck | [用户手册](hcu-envcheck/docs/USER_GUIDE.md)、[设计](hcu-envcheck/docs/DESIGN.md)、[能力与验证边界](hcu-envcheck/docs/KNOWN_LIMITATIONS.md) |
| hcu-training-reliability | [部署与启动](hcu-training-reliability/docs/deployment_guide.md)、[故障恢复与日常操作](hcu-training-reliability/docs/recovery_workflow.md)、[配置参考](hcu-training-reliability/docs/config_guide.md) |
| hygon-ft-k8s | [安装指南](hygon-ft-k8s/docs/INSTALLATION_CN.md)、[运行与恢复](hygon-ft-k8s/docs/OPERATIONS_AND_RECOVERY_CN.md)、[故障处理语义](hygon-ft-k8s/docs/FAULT_HANDLING_CN.md) |
| stack-analyzer | [采集与分析操作手册](stack-analyzer/docs/COLLECTION_AND_ANALYSIS_CN.md)、[分析处理链路](stack-analyzer/docs/ANALYSIS_PIPELINE_CN.md) |

## 🔒 安全边界

- 主动带宽、RCCL、完整筛机、故障模拟和恢复测试只能在已确认空闲的节点上执行。
- `hcu-training-reliability` 当前可能按 `python` 关键字终止目标节点进程，要求训练节点独占。
- `hygon-ft-k8s` 可能创建集群级资源、taint 节点、删除 Pod 并触发任务重建。
- `stack-analyzer` 需要 ptrace 权限；分析结果是诊断证据，不会自动执行驱逐。
- `hcu_resiliency_ext` 只能在已验证的第三方库版本上使用。

## 🧩 兼容性

- Python ≥3.10，Bash，目标节点免密 SSH。
- 训练框架：Megatron 系训练框架（stack-analyzer 拓扑分析与 hcu_resiliency_ext 适配场景）。
- Kubernetes：需要 Volcano / Kubeflow Training Operator 配合（hygon-ft-k8s 前置条件）。
- 资源调度：Slurm 由用户在工具外申请节点（hcu-envcheck），hcu-training-reliability 支持 Slurm 作业校验。

## 🛠️ 贡献指南

- 涉及行为、API、部署或兼容性的实质性变更，请先提交 issue 讨论。
- 提交前运行受影响的单元测试（`python -m pytest`），Shell 脚本使用 `bash -n` 校验。
- 详细流程见 [CONTRIBUTING](CONTRIBUTING.md)。

## ⚖️ 相关说明

- [LICENSE](LICENSE)
- [第三方开源软件声明](THIRD_PARTY_NOTICES.md)
- [安全漏洞报告](SECURITY.md)
- [行为准则](CODE_OF_CONDUCT.md)

## 🤝 建议与交流

欢迎大家提交 issue 反馈问题与建议，我们会尽快回复。

## License

Copyright (c) 2026 Hygon Information Technology Co., Ltd.

This project is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) for the license terms. Third-party components and
attributions are documented in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
