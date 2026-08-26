<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hygon-ft-k8s 目录与文档导航

`hygon-ft-k8s` 是独立的 Kubernetes Add-on，不进入 `cluster_manager` Python 包，也不接管 mpirun、hostfile、node pool 或备用节点逻辑。

## 1. 代码和资源目录

```text
hygon-ft-k8s/
├── hygon_ft/
│   ├── operator/controller.py       # FaultEvent、节点隔离、Pod 处置和告警
│   ├── webhook/server.py            # PyTorchJob/Volcano Job Admission 注入
│   └── nodehealth/agent.py          # training 节点 NHC 调用和事件上报
├── runtime/
│   └── ft-launcher                  # 训练命令包装、日志和故障确认
├── manifests/
│   ├── crds/faultevents.yaml        # FaultEvent CRD
│   ├── base/                        # FT 控制面清单
│   └── examples/                    # 通用训练任务示例
├── packaging/docker/                # 控制器镜像构建文件
├── scripts/                         # 安装、提交、验证、恢复辅助和卸载脚本
├── docs/                            # 设计、配置、运行和排障文档
├── tests/                           # Webhook、Operator 和 NodeHealth 单元测试
├── README.md                        # 项目入口和快速开始
└── start.sh                         # install/status/recover/cleanup 统一入口
```

## 2. 分层职责

| 目录 | 放置内容 | 不应放置 |
|---|---|---|
| `hygon_ft/` | Kubernetes 控制面 Python 代码 | mpirun launcher、训练算法 |
| `runtime/` | 注入训练 Pod 的轻量运行时 | 完整训练镜像、宿主机 NHC |
| `manifests/base/` | Operator、Webhook、NodeHealth 公共资源 | Volcano、Training Operator 安装清单 |
| `manifests/examples/` | 可泛化的 PyTorchJob/Volcano Job 示例 | 客户路径、凭据、可直接访问的内部镜像 |
| `scripts/` | 管理员执行的安装和运维入口 | 训练框架实现 |
| `packaging/docker/` | 控制器镜像定义及依赖 | 训练镜像定义 |

## 3. 文档职责

| 文档 | 唯一职责 |
|---|---|
| [架构说明](ARCHITECTURE_CN.md) | 设计边界、组件和高层数据流 |
| [代码走读](CODE_WALKTHROUGH_CN.md) | 模块、函数和资源调用关系 |
| [安装指南](INSTALLATION_CN.md) | 镜像、节点角色和控制面安装 |
| [训练任务配置](WORKLOAD_CONFIGURATION_CN.md) | FT label、annotation 和工作负载约束 |
| [故障处理语义](FAULT_HANDLING_CN.md) | 故障分类、taint、Pod 处置和重启粒度 |
| [LogMonitor 协议](LOG_MONITOR_INTEGRATION_CN.md) | marker、`/report` 和告警接口 |
| [运行与恢复](OPERATIONS_AND_RECOVERY_CN.md) | 提交、观察、故障确认和节点恢复 |
| [故障模拟](FAULT_INJECTION_CN.md) | NHC 和 NodeNotReady 演练步骤 |
| [常见问题排查](TROUBLESHOOTING_CN.md) | 按症状定位问题 |
| [生产部署拓扑](PRODUCTION_TOPOLOGY_CN.md) | 生产 HA、规模和具体故障场景 |

`docs/train_volcano.yaml` 是环境专用参考清单；通用示例位于 `manifests/examples/`。
