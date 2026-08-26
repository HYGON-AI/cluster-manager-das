<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Hygon FT Kubernetes Add-on

`hygon-ft-k8s` 为 PyTorchJob 和 Volcano Job 提供训练日志监控、节点健康检查、故障节点隔离和训练任务恢复能力。

项目不安装或替代 Volcano、Kubeflow Training Operator、kube-scheduler 和设备插件。完整组件关系和故障处理链路参见 [架构说明](docs/ARCHITECTURE_CN.md)。

## 前置条件

### 公共条件

- 管理节点可以执行 `kubectl`、`bash`、`openssl`、`awk`、`grep`、`sed`、`base64` 和 `mktemp`。
- 当前账号可以创建 CRD、RBAC、MutatingWebhookConfiguration、Deployment 和 DaemonSet，并可以 patch Node、delete Pod。
- 所有相关节点能够拉取或已经加载 `hygon/ft-controller` 镜像。
- 训练镜像包含 Bash、Python 3 和 `tee`。
- 训练 Pod 可以访问共享日志目录以及 `ft.hygon.io/log-monitor-command` 引用的代码和 Python 依赖。

### 工作负载条件

| 工作负载 | 必须提前安装 | 关键要求 |
|---|---|---|
| Volcano Job | Volcano，存在 `jobs.batch.volcano.sh` CRD | YAML 使用 `schedulerName: volcano`，配置 `PodFailed -> RestartJob` |
| PyTorchJob | Training Operator、Volcano PodGroup CRD | 设置 `schedulingPolicy.minAvailable`，Training Operator 使用 Volcano gang scheduler |

`scripts/install.sh` 只检查这些外部依赖，不负责安装 Volcano 或 Training Operator。

## 快速开始

以下命令均在 `hygon-ft-k8s` 目录执行：

```bash
cd /path/to/hcu_cluster_manager/hygon-ft-k8s
```

### 1. 准备控制器镜像

`hygon/ft-controller:latest` 包含 Operator、Webhook、NodeHealth Agent、ft-launcher 和容错运行依赖。它不是训练镜像，也不包含 NHC 实现；NodeHealth Agent 通过 ConfigMap 中的适配脚本进入宿主机执行 `run_nhc`，因此每个训练节点必须预先提供该命令。训练镜像由任务 YAML 中的 `image:` 决定。

构建镜像并保存 tar：

```bash
bash scripts/build_images.sh \
  hygon/ft-controller:latest \
  /path/to/shared_root/hygon-ft-controller-latest.tar
```

不使用镜像仓库时，将 tar 加载到可能运行控制面、NodeHealth Agent 或训练 Pod 的节点：

```bash
bash scripts/load_image_to_nodes.sh \
  hygon/ft-controller:latest \
  /path/to/shared_root/hygon-ft-controller-latest.tar
```

### 2. 配置节点角色

```bash
export SYSTEM_NODES="system-node-1"
export TRAINING_NODES="train-node-1 train-node-2 train-node-3 train-node-4"

bash scripts/label_nodes_example.sh
```

| 角色 | 用途 | 安装的节点状态 |
|---|---|---|
| system | 运行 Operator 和 Webhook | `node-role.hygon.io/system=true`，默认添加 `NoSchedule` taint |
| training | 运行训练和 NodeHealth Agent | `node-role.hygon.io/training=true`、`accelerator.hygon.io/enabled=true` |

节点角色通常只需配置一次。system 与 training 节点重叠时，应设置 `APPLY_SYSTEM_TAINT=false`，否则训练 Pod 无法调度到该节点。

检查节点角色：

```bash
kubectl get nodes \
  -L node-role.hygon.io/system,node-role.hygon.io/training,accelerator.hygon.io/enabled
```

### 3. 准备训练 YAML

仓库提供以下示例：

| 文件 | 工作负载 | 规模 |
|---|---|---|
| `manifests/examples/train_llama_volcano.yaml` | Volcano Job | 4 节点，每 Pod 8 张 HCU |
| `manifests/examples/train_llama.yaml` | PyTorchJob | 4 节点，每 Pod 8 张 HCU |
| `manifests/examples/pytorchjob-2n16g-ft.yaml` | PyTorchJob | 2 节点 16 卡 |
| `manifests/examples/pytorchjob-8n64g-ft.yaml` | PyTorchJob | 8 节点 64 卡 |

> **环境适配提示：** `docs/train_volcano.yaml` 是环境专用参考，不是可直接运行的示例。其中的 `your-registry.example.com/...` 是镜像占位符。部署前必须将 master 和 worker 的 `image` 替换为集群可访问的 HCU 训练镜像，并同步调整 `/public/home/user` 路径、离线 wheel、数据集及 hostPath 配置。

提交前至少检查：

- 训练镜像地址。
- 所有节点可访问的共享目录。
- 训练脚本、数据集、tokenizer 和 checkpoint 路径。
- HCU、CPU、内存和 `/dev/shm` 资源申请。
- `NNODES`、副本数和 gang scheduling 的 `minAvailable` 是否一致。
- YAML 中的 namespace 是否与提交参数一致。

### 4. 安装容错组件

```bash
export TRAIN_YAML=/path/to/train.yaml
export FT_CONTROLLER_IMAGE=hygon/ft-controller:latest
export SYSTEM_NODES="system-node-1"
export TRAINING_NODES="train-node-1 train-node-2 train-node-3 train-node-4"

bash start.sh install
```

安装过程会：

- 检查工作负载 CRD、调度器和任务恢复配置。
- 创建 FaultEvent CRD、`hygon-ft` namespace、RBAC 和 NHC ConfigMap。
- 生成或复用 Webhook CA 和服务端证书。
- 部署 Operator、Webhook 和 NodeHealth Agent。
- 注册 PyTorchJob/Volcano Job Admission Webhook。
- 滚动重启 FT 控制面，使相同 `latest` tag 的新镜像生效。

安装不会提交训练 YAML，也不会重启已经运行的训练任务。

节点已经具有角色 label 时，可以复用现有配置：

```bash
SYSTEM_NODES= TRAINING_NODES= bash start.sh install
```

只更新控制面、不检查训练 YAML 时：

```bash
TRAIN_YAML= bash start.sh install
```

### 5. 提交训练任务

```bash
bash scripts/submit_job.sh default "${TRAIN_YAML}"
```

`default` 是 Kubernetes namespace，不是 Volcano queue。推荐将安装和提交串联，避免安装失败后继续提交：

### 6. 验证状态

```bash
bash start.sh status
kubectl -n hygon-ft get pods -o wide
kubectl -n hygon-ft get faultevents -o wide
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide
```

Volcano Job 可以执行非破坏性验证：

```bash
bash scripts/verify_volcano.sh "${TRAIN_YAML}"
```

需要验证 NHC、FaultEvent、节点隔离和任务重建链路时，参见
[故障模拟与验证](docs/FAULT_INJECTION_CN.md)。

任务 annotation、LogMonitor 接入方式，以及 Volcano Job/PyTorchJob 的完整约束参见
[训练任务配置](docs/WORKLOAD_CONFIGURATION_CN.md)。

## 运维

### 状态和日志

```bash
bash start.sh status
kubectl -n hygon-ft logs deployment/ft-operator --tail=200
kubectl -n hygon-ft logs deployment/ft-webhook --tail=200
kubectl -n hygon-ft get faultevents -o wide
kubectl -n hygon-ft describe faultevent <event-name>
```

持久化目录中的常见文件：

| 文件 | 作用 |
|---|---|
| `launcher-<pod>.log` | launcher 启动、marker 上报和 Operator 确认过程 |
| `train-<pod>.log` | 训练标准输出和标准错误 |
| `log-monitor-events-<pod>.jsonl` | LogMonitor 识别故障的结构化记录 |
| `.offset/<pod-uid>/*.offset` | 同一个 Pod 内 LogMonitor 重启后的续读位置 |

控制面日志默认通过 `kubectl logs` 查看。需要持久化 Operator、Webhook 或 NodeHealth Agent 日志时，应为控制面 manifest 增加持久卷。

训练 Pod 未创建、长期 Pending、没有日志、反复重建或故障节点未隔离时，参见
[常见问题排查](docs/TROUBLESHOOTING_CN.md)。

### 节点恢复

节点硬件、网络和 NHC 恢复后，推荐执行自动恢复检查：

```bash
bash start.sh recover fault-node normal-node-1,normal-node-2
```

不指定参考节点时，程序自动选择两个 Ready 且未被同类 taint 隔离的节点：

```bash
bash start.sh recover fault-node
```

恢复过程默认执行两轮宿主侧和 Pod 内检查，全部通过后只删除 `ft.hygon.io/node-unhealthy:NoSchedule`。非交互执行必须显式确认：

```bash
CONFIRM_TAINT_RECOVERY=yes \
bash start.sh recover fault-node normal-node-1,normal-node-2
```

登录节点 Python 需要能够导入 `kubernetes`。依赖位于离线目录时：

```bash
PYTHON_DEPS_DIR=/path/to/python-deps \
bash start.sh recover fault-node normal-node-1,normal-node-2
```

系统 Python 已安装依赖时，可以禁用默认离线目录：

```bash
PYTHON_DEPS_DIR= bash start.sh recover fault-node
```

不要在节点仍然异常时直接删除 taint。确认无需执行恢复检查时，可以手工删除：

```bash
kubectl taint node fault-node ft.hygon.io/node-unhealthy-
```

### 卸载

```bash
bash start.sh cleanup
```

交互模式需要输入 `yes`。非交互执行必须显式确认：

```bash
CONFIRM_HYGON_FT_CLEANUP=yes bash start.sh cleanup
```

cleanup 会删除 Hygon FT namespace、控制面、CRD、Webhook、RBAC，以及 FT 管理的节点角色 label 和 taint。默认还会删除 FT training 节点上的 `accelerator.hygon.io/enabled` label；该 label 被其他系统共用时可以保留：

```bash
REMOVE_ACCELERATOR_LABEL=false bash start.sh cleanup
```

cleanup 不会卸载 Volcano、Training Operator，不会删除用户训练任务、外部日志、控制器镜像或镜像 tar。

## 进一步阅读

| 目标 | 文档 |
|---|---|
| 了解目录和选择文档 | [目录与文档导航](docs/DIRECTORY_STRUCTURE_CN.md) |
| 理解组件与边界 | [架构说明](docs/ARCHITECTURE_CN.md) |
| 安装控制面 | [安装指南](docs/INSTALLATION_CN.md) |
| 配置 Volcano Job 或 PyTorchJob | [训练任务配置](docs/WORKLOAD_CONFIGURATION_CN.md) |
| 提交任务、观察状态和恢复节点 | [运行与恢复手册](docs/OPERATIONS_AND_RECOVERY_CN.md) |
| 理解不同故障的准确动作 | [故障处理语义](docs/FAULT_HANDLING_CN.md) |
| 接入 LogMonitor 或实现 marker 上报 | [LogMonitor、marker 与故障上报协议](docs/LOG_MONITOR_INTEGRATION_CN.md) |
| 定位安装、调度、日志或恢复问题 | [常见问题排查](docs/TROUBLESHOOTING_CN.md) |
| 在测试集群执行故障演练 | [故障模拟与验证](docs/FAULT_INJECTION_CN.md) |
| 规划管理面、训练面和备用容量 | [生产部署拓扑](docs/PRODUCTION_TOPOLOGY_CN.md) |
| 阅读 Webhook、Operator、Agent 和 launcher 实现 | [代码走读](docs/CODE_WALKTHROUGH_CN.md) |

## License

This subproject is licensed under the Apache License, Version 2.0. See the
repository-level [LICENSE](../LICENSE), [NOTICE](../NOTICE), and
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
