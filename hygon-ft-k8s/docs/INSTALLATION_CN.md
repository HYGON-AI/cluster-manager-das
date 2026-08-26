<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hygon-ft Kubernetes FT Add-on 安装指南

本文只说明 FT 控制面的构建、节点准备、安装和验证。训练任务的 label、annotation 和调度约束见[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)。

## 1. 前置条件

公共条件：

- 管理节点具有 `kubectl`、`bash`、`openssl`、`base64`、`grep`、`sed`、`mktemp` 和 `tr`。
- 当前账号可以创建 CRD、RBAC、Deployment、DaemonSet、Service、Secret 和 MutatingWebhookConfiguration，并可以 patch Node、delete Pod。
- 所有目标节点能够拉取或已经加载控制器镜像。
- 每个 training 节点预装可执行的宿主机 `run_nhc`。

按工作负载选择外部组件：

| 工作负载 | 外部前置组件 |
|---|---|
| Volcano Job | Volcano，包含 `jobs.batch.volcano.sh` CRD 和 `batch.volcano.sh/v1alpha1` API |
| PyTorchJob | Kubeflow Training Operator、PyTorchJob CRD、Volcano PodGroup CRD；Training Operator 使用 Volcano gang scheduler |

安装脚本只检查这些组件，不负责安装或升级它们。

## 2. 构建和分发镜像

在仓库根目录执行：

```bash
cd hygon-ft-k8s
bash scripts/build_images.sh \
  hygon/ft-controller:latest \
  /path/to/shared_root/hygon-ft-controller-latest.tar
```

控制器镜像包含 Operator、Webhook、NodeHealth Agent 和 `ft-launcher`，不包含训练程序、LogMonitor 完整包或宿主机 NHC。

不能使用镜像仓库时，将 tar 加载到所有可能运行 system Pod、NodeHealth Agent 或 runtime initContainer 的节点：

```bash
bash scripts/load_image_to_nodes.sh \
  hygon/ft-controller:latest \
  /path/to/shared_root/hygon-ft-controller-latest.tar
```

## 3. 配置节点角色

```bash
SYSTEM_NODES="system-node-1 system-node-2" \
TRAINING_NODES="train-node-1 train-node-2 train-node-3 train-node-4" \
bash scripts/label_nodes_example.sh
```

- system 节点运行 Operator 和 Webhook。
- training 节点运行训练 Pod 和 NodeHealth Agent。
- system 与 training 重叠时设置 `APPLY_SYSTEM_TAINT=false`，否则 system 的 `NoSchedule` taint 会阻止训练 Pod 调度。

节点已经具备正确 label 时，可以在安装时令 `SYSTEM_NODES`、`TRAINING_NODES` 为空，避免重复修改。

## 4. 安装控制面

```bash
export FT_CONTROLLER_IMAGE=hygon/ft-controller:latest
export SYSTEM_NODES="system-node-1 system-node-2"
export TRAINING_NODES="train-node-1 train-node-2 train-node-3 train-node-4"

# 可选：安装前同时校验一份 PyTorchJob 或 Volcano Job。
export TRAIN_YAML=/path/to/train.yaml

bash start.sh install
```

只安装或更新控制面、不校验训练任务：

```bash
TRAIN_YAML= bash start.sh install
```

也可以直接执行底层脚本：

```bash
bash scripts/install.sh "${TRAIN_YAML:-}"
```

安装过程会：

1. 校验可选训练 YAML 对应的 CRD、调度器和恢复策略。
2. 创建 FaultEvent CRD、`hygon-ft` namespace 和 RBAC。
3. 创建 NHC 适配 ConfigMap。
4. 生成或复用 Webhook CA、证书和 Secret。
5. 部署 Operator、Webhook 和 NodeHealth Agent。
6. 等待控制面 rollout 完成后注册 Admission Webhook。

安装不会提交训练 YAML，也不会修改正在运行的训练任务。

## 5. 安装后验证

```bash
bash start.sh status
kubectl -n hygon-ft get deployments,daemonsets,pods,services -o wide
kubectl get mutatingwebhookconfiguration hygon-ft-pytorchjob-mutator
```

确认：

- Operator、Webhook Deployment 可用。
- 每个带 `node-role.hygon.io/training=true` 的可调度节点都有 NodeHealth Pod。
- Webhook Service 有 ready endpoint。
- NodeHealth 日志显示宿主机 `run_nhc` 可以执行，而不是 probe error。

Volcano Job 可以在提交前执行 server-side dry-run 注入验证：

```bash
bash scripts/verify_volcano.sh /path/to/train.yaml
```

训练任务提交和日常观察见[运行与恢复手册](OPERATIONS_AND_RECOVERY_CN.md)，安装故障见[常见问题排查](TROUBLESHOOTING_CN.md)。
