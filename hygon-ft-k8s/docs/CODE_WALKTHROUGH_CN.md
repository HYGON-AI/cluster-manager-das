<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hygon-ft Kubernetes Add-on 代码走读

本文按当前模块和稳定的函数名称说明实现关系，不引用源码行号，也不重复安装命令、任务参数或运维步骤。源码调整后，应以本文链接的文件和实际清单为准。

## 1. 组件和边界

| 组件 | 入口 | 作用 |
|---|---|---|
| 安装脚本 | `start.sh install`、`scripts/install.sh` | 校验外部依赖，安装 CRD、RBAC、证书、Operator、Webhook 和 NodeHealth Agent |
| Admission Webhook | `python -m hygon_ft.webhook.server` | 为启用 FT 的 PyTorchJob 和 Volcano Job 注入 runtime |
| FT Operator | `python -m hygon_ft.operator.controller` | 接收并聚合 FaultEvent，执行 taint、删 Pod、状态确认和告警 |
| NodeHealth Agent | `python -m hygon_ft.nodehealth.agent` | 仅在带 training label 的节点运行宿主机 `run_nhc`，异常时创建 FaultEvent |
| ft-launcher | `/opt/hygon-ft/ft-launcher` | 包装训练命令，启动日志监控，上报故障并等待 Operator 确认 |

本项目不安装 Volcano、Kubeflow Training Operator、调度器、设备插件或宿主机 NHC，也不负责保存训练 checkpoint。

## 2. 资源与进程入口

Kubernetes 资源与 Python/Shell 入口的映射如下：

- [`scripts/install.sh`](../scripts/install.sh)：安装编排和工作负载前置检查。
- [`manifests/base/02-operator-deployment.yaml`](../manifests/base/02-operator-deployment.yaml)：Operator Deployment 和故障上报 Service。
- [`manifests/base/03-webhook-deployment.yaml`](../manifests/base/03-webhook-deployment.yaml)：Webhook Deployment 和 Service。
- [`manifests/base/04-mutatingwebhookconfiguration.yaml`](../manifests/base/04-mutatingwebhookconfiguration.yaml)：Admission 规则和 object selector。
- [`manifests/base/06-nodehealth-daemonset.yaml`](../manifests/base/06-nodehealth-daemonset.yaml)：启动 `python -m hygon_ft.nodehealth.agent`。
- [`manifests/base/05-nodehealth-config.yaml`](../manifests/base/05-nodehealth-config.yaml)：挂载宿主机 NHC 适配脚本。

## 3. Webhook 注入链路

实现位于 [`hygon_ft/webhook/server.py`](../hygon_ft/webhook/server.py)，使用 Python 标准库 `ThreadingHTTPServer` 提供 HTTPS `/mutate` 和 `/healthz`，不依赖 Flask。

### 3.1 触发条件

`MutatingWebhookConfiguration` 使用 `metadata.labels.ft.hygon.io/enabled=true` 对象选择器。完整 label 和 annotation 由[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)统一说明。

Webhook 当前处理：

- `kubeflow.org/v1` 的 `PyTorchJob`；
- `batch.volcano.sh/v1alpha1` 的 `Job`。

### 3.2 核心函数

- `_workload_enabled()`：读取顶层 label/annotation 的启用状态。
- `_inject_replica_template()`：修改一个 PyTorch replica 或 Volcano task 的 Pod template。
- `mutate_pytorchjob()`：遍历 `spec.pytorchReplicaSpecs`。
- `mutate_volcano_job()`：遍历 `spec.tasks`。
- `build_patch()`：生成 AdmissionReview 使用的 JSONPatch。
- `build_admission_response()`：识别工作负载类型并构造响应。

### 3.3 注入内容

对每个训练 Pod template，Webhook 会：

1. 添加 `ft.hygon.io/enabled`、`ft.hygon.io/job-name` 和 replica role label。
2. 默认注入 initContainer，从控制器 runtime 镜像复制 `ft-launcher`。
3. 将共享 volume 挂载到训练容器的 `/opt/hygon-ft`。
4. 注入 Job、Pod、Node、日志、FaultEvent 上报地址等环境变量。
5. 将原训练命令包装为：

```text
/opt/hygon-ft/ft-launcher -- <原 command 和 args>
```

顶层 `ft.hygon.io/injected=true` 用于避免重复注入。

## 4. ft-launcher 和日志故障链路

实现位于 [`runtime/ft-launcher`](../runtime/ft-launcher)。

launcher 负责启动原训练命令、管理可选的 `FT_EXTERNAL_MONITOR_CMD`、消费故障 marker、调用 `/report` 并等待确认。marker 字段和 HTTP 契约见[LogMonitor 协议](LOG_MONITOR_INTEGRATION_CN.md)。

`ft-launcher` 本身不解析 Megatron 日志。日志判定由训练镜像或共享目录中提供的 LogMonitor 完成；控制器镜像只负责提供 launcher。

Volcano Job 应为所有 task 配置 `PodFailed/PodEvicted -> RestartJob`，保证静态分布式训练的所有 rank 同代重建。

## 5. Operator 和 FaultEvent

实现位于 [`hygon_ft/operator/controller.py`](../hygon_ft/operator/controller.py)。Operator 包含以下主路径：

- Leader election：多副本中仅 leader 执行有副作用的处理。
- Reporter HTTP 服务：接收 launcher 的故障报告并创建 FaultEvent，提供确认查询。
- FaultEvent watch：持续读取 `ft.hygon.io/v1alpha1` 事件。
- 故障聚合：在聚合窗口内选择证据更强的根故障，抑制重复或派生事件。
- 节点隔离：按事件动作给异常节点添加 `NoSchedule` taint。
- Pod 处置：按工作负载种类和事件动作删除单 Pod、同一任务 Pod 或节点上的 FT Pod。
- 状态与告警：把执行结果写入 `FaultEvent.status`，再发送可选告警。

Operator 不决定新 Pod 调度到哪个节点；实际放置仍由 kube-scheduler 或 Volcano 完成。

## 6. NodeHealth Agent

实现位于 [`hygon_ft/nodehealth/agent.py`](../hygon_ft/nodehealth/agent.py)。DaemonSet 清单位于 [`manifests/base/06-nodehealth-daemonset.yaml`](../manifests/base/06-nodehealth-daemonset.yaml)。

### 6.1 部署范围

DaemonSet 设置：

```yaml
nodeSelector:
  node-role.hygon.io/training: "true"
```

因此 NodeHealth Pod 只部署到训练节点，不是在集群每个节点运行。

### 6.2 NHC 来源

镜像不包含 NHC fallback。ConfigMap [`manifests/base/05-nodehealth-config.yaml`](../manifests/base/05-nodehealth-config.yaml) 提供的是适配脚本，它通过 `nsenter` 进入宿主机命名空间并查找、执行宿主机的 `run_nhc`。

宿主机必须提供可执行的 `run_nhc`，或通过 `NHC_HOST_COMMAND` 指定绝对路径/命令。找不到命令或输出协议无效属于 probe error，不会被当作节点硬件故障。

适配器要求宿主机 NHC 输出：

```text
[CHECK RESULT]: PASSED
```

返回值约定：

| 返回值 | NodeHealth 处理 |
|---:|---|
| `0` | 健康 |
| `2` | 节点异常，可创建 FaultEvent |
| `3` | 仅报告，不创建 FaultEvent |
| 其他 | 探测器错误，不创建 FaultEvent |

### 6.3 当前默认值

DaemonSet 和代码当前一致：

| 环境变量 | 默认/清单值 | 作用 |
|---|---:|---|
| `NHC_INTERVAL_SECONDS` | `30` | 检查间隔 |
| `NHC_TIMEOUT_SECONDS` | `300` | 单次检查超时 |
| `NHC_FAILURE_SUPPRESS_SECONDS` | `300` | 重复故障上报抑制时间 |
| `NHC_TIMEOUT_IS_FAILURE` | `false` | 超时默认只记录 probe error |
| `FT_TAINT_NODE_ON_NHC_FAIL` | `true` | NHC 明确失败时请求 taint |
| `FT_DELETE_PODS_ON_NHC_FAIL` | `false` | NHC 事件默认不直接请求删除 Pod |

明确的 NHC 失败会创建 `NodeHealthCheckFailed` FaultEvent。默认动作是请求 taint 节点、不直接设置 `deletePods=true`；后续处置由 Operator 当前策略决定。

## 7. 工作负载处置分支

`FaultController._process_fault()` 根据 FaultEvent action、故障类型和 `workloadKind` 决定是否 taint、删除当前 Pod、删除同一任务的 FT Pod或只回写状态。Volcano 与 PyTorchJob 的准确恢复条件统一见[故障处理语义](FAULT_HANDLING_CN.md)。

## 8. 镜像内容与外部依赖

[`packaging/docker/Dockerfile`](../packaging/docker/Dockerfile) 当前复制：

- `hygon_ft/`：Webhook、Operator 和 NodeHealth Agent Python 代码；
- `runtime/`：`ft-launcher`；
- Python/Kubernetes 依赖和基础命令；
- 仓库级许可证文件。

镜像不包含：

- 宿主机 `run_nhc` 实现；
- Volcano 或 Training Operator；
- 训练程序、模型、数据集和 checkpoint；
- 完整 LogMonitor Python 包。

部署入口见[安装指南](INSTALLATION_CN.md)，运行验证见[运行与恢复手册](OPERATIONS_AND_RECOVERY_CN.md)。
