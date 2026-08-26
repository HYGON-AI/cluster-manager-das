<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Hygon FT Kubernetes 架构说明

本文只介绍 `hygon-ft-k8s` 的设计边界、组件职责和高层数据流。具体故障决策见[故障处理语义](FAULT_HANDLING_CN.md)，部署与使用方法参见项目根目录的 `README.md`。

## 1. 设计边界

`hygon-ft-k8s` 为 PyTorchJob 和 Volcano Job 提供训练日志监控、节点健康检查、故障聚合、节点隔离和任务恢复能力。

它不负责 HCU 调度，也不替代以下组件：

- Kubernetes kube-scheduler
- Volcano
- Kubeflow Training Operator
- HCU、RDMA 等设备插件
- 训练框架自身的 checkpoint 和数据恢复机制

训练任务能否从断点继续，最终取决于训练脚本是否正确保存和加载 checkpoint。

## 2. 核心组件

| 组件 | 部署方式 | 作用 |
|---|---|---|
| `ft-webhook` | Deployment | 拦截启用容错的 PyTorchJob/Volcano Job，向训练 Pod 注入 ft-launcher、Pod 上下文和容错环境变量 |
| `ft-operator` | Deployment | 接收并聚合 FaultEvent，选择根因，taint 节点，删除训练 Pod，回写处理状态并发送告警 |
| `nodehealth-agent` | DaemonSet | 在训练节点周期执行 NHC，发现节点异常后创建明确节点类型的 FaultEvent |
| `ft-launcher` | 每个训练 Pod | 包装训练命令、保存日志、启动 LogMonitor、读取 marker 并向 Operator 上报 |
| `LogMonitor` | 每个训练 Pod | 解析训练日志，识别进程退出、通信错误、NaN/Inf、Torchrun Root Cause 和 hang |
| `FaultEvent` | CRD | 在 LogMonitor、NodeHealth Agent、ft-launcher 和 Operator 之间传递故障及处理状态 |

## 3. 部署拓扑

节点分为两个逻辑角色：

| 节点角色 | 运行组件 | 节点配置 |
|---|---|---|
| system | Operator、Webhook | `node-role.hygon.io/system=true`，可选 `NoSchedule` taint |
| training | 训练 Pod、NodeHealth Agent | `node-role.hygon.io/training=true`、`accelerator.hygon.io/enabled=true` |

两类节点可以重叠，但 system 节点启用 `NoSchedule` taint 时不应同时承担训练任务。生产环境建议使用独立 system 节点。

## 4. 日志故障处理链路

```text
训练进程输出日志
  -> 每个训练 Pod 的 LogMonitor 解析本地日志
  -> 检测到故障后写入 marker
  -> ft-launcher 读取 marker 并 POST /report
  -> Operator 创建 FaultEvent
  -> 聚合同一任务短时间内的多个 FaultEvent
  -> 选择高置信根因并定位节点
  -> 有明确节点时先添加 ft.hygon.io/node-unhealthy:NoSchedule
  -> Operator 回写 FaultEvent.status
  -> ft-launcher 确认处理成功后退出
  -> Volcano 或 Training Operator 重建训练任务
```

Operator 的根因选择顺序为：

```text
明确节点故障
> Torchrun Root Cause
> 通信级联错误
> 普通训练错误
```

多个训练 Pod 同时发现异常时，Operator 只处理聚合窗口内优先级最高且最早出现的事件，其余事件标记为 suppressed，避免重复 taint 和重复重建。

## 5. 节点故障处理链路

### 5.1 NHC 故障

```text
NodeHealth Agent 周期执行 NHC
  -> NHC 返回节点故障
  -> 创建 NodeHealthCheckFailed FaultEvent
  -> Operator taint 故障节点
  -> 处理运行在该节点上的 FT 训练任务
  -> 工作负载控制器在健康节点重建训练 Pod
```

NHC 探针自身异常默认只记录日志，不会直接隔离节点，避免把检查工具故障误判为硬件故障。

### 5.2 Node NotReady

Operator 监听 Node Ready condition。节点持续处于 `False` 或 `Unknown` 并超过宽限时间后，创建 `NodeNotReady` FaultEvent，隔离节点并处理该节点上的训练 Pod。

## 6. 工作负载恢复

| 工作负载 | 恢复控制器 | 恢复方式 |
|---|---|---|
| Volcano Job | Volcano Controller | launcher 非零退出触发 `PodFailed -> RestartJob`，重建整个 Job |
| PyTorchJob | FT Operator + Kubeflow Training Operator | 可定位节点的根故障可触发整组 FT Pod 删除；普通故障由当前 replica 的 `restartPolicy` 处理 |

PyTorchJob 通过 Volcano PodGroup 实现 gang scheduling。资源不足总副本数时，训练 Pod 保持 Pending，避免部分 rank 先运行并占用资源。

## 7. 节点恢复

故障节点修复后，`start.sh recover` 会在目标节点创建临时特权 Pod，并通过 `nsenter` 调用该节点 PATH 中预装的 `run_nhc`。恢复流程不包含 `node_check` 源码，也不使用控制器镜像内的 NHC 回退副本。

全部轮次通过后，只删除 Hygon FT 管理的 `ft.hygon.io/node-unhealthy:NoSchedule`，不修改其他系统的 taint。

## 8. 代码边界

Kubernetes 模块的目录职责见[目录结构](DIRECTORY_STRUCTURE_CN.md)，关键函数和资源调用关系见[代码走读](CODE_WALKTHROUGH_CN.md)。原 `cluster_manager/` 下的 mpirun 容错链路保持独立，不受 K8s 控制面安装和卸载影响。
