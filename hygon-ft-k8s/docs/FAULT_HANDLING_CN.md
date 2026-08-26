<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# K8s 容错故障处理语义

本文是故障分类、节点隔离和工作负载恢复语义的权威说明。marker 和 HTTP 协议见[LogMonitor 集成](LOG_MONITOR_INTEGRATION_CN.md)，现场操作见[运行与恢复手册](OPERATIONS_AND_RECOVERY_CN.md)。

## 1. 公共处理规则

日志故障通过 launcher 上报，NodeHealth Agent 和 Node watch 直接创建 FaultEvent。Operator 对日志事件按 Job 和时间窗口聚合，根因优先级为：

```text
明确节点故障 > Torchrun Root Cause > 通信级联错误 > 普通训练错误
```

只有 leader 执行 taint、删除 Pod、状态回写和告警。其他 Operator 副本提供高可用，但不重复执行有副作用的动作。

`NoSchedule` 只阻止后续调度，不会驱逐已经运行的 Pod。节点隔离和训练退出/Pod 删除必须分别成功，才能形成完整恢复闭环。

## 2. 日志故障动作

LogMonitor 根据证据生成动作：

| 故障证据 | 默认 `taintNode` | 说明 |
|---|---:|---|
| 明确识别到故障节点 | `true` | 可直接隔离对应节点 |
| Torchrun Root Cause | `true` | Operator 先解析对应 Pod 和节点 |
| 通信错误 | `false` | 通常是其他故障的级联结果 |
| 普通 hang、loss、Inf、timeout | `false` | 不能仅凭当前 Pod 推断节点硬件故障 |

Operator 处理并回写 `FaultEvent.status` 后，launcher 等待确认，然后终止当前训练进程并以非零状态退出。

## 3. Volcano Job

Volcano Job 的所有 Master/Worker task 都必须配置：

```yaml
policies:
  - events:
      - PodFailed
      - PodEvicted
    action: RestartJob
```

普通日志故障的恢复链路是：

```text
Operator 处理 FaultEvent
  -> launcher 收到确认并退出 89
  -> PodFailed
  -> Volcano RestartJob
  -> 所有 rank 从 checkpoint 同代重建
```

能定位节点的根故障会先 taint 节点。当前 Operator 不主动批量删除 Volcano Job 的全部 Pod；整任务恢复由 `RestartJob` policy 保证。

## 4. PyTorchJob

PyTorchJob 的恢复粒度取决于故障证据：

- 对 `TrainingRootCause`、`TrainingNodeFault` 等可定位节点且 taint 成功的故障，Operator 按 `ft.hygon.io/job-name` 删除该 PyTorchJob 的活动 FT Pod，Training Operator 再创建整组 Pod。
- 对没有节点证据的普通 hang、loss、Inf 或 timeout，Operator 不执行整组删除；报告 Pod 的 launcher 非零退出后，由 replica 的 `restartPolicy` 处理。

因此，PyTorchJob 的 `schedulingPolicy.minAvailable` 只保证 gang scheduling，不等价于 Volcano 的确定性 `RestartJob`。静态 Megatron 训练如要求任何故障都整体重建，应在上线前验证 Training Operator 版本和现场重启策略。

## 5. NHC 明确故障

NodeHealth Agent 只在 training 节点运行，并通过 ConfigMap 适配器调用宿主机 `run_nhc`：

| 结果 | 处理 |
|---|---|
| 返回 `0` 且协议为 PASSED | 节点健康 |
| 返回 `2` | 创建 `NodeHealthCheckFailed` FaultEvent |
| 返回 `3` | 仅记录报告，不创建 FaultEvent |
| 其他返回值或协议错误 | probe error，不隔离节点 |
| 超时 | 默认只记录；`NHC_TIMEOUT_IS_FAILURE=true` 时按故障处理 |

当前 DaemonSet 对明确 NHC 故障设置 `taintNode=true`、`deletePods=false`。在 taint 成功后，Operator 根据 `NodeHealthCheckFailed` 类型查找故障节点上的 FT Job，并按任务删除活动 Pod；这一步来自 Operator 策略，不是 NHC 事件直接请求 `deletePods=true`。

## 6. Node NotReady

节点持续 `Ready=False/Unknown` 超过宽限时间后，Operator 创建 `NodeNotReady` FaultEvent，请求 taint 并删除该节点上的 FT Pod。重新创建和调度仍由 Volcano、Training Operator 及调度器完成。

## 7. 恢复成功条件

一次故障不能只以“Pod 重新出现”判定恢复成功，至少确认：

1. FaultEvent 已 `processed=true`，要求的 action 均成功。
2. 故障节点存在 Hygon FT `NoSchedule` taint。
3. 新 Pod 没有重新落到故障节点。
4. 所有 rank 属于同一轮重建，通信域重新建立。
5. 训练从有效 checkpoint 恢复并继续产生迭代日志。
