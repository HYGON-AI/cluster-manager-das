<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# LogMonitor、marker 与故障上报协议

本文只说明 LogMonitor 与 `ft-launcher`、Operator 之间的运行时协议。任务 annotation 的完整定义见[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)，不同工作负载的重启策略见[故障处理流程](FAULT_HANDLING_CN.md)。

## 1. 组件关系

```text
训练 Pod
  -> ft-launcher 启动训练命令并保存 stdout/stderr
  -> ft-launcher 启动可选的 LogMonitor
  -> LogMonitor 读取 FT_LOG_FILE
  -> 发现故障后原子写入 FT_FAULT_MARKER_FILE
  -> ft-launcher POST /report
  -> Operator 创建并处理 FaultEvent
  -> ft-launcher GET /report/<event-name> 等待确认
  -> 确认完成后终止训练并非零退出
```

Webhook 默认向所有活动 replica 注入外部监控命令，即 `ft.hygon.io/log-monitor-roles` 的默认值为 `all`。Worker 默认关闭周期通知；日志无更新检测只在最后一个活动 replica 组启用，避免多个 Pod 对同一训练停顿重复告警。

如明确只需要 Master 日志，可以设置 `ft.hygon.io/log-monitor-roles: master`，但这样无法发现仅出现在 Worker 本地日志中的异常。

## 2. LogMonitor 接入约定

控制器 runtime 镜像只向训练容器复制 `ft-launcher`，不提供完整 LogMonitor Python 包。训练容器必须通过以下方式之一提供 LogMonitor：

- 训练镜像内安装 LogMonitor 和依赖；
- 挂载共享代码目录，并让 `log-monitor-command` 指向其中的入口。

命令必须读取 Webhook 注入的日志文件：

```yaml
ft.hygon.io/log-monitor-command: >-
  python /path/to/log_monitor.py --log-file "${FT_LOG_FILE}" --mode k8s
```

相关环境变量由 Webhook 和 launcher 提供：

| 变量 | 作用 |
|---|---|
| `FT_LOG_FILE` | 当前 Pod 的训练日志文件 |
| `FT_LOG_DIR` | 训练、launcher 和监控审计日志目录 |
| `FT_FAULT_MARKER_FILE` | LogMonitor 与 launcher 的故障 marker |
| `LOG_MONITOR_K8S_EVENT_FILE` | LogMonitor JSONL 审计文件 |
| `FT_POD_NAME`、`FT_POD_UID` | 当前 Pod 身份 |
| `FT_POD_NAMESPACE`、`FT_NODE_NAME` | Pod namespace 和节点 |
| `FT_JOB_NAME`、`FT_WORKLOAD_KIND` | 工作负载上下文 |
| `FT_REPLICA_ROLE` | Master、Worker 或 Volcano task 名 |

## 3. marker 文件协议

LogMonitor 在 K8s 模式下将故障写入 `FT_FAULT_MARKER_FILE`。当前项目生成的 marker 包含：

```json
{
  "type": "TrainingHang",
  "severity": "Critical",
  "source": "log-monitor",
  "reason": "training_log_hang",
  "message": "training log stopped updating",
  "nodeName": "train-node-3",
  "podNamespace": "default",
  "podName": "llama-worker-2",
  "jobName": "llama-train",
  "workloadKind": "VolcanoJob",
  "replicaRole": "worker",
  "faultClass": "explicit_node",
  "confidence": 100,
  "action": {
    "taintNode": true,
    "deletePod": false,
    "deletePods": false
  },
  "launcherAction": "faultevent"
}
```

marker 使用临时文件加原子替换写入。已有 marker 尚未消费时，LogMonitor 保留第一个故障，避免后续级联错误覆盖更早的根因证据。

`launcherAction=faultevent` 表示先上报并等待 Operator；`kill`、`terminate`、`local-kill` 或 `kill-process` 表示只在当前 Pod 内终止训练，不进入 FaultEvent 确认链路。

## 4. `/report` 接口

默认地址：

```text
http://ft-operator.hygon-ft.svc.cluster.local:8080/report
```

`ft-launcher` 将 marker 内容 POST 到该接口。Operator 使用自己的 RBAC 创建 FaultEvent，训练 Pod 无需获得创建 CRD 或访问 Kubernetes API 的权限。

成功响应包含 FaultEvent 名称。launcher 随后轮询：

```text
GET /report/<FaultEvent名称>
```

只有响应同时满足以下条件，launcher 才认为故障处置成功：

- `processed=true`；
- `readyToRestart=true`。

默认确认超时为 30 秒、轮询间隔为 1 秒。动作失败或等待超时会返回非零状态，不会把未完成隔离误报为成功。

## 5. 故障分类

LogMonitor 的 K8s 适配会将本地事件转换为统一报告：

| 日志事件 | FaultEvent 类型 |
|---|---|
| hang | `TrainingHang` |
| 进程退出 | `TrainingProcessExit` |
| loss/NaN | `TrainingLoss` 或 `TrainingNaN` |
| Inf | `TrainingInf` |
| 日志超时 | `TrainingTimeout` |

根因证据优先级为：明确节点、Torchrun Root Cause、通信错误、普通错误。只有发现明确节点或 Root Cause 时，日志报告才默认请求 taint；普通 loss、hang 或 timeout 不会凭当前 Pod 所在节点推断硬件故障。

## 6. 与 NHC 的区别

| 来源 | 上报方式 | 节点证据 |
|---|---|---|
| LogMonitor | marker → launcher → `/report` | 可能有，也可能没有 |
| NodeHealth Agent | 直接创建 FaultEvent | 明确为当前训练节点 |

NHC 的返回码、超时和 Pod 处置规则见[故障处理流程](FAULT_HANDLING_CN.md)。

## 7. 飞书与外部告警

告警由 Operator 统一发送，不由训练 Pod 直接发送。可以创建 Secret 配置飞书 Webhook：

```bash
kubectl -n hygon-ft create secret generic hygon-ft-alert \
  --from-literal=feishu-webhook-url='https://open.feishu.cn/open-apis/bot/v2/hook/REPLACE_ME'
kubectl -n hygon-ft rollout restart deployment/ft-operator
```

也可以配置现有告警命令：

```bash
kubectl -n hygon-ft create secret generic hygon-ft-alert \
  --from-literal=alert-command='/opt/hygon-alert/bin/feishu-alert'
```

Operator 会通过标准输入向该命令传递故障 JSON。

## 8. 实现位置

| 文件 | 职责 |
|---|---|
| `cluster_manager/cluster_manager/monitor/log_event_sink.py` | 将日志事件转换为 K8s 报告并原子写 marker |
| `runtime/ft-launcher` | 管理训练/监控进程，上报 marker 并等待确认 |
| `hygon_ft/webhook/server.py` | 注入日志和上报环境变量 |
| `hygon_ft/operator/controller.py` | 提供 `/report`、创建和处理 FaultEvent |
