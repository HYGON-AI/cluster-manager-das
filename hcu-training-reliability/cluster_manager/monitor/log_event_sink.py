# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import shlex
import subprocess
import threading
from datetime import datetime, timezone
from typing import Mapping, Optional

from cluster_manager.config.global_config import logger
import cluster_manager.config.global_config as global_config
from cluster_manager.event.event_bus import Event


FAULT_EVENT_TYPES = {"hang", "exit", "loss", "inf", "slow", "timeout"}
K8S_MARKER_EVENT_TYPES = FAULT_EVENT_TYPES - {"slow"}

K8S_FAULT_TYPE_MAP = {
    "hang": "TrainingHang",
    "exit": "TrainingProcessExit",
    "loss": "TrainingLoss",
    "inf": "TrainingInf",
    "slow": "TrainingSlow",
    "timeout": "TrainingTimeout",
}

K8S_FAULT_REASON_MAP = {
    "hang": "training_log_hang",
    "exit": "training_process_exit",
    "loss": "training_loss_abnormal",
    "inf": "training_inf_detected",
    "slow": "training_step_slow",
    "timeout": "training_log_timeout",
}


def is_fault_event(event: Event) -> bool:
    payload = event.payload or {}
    return payload.get("type") in FAULT_EVENT_TYPES


def event_to_record(event: Event) -> dict:
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "log_monitor",
        "event_type": event.type,
        "payload": event.payload or {},
    }


def _first_value(*values):
    for value in values:
        if value is not None and value != "":
            return value
    return ""


def _detected_node_from_log(data: Mapping, detail_type: str) -> str:
    """Return a node explicitly identified by LogMonitor's parsed log data."""
    detected_node = _first_value(
        data.get("node"),
        data.get("fault_node"),
        data.get("target_node"),
        data.get("node_name"),
    )
    if detail_type == "node":
        detected_node = _first_value(
            detected_node,
            data.get("fault_info"),
            data.get("fault_pid"),
            data.get("fault_rank"),
        )
    return str(detected_node)


def _pod_name_from_host(host: str) -> str:
    """Normalize a torchrun host/FQDN to the Kubernetes Pod name."""
    return str(host or "").strip().split(".", 1)[0]


def build_k8s_fault_report(event: Event, environ: Optional[Mapping[str, str]] = None) -> dict:
    """Convert a LogMonitor event to the ft-operator /report contract."""
    env = environ if environ is not None else os.environ
    payload = event.payload or {}
    data = payload.get("data", {}) or {}
    event_type = str(payload.get("type") or "unknown")
    detail_type = str(data.get("type") or "").lower()
    detected_node = _detected_node_from_log(data, detail_type)
    is_node_fault = event_type == "exit" and detail_type == "node"
    is_root_cause = detail_type == "root_cause"
    is_communication = detail_type == "communication"

    current_pod_name = str(env.get("FT_POD_NAME", ""))
    root_cause_pod = _pod_name_from_host(data.get("host", "")) if is_root_cause else ""
    pod_name = str(_first_value(root_cause_pod, current_pod_name))
    contextual_node = env.get("FT_NODE_NAME", "")
    if root_cause_pod and current_pod_name and root_cause_pod != current_pod_name:
        contextual_node = ""
    node_name = str(_first_value(detected_node, contextual_node))
    pod_namespace = str(env.get("FT_POD_NAMESPACE", ""))

    fault_type = K8S_FAULT_TYPE_MAP.get(event_type, "TrainingFault")
    if event_type == "loss" and "nan" in detail_type:
        fault_type = "TrainingNaN"
    elif is_node_fault:
        fault_type = "TrainingNodeFault"
    elif is_root_cause:
        fault_type = "TrainingRootCause"

    # Explicit node evidence is strongest; torchrun's first-observed root cause
    # is also eligible after its Pod has been resolved to a node.
    if detected_node:
        fault_class = "explicit_node"
        confidence = 100
    elif is_root_cause:
        fault_class = "root_cause"
        confidence = 80
    elif is_communication:
        fault_class = "communication"
        confidence = 40
    else:
        fault_class = "generic"
        confidence = 20

    # The launcher exits only after the Operator has processed this event. Pod
    # lifecycle remains owned by Volcano's PodFailed -> RestartJob policy.
    action = {
        "taintNode": bool(detected_node or is_root_cause),
        "deletePod": False,
        "deletePods": False,
    }

    report = {
        "type": fault_type,
        "severity": env.get("LOG_MONITOR_K8S_FAULT_SEVERITY", "Critical"),
        "source": "log-monitor",
        "reason": K8S_FAULT_REASON_MAP.get(event_type, "log_monitor_fault"),
        "message": format_fault_message(event),
        "observedAt": datetime.now(timezone.utc).isoformat(),
        "nodeName": node_name,
        "podNamespace": pod_namespace,
        "podName": pod_name,
        "jobName": env.get("FT_JOB_NAME", ""),
        "workloadKind": env.get("FT_WORKLOAD_KIND", ""),
        "replicaRole": env.get("FT_REPLICA_ROLE", ""),
        "faultClass": fault_class,
        "confidence": confidence,
        "action": action,
    }
    optional_fields = {
        "host": data.get("host"),
        "rank": data.get("rank"),
        "localRank": data.get("local_rank"),
        "exitCode": data.get("exit_code"),
    }
    report.update(
        {key: value for key, value in optional_fields.items() if value not in (None, "")}
    )
    return report


def build_k8s_fault_marker(event: Event, environ: Optional[Mapping[str, str]] = None) -> dict:
    marker = build_k8s_fault_report(event, environ)
    marker["launcherAction"] = "faultevent"
    return marker


def format_fault_message(event: Event) -> str:
    payload = event.payload or {}
    data = payload.get("data", {}) or {}
    event_type = payload.get("type", "unknown")
    lines = [
        "----日志监控检测到训练异常----",
        f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"异常类型: {event_type}",
    ]

    cur_iter = payload.get("cur_iter") or payload.get("iter")
    if cur_iter is not None:
        lines.append(f"当前iter: {cur_iter}")

    if event_type == "hang":
        lines.append("异常信息: 训练日志长时间无更新或启动后未产生日志")
    elif event_type == "exit":
        lines.append(f"故障类型: {data.get('type', 'unknown')}")
        if data.get("host"):
            lines.append(f"host: {data.get('host')}")
        if data.get("rank") is not None:
            lines.append(f"rank: {data.get('rank')} local_rank: {data.get('local_rank')}")
        fault_info = data.get("fault_info") or data.get("fault_pid")
        if fault_info:
            lines.append(f"故障信息: {fault_info}")
        if data.get("exit_code") is not None:
            lines.append(f"退出码: {data.get('exit_code')}")
    elif event_type in ("loss", "inf"):
        lines.append(f"故障类型: {data.get('type', event_type)}")
        if data.get("rank") is not None:
            lines.append(f"故障rank: {data.get('rank')}")
        if data.get("node"):
            lines.append(f"故障节点: {data.get('node')}")
        if data.get("message"):
            lines.append(f"异常信息: {data.get('message')}")
    elif event_type == "slow":
        lines.append(f"异常iter: {payload.get('iter')}")
        lines.append(f"当前耗时: {payload.get('iter_time')}ms")
        lines.append(f"平均耗时: {payload.get('avg_time')}ms")
    else:
        message = payload.get("message")
        if message:
            lines.append(f"异常信息: {message}")

    return "\n".join(lines)


class EventBusLogSink:
    def __init__(self, event_bus):
        self.event_bus = event_bus

    def publish(self, event: Event):
        if not self.event_bus:
            logger.warning("[LogEventSink] event_bus is empty, drop event: %s", event)
            return
        self.event_bus.publish(event)


class FeishuLogSink:
    def __init__(self, notify):
        self.notify = notify

    def publish(self, event: Event):
        if not is_fault_event(event):
            logger.debug("[LogEventSink] standalone mode skip non-fault event: %s", event)
            return
        self.notify.send_feishu_alert(format_fault_message(event))


class K8sLogSink:
    """
    Kubernetes adapter for log-monitor fault events.

    Faults are written atomically to FT_FAULT_MARKER_FILE. ft-launcher consumes
    the marker and decides whether to terminate locally or report a FaultEvent.
    JSONL is retained as a local audit record.
    """

    def __init__(
        self,
        event_file: Optional[str] = None,
        marker_file: Optional[str] = None,
        command: Optional[str] = None,
    ):
        workspace = os.path.join(global_config.WORK_DIR, "workspace")
        self.event_file = (
            event_file
            or os.environ.get("LOG_MONITOR_K8S_EVENT_FILE")
            or os.path.join(workspace, "k8s_log_monitor_events.jsonl")
        )
        self.marker_file = (
            marker_file
            or os.environ.get("LOG_MONITOR_K8S_MARKER_FILE")
            or os.environ.get("FT_FAULT_MARKER_FILE")
            or "/tmp/hygon-ft-fault.json"
        )
        self.command = command or os.environ.get("LOG_MONITOR_K8S_EVENT_CMD")
        self._marker_lock = threading.Lock()

    def _write_marker(self, marker: dict) -> bool:
        marker_dir = os.path.dirname(self.marker_file)
        temp_file = self.marker_file + ".tmp"
        marker_payload = json.dumps(marker, ensure_ascii=False, default=str)

        with self._marker_lock:
            if os.path.exists(self.marker_file):
                logger.warning(
                    "[LogEventSink] fault marker already exists, keep existing marker: %s",
                    self.marker_file,
                )
                return False

            try:
                if marker_dir:
                    os.makedirs(marker_dir, exist_ok=True)
                with open(temp_file, "w", encoding="utf-8") as f:
                    f.write(marker_payload)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_file, self.marker_file)
                logger.info("[LogEventSink] k8s fault marker written to %s", self.marker_file)
                return True
            except Exception as e:
                logger.exception("[LogEventSink] failed to write k8s fault marker: %s", e)
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except OSError:
                    logger.warning("[LogEventSink] failed to remove temporary marker %s", temp_file)
                return False

    def publish(self, event: Event):
        if not is_fault_event(event):
            logger.debug("[LogEventSink] k8s mode skip non-fault event: %s", event)
            return

        record = event_to_record(event)
        marker = build_k8s_fault_marker(event)
        record["fault_marker"] = marker
        payload = json.dumps(record, ensure_ascii=False, default=str)

        try:
            event_dir = os.path.dirname(self.event_file)
            if event_dir:
                os.makedirs(event_dir, exist_ok=True)
            with open(self.event_file, "a", encoding="utf-8") as f:
                f.write(payload + "\n")
            logger.info("[LogEventSink] k8s fault event written to %s", self.event_file)
        except Exception as e:
            logger.exception("[LogEventSink] failed to write k8s event file: %s", e)

        event_type = str((event.payload or {}).get("type") or "")
        if event_type in K8S_MARKER_EVENT_TYPES:
            self._write_marker(marker)
        else:
            logger.info("[LogEventSink] k8s event type=%s is audit-only; marker not written", event_type)

        if not self.command:
            return

        try:
            command = shlex.split(self.command)
            if not command:
                logger.warning("[LogEventSink] empty k8s event command; skip")
                return
            proc = subprocess.run(
                command,
                input=payload,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=30,
                check=False,
            )
            if proc.returncode != 0:
                logger.warning(
                    "[LogEventSink] k8s event command failed, code=%s, output=%s",
                    proc.returncode,
                    (proc.stdout or "")[:1000],
                )
        except Exception as e:
            logger.exception("[LogEventSink] failed to run k8s event command: %s", e)


def create_log_event_sink(mode: str, notify=None, event_bus=None):
    mode = (mode or "").lower()
    if mode in ("fault_tolerance", "cluster", "event_bus"):
        return EventBusLogSink(event_bus)
    if mode == "k8s":
        return K8sLogSink()
    if mode == "standalone":
        return FeishuLogSink(notify)
    if event_bus is not None:
        return EventBusLogSink(event_bus)
    return FeishuLogSink(notify)
