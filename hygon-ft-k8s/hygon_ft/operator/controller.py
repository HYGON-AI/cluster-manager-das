# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

from kubernetes import client, config, watch
from kubernetes.client import ApiException


GROUP = "ft.hygon.io"
VERSION = "v1alpha1"
PLURAL = "faultevents"
FT_ENABLED_SELECTOR = "ft.hygon.io/enabled=true"
FT_JOB_LABEL = "ft.hygon.io/job-name"

FAULT_CLASS_PRIORITY = {
    "explicit_node": 300,
    "root_cause": 200,
    "communication": 100,
    "generic": 0,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_k8s_time(value) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not value:
        return datetime.fromtimestamp(0, timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(0, timezone.utc)


def safe_name(value: str, max_len: int = 50) -> str:
    text = re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip("-.")
    return (text or "unknown")[:max_len].strip("-.") or "unknown"


def pod_name_from_host(host: str) -> str:
    return str(host or "").strip().split(".", 1)[0]


def optional_int(value) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def infer_fault_class(fault: Dict) -> str:
    spec = fault.get("spec", {}) or {}
    fault_class = str(spec.get("faultClass") or "").lower()
    if fault_class in FAULT_CLASS_PRIORITY:
        return fault_class

    fault_type = str(spec.get("type") or "").lower()
    if fault_type in {"nodehealthcheckfailed", "nodenotready", "trainingnodefault"}:
        return "explicit_node"
    if fault_type == "trainingrootcause":
        return "root_cause"
    if "communication" in fault_type or "connection" in fault_type:
        return "communication"
    return "generic"


def select_root_fault(faults: List[Dict]) -> Optional[Dict]:
    """Pick highest-confidence evidence, then the earliest observation."""
    if not faults:
        return None

    def sort_key(fault: Dict) -> Tuple[int, datetime, str]:
        metadata = fault.get("metadata", {}) or {}
        spec = fault.get("spec", {}) or {}
        priority = FAULT_CLASS_PRIORITY[infer_fault_class(fault)]
        observed_at = parse_k8s_time(spec.get("observedAt") or metadata.get("creationTimestamp"))
        return -priority, observed_at, str(metadata.get("name") or "")

    return min(faults, key=sort_key)


def fault_event_restart_readiness(fault: Dict) -> Tuple[bool, bool, str]:
    """Return whether an event is processed and safe for the launcher to exit."""
    status = fault.get("status", {}) or {}
    if status.get("processed") is not True:
        return False, False, "FaultEvent is still pending"

    actions = status.get("actions", []) or []
    if any(action.get("type") == "Suppressed" and action.get("success") is True for action in actions):
        return True, True, "FaultEvent was aggregated under the selected root fault"

    failed_actions = [
        action
        for action in actions
        if action.get("type") in {"TaintNode", "DeletePod"}
        and action.get("success") is False
    ]
    if failed_actions:
        messages = [str(action.get("message") or "unknown error") for action in failed_actions]
        return True, False, f"fault handling action failed: {'; '.join(messages)}"

    requested = (fault.get("spec", {}) or {}).get("action", {}) or {}
    required_actions = []
    if requested.get("taintNode"):
        required_actions.append("TaintNode")
    if requested.get("deletePod") or requested.get("deletePods"):
        required_actions.append("DeletePod")

    for action_type in required_actions:
        matching = [action for action in actions if action.get("type") == action_type]
        if not matching:
            return True, False, f"required action {action_type} was not recorded"
        if not any(action.get("success") is True for action in matching):
            messages = [str(action.get("message") or "unknown error") for action in matching]
            return True, False, f"required action {action_type} failed: {'; '.join(messages)}"

    return True, True, "FaultEvent actions completed"


def load_kube() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


class FaultController:
    def __init__(self) -> None:
        self.namespace = os.getenv("POD_NAMESPACE", "hygon-ft")
        self.identity = os.getenv("POD_NAME") or socket.gethostname()
        self.custom_api = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()
        self.coordination_api = client.CoordinationV1Api()
        self.taint_key = os.getenv("FT_UNHEALTHY_TAINT_KEY", "ft.hygon.io/node-unhealthy")
        self.taint_effect = os.getenv("FT_UNHEALTHY_TAINT_EFFECT", "NoSchedule")
        self.delete_grace_seconds = int(os.getenv("FT_DELETE_POD_GRACE_SECONDS", "0"))
        self.delete_pod_after_taint = os.getenv("FT_DELETE_POD_AFTER_TAINT", "true").lower() == "true"
        self.feishu_webhook_url = os.getenv("FEISHU_WEBHOOK_URL", "")
        self.alert_command = os.getenv("FT_ALERT_COMMAND", "")
        self.reporter_port = int(os.getenv("FT_REPORTER_PORT", "8080"))
        self.leader_election_enabled = os.getenv("FT_LEADER_ELECTION", "true").lower() == "true"
        self.lease_name = os.getenv("FT_LEADER_LEASE_NAME", "ft-operator-leader")
        self.lease_duration_seconds = int(os.getenv("FT_LEASE_DURATION_SECONDS", "30"))
        self.leader_renew_seconds = int(os.getenv("FT_LEADER_RENEW_SECONDS", "10"))
        self.enable_node_watch = os.getenv("FT_ENABLE_NODE_NOT_READY_WATCH", "true").lower() == "true"
        self.node_not_ready_grace_seconds = int(os.getenv("FT_NODE_NOT_READY_GRACE_SECONDS", "60"))
        self.aggregate_window_seconds = float(os.getenv("FT_FAULT_AGGREGATE_WINDOW_SECONDS", "5"))
        self.ranks_per_pod = int(os.getenv("FT_RANKS_PER_POD", "8"))
        self._leader_lock = threading.Lock()
        self._is_leader = not self.leader_election_enabled
        self._node_not_ready_since: Dict[str, float] = {}
        self._node_not_ready_event_sent = set()
        self._aggregation_lock = threading.Lock()
        self._pending_faults: Dict[str, Dict[str, Dict]] = {}
        self._aggregation_timers: Dict[str, threading.Timer] = {}

    def is_leader(self) -> bool:
        with self._leader_lock:
            return self._is_leader

    def set_leader(self, value: bool) -> None:
        with self._leader_lock:
            changed = self._is_leader != value
            self._is_leader = value
        if changed:
            state = "leader" if value else "standby"
            print(f"[operator] identity={self.identity} is now {state}", flush=True)

    def start_leader_election(self) -> None:
        if not self.leader_election_enabled:
            self.set_leader(True)
            return
        thread = threading.Thread(target=self.leader_election_loop, daemon=True)
        thread.start()

    def leader_election_loop(self) -> None:
        while True:
            try:
                self.try_acquire_or_renew_lease()
            except Exception as exc:
                print(f"[operator] leader election error: {exc}", flush=True)
                self.set_leader(False)
            time.sleep(self.leader_renew_seconds)

    def try_acquire_or_renew_lease(self) -> None:
        now = datetime.now(timezone.utc)
        try:
            lease = self.coordination_api.read_namespaced_lease(self.lease_name, self.namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
            body = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": self.lease_name, "namespace": self.namespace},
                "spec": {
                    "holderIdentity": self.identity,
                    "leaseDurationSeconds": self.lease_duration_seconds,
                    "renewTime": now_iso(),
                },
            }
            self.coordination_api.create_namespaced_lease(self.namespace, body)
            self.set_leader(True)
            return

        holder = lease.spec.holder_identity if lease.spec else ""
        renew_time = parse_k8s_time(lease.spec.renew_time if lease.spec else None)
        expired = (now - renew_time).total_seconds() > self.lease_duration_seconds
        if holder in {"", self.identity} or expired:
            body = {
                "spec": {
                    "holderIdentity": self.identity,
                    "leaseDurationSeconds": self.lease_duration_seconds,
                    "renewTime": now_iso(),
                }
            }
            self.coordination_api.patch_namespaced_lease(self.lease_name, self.namespace, body)
            self.set_leader(True)
        else:
            self.set_leader(False)

    def start_reporter_server(self) -> None:
        controller = self

        class ReporterHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == "/healthz":
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                    return

                prefix = "/report/"
                if parsed.path.startswith(prefix):
                    event_name = urllib.parse.unquote(parsed.path[len(prefix):]).strip()
                    if not event_name or "/" in event_name:
                        self.send_response(400)
                        self.end_headers()
                        return
                    try:
                        fault = controller.custom_api.get_namespaced_custom_object(
                            GROUP,
                            VERSION,
                            controller.namespace,
                            PLURAL,
                            event_name,
                        )
                        processed, ready, message = fault_event_restart_readiness(fault)
                        response = json.dumps(
                            {
                                "ok": True,
                                "faultEvent": event_name,
                                "processed": processed,
                                "readyToRestart": ready,
                                "message": message,
                                "actions": (fault.get("status", {}) or {}).get("actions", []),
                            }
                        ).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response)
                    except ApiException as exc:
                        response = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                        self.send_response(404 if exc.status == 404 else 500)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response)
                    except Exception as exc:
                        response = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                        self.send_response(500)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response)
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.path != "/report":
                    self.send_response(404)
                    self.end_headers()
                    return

                try:
                    content_length = int(self.headers.get("Content-Length", "0"))
                    raw_body = self.rfile.read(content_length)
                    payload = json.loads(raw_body.decode("utf-8"))
                    event_name = controller.create_fault_event_from_report(payload)
                    response = json.dumps({"ok": True, "faultEvent": event_name}).encode("utf-8")
                    self.send_response(201)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                except Exception as exc:
                    response = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)

            def log_message(self, format: str, *args) -> None:
                print(f"[operator-reporter] {format % args}", flush=True)

        server = ThreadingHTTPServer(("0.0.0.0", self.reporter_port), ReporterHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        print(f"[operator-reporter] listening on :{self.reporter_port}", flush=True)

    def resolve_pod_node(self, namespace: str, pod_name: str) -> str:
        if not namespace or not pod_name:
            return ""
        try:
            pod = self.core_api.read_namespaced_pod(pod_name, namespace)
            return str(pod.spec.node_name or "")
        except ApiException as exc:
            if exc.status != 404:
                print(
                    f"[operator-reporter] resolve pod node failed {namespace}/{pod_name}: {exc}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"[operator-reporter] resolve pod node failed {namespace}/{pod_name}: {exc}",
                flush=True,
            )
        return ""

    @staticmethod
    def _pod_node_rank(pod) -> Tuple[int, str]:
        labels = pod.metadata.labels or {}
        role = str(labels.get("ft.hygon.io/replica-role") or "").lower()
        name = str(pod.metadata.name or "")
        if role == "master":
            return 0, name
        ordinal_match = re.search(r"-(\d+)$", name)
        ordinal = int(ordinal_match.group(1)) if ordinal_match else 0
        if role == "worker":
            return ordinal + 1, name
        return 100000 + ordinal, name

    def resolve_pod_from_rank(
        self,
        namespace: str,
        job_name: str,
        rank: Optional[int],
    ) -> Tuple[str, str]:
        if not namespace or not job_name or rank is None or self.ranks_per_pod <= 0:
            return "", ""
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace,
                label_selector=f"{FT_JOB_LABEL}={job_name}",
            ).items
        except Exception as exc:
            print(f"[operator-reporter] rank-to-pod query failed: {exc}", flush=True)
            return "", ""

        target_node_rank = int(rank) // self.ranks_per_pod
        for pod in pods:
            node_rank, pod_name = self._pod_node_rank(pod)
            if node_rank == target_node_rank:
                return pod_name, str(pod.spec.node_name or "")
        return "", ""

    def create_fault_event_from_report(self, payload: Dict) -> str:
        source = payload.get("source") or "ft-launcher"
        event_type = payload.get("type") or "TrainingFault"
        pod_name = payload.get("podName") or payload.get("pod_name")
        pod_namespace = payload.get("podNamespace") or payload.get("pod_namespace")
        node_name = payload.get("nodeName") or payload.get("node_name")
        job_name = payload.get("jobName") or payload.get("job_name")
        workload_kind = payload.get("workloadKind") or payload.get("workload_kind")
        replica_role = payload.get("replicaRole") or payload.get("replica_role")
        host = payload.get("host") or ""
        rank = optional_int(payload.get("rank"))
        local_rank = optional_int(payload.get("localRank"))
        exit_code = optional_int(payload.get("exitCode"))
        if host:
            pod_name = pod_name_from_host(host)
        elif rank is not None:
            mapped_pod, mapped_node = self.resolve_pod_from_rank(
                pod_namespace,
                job_name,
                rank,
            )
            pod_name = mapped_pod or pod_name
            node_name = mapped_node or node_name
        if pod_name and not node_name:
            node_name = self.resolve_pod_node(pod_namespace, pod_name)
        if not pod_name and rank is not None:
            pod_name, mapped_node = self.resolve_pod_from_rank(pod_namespace, job_name, rank)
            node_name = node_name or mapped_node
        action = payload.get("action") or {"taintNode": False, "deletePod": True, "deletePods": False}
        if not isinstance(action, dict):
            action = {"taintNode": False, "deletePod": True, "deletePods": False}

        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "FaultEvent",
            "metadata": {
                "generateName": "launcher-",
                "namespace": self.namespace,
                "labels": {
                    "ft.hygon.io/source": source,
                },
            },
            "spec": {
                "type": event_type,
                "severity": payload.get("severity", "Critical"),
                "source": source,
                "reason": payload.get("reason", event_type),
                "message": payload.get("message", ""),
                "observedAt": payload.get("observedAt", now_iso()),
                "nodeName": node_name or "",
                "podNamespace": pod_namespace or "",
                "podName": pod_name or "",
                "jobName": job_name or "",
                "workloadKind": workload_kind or "",
                "replicaRole": replica_role or "",
                "faultClass": payload.get("faultClass", "generic"),
                "confidence": optional_int(payload.get("confidence")) or 0,
                "action": action,
            },
        }
        optional_fields = {
            "host": host,
            "rank": rank,
            "localRank": local_rank,
            "exitCode": exit_code,
        }
        body["spec"].update(
            {key: value for key, value in optional_fields.items() if value not in (None, "")}
        )
        created = self.custom_api.create_namespaced_custom_object(GROUP, VERSION, self.namespace, PLURAL, body)
        name = created.get("metadata", {}).get("name", "")
        print(f"[operator-reporter] created FaultEvent {self.namespace}/{name} from {source}", flush=True)
        return name

    def run(self) -> None:
        print("[operator] starting FaultEvent controller", flush=True)
        while True:
            if not self.is_leader():
                time.sleep(2)
                continue
            stream = watch.Watch()
            try:
                for event in stream.stream(
                    self.custom_api.list_namespaced_custom_object,
                    GROUP,
                    VERSION,
                    self.namespace,
                    PLURAL,
                    timeout_seconds=300,
                ):
                    if not self.is_leader():
                        break
                    obj = event.get("object", {})
                    if event.get("type") in {"ADDED", "MODIFIED"}:
                        self.reconcile(obj)
            except Exception as exc:
                print(f"[operator] watch loop error: {exc}", flush=True)
                time.sleep(5)

    def start_node_not_ready_watch(self) -> None:
        if not self.enable_node_watch:
            return
        thread = threading.Thread(target=self.node_not_ready_watch_loop, daemon=True)
        thread.start()

    def node_not_ready_watch_loop(self) -> None:
        print("[operator] starting Node NotReady watcher", flush=True)
        while True:
            if not self.is_leader():
                time.sleep(2)
                continue

    def handle_node_condition(self, node) -> None:
        node_name = node.metadata.name
        ready_status = "Unknown"
        ready_reason = "ReadyConditionMissing"
        for condition in node.status.conditions or []:
            if condition.type == "Ready":
                ready_status = condition.status
                ready_reason = condition.reason or ready_reason
                break

        if ready_status == "True":
            self._node_not_ready_since.pop(node_name, None)
            self._node_not_ready_event_sent.discard(node_name)
            return

        first_seen = self._node_not_ready_since.setdefault(node_name, time.time())
        not_ready_seconds = int(time.time() - first_seen)
        if not_ready_seconds < self.node_not_ready_grace_seconds:
            return
        if node_name in self._node_not_ready_event_sent:
            return

        self.create_node_not_ready_fault_event(node_name, ready_status, ready_reason, not_ready_seconds)
        self._node_not_ready_event_sent.add(node_name)

    def create_node_not_ready_fault_event(
        self,
        node_name: str,
        ready_status: str,
        ready_reason: str,
        not_ready_seconds: int,
    ) -> None:
        first_seen = int(self._node_not_ready_since.get(node_name, time.time()))
        event_name = f"node-notready-{safe_name(node_name, 40)}-{first_seen}"
        try:
            self.custom_api.get_namespaced_custom_object(GROUP, VERSION, self.namespace, PLURAL, event_name)
            return
        except ApiException as exc:
            if exc.status != 404:
                print(f"[operator] get node-notready FaultEvent failed: {exc}", flush=True)
                return

        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "FaultEvent",
            "metadata": {
                "name": event_name,
                "namespace": self.namespace,
                "labels": {
                    "ft.hygon.io/source": "node-watch",
                },
            },
            "spec": {
                "nodeName": node_name,
                "type": "NodeNotReady",
                "severity": "Critical",
                "source": "node-watch",
                "reason": ready_reason,
                "message": f"Node {node_name} Ready={ready_status} for {not_ready_seconds}s",
                "observedAt": now_iso(),
                "action": {
                    "taintNode": True,
                    "deletePods": True,
                },
            },
        }
        try:
            self.custom_api.create_namespaced_custom_object(GROUP, VERSION, self.namespace, PLURAL, body)
            print(f"[operator] created NodeNotReady FaultEvent for {node_name}", flush=True)
        except ApiException as exc:
            if exc.status != 409:
                print(f"[operator] create NodeNotReady FaultEvent failed: {exc}", flush=True)

    def reconcile(self, fault: Dict) -> None:
        metadata = fault.get("metadata", {})
        status = fault.get("status", {})
        if status.get("processed") is True:
            return

        spec = fault.get("spec", {})
        job_name = str(spec.get("jobName") or "")
        if not job_name or self.aggregate_window_seconds <= 0:
            self._process_fault(fault)
            return

        name = str(metadata.get("name") or "")
        namespace = str(metadata.get("namespace") or self.namespace)
        group_key = f"{namespace}/{job_name}"
        with self._aggregation_lock:
            pending = self._pending_faults.setdefault(group_key, {})
            pending[name] = fault
            if group_key not in self._aggregation_timers:
                timer = threading.Timer(
                    self.aggregate_window_seconds,
                    self._resolve_fault_group,
                    args=(group_key,),
                )
                timer.daemon = True
                self._aggregation_timers[group_key] = timer
                timer.start()
                print(
                    f"[operator] aggregating job={group_key} for {self.aggregate_window_seconds}s",
                    flush=True,
                )

    def _resolve_fault_group(self, group_key: str) -> None:
        if not self.is_leader():
            with self._aggregation_lock:
                timer = threading.Timer(1.0, self._resolve_fault_group, args=(group_key,))
                timer.daemon = True
                self._aggregation_timers[group_key] = timer
                timer.start()
            return

        with self._aggregation_lock:
            pending = self._pending_faults.pop(group_key, {})
            self._aggregation_timers.pop(group_key, None)

        faults = list(pending.values())
        winner = select_root_fault(faults)
        if not winner:
            return

        winner_name = str((winner.get("metadata", {}) or {}).get("name") or "")
        winner_class = infer_fault_class(winner)
        print(
            f"[operator] selected root fault job={group_key} event={winner_name} class={winner_class}",
            flush=True,
        )
        self._process_fault(winner)

        for fault in faults:
            name = str((fault.get("metadata", {}) or {}).get("name") or "")
            if not name or name == winner_name:
                continue
            self.patch_status(
                name,
                [
                    {
                        "type": "Suppressed",
                        "success": True,
                        "message": f"aggregated under {winner_name} ({winner_class})",
                    }
                ],
                processed=True,
            )

    def _process_fault(self, fault: Dict) -> None:
        metadata = fault.get("metadata", {})
        name = metadata.get("name")
        spec = fault.get("spec", {})
        node_name = spec.get("nodeName")
        pod_name = spec.get("podName")
        pod_namespace = spec.get("podNamespace") or metadata.get("namespace")
        job_name = spec.get("jobName")
        workload_kind = str(spec.get("workloadKind") or "")
        action = spec.get("action", {})
        if not node_name and not pod_name:
            actions = [{"type": "Skip", "message": "spec.nodeName and spec.podName are both empty"}]
            self.patch_status(name, actions, processed=True)
            self.send_alert(fault, actions)
            return

        print(f"[operator] processing FaultEvent {name} node={node_name} pod={pod_namespace}/{pod_name}", flush=True)
        actions: List[Dict] = []

        taint_requested = bool(node_name and action.get("taintNode", True))
        taint_succeeded = False
        if taint_requested:
            taint_action = self.taint_node(node_name, spec.get("type", "NodeUnhealthy"))
            actions.append(taint_action)
            taint_succeeded = taint_action.get("success") is True

        explicit_delete_pod = action.get("deletePod") is True
        explicit_delete_pods = action.get("deletePods") is True
        delete_after_taint = (
            self.delete_pod_after_taint
            and taint_requested
            and taint_succeeded
            and spec.get("type")
            in {"TrainingRootCause", "TrainingNodeFault", "NodeHealthCheckFailed"}
            and workload_kind.lower() in {"", "pytorchjob"}
        )

        if delete_after_taint and pod_namespace and job_name and self.should_restart_entire_job(
            pod_namespace, job_name, workload_kind
        ):
            actions.extend(self.delete_ft_job_pods(pod_namespace, job_name))
        elif pod_name and (explicit_delete_pod or delete_after_taint):
            actions.append(self.delete_named_pod(pod_namespace, pod_name, node_name))
        elif node_name and (explicit_delete_pods or delete_after_taint):
            if delete_after_taint:
                actions.extend(self.delete_ft_jobs_for_node(node_name))
            else:
                actions.extend(self.delete_ft_pods_on_node(node_name))
        elif taint_requested and not taint_succeeded and self.delete_pod_after_taint:
            actions.append(
                {
                    "type": "DeletePod",
                    "namespace": pod_namespace,
                    "podName": pod_name,
                    "nodeName": node_name,
                    "success": False,
                    "message": "skip delete because node taint failed",
                }
            )

        self.patch_status(name, actions, processed=True)
        self.send_alert(fault, actions)

    def should_restart_entire_job(self, namespace: str, job_name: str, workload_kind: str) -> bool:
        normalized_kind = workload_kind.lower()
        if normalized_kind == "volcanojob":
            return False
        if normalized_kind == "pytorchjob":
            return True
        try:
            self.custom_api.get_namespaced_custom_object(
                "kubeflow.org",
                "v1",
                namespace,
                "pytorchjobs",
                job_name,
            )
            print(
                f"[operator] detected PyTorchJob {namespace}/{job_name}; restart all job pods",
                flush=True,
            )
            return True
        except ApiException as exc:
            if exc.status != 404:
                print(
                    f"[operator] query PyTorchJob {namespace}/{job_name} failed: {exc}; "
                    "restart all job pods by ft label",
                    flush=True,
                )
                return True
        except Exception as exc:
            print(
                f"[operator] query PyTorchJob {namespace}/{job_name} failed: {exc}; "
                "restart all job pods by ft label",
                flush=True,
            )
            return True
        return False

    def taint_node(self, node_name: str, reason: str) -> Dict:
        try:
            node = self.core_api.read_node(node_name)
            taints = list(node.spec.taints or [])
            exists = any(t.key == self.taint_key and t.effect == self.taint_effect for t in taints)
            if not exists:
                serialized_taints = []
                for taint in taints:
                    item = {"key": taint.key, "effect": taint.effect}
                    if taint.value is not None:
                        item["value"] = taint.value
                    if taint.time_added is not None:
                        item["timeAdded"] = taint.time_added.isoformat()
                    serialized_taints.append(item)
                serialized_taints.append({"key": self.taint_key, "value": reason, "effect": self.taint_effect})
                body = {"spec": {"taints": serialized_taints}}
                self.core_api.patch_node(node_name, body)
                print(f"[operator] tainted node {node_name} with {self.taint_key}", flush=True)
            return {"type": "TaintNode", "nodeName": node_name, "success": True, "message": self.taint_key}
        except Exception as exc:
            print(f"[operator] taint node failed node={node_name}: {exc}", flush=True)
            return {"type": "TaintNode", "nodeName": node_name, "success": False, "message": str(exc)}

    def delete_ft_pods_on_node(self, node_name: str) -> List[Dict]:
        actions: List[Dict] = []
        try:
            pods = self.core_api.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}",
                label_selector=FT_ENABLED_SELECTOR,
            ).items
        except Exception as exc:
            return [{"type": "ListPods", "nodeName": node_name, "success": False, "message": str(exc)}]

        for pod in pods:
            pod_name = pod.metadata.name
            namespace = pod.metadata.namespace
            phase = pod.status.phase
            if phase in {"Succeeded", "Failed"}:
                continue
            try:
                self.core_api.delete_namespaced_pod(
                    pod_name,
                    namespace,
                    grace_period_seconds=self.delete_grace_seconds,
                    body=client.V1DeleteOptions(grace_period_seconds=self.delete_grace_seconds),
                )
                print(f"[operator] deleted ft pod {namespace}/{pod_name} on unhealthy node {node_name}", flush=True)
                actions.append(
                    {
                        "type": "DeletePod",
                        "namespace": namespace,
                        "podName": pod_name,
                        "nodeName": node_name,
                        "success": True,
                    }
                )
            except ApiException as exc:
                if exc.status == 404:
                    continue
                actions.append(
                    {
                        "type": "DeletePod",
                        "namespace": namespace,
                        "podName": pod_name,
                        "nodeName": node_name,
                        "success": False,
                        "message": str(exc),
                    }
                )
        if not actions:
            actions.append({"type": "DeletePod", "nodeName": node_name, "success": True, "message": "no ft pods"})
        return actions

    def delete_ft_jobs_for_node(self, node_name: str) -> List[Dict]:
        try:
            pods = self.core_api.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}",
                label_selector=FT_ENABLED_SELECTOR,
            ).items
        except Exception as exc:
            return [{"type": "ListPods", "nodeName": node_name, "success": False, "message": str(exc)}]

        jobs = []
        seen = set()
        for pod in pods:
            phase = pod.status.phase
            if phase in {"Succeeded", "Failed"}:
                continue
            labels = pod.metadata.labels or {}
            job_name = labels.get(FT_JOB_LABEL)
            namespace = pod.metadata.namespace
            if not job_name or not namespace:
                continue
            key = (namespace, job_name)
            if key in seen:
                continue
            seen.add(key)
            jobs.append(key)

        actions: List[Dict] = []
        for namespace, job_name in jobs:
            print(
                f"[operator] node fault on {node_name}; restart all pods for job {namespace}/{job_name}",
                flush=True,
            )
            actions.extend(self.delete_ft_job_pods(namespace, job_name))

        if not actions:
            actions.append({"type": "DeletePod", "nodeName": node_name, "success": True, "message": "no ft job pods"})
        return actions

    def delete_ft_job_pods(self, namespace: str, job_name: str) -> List[Dict]:
        actions: List[Dict] = []
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace,
                label_selector=f"{FT_ENABLED_SELECTOR},{FT_JOB_LABEL}={job_name}",
            ).items
        except Exception as exc:
            return [
                {
                    "type": "DeletePod",
                    "namespace": namespace,
                    "jobName": job_name,
                    "success": False,
                    "message": f"list job pods failed: {exc}",
                }
            ]

        for pod in pods:
            pod_name = pod.metadata.name
            phase = pod.status.phase
            if phase in {"Succeeded", "Failed"}:
                continue
            try:
                self.core_api.delete_namespaced_pod(
                    pod_name,
                    namespace,
                    grace_period_seconds=self.delete_grace_seconds,
                    body=client.V1DeleteOptions(grace_period_seconds=self.delete_grace_seconds),
                )
                print(
                    f"[operator] deleted PyTorchJob pod {namespace}/{pod_name} for restart",
                    flush=True,
                )
                actions.append(
                    {
                        "type": "DeletePod",
                        "namespace": namespace,
                        "podName": pod_name,
                        "jobName": job_name,
                        "success": True,
                    }
                )
            except ApiException as exc:
                if exc.status == 404:
                    continue
                actions.append(
                    {
                        "type": "DeletePod",
                        "namespace": namespace,
                        "podName": pod_name,
                        "jobName": job_name,
                        "success": False,
                        "message": str(exc),
                    }
                )
        if not actions:
            actions.append(
                {
                    "type": "DeletePod",
                    "namespace": namespace,
                    "jobName": job_name,
                    "success": True,
                    "message": "no active PyTorchJob pods",
                }
            )
        return actions

    def delete_named_pod(self, namespace: str, pod_name: str, node_name: str = "") -> Dict:
        try:
            self.core_api.delete_namespaced_pod(
                pod_name,
                namespace,
                grace_period_seconds=self.delete_grace_seconds,
                body=client.V1DeleteOptions(grace_period_seconds=self.delete_grace_seconds),
            )
            print(f"[operator] deleted fault pod {namespace}/{pod_name}", flush=True)
            return {
                "type": "DeletePod",
                "namespace": namespace,
                "podName": pod_name,
                "nodeName": node_name,
                "success": True,
            }
        except ApiException as exc:
            if exc.status == 404:
                return {
                    "type": "DeletePod",
                    "namespace": namespace,
                    "podName": pod_name,
                    "nodeName": node_name,
                    "success": True,
                    "message": "pod already deleted",
                }
            return {
                "type": "DeletePod",
                "namespace": namespace,
                "podName": pod_name,
                "nodeName": node_name,
                "success": False,
                "message": str(exc),
            }

    def patch_status(self, name: str, actions: List[Dict], processed: bool) -> None:
        if not name:
            return
        body = {
            "status": {
                "processed": processed,
                "processedAt": now_iso(),
                "actions": actions,
            }
        }
        try:
            self.custom_api.patch_namespaced_custom_object_status(
                GROUP,
                VERSION,
                self.namespace,
                PLURAL,
                name,
                body,
            )
        except ApiException as exc:
            print(f"[operator] patch status failed for {name}: {exc}", flush=True)

    def send_alert(self, fault: Dict, actions: List[Dict]) -> None:
        if not self.feishu_webhook_url and not self.alert_command:
            return

        metadata = fault.get("metadata", {})
        spec = fault.get("spec", {})
        payload = {
            "faultEvent": {
                "namespace": metadata.get("namespace"),
                "name": metadata.get("name"),
                "type": spec.get("type"),
                "severity": spec.get("severity"),
                "source": spec.get("source"),
                "reason": spec.get("reason"),
                "nodeName": spec.get("nodeName"),
                "podNamespace": spec.get("podNamespace"),
                "podName": spec.get("podName"),
                "jobName": spec.get("jobName"),
                "message": spec.get("message"),
            },
            "actions": actions,
        }

        if self.alert_command:
            try:
                alert_command = shlex.split(self.alert_command)
                if not alert_command:
                    raise ValueError("FT_ALERT_COMMAND is empty")
                subprocess.run(
                    alert_command,
                    input=json.dumps(payload, ensure_ascii=False),
                    text=True,
                    timeout=10,
                    check=False,
                )
            except Exception as exc:
                print(f"[operator] alert command failed: {exc}", flush=True)

        if self.feishu_webhook_url:
            text = (
                f"[hygon-ft] training fault processed\n"
                f"type={spec.get('type')} severity={spec.get('severity')} source={spec.get('source')}\n"
                f"job={spec.get('jobName')} pod={spec.get('podNamespace')}/{spec.get('podName')} node={spec.get('nodeName')}\n"
                f"reason={spec.get('reason')}\n"
                f"actions={json.dumps(actions, ensure_ascii=False)}"
            )
            body = json.dumps({"msg_type": "text", "content": {"text": text}}, ensure_ascii=False).encode("utf-8")
            request = urllib.request.Request(
                self.feishu_webhook_url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    print(f"[operator] feishu alert response={response.status}", flush=True)
            except urllib.error.URLError as exc:
                print(f"[operator] feishu alert failed: {exc}", flush=True)


def main() -> None:
    load_kube()
    controller = FaultController()
    controller.start_reporter_server()
    controller.start_leader_election()
    controller.start_node_not_ready_watch()
    controller.run()


if __name__ == "__main__":
    main()
