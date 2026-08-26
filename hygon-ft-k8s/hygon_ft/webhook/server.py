# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import base64
import copy
import json
import os
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

ENABLE_KEY = "ft.hygon.io/enabled"
INJECTED_KEY = "ft.hygon.io/injected"
JOB_LABEL = "ft.hygon.io/job-name"
DEFAULT_RUNTIME_IMAGE = os.getenv("FT_RUNTIME_IMAGE", "hygon/ft-controller:latest")
DEFAULT_LAUNCHER_PATH = "/opt/hygon-ft/ft-launcher"


def _truthy(value: Any) -> bool:
    return str(value).lower() in {"true", "1", "yes", "on"}


def _workload_enabled(metadata: Dict[str, Any]) -> bool:
    annotations = metadata.get("annotations", {})
    labels = metadata.get("labels", {})
    return _truthy(annotations.get(ENABLE_KEY, "false")) or _truthy(labels.get(ENABLE_KEY, "false"))


def _ensure_dict(parent: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        value = {}
        parent[key] = value
    return value


def _ensure_list(parent: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    value = parent.get(key)
    if not isinstance(value, list):
        value = []
        parent[key] = value
    return value


def _upsert_by_name(items: List[Dict[str, Any]], item: Dict[str, Any]) -> None:
    name = item.get("name")
    for idx, existing in enumerate(items):
        if existing.get("name") == name:
            merged = copy.deepcopy(existing)
            merged.update(item)
            items[idx] = merged
            return
    items.append(item)


def _add_env(container: Dict[str, Any], name: str, value: Any = None, field_path: str = "") -> None:
    env = _ensure_list(container, "env")
    entry: Dict[str, Any] = {"name": name}
    if field_path:
        entry["valueFrom"] = {"fieldRef": {"fieldPath": field_path}}
    else:
        entry["value"] = str(value)
    _upsert_by_name(env, entry)


def _monitor_enabled_for_role(replica_name: str, annotations: Dict[str, str]) -> bool:
    roles_text = annotations.get("ft.hygon.io/log-monitor-roles", "all")
    roles = {item.strip().lower() for item in roles_text.split(",") if item.strip()}
    if not roles:
        roles = {"all"}
    if "*" in roles or "all" in roles:
        return True
    role = replica_name.lower()
    return role in roles


def _inject_replica_template(
    replica_name: str,
    replica_spec: Dict[str, Any],
    job_name: str,
    annotations: Dict[str, str],
    workload_kind: str,
    enable_no_update_group: bool = False,
) -> None:
    template = _ensure_dict(replica_spec, "template")
    metadata = _ensure_dict(template, "metadata")
    pod_annotations = _ensure_dict(metadata, "annotations")
    pod_labels = _ensure_dict(metadata, "labels")
    pod_annotations[INJECTED_KEY] = "true"
    pod_labels[ENABLE_KEY] = "true"
    pod_labels[JOB_LABEL] = job_name
    pod_labels["ft.hygon.io/replica-role"] = replica_name.lower()

    pod_spec = _ensure_dict(template, "spec")
    inject_launcher = _truthy(annotations.get("ft.hygon.io/inject-launcher", "true"))
    launcher_path = annotations.get("ft.hygon.io/launcher-path", DEFAULT_LAUNCHER_PATH)
    launcher_interpreter = annotations.get("ft.hygon.io/launcher-interpreter", "")

    # The default launcher is copied from the runtime image. Shared-filesystem
    # deployments can point launcher-path at an existing executable instead.
    if inject_launcher and launcher_path == DEFAULT_LAUNCHER_PATH:
        volumes = _ensure_list(pod_spec, "volumes")
        _upsert_by_name(volumes, {"name": "hygon-ft-bin", "emptyDir": {}})

        init_containers = _ensure_list(pod_spec, "initContainers")
        _upsert_by_name(
            init_containers,
            {
                "name": "hygon-ft-init",
                "image": annotations.get("ft.hygon.io/runtime-image", DEFAULT_RUNTIME_IMAGE),
                "imagePullPolicy": annotations.get("ft.hygon.io/runtime-image-pull-policy", "IfNotPresent"),
                "command": ["bash", "-lc"],
                "args": ["cp /opt/hygon-ft/ft-launcher /hygon-ft-bin/ft-launcher && chmod +x /hygon-ft-bin/ft-launcher"],
                "volumeMounts": [{"name": "hygon-ft-bin", "mountPath": "/hygon-ft-bin"}],
            },
        )

    containers = _ensure_list(pod_spec, "containers")
    if not containers:
        return

    target_index = 0
    for idx, container in enumerate(containers):
        if container.get("name") in {"pytorch", "training", "trainer"}:
            target_index = idx
            break

    container = containers[target_index]
    if inject_launcher and launcher_path == DEFAULT_LAUNCHER_PATH:
        mounts = _ensure_list(container, "volumeMounts")
        _upsert_by_name(mounts, {"name": "hygon-ft-bin", "mountPath": "/opt/hygon-ft", "readOnly": True})

    _add_env(container, "FT_ENABLE", "true")
    _add_env(container, "FT_JOB_NAME", job_name)
    _add_env(container, "FT_WORKLOAD_KIND", workload_kind)
    _add_env(container, "FT_REPLICA_ROLE", replica_name)
    _add_env(container, "FT_REPLICA_COUNT", replica_spec.get("replicas", 1))
    _add_env(container, "FT_POD_NAME", field_path="metadata.name")
    _add_env(container, "FT_POD_UID", field_path="metadata.uid")
    _add_env(container, "FT_POD_NAMESPACE", field_path="metadata.namespace")
    _add_env(container, "FT_NODE_NAME", field_path="spec.nodeName")
    log_dir = annotations.get("ft.hygon.io/log-dir")
    log_file = annotations.get("ft.hygon.io/log-file")
    if log_dir:
        _add_env(container, "FT_LOG_DIR", log_dir)
    if log_file:
        _add_env(container, "FT_LOG_FILE", log_file)
    elif not log_dir:
        _add_env(container, "FT_LOG_FILE", "/tmp/hygon-ft-train.log")
    _add_env(container, "FT_FAULT_MARKER_FILE", annotations.get("ft.hygon.io/fault-marker-file", "/tmp/hygon-ft-fault.json"))
    _add_env(container, "FT_FAULT_EVENT_ENABLED", annotations.get("ft.hygon.io/fault-event-enabled", "true"))
    _add_env(
        container,
        "FT_FAULT_REPORT_URL",
        annotations.get("ft.hygon.io/fault-report-url", "http://ft-operator.hygon-ft.svc.cluster.local:8080/report"),
    )
    _add_env(container, "FT_EXIT_AFTER_FAULT_EVENT_REPORT", annotations.get("ft.hygon.io/exit-after-fault-event-report", "true"))
    _add_env(container, "FT_FAULT_EVENT_ACK_TIMEOUT_SECONDS", annotations.get("ft.hygon.io/fault-event-ack-timeout-seconds", "30"))
    _add_env(container, "FT_FAULT_EVENT_ACK_INTERVAL_SECONDS", annotations.get("ft.hygon.io/fault-event-ack-interval-seconds", "1"))
    _add_env(container, "FT_SCAN_INTERVAL_SECONDS", annotations.get("ft.hygon.io/scan-interval-seconds", "10"))
    if annotations.get("ft.hygon.io/log-monitor-command") and _monitor_enabled_for_role(replica_name, annotations):
        _add_env(container, "FT_EXTERNAL_MONITOR_CMD", annotations["ft.hygon.io/log-monitor-command"])
        _add_env(container, "LOG_MONITOR_ENABLE_NO_UPDATE", str(enable_no_update_group).lower())
        _add_env(container, "LOG_MONITOR_LAST_NODE_ONLY", "true")
        if replica_name.lower() != "master":
            _add_env(container, "ENABLE_REGULAR_NOTIFY", "false")

    if inject_launcher:
        original_command = container.get("command", [])
        original_args = container.get("args", [])
        original = []
        if isinstance(original_command, list):
            original.extend(original_command)
        if isinstance(original_args, list):
            original.extend(original_args)
        already_wrapped = any(str(item).rstrip("/").endswith("ft-launcher") for item in original_command)
        if original and not already_wrapped:
            container["command"] = [launcher_interpreter, launcher_path] if launcher_interpreter else [launcher_path]
            container["args"] = ["--"] + original


def mutate_pytorchjob(obj: Dict[str, Any]) -> Dict[str, Any]:
    mutated = copy.deepcopy(obj)
    metadata = _ensure_dict(mutated, "metadata")
    annotations = _ensure_dict(metadata, "annotations")
    if not _workload_enabled(metadata):
        return obj
    if _truthy(annotations.get(INJECTED_KEY, "false")):
        return obj

    job_name = metadata.get("name", "unknown")
    annotations[INJECTED_KEY] = "true"

    spec = _ensure_dict(mutated, "spec")
    replicas = _ensure_dict(spec, "pytorchReplicaSpecs")
    replica_items = [
        (replica_name, replica_spec)
        for replica_name, replica_spec in replicas.items()
        if isinstance(replica_spec, dict) and int(replica_spec.get("replicas", 1) or 0) > 0
    ]
    for index, (replica_name, replica_spec) in enumerate(replica_items):
        _inject_replica_template(
            replica_name,
            replica_spec,
            job_name,
            annotations,
            "PyTorchJob",
            enable_no_update_group=index == len(replica_items) - 1,
        )
    return mutated


def mutate_volcano_job(obj: Dict[str, Any]) -> Dict[str, Any]:
    mutated = copy.deepcopy(obj)
    metadata = _ensure_dict(mutated, "metadata")
    annotations = _ensure_dict(metadata, "annotations")
    if not _workload_enabled(metadata):
        return obj
    if _truthy(annotations.get(INJECTED_KEY, "false")):
        return obj

    job_name = metadata.get("name", "unknown")
    annotations[INJECTED_KEY] = "true"

    spec = _ensure_dict(mutated, "spec")
    tasks = _ensure_list(spec, "tasks")
    active_tasks = [
        task
        for task in tasks
        if isinstance(task, dict) and int(task.get("replicas", 1) or 0) > 0
    ]
    for index, task in enumerate(active_tasks):
        replica_name = str(task.get("name") or "worker")
        _inject_replica_template(
            replica_name,
            task,
            job_name,
            annotations,
            "VolcanoJob",
            enable_no_update_group=index == len(active_tasks) - 1,
        )
    return mutated


def build_patch(original: Dict[str, Any], mutated: Dict[str, Any]) -> List[Dict[str, Any]]:
    if original == mutated:
        return []
    patch: List[Dict[str, Any]] = []
    original_annotations = original.get("metadata", {}).get("annotations")
    mutated_annotations = mutated.get("metadata", {}).get("annotations")
    if original_annotations != mutated_annotations:
        patch.append(
            {
                "op": "add" if original_annotations is None else "replace",
                "path": "/metadata/annotations",
                "value": mutated_annotations or {},
            }
        )

    original_replicas = original.get("spec", {}).get("pytorchReplicaSpecs")
    mutated_replicas = mutated.get("spec", {}).get("pytorchReplicaSpecs")
    if original_replicas != mutated_replicas:
        patch.append(
            {
                "op": "add" if original_replicas is None else "replace",
                "path": "/spec/pytorchReplicaSpecs",
                "value": mutated_replicas or {},
            }
        )

    original_tasks = original.get("spec", {}).get("tasks")
    mutated_tasks = mutated.get("spec", {}).get("tasks")
    if original_tasks != mutated_tasks:
        patch.append(
            {
                "op": "add" if original_tasks is None else "replace",
                "path": "/spec/tasks",
                "value": mutated_tasks or [],
            }
        )
    return patch


def build_admission_response(review: Dict[str, Any]) -> Dict[str, Any]:
    admission_request = review.get("request", {})
    uid = admission_request.get("uid")
    obj = admission_request.get("object", {})

    allowed = True
    patch: List[Dict[str, Any]] = []
    try:
        api_version = obj.get("apiVersion", "")
        kind = obj.get("kind", "")
        if api_version == "kubeflow.org/v1" and kind == "PyTorchJob":
            mutated = mutate_pytorchjob(obj)
            patch = build_patch(obj, mutated)
        elif api_version.startswith("batch.volcano.sh/") and kind == "Job":
            mutated = mutate_volcano_job(obj)
            patch = build_patch(obj, mutated)
    except Exception as exc:  # Keep apiserver response explicit.
        return {
            "apiVersion": review.get("apiVersion", "admission.k8s.io/v1"),
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": False,
                "status": {"message": f"hygon-ft webhook mutation failed: {exc}"},
            },
        }

    response: Dict[str, Any] = {"uid": uid, "allowed": allowed}
    if patch:
        encoded = base64.b64encode(json.dumps(patch).encode("utf-8")).decode("ascii")
        response["patchType"] = "JSONPatch"
        response["patch"] = encoded

    return {
        "apiVersion": review.get("apiVersion", "admission.k8s.io/v1"),
        "kind": "AdmissionReview",
        "response": response,
    }


class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] == "/healthz":
            self._send(200, b"ok", "text/plain")
            return
        self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/mutate":
            self._send(404, b"not found", "text/plain")
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            review = json.loads(self.rfile.read(content_length).decode("utf-8"))
            payload = json.dumps(build_admission_response(review)).encode("utf-8")
            self._send(200, payload, "application/json")
        except Exception as exc:
            payload = json.dumps({"error": str(exc)}).encode("utf-8")
            self._send(400, payload, "application/json")

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:
        print(f"[webhook] {format % args}", flush=True)


def main() -> None:
    cert_file = os.getenv("TLS_CERT_FILE", "/tls/tls.crt")
    key_file = os.getenv("TLS_KEY_FILE", "/tls/tls.key")
    port = int(os.getenv("WEBHOOK_PORT", "9443"))
    server = ThreadingHTTPServer(("0.0.0.0", port), WebhookHandler)
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    server.socket = tls_context.wrap_socket(server.socket, server_side=True)
    print(f"[webhook] listening on :{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
