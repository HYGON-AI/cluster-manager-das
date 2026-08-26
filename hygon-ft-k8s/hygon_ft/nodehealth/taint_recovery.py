# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import json
import os
import random
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

client = None
config = None
ApiException = Exception


DEFAULT_NAMESPACE = "hygon-ft"
DEFAULT_TAINT_KEY = "ft.hygon.io/node-unhealthy"
DEFAULT_TAINT_EFFECT = "NoSchedule"
DEFAULT_IMAGE = "hygon/ft-controller:latest"
DEFAULT_SCRIPT_PATH = "/opt/hygon-ft/nhc/run_nhc.sh"
DEFAULT_LOG_FILE = "/tmp/FAILED_NODES_CHECK"


def ensure_kubernetes() -> Tuple[Any, Any, Any]:
    global client, config, ApiException
    if client is not None and config is not None:
        return client, config, ApiException
    try:
        from kubernetes import client as kube_client, config as kube_config
        from kubernetes.client import ApiException as kube_api_exception
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "python package 'kubernetes' is required when running taint recovery checks"
        ) from exc
    client = kube_client
    config = kube_config
    ApiException = kube_api_exception
    return client, config, ApiException


@dataclass
class RecoveryConfig:
    namespace: str = DEFAULT_NAMESPACE
    image: str = DEFAULT_IMAGE
    script_path: str = DEFAULT_SCRIPT_PATH
    taint_key: str = DEFAULT_TAINT_KEY
    taint_effect: str = DEFAULT_TAINT_EFFECT
    check_times: int = 5
    check_interval_seconds: int = 60
    pod_start_timeout_seconds: int = 120
    pod_finish_timeout_seconds: int = 1800
    check_timeout_seconds: int = 600
    remove_taint: bool = False
    delete_check_pod: bool = True
    log_file: str = DEFAULT_LOG_FILE
    detail_log_file: str = ""


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def load_kube() -> None:
    _, kube_config, _ = ensure_kubernetes()
    try:
        kube_config.load_incluster_config()
    except kube_config.ConfigException:
        kube_config.load_kube_config()


def sanitize_name(value: str) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower())
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:40] or "node"


def parse_nodes(value: Optional[str]) -> List[str]:
    if not value:
        return []
    result = []
    for item in re.split(r"[,\s]+", value.strip()):
        if item and item not in result:
            result.append(item)
    return result


def node_is_ready(node: client.V1Node) -> bool:
    for condition in node.status.conditions or []:
        if condition.type == "Ready":
            return condition.status == "True"
    return False


def has_taint(node: client.V1Node, key: str, effect: str) -> bool:
    for taint in node.spec.taints or []:
        if taint.key == key and taint.effect == effect:
            return True
    return False


def choose_normal_nodes(
    core_api: client.CoreV1Api,
    target_node: str,
    taint_key: str,
    taint_effect: str,
    count: int = 2,
) -> List[str]:
    candidates = []
    for node in core_api.list_node().items:
        name = node.metadata.name
        if name == target_node:
            continue
        if not node_is_ready(node):
            continue
        if has_taint(node, taint_key, taint_effect):
            continue
        candidates.append(name)
    random.shuffle(candidates)
    return candidates[:count]


def build_recovery_pod_name(node_name: str, round_index: int) -> str:
    suffix = f"{round_index}-{int(time.time() * 1000)}"
    prefix = "taint-recovery-"
    max_node_length = max(1, 63 - len(prefix) - len(suffix) - 1)
    return f"{prefix}{sanitize_name(node_name)[:max_node_length]}-{suffix}"


def build_check_pod(
    pod_name: str,
    target_node: str,
    normal_nodes: Sequence[str],
    cfg: RecoveryConfig,
    round_index: int,
) -> client.V1Pod:
    env = [
        client.V1EnvVar(name="NODE_HEALTH_TARGET_NODE", value=target_node),
        client.V1EnvVar(name="NODE_HEALTH_REFERENCE_NODES", value=",".join(normal_nodes)),
        client.V1EnvVar(name="NODE_HEALTH_CHECK_TIMES", value="1"),
        client.V1EnvVar(name="NODE_HEALTH_CHECK_INTERVAL_SECONDS", value=str(cfg.check_interval_seconds)),
        client.V1EnvVar(name="RUN_NHC_INTERVAL_SECONDS", value=str(cfg.check_interval_seconds)),
        client.V1EnvVar(name="NODE_HEALTH_CHECK_ROUND", value=str(round_index)),
        client.V1EnvVar(name="NODE_HEALTH_CHECK_TIMEOUT_SECONDS", value=str(cfg.check_timeout_seconds)),
        client.V1EnvVar(name="NODE_HEALTH_LOG_FILE", value=cfg.log_file),
        client.V1EnvVar(name="NODE_HEALTH_DETAIL_LOG_FILE", value=cfg.detail_log_file),
        client.V1EnvVar(name="NODE_HEALTH_DETAIL_LOG_STDOUT", value="1"),
        client.V1EnvVar(name="NODE_HEALTH_CHECK_STAGE", value="pod"),
        client.V1EnvVar(name="NHC_HOST_COMMAND", value="run_nhc"),
    ]
    command = ["bash", "-lc", f"chmod +x {cfg.script_path} 2>/dev/null || true; exec bash {cfg.script_path}"]
    container = client.V1Container(
        name="checker",
        image=cfg.image,
        image_pull_policy="IfNotPresent",
        command=command,
        env=env,
        security_context=client.V1SecurityContext(privileged=True),
        volume_mounts=[
            client.V1VolumeMount(name="nhc-script", mount_path="/opt/hygon-ft/nhc", read_only=True),
            client.V1VolumeMount(name="host-proc", mount_path="/host/proc", read_only=True),
            client.V1VolumeMount(name="host-dev", mount_path="/dev", read_only=True),
            client.V1VolumeMount(name="host-sys", mount_path="/sys", read_only=True),
            client.V1VolumeMount(name="host-tmp", mount_path="/tmp"),
        ],
    )
    volumes = [
        client.V1Volume(
            name="nhc-script",
            config_map=client.V1ConfigMapVolumeSource(name="nodehealth-config", default_mode=0o555),
        ),
        client.V1Volume(name="host-proc", host_path=client.V1HostPathVolumeSource(path="/proc", type="Directory")),
        client.V1Volume(name="host-dev", host_path=client.V1HostPathVolumeSource(path="/dev", type="Directory")),
        client.V1Volume(name="host-sys", host_path=client.V1HostPathVolumeSource(path="/sys", type="Directory")),
        client.V1Volume(name="host-tmp", host_path=client.V1HostPathVolumeSource(path="/tmp", type="Directory")),
    ]
    metadata = client.V1ObjectMeta(
        name=pod_name,
        namespace=cfg.namespace,
        labels={
            "app": "taint-recovery-checker",
            "ft.hygon.io/source": "nodehealth-taint-recovery",
            "ft.hygon.io/node": sanitize_name(target_node),
        },
    )
    spec = client.V1PodSpec(
        restart_policy="Never",
        node_name=target_node,
        host_pid=True,
        host_network=True,
        tolerations=[client.V1Toleration(operator="Exists")],
        containers=[container],
        volumes=volumes,
    )
    return client.V1Pod(metadata=metadata, spec=spec)


def wait_for_pod_phase(
    core_api: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    phases: Iterable[str],
    timeout_seconds: int,
) -> client.V1Pod:
    wanted = set(phases)
    deadline = time.monotonic() + timeout_seconds
    last_pod = None
    while time.monotonic() < deadline:
        pod = core_api.read_namespaced_pod(pod_name, namespace)
        last_pod = pod
        if pod.status.phase in wanted:
            return pod
        time.sleep(2)
    phase = last_pod.status.phase if last_pod else "Unknown"
    raise TimeoutError(f"pod {namespace}/{pod_name} did not reach {sorted(wanted)} before timeout, current phase={phase}")


def get_pod_logs(core_api: client.CoreV1Api, namespace: str, pod_name: str) -> str:
    errors = []
    for previous in (False, True):
        try:
            logs = core_api.read_namespaced_pod_log(
                pod_name,
                namespace,
                container="checker",
                timestamps=False,
                previous=previous,
            )
            if logs:
                return logs
        except ApiException as exc:
            errors.append(f"previous={previous}: {exc}")
    return "failed to read pod logs: " + " | ".join(errors)


def parse_nhc_recovery_result(text: str, pod_phase: str) -> Dict[str, Any]:
    markers = re.findall(r"^\[CHECK RESULT\]:\s*(.*?)\s*$", text, re.MULTILINE)
    result_text = markers[-1].strip() if markers else ""
    if pod_phase == "Succeeded" and result_text in {"PASS", "PASSED"}:
        return {"result": "PASS", "nhc_result": result_text}
    if result_text:
        return {
            "result": "FAIL",
            "failed_items": ["run_nhc"],
            "nhc_result": result_text,
            "detail": "run_nhc reported a node health failure",
        }
    return {
        "result": "FAIL",
        "failed_items": ["checker_output"],
        "detail": "run_nhc did not produce a valid [CHECK RESULT] marker",
        "logs_tail": text[-2000:],
    }


def default_detail_log_file(node_name: str) -> str:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d%H%M%S")
    return f"/tmp/taint-recovery-{sanitize_name(node_name)}-{timestamp}.log"


def init_detail_log(node_name: str, selected_normal_nodes: Sequence[str], cfg: RecoveryConfig) -> None:
    if not cfg.detail_log_file:
        return
    try:
        with open(cfg.detail_log_file, "a", encoding="utf-8") as handle:
            handle.write(f"===== {now_iso()} taint recovery start =====\n")
            handle.write(f"node: {node_name}\n")
            handle.write(f"normal_nodes: {','.join(selected_normal_nodes)}\n")
            handle.write(f"check_times: {cfg.check_times}\n")
            handle.write(f"check_interval_seconds: {cfg.check_interval_seconds}\n")
            handle.write(f"taint: {cfg.taint_key}:{cfg.taint_effect}\n\n")
    except OSError:
        pass


def append_detail_log_message(cfg: RecoveryConfig, message: str) -> None:
    if not cfg.detail_log_file:
        return
    try:
        with open(cfg.detail_log_file, "a", encoding="utf-8") as handle:
            handle.write(message)
            if not message.endswith("\n"):
                handle.write("\n")
    except OSError:
        pass


def remove_owned_taint(core_api: client.CoreV1Api, node_name: str, cfg: RecoveryConfig) -> Dict[str, Any]:
    node = core_api.read_node(node_name)
    taints = []
    removed = False
    for taint in node.spec.taints or []:
        if taint.key == cfg.taint_key and taint.effect == cfg.taint_effect:
            removed = True
            continue
        item = {"key": taint.key, "effect": taint.effect}
        if taint.value is not None:
            item["value"] = taint.value
        if taint.time_added is not None:
            item["timeAdded"] = taint.time_added.isoformat()
        taints.append(item)
    if removed:
        core_api.patch_node(node_name, {"spec": {"taints": taints}})
    return {"removed": removed, "taint_key": cfg.taint_key, "taint_effect": cfg.taint_effect}


def run_pod_recovery_checks(
    core_api: client.CoreV1Api,
    node_name: str,
    selected_normal_nodes: Sequence[str],
    cfg: RecoveryConfig,
    round_index: int,
) -> Dict[str, Any]:
    pod_name = build_recovery_pod_name(node_name, round_index)
    pod = build_check_pod(pod_name, node_name, selected_normal_nodes, cfg, round_index)
    created = False
    logs = ""
    try:
        core_api.create_namespaced_pod(cfg.namespace, pod)
        created = True
        wait_for_pod_phase(core_api, cfg.namespace, pod_name, {"Running", "Succeeded", "Failed"}, cfg.pod_start_timeout_seconds)
        finished_pod = wait_for_pod_phase(core_api, cfg.namespace, pod_name, {"Succeeded", "Failed"}, cfg.pod_finish_timeout_seconds)
        logs = get_pod_logs(core_api, cfg.namespace, pod_name)
        if logs:
            append_detail_log_message(cfg, logs if logs.endswith("\n") else logs + "\n")
        result = parse_nhc_recovery_result(logs, finished_pod.status.phase)
        result.setdefault("node", node_name)
        result.setdefault("normal_nodes", selected_normal_nodes[:2])
        result.setdefault("pod", f"{cfg.namespace}/{pod_name}")
        result.setdefault("time", now_iso())
        result.setdefault("round", round_index)
        result["pod_phase"] = finished_pod.status.phase
        return result
    except Exception as exc:
        return {
            "result": "FAIL",
            "failed_items": ["checker_pod"],
            "node": node_name,
            "normal_nodes": selected_normal_nodes[:2],
            "pod": f"{cfg.namespace}/{pod_name}",
            "time": now_iso(),
            "round": round_index,
            "detail": str(exc),
            "logs": logs[-4000:],
        }
    finally:
        if created and cfg.delete_check_pod:
            try:
                core_api.delete_namespaced_pod(
                    pod_name,
                    cfg.namespace,
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                    grace_period_seconds=0,
                )
            except ApiException as exc:
                if exc.status != 404:
                    print(f"[taint-recovery] delete check pod failed: {exc}", flush=True)


def check_taint_recovery(
    node_name: str,
    normal_nodes: Optional[Sequence[str]] = None,
    cfg: Optional[RecoveryConfig] = None,
) -> Dict[str, Any]:
    cfg = cfg or RecoveryConfig()
    try:
        load_kube()
        core_api = client.CoreV1Api()
        node = core_api.read_node(node_name)
    except Exception as exc:
        return {
            "result": "FAIL",
            "failed_items": ["kubernetes"],
            "node": node_name,
            "time": now_iso(),
            "detail": str(exc),
        }
    if not node_is_ready(node):
        return {
            "result": "FAIL",
            "failed_items": ["Node Ready"],
            "node": node_name,
            "time": now_iso(),
            "detail": "target node is not Ready",
        }
    selected_normal_nodes = list(normal_nodes or [])
    if len(selected_normal_nodes) < 2:
        selected_normal_nodes = choose_normal_nodes(core_api, node_name, cfg.taint_key, cfg.taint_effect, 2)
    if len(selected_normal_nodes) < 2:
        return {
            "result": "FAIL",
            "failed_items": ["ib_write_bw"],
            "node": node_name,
            "time": now_iso(),
            "detail": "need at least two Ready non-tainted normal nodes for ib_write_bw baseline",
        }

    selected_normal_nodes = selected_normal_nodes[:2]
    init_detail_log(node_name, selected_normal_nodes, cfg)
    last_result: Dict[str, Any] = {
        "result": "PASS",
        "node": node_name,
        "normal_nodes": selected_normal_nodes,
        "time": now_iso(),
    }
    for round_index in range(1, cfg.check_times + 1):
        pod_result = run_pod_recovery_checks(core_api, node_name, selected_normal_nodes, cfg, round_index)
        if pod_result.get("result") != "PASS":
            pod_result.setdefault("round", round_index)
            pod_result.setdefault("time", now_iso())
            if cfg.detail_log_file:
                pod_result.setdefault("detail_log_file", cfg.detail_log_file)
            return pod_result

        last_result = pod_result
        if round_index < cfg.check_times and cfg.check_interval_seconds > 0:
            append_detail_log_message(
                cfg,
                f"===== {now_iso()} wait {cfg.check_interval_seconds}s before round {round_index + 1} =====\n\n",
            )
            time.sleep(cfg.check_interval_seconds)

    result = dict(last_result)
    result["rounds"] = cfg.check_times
    result["normal_nodes"] = selected_normal_nodes
    result.setdefault("time", now_iso())
    if cfg.detail_log_file:
        result["detail_log_file"] = cfg.detail_log_file
    if cfg.remove_taint:
        result["untaint"] = remove_owned_taint(core_api, node_name, cfg)
        append_detail_log_message(
            cfg,
            f"===== {now_iso()} untaint result =====\n{json.dumps(result['untaint'], ensure_ascii=False, sort_keys=True)}\n\n",
        )
    append_detail_log_message(cfg, f"===== {now_iso()} taint recovery finish result={result.get('result')} =====\n")
    return result


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


def build_config_from_env(args: argparse.Namespace) -> RecoveryConfig:
    detail_log_file = args.detail_log_file or os.getenv("RECOVERY_DETAIL_LOG_FILE", "")
    if not detail_log_file and not args.no_detail_log:
        detail_log_file = default_detail_log_file(args.node)

    check_times = args.check_times or get_env_int("CHECK_FILENODES_TIMES", 5)
    if check_times <= 0:
        check_times = 5

    check_interval_seconds = args.check_interval_seconds
    if check_interval_seconds < 0:
        check_interval_seconds = args.run_nhc_interval_seconds
    if check_interval_seconds < 0:
        check_interval_seconds = get_env_int(
            "CHECK_FILENODES_INTERVAL_TIME",
            get_env_int("RUN_NHC_INTERVAL_SECONDS", 60),
        )
    if check_interval_seconds < 0:
        check_interval_seconds = 60

    return RecoveryConfig(
        namespace=args.namespace or os.getenv("POD_NAMESPACE", DEFAULT_NAMESPACE),
        image=args.image or os.getenv("TAINT_RECOVERY_IMAGE", DEFAULT_IMAGE),
        script_path=args.script_path or os.getenv("TAINT_RECOVERY_SCRIPT_PATH", DEFAULT_SCRIPT_PATH),
        taint_key=args.taint_key or os.getenv("FT_UNHEALTHY_TAINT_KEY", DEFAULT_TAINT_KEY),
        taint_effect=args.taint_effect or os.getenv("FT_UNHEALTHY_TAINT_EFFECT", DEFAULT_TAINT_EFFECT),
        check_times=check_times,
        check_interval_seconds=check_interval_seconds,
        pod_start_timeout_seconds=args.pod_start_timeout_seconds,
        pod_finish_timeout_seconds=args.pod_finish_timeout_seconds,
        check_timeout_seconds=args.check_timeout_seconds,
        remove_taint=args.remove_taint,
        delete_check_pod=not args.keep_pod,
        log_file=args.log_file or os.getenv("RECOVERY_LOG_FILE", DEFAULT_LOG_FILE),
        detail_log_file=detail_log_file,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether an unhealthy tainted node can be recovered.")
    parser.add_argument("--node", required=True, help="Target tainted node name.")
    parser.add_argument("--normal-nodes", default="", help="Comma or whitespace separated normal nodes for ib_write_bw baseline.")
    parser.add_argument("--namespace", default="")
    parser.add_argument("--image", default="")
    parser.add_argument("--script-path", default="")
    parser.add_argument("--taint-key", default="")
    parser.add_argument("--taint-effect", default="")
    parser.add_argument("--check-times", type=int, default=0)
    parser.add_argument("--check-interval-seconds", type=int, default=-1, help="Seconds to wait between two full recovery-check rounds.")
    parser.add_argument("--run-nhc-interval-seconds", type=int, default=-1, help="Deprecated alias for --check-interval-seconds.")
    parser.add_argument("--pod-start-timeout-seconds", type=int, default=120)
    parser.add_argument("--pod-finish-timeout-seconds", type=int, default=1800)
    parser.add_argument("--check-timeout-seconds", type=int, default=600)
    parser.add_argument("--log-file", default="")
    parser.add_argument("--detail-log-file", default="", help="Full text log file shared by host and checker Pod. Defaults to /tmp/taint-recovery-<node>-<time>.log.")
    parser.add_argument("--no-detail-log", action="store_true", help="Disable per-run full text log file.")
    parser.add_argument("--remove-taint", action="store_true", help="Remove only the owned unhealthy taint after all checks pass.")
    parser.add_argument("--keep-pod", action="store_true", help="Keep the checker Pod for debugging.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    cfg = build_config_from_env(args)
    result = check_taint_recovery(args.node, parse_nodes(args.normal_nodes), cfg)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("result") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
