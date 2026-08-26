# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import socket
import subprocess
import time
from datetime import datetime, timezone
from typing import Dict

from kubernetes import client, config
from kubernetes.client import ApiException


GROUP = "ft.hygon.io"
VERSION = "v1alpha1"
PLURAL = "faultevents"


def now_iso() -> str:
    # UTC timestamp used in saved NodeHealth diagnostics.
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def load_kube() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def trim(value: str, limit: int = 3500) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def create_fault_event(
    api: client.CustomObjectsApi,
    namespace: str,
    node_name: str,
    reason: str,
    stdout: str,
    stderr: str,
    exit_code: int,
    observed_at: str,
) -> None:
    ts = int(time.time())
    name = f"nhc-{node_name.lower().replace('_', '-')}-{ts}"
    body: Dict = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "FaultEvent",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "annotations": {
                "ft.hygon.io/node": node_name,
            },
            "labels": {
                "ft.hygon.io/source": "nodehealth-agent",
            },
        },
        "spec": {
            "nodeName": node_name,
            "type": "NodeHealthCheckFailed",
            "severity": "Critical",
            "source": "nodehealth-agent",
            "reason": reason,
            "message": trim(f"run_nhc.sh exit={exit_code}\nstdout:\n{stdout}\nstderr:\n{stderr}"),
            # Keep the actual probe-completion time, rather than the
            # later API-create time after failure suppression/retry handling.
            "observedAt": observed_at,
            "action": {
                "taintNode": os.getenv("FT_TAINT_NODE_ON_NHC_FAIL", "false").lower() == "true",
                "deletePods": os.getenv("FT_DELETE_PODS_ON_NHC_FAIL", "false").lower() == "true",
            },
        },
    }
    api.create_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, body)
    print(
        f"[nodehealth] created FaultEvent {namespace}/{name} "
        f"observedAt={observed_at}",
        flush=True,
    )


def main() -> None:
    load_kube()
    custom_api = client.CustomObjectsApi()

    namespace = os.getenv("POD_NAMESPACE", "hygon-ft")
    node_name = os.getenv("NODE_NAME") or socket.gethostname()
    script_path = os.getenv("NHC_SCRIPT_PATH", "/opt/hygon-ft/nhc/run_nhc.sh")
    interval = int(os.getenv("NHC_INTERVAL_SECONDS", "30"))
    timeout = int(os.getenv("NHC_TIMEOUT_SECONDS", "300"))
    suppress_seconds = int(os.getenv("NHC_FAILURE_SUPPRESS_SECONDS", "300"))
    timeout_is_failure = os.getenv("NHC_TIMEOUT_IS_FAILURE", "false").lower() == "true"
    last_failure_at = 0.0

    print(
        f"[nodehealth] started startedAt={now_iso()} "
        f"node={node_name} script={script_path}",
        flush=True,
    )

    while True:
        try:
            result = subprocess.run(
                ["bash", script_path],
                text=True,
                capture_output=True,
                timeout=timeout,
            )
            probe_text = f"{result.stdout}\n{result.stderr}"
            # Capture this once, immediately after the probe completes.  It is
            # the diagnostic time for this result and remains stable if a
            # FaultEvent is created a little later.
            checked_at = now_iso()
            if result.returncode == 0 and "[NHC PROBE ERROR]" not in probe_text:
                print(
                    "[nodehealth] healthy "
                    f"checkedAt={checked_at} exit={result.returncode} "
                    f"stdout={trim(result.stdout, 800)} "
                    f"stderr={trim(result.stderr, 800)}",
                    flush=True,
                )
            elif result.returncode == 2:
                now = time.time()
                print(
                    f"[nodehealth] unhealthy checkedAt={checked_at} "
                    f"exit={result.returncode} "
                    f"stdout={trim(result.stdout, 800)} stderr={trim(result.stderr, 800)}",
                    flush=True,
                )
                if now - last_failure_at >= suppress_seconds:
                    create_fault_event(
                        custom_api,
                        namespace,
                        node_name,
                        "run_nhc_failed",
                        result.stdout,
                        result.stderr,
                        result.returncode,
                        checked_at,
                    )
                    last_failure_at = now
            elif result.returncode == 3 or "[NHC REPORT ONLY]" in probe_text:
                print(
                    "[nodehealth] report-only NHC finding; no FaultEvent created "
                    f"checkedAt={checked_at} exit={result.returncode} "
                    f"stdout={trim(result.stdout, 800)} "
                    f"stderr={trim(result.stderr, 800)}",
                    flush=True,
                )
            else:
                print(
                    "[nodehealth] probe error; no FaultEvent created "
                    f"checkedAt={checked_at} exit={result.returncode} "
                    f"stdout={trim(result.stdout, 800)} "
                    f"stderr={trim(result.stderr, 800)}",
                    flush=True,
                )
        except subprocess.TimeoutExpired as exc:
            now = time.time()
            checked_at = now_iso()
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            print(
                f"[nodehealth] run_nhc.sh timeout checkedAt={checked_at} "
                f"after {timeout}s timeout_is_failure={timeout_is_failure}",
                flush=True,
            )
            if timeout_is_failure and now - last_failure_at >= suppress_seconds:
                create_fault_event(
                    custom_api,
                    namespace,
                    node_name,
                    "run_nhc_timeout",
                    stdout,
                    stderr,
                    124,
                    checked_at,
                )
                last_failure_at = now
        except ApiException as exc:
            print(
                f"[nodehealth] Kubernetes API error checkedAt={now_iso()}: {exc}",
                flush=True,
            )
        except Exception as exc:
            print(
                f"[nodehealth] unexpected error checkedAt={now_iso()}: {exc}",
                flush=True,
            )

        time.sleep(interval)


if __name__ == "__main__":
    main()
