# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Read-only container identity checks shared by status, probes and active tests.

Container lifecycle changes are explicit operations in ``lifecycle.py``.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import replace
from pathlib import Path
from typing import Any, Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig
from hcu_envcheck.baremetal_cluster import (
    BaremetalPreflightPolicy,
    build_remote_probe_command,
    evaluate_node_result,
)


class ContainerPreflightError(RuntimeError):
    """Active execution was stopped before creating a run directory."""

    def __init__(self, report: dict[str, Any]):
        super().__init__("container active-test preflight failed")
        self.report = report


def fold_node_names(nodes: Sequence[str]) -> str:
    """Render compact, unambiguous host ranges while retaining node identity."""

    ordered = sorted(
        set(nodes),
        key=lambda name: tuple(
            (1, int(part)) if part.isdigit() else (0, part)
            for part in re.split(r"(\d+)", name)
        ),
    )
    buckets: dict[tuple[str, int], list[int]] = {}
    plain: list[str] = []
    for node in ordered:
        match = re.fullmatch(r"(.*?)(\d+)", node)
        if match is None:
            plain.append(node)
            continue
        buckets.setdefault((match.group(1), len(match.group(2))), []).append(int(match.group(2)))
    labels = plain[:]
    for (prefix, width), numbers in buckets.items():
        if len(numbers) == 1:
            labels.append(f"{prefix}{numbers[0]:0{width}d}")
            continue
        ranges: list[str] = []
        first = previous = numbers[0]
        for number in numbers[1:] + [None]:
            if number is not None and number == previous + 1:
                previous = number
                continue
            begin = f"{first:0{width}d}"
            end = f"{previous:0{width}d}"
            ranges.append(begin if first == previous else f"{begin}-{end}")
            if number is not None:
                first = previous = number
        labels.append(f"{prefix}[{','.join(ranges)}]")
    return ",".join(labels)


def grouped_issue_lines(issues: Sequence[dict[str, str]]) -> list[str]:
    """One terminal line per identical cause, with a folded node list."""

    groups: dict[tuple[str, str], list[str]] = {}
    for item in issues:
        key = (str(item.get("code") or "UNKNOWN"), str(item.get("message") or ""))
        groups.setdefault(key, []).append(str(item.get("node") or "cluster"))
    return [f"nodes={fold_node_names(nodes)} code={code} reason={message}" for (code, message), nodes in groups.items()]


def evaluate_container_inventory(
    nodes: Sequence[str],
    results: dict[str, Any],
    *,
    container_name: str,
    expected_image: str | None,
    launcher: str,
    allow_root_mpi: bool,
) -> dict[str, Any]:
    """Classify exact-name Docker inspect results without choosing a leader baseline."""

    inventory: dict[str, dict[str, Any]] = {}
    issues: list[dict[str, str]] = []

    def issue(node: str, code: str, message: str) -> None:
        issues.append({"node": node, "code": code, "message": message})

    for node in nodes:
        result = results.get(node)
        item: dict[str, Any] = {"container_name": container_name, "status": "UNKNOWN"}
        inventory[node] = item
        if result is None:
            issue(node, "CONTAINER_INSPECT_MISSING", "no remote Docker inspect result")
            continue
        item["returncode"] = result.returncode
        item["error_kind"] = result.error_kind
        item["evidence_dir"] = result.result_dir
        if not result.success:
            stderr = (result.stderr or "").strip()
            if "No such object" in stderr or "No such container" in stderr:
                issue(node, "CONTAINER_MISSING", f"container {container_name!r} does not exist")
            else:
                issue(node, "CONTAINER_INSPECT_FAILED", f"Docker/SSH inspect failed: {result.error_kind or stderr or result.returncode}")
            continue
        fields = result.stdout.strip().split("|", 4)
        if len(fields) != 5:
            issue(node, "CONTAINER_INSPECT_INVALID", "expected exact-name Docker metadata (name, image, ID, running, user)")
            continue
        actual_name, image, image_id, running, user = fields
        if actual_name != f"/{container_name}":
            issue(node, "CONTAINER_NAME_MISMATCH", f"requested {container_name!r}, got {actual_name!r}")
            continue
        if not image or not image_id.startswith("sha256:"):
            issue(node, "CONTAINER_INSPECT_INVALID", "Docker did not return a container image tag and immutable image ID")
            continue
        item.update({
            "status": "PRESENT",
            "image": image,
            "image_id": image_id,
            "running": running == "true",
            "configured_user": user or "root (image default)",
        })
        if not item["running"]:
            issue(node, "CONTAINER_STOPPED", f"container {container_name!r} is not running")
        if expected_image and image != expected_image:
            issue(node, "CONTAINER_IMAGE_MISMATCH", f"container image {image!r} != requested {expected_image!r}; recreate required")
        if launcher in {"mpirun", "mpirun-torchrun"} and user.split(":", 1)[0] in {"", "0", "root"} and not allow_root_mpi:
            issue(node, "MPI_ROOT_FORBIDDEN", "container runs as root but the internal MPI root policy was not enabled")

    present = [entry for entry in inventory.values() if entry.get("status") == "PRESENT"]
    image_names = {entry.get("image") for entry in present}
    image_ids = {entry.get("image_id") for entry in present}
    if len(image_names) > 1 or len(image_ids) > 1:
        issue("cluster", "CONTAINER_IMAGE_INCONSISTENT", "container image names or immutable image IDs differ across nodes; recreate all containers from one image")
    image_reference = expected_image or (next(iter(image_names)) if len(image_names) == 1 else None)
    return {
        "nodes": inventory,
        "issues": issues,
        "image_reference": image_reference,
        "image_ids": sorted(str(value) for value in image_ids),
    }


def run_container_status(
    *, nodes: Sequence[str], container_name: str, expected_image: str | None,
    transport: str, concurrency: int, run_dir: Path, execution_config=None,
) -> dict[str, Any]:
    """Inspect existing containers on the host; never source env.sh or pull images."""

    executor = BaremetalClusterExecutor(
        nodes,
        replace(execution_config, output_root=run_dir / "container-status-evidence", command_timeout_seconds=30)
        if execution_config is not None else BaremetalExecutionConfig(
            output_root=run_dir / "container-status-evidence",
            transport=transport,
            concurrency=concurrency,
            command_timeout_seconds=30,
        ),
    )
    inspected = executor.execute(
        "container-status",
        ["docker", "inspect", "--type", "container", "--format",
         "{{.Name}}|{{.Config.Image}}|{{.Image}}|{{.State.Running}}|{{.Config.User}}",
         container_name],
    )
    report = evaluate_container_inventory(
        nodes, inspected.nodes, container_name=container_name,
        expected_image=expected_image, launcher="", allow_root_mpi=False,
    )
    expanded_issues: list[dict[str, str]] = []
    for issue in report["issues"]:
        if issue["node"] == "cluster" and issue["code"] == "CONTAINER_IMAGE_INCONSISTENT":
            expanded_issues.extend(
                {**issue, "node": node}
                for node, item in report["nodes"].items() if item.get("status") == "PRESENT"
            )
        else:
            expanded_issues.append(issue)
    report["issues"] = expanded_issues
    invalid_nodes = {issue["node"] for issue in report["issues"] if issue["node"] != "cluster"}
    report["ready_nodes"] = [node for node in nodes if node not in invalid_nodes]
    report["status"] = "PASS" if not report["issues"] else "FAIL"
    report["expected_image_specified"] = expected_image is not None
    return report


def container_status_excluded_records(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Represent rejected containers as unprobed nodes in a basic report."""

    ready = set(report["ready_nodes"])
    cluster_issues = [issue for issue in report["issues"] if issue["node"] == "cluster"]
    records: dict[str, dict[str, Any]] = {}
    for node, item in report["nodes"].items():
        if node in ready:
            continue
        issues = [issue for issue in report["issues"] if issue["node"] == node]
        if item.get("status") == "PRESENT":
            issues += cluster_issues
        known_bad = any(issue["code"] in {
            "CONTAINER_MISSING", "CONTAINER_STOPPED", "CONTAINER_NAME_MISMATCH",
            "CONTAINER_IMAGE_MISMATCH", "CONTAINER_IMAGE_INCONSISTENT",
        } for issue in issues)
        records[node] = {
            "node": node,
            "status": "BLOCKED" if known_bad else "INCOMPLETE",
            "reachable": (
                item.get("status") == "PRESENT"
                or item.get("error_kind") == "REMOTE_COMMAND_FAILED"
                or any(issue["code"] in {"CONTAINER_MISSING", "CONTAINER_NAME_MISMATCH", "CONTAINER_INSPECT_INVALID"} for issue in issues)
            ),
            "device_count": None,
            "devices": [],
            "metric_summary": {"max_vram_used_percent": None, "max_hcu_util_percent": None},
            "findings": [{"severity": "FAIL" if known_bad else "UNKNOWN",
                          "reason_code": issue["code"], "message": issue["message"]} for issue in issues],
            "checks": [],
            "environment": {},
            "software_environment": {"mode": "NOT_SELECTED", "status": "NOT_CHECKED",
                                     "message": "container status failed; environment probe was skipped"},
            "container_status": {key: value for key, value in item.items() if key != "evidence_dir"},
            "probe_transport": {"returncode": item.get("returncode"), "error_kind": "CONTAINER_STATUS_FAILED"},
        }
    return records


def run_container_preflight(
    *,
    nodes: Sequence[str],
    container_name: str,
    expected_image: str | None,
    launcher: str,
    allow_root_mpi: bool,
    env_script: str | None,
    container_shell: str,
    transport: str,
    concurrency: int,
    run_dir: Path,
    check_idle: bool = True,
    container_workdir: str | None = None,
    remote_python: str = "python3",
    mpi_groups: Sequence[Sequence[str]] = (),
    container_ssh_port: int = 25901,
    execution_config=None,
    run_token: str | None = None,
) -> dict[str, Any]:
    """Inspect containers, then sample HCU/DCU state on available nodes."""

    report = run_container_status(
        nodes=nodes, container_name=container_name, expected_image=expected_image,
        transport=transport, concurrency=concurrency, run_dir=run_dir / "preflight", execution_config=execution_config,
    )
    if launcher in {"mpirun", "mpirun-torchrun"} and not allow_root_mpi:
        for node in report["ready_nodes"]:
            user = str(report["nodes"][node].get("configured_user") or "").split(":", 1)[0]
            if user in {"", "0", "root", "root (image default)"}:
                report["issues"].append({
                    "node": node, "code": "MPI_ROOT_FORBIDDEN",
                    "message": "container runs as root but the internal MPI root policy was not enabled",
                })
    ready_nodes = list(report["ready_nodes"])
    if ready_nodes:
        # Validate env.sh independently of DCU sampling so an arbitrary
        # module/DTK/Conda error has an explicit reason even with no stderr.
        from .env import bootstrap_command
        from .task_control import managed_command
        environment_command = bootstrap_command(env_script, ["true"], shell=container_shell, workdir=container_workdir)
        environment_command = ["timeout", "--signal=TERM", "--kill-after=5s", "165s", *environment_command]
        if run_token:
            environment_command = managed_command(environment_command, run_token)
        environment = BaremetalClusterExecutor(
            ready_nodes,
            replace(execution_config, output_root=run_dir / "preflight" / "environment-evidence", command_timeout_seconds=180)
            if execution_config is not None else BaremetalExecutionConfig(
                output_root=run_dir / "preflight" / "environment-evidence",
                transport=transport, concurrency=concurrency,
                command_timeout_seconds=180,
            ),
        ).execute("active-environment-check", [
            "docker", "exec", *(["-w", container_workdir] if container_workdir else []), container_name,
            *environment_command,
        ])
        failed_environment = set()
        for node in ready_nodes:
            result = environment.nodes.get(node)
            if result is None or not result.success:
                report["execution_failed"] = True
                failed_environment.add(node)
                report["issues"].append({
                    "node": node, "code": "ENV_SCRIPT_ERROR",
                    "message": (result.stderr or "environment setup failed")[:500] if result else "no environment result",
                })
        ready_nodes = [node for node in ready_nodes if node not in failed_environment]
        report["ready_nodes"] = ready_nodes
    if ready_nodes and check_idle:
        policy = BaremetalPreflightPolicy(
            samples=3,
            busy_sample_quorum=2,
            sample_interval_seconds=1.0,
            rdma_counter_interval_seconds=0,
            software_mode="host-python",
            env_script=env_script,
            execution_scope="container",
            container_name=container_name,
            container_shell=container_shell,
            container_workdir=container_workdir,
            check_categories=("resource",),
            run_token=run_token,
        )
        probe_config = replace(execution_config, output_root=run_dir / "preflight" / "resource-evidence", command_timeout_seconds=180) \
            if execution_config is not None else BaremetalExecutionConfig(
            output_root=run_dir / "preflight" / "resource-evidence",
            transport=transport,
            concurrency=concurrency,
            command_timeout_seconds=180,
        )
        probe = BaremetalClusterExecutor(ready_nodes, probe_config).execute(
            "active-resource-probe", build_remote_probe_command(policy, remote_python),
        )
        for node in ready_nodes:
            record = evaluate_node_result(node, probe.nodes[node], policy)
            item = report["nodes"][node]
            remote_stderr = (probe.nodes[node].stderr or "").strip()
            if remote_stderr:
                item["resource_probe_stderr_excerpt"] = remote_stderr[:600]
                if any(marker in remote_stderr for marker in (
                    "cp: cannot stat", "conda: command not found",
                    "module: command not found",
                )) or (
                    env_script is not None and any(
                        env_script in line and "No such file or directory" in line
                        for line in remote_stderr.splitlines()
                    )
                ):
                    report["issues"].append({
                        "node": node, "code": "ENV_SCRIPT_ERROR",
                        "message": f"environment setup emitted an error: {remote_stderr.splitlines()[0][:240]}",
                    })
            devices = record.get("devices") or []
            item["resource"] = {
                "status": record.get("status"),
                "device_count": record.get("device_count"),
                "metric_summary": record.get("metric_summary"),
                "findings": [
                    {key: finding.get(key) for key in ("severity", "reason_code", "message", "device_id")}
                    for finding in record.get("findings") or []
                ],
                "devices": [
                    {key: device.get(key) for key in ("device_id", "status", "reason_codes", "used_mib", "memory_used_percent", "hcu_util_percent")}
                    for device in devices
                ],
                "evidence_dir": (record.get("probe_transport") or {}).get("result_dir"),
            }
            if not record.get("reachable") or not devices:
                first = (record.get("findings") or [{}])[0]
                report["issues"].append({"node": node, "code": "RESOURCE_EVIDENCE_INCOMPLETE", "message": f"cannot confirm HCU/DCU idle state: {first.get('reason_code') or 'no devices'} {str(first.get('message') or '')[:200]}; see resource evidence"})
                continue
            non_idle = [device for device in devices if device.get("status") == "FAIL"]
            uncertain = [device for device in devices if device.get("status") not in {"PASS", "FAIL"}]
            if non_idle:
                ids = ",".join(str(device.get("device_id")) for device in non_idle)
                reasons = sorted({str(code) for device in non_idle for code in device.get("reason_codes") or []})
                used = [float(device["used_mib"]) for device in non_idle if device.get("used_mib") is not None]
                report["issues"].append({"node": node, "code": "DCU_BUSY", "message": f"HCU/DCU devices {ids} failed idle checks; reasons={','.join(reasons) or '-'} max_used_mib={max(used) if used else '-'}; see resource evidence"})
            if uncertain:
                ids = ",".join(str(device.get("device_id")) for device in uncertain)
                report["issues"].append({"node": node, "code": "DCU_IDLE_UNCERTAIN", "message": f"HCU/DCU devices {ids} have WARN/UNKNOWN idle status; confirm before active tests"})

    if mpi_groups and not report["issues"]:
        report["issues"].extend(verify_container_mpi_peers(
            nodes=nodes, groups=mpi_groups, container_name=container_name,
            port=container_ssh_port, transport=transport, concurrency=concurrency, run_dir=run_dir,
            execution_config=execution_config,
        ))
    image = report.get("image_reference")
    if image:
        report["recreate_command_template"] = (
            "bin/hcu-cluster-run per-node-container container-recreate -f <hostfile> --container "
            + shlex.quote(container_name) + " -i " + shlex.quote(str(image))
            + " --yes"
            + " -v <required-host-mount:container-mount>"
        )
        report["offline_image_hint"] = "If image pull fails, retry with --image-tar <shared-image.tar> after placing the tar on every node's shared path."
    report["status"] = "FAIL" if report["issues"] else "PASS"
    return report


def verify_container_mpi_peers(*, nodes, groups, container_name, port, transport, concurrency, run_dir, execution_config=None):
    """Compare namespaces AND UID via Docker vs the actual MPI SSH route.

    A listening port alone does not prove that ssh enters the named container.
    This read-only probe never starts MPI, touches a GPU or changes sshd.
    """
    from concurrent.futures import ThreadPoolExecutor
    fingerprint = ('set -e; printf "__HCU_MPI_BOOT__="; cat /proc/sys/kernel/random/boot_id; '
                   'printf "__HCU_MPI_MNT__="; readlink /proc/self/ns/mnt; '
                   'printf "__HCU_MPI_PID__="; readlink /proc/self/ns/pid; '
                   'printf "__HCU_MPI_UID__="; id -u')
    def identity(stdout):
        values = {}
        for line in stdout.splitlines():
            match = re.fullmatch(r"__HCU_MPI_(BOOT|MNT|PID|UID)__=(.+)", line.strip())
            if match:
                if match[1] in values:
                    return None
                values[match[1]] = match[2]
        return values if set(values) == {"BOOT", "MNT", "PID", "UID"} else None
    config = replace(execution_config, output_root=run_dir / "mpi-identity", command_timeout_seconds=30) \
        if execution_config is not None else BaremetalExecutionConfig(output_root=run_dir / "mpi-identity", transport=transport,
                                     concurrency=concurrency, command_timeout_seconds=30)
    expected = BaremetalClusterExecutor(nodes, config).execute(
        "mpi-docker-identity", ["docker", "exec", container_name, "sh", "-c", fingerprint])
    def check(pair):
        leader, node = pair
        # MPI's default ssh identity/user are deliberately preserved. The UID
        # comparison rejects a different SSH user that cleanup cannot control.
        command = ["docker", "exec", container_name, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                   "-p", str(port), node, shlex.join(["sh", "-c", fingerprint])]
        current = BaremetalClusterExecutor([leader], config).execute("mpi-peer-" + node, command)
        actual, reference = current.nodes.get(leader), expected.nodes.get(node)
        if not actual or not actual.success:
            return {"node": node, "code": "CONTAINER_SSH_UNREACHABLE",
                    "message": f"leader={leader} cannot reach container SSH port={port}: " +
                               ((actual.stderr or actual.error_kind or str(actual.returncode))[:300] if actual else "no SSH evidence")}
        actual_identity = identity(actual.stdout)
        if not reference or not reference.success or actual_identity is None or actual_identity != identity(reference.stdout):
            return {"node": node, "code": "CONTAINER_SSH_IDENTITY_MISMATCH",
                    "message": f"leader={leader} ssh -p {port} does not match docker exec {container_name!r} namespace/user; check container SSH port and user"}
        return None
    pairs = [(group[0], node) for group in groups if len(group) > 1 for node in group]
    with ThreadPoolExecutor(max_workers=min(concurrency, max(1, len(pairs)))) as pool:
        return [issue for issue in pool.map(check, pairs) if issue]
