# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import math
import re
import shlex
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from .active_rdma import SlurmActiveCheckRunner
from .baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig, BaremetalNodeResult
from .models import Finding
from .output import atomic_write_text_exclusive


RunText = Callable[..., subprocess.CompletedProcess[str]]
Which = Callable[[str], str | None]

_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_./:@%+=,-]+$")
_SAFE_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HCA_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_SSH_SENTINEL_RE = re.compile(r"^__HCU_ENVCHECK_IB_RC_[0-9a-f]+__=(-?\d+)\r?$")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    atomic_write_text_exclusive(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


def _safe_token(value: str, label: str) -> str:
    if not value or value.startswith("-") or "\x00" in value or not _SAFE_TOKEN_RE.fullmatch(value):
        raise ValueError(f"{label} contains unsafe characters: {value!r}")
    return value


def _safe_command(command: Sequence[str], label: str) -> tuple[str, ...]:
    if isinstance(command, (str, bytes)) or not command:
        raise ValueError(f"{label} must be a non-empty argument sequence")
    normalized: list[str] = []
    for index, item in enumerate(command):
        value = str(item)
        if (
            not value
            or "\x00" in value
            or not _SAFE_TOKEN_RE.fullmatch(value)
            or (index == 0 and value.startswith("-"))
        ):
            raise ValueError(f"{label} argument contains unsafe characters: {value!r}")
        normalized.append(value)
    return tuple(normalized)


def _safe_env(environment: dict[str, str]) -> dict[str, str]:
    safe: dict[str, str] = {}
    for name, value in environment.items():
        if not _SAFE_ENV_NAME_RE.fullmatch(name):
            raise ValueError(f"unsafe environment variable name: {name!r}")
        if not isinstance(value, str) or any(ch in value for ch in "\x00\r\n"):
            raise ValueError(f"unsafe environment variable value for {name}")
        safe[name] = value
    return safe


def _result_finding(severity: str, reason_code: str, message: str) -> dict[str, Any]:
    return asdict(Finding(severity, reason_code, message))


def _command_summary(result: BaremetalNodeResult) -> dict[str, Any]:
    return result.to_command_result().summary()


@dataclass(frozen=True)
class NHCCheckConfig:
    enabled: bool = False
    command: tuple[str, ...] = ("run_nhc",)
    installation_source: str | None = None
    config: str | None = None
    selected: str | None = None
    removed: str | None = None
    extra_args: tuple[str, ...] = ()
    environment: dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 600.0

    def validate(self) -> None:
        if not self.enabled:
            return
        _safe_command(self.command, "NHC command")
        if self.installation_source is not None:
            _safe_token(self.installation_source, "NHC installation source")
        if self.config is not None:
            _safe_token(self.config, "NHC config")
        if self.selected is not None:
            _safe_token(self.selected, "NHC selected")
        if self.removed is not None:
            _safe_token(self.removed, "NHC removed")
        if self.extra_args:
            _safe_command(self.extra_args, "NHC extra args")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 3600:
            raise ValueError("nhc timeout must be in (0, 3600]")
        _safe_env(self.environment)

    def argv(self) -> list[str]:
        self.validate()
        command = list(self.command)
        if self.config:
            command.extend(["--config", self.config])
        if self.selected:
            command.extend(["--selected", self.selected])
        if self.removed:
            command.extend(["--removed", self.removed])
        command.extend(self.extra_args)
        return command


@dataclass(frozen=True)
class IBStateCheckConfig:
    enabled: bool = False
    command: tuple[str, ...] = ("ibstat",)
    timeout_seconds: float = 30.0

    def validate(self) -> None:
        if not self.enabled:
            return
        _safe_command(self.command, "ibstat command")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 300:
            raise ValueError("ibstat timeout must be in (0, 300]")


@dataclass(frozen=True)
class IBWriteBandwidthConfig:
    enabled: bool = False
    tool: str = "ib_write_bw"
    protocol: str = "ib"
    device: str | None = None
    ib_port: int = 1
    gid_index: int | None = None
    control_port: int = 18515
    message_bytes: int = 1 << 20
    iterations: int = 1000
    minimum_average_gbps: float | None = None
    startup_grace_seconds: float = 1.0
    timeout_seconds: float = 120.0
    concurrency: int = 1
    max_tests: int = 1024

    def validate(self) -> None:
        if not self.enabled:
            return
        _safe_token(self.tool, "IB tool")
        if self.tool not in {"ib_write_bw", "ib_send_bw", "ib_read_bw"}:
            raise ValueError("IB tool must be ib_write_bw, ib_send_bw, or ib_read_bw")
        if self.protocol not in {"ib", "roce"}:
            raise ValueError("IB protocol must be ib or roce")
        if self.device is not None:
            if not _HCA_RE.fullmatch(self.device):
                raise ValueError(f"IB device contains unsafe characters: {self.device!r}")
        if not 1 <= self.ib_port <= 255:
            raise ValueError("IB port must be between 1 and 255")
        if self.protocol == "roce" and self.gid_index is None:
            raise ValueError("RoCE IB bandwidth tests require --ib-gid-index")
        if self.gid_index is not None and not 0 <= self.gid_index <= 255:
            raise ValueError("IB gid index must be between 0 and 255")
        if not 1024 <= self.control_port <= 65535:
            raise ValueError("IB control port must be between 1024 and 65535")
        if not 1 <= self.message_bytes <= 64 * 1024 * 1024:
            raise ValueError("IB message bytes must be between 1 and 64 MiB")
        if not 1 <= self.iterations <= 100_000:
            raise ValueError("IB iterations must be between 1 and 100000")
        if self.minimum_average_gbps is not None and (
            not math.isfinite(self.minimum_average_gbps) or self.minimum_average_gbps < 0
        ):
            raise ValueError("IB minimum bandwidth must be finite and non-negative")
        if self.startup_grace_seconds < 0 or self.startup_grace_seconds > 30:
            raise ValueError("IB startup grace must be between 0 and 30 seconds")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 1800:
            raise ValueError("IB timeout must be in (0, 1800]")
        if not 1 <= self.concurrency <= 128:
            raise ValueError("IB concurrency must be between 1 and 128")
        if not 1 <= self.max_tests <= 4096:
            raise ValueError("IB maximum tests must be between 1 and 4096")

    def perftest_argv(
        self,
        *,
        server: str | None = None,
        device: str | None = None,
        control_port: int | None = None,
    ) -> list[str]:
        self.validate()
        command = [
            self.tool,
            "--connection=RC",
            f"--ib-port={self.ib_port}",
            f"--port={self.control_port if control_port is None else control_port}",
            f"--size={self.message_bytes}",
            f"--iters={self.iterations}",
            "--report_gbits",
        ]
        selected_device = self.device if device is None else device
        if selected_device:
            if not _HCA_RE.fullmatch(selected_device):
                raise ValueError(f"IB device contains unsafe characters: {selected_device!r}")
            command.append(f"--ib-dev={selected_device}")
        if self.gid_index is not None:
            command.append(f"--gid-index={self.gid_index}")
        if server is not None:
            command.append(server)
        return command


@dataclass(frozen=True)
class ClusterExtraCheckConfig:
    ib_state: IBStateCheckConfig = field(default_factory=IBStateCheckConfig)
    nhc: NHCCheckConfig = field(default_factory=NHCCheckConfig)
    ib: IBWriteBandwidthConfig = field(default_factory=IBWriteBandwidthConfig)

    def validate(self) -> None:
        self.ib_state.validate()
        self.nhc.validate()
        self.ib.validate()


@dataclass(frozen=True)
class IBTestSpec:
    source: str
    destination: str
    source_hca: str
    destination_hca: str
    rail_index: int

    @property
    def name(self) -> str:
        return (
            f"{self.source}:{self.source_hca}"
            f"->{self.destination}:{self.destination_hca}"
        )


def build_ib_test_plan(
    nodes: Sequence[str],
    hcas_by_node: dict[str, Sequence[str]],
) -> list[IBTestSpec]:
    ordered = list(dict.fromkeys(str(node) for node in nodes))
    if len(ordered) < 2:
        return []
    missing = [node for node in ordered if not hcas_by_node.get(node)]
    if missing:
        raise ValueError(f"missing supported HCA inventory for nodes: {','.join(missing)}")
    counts = {len(hcas_by_node[node]) for node in ordered}
    if len(counts) != 1:
        details = ", ".join(f"{node}={len(hcas_by_node[node])}" for node in ordered)
        raise ValueError(f"HCA count mismatch across selected nodes: {details}")
    plan: list[IBTestSpec] = []
    rail_count = counts.pop()
    for source in ordered:
        for destination in ordered:
            if source == destination:
                continue
            for rail_index in range(rail_count):
                plan.append(
                    IBTestSpec(
                        source=source,
                        destination=destination,
                        source_hca=str(hcas_by_node[source][rail_index]),
                        destination_hca=str(hcas_by_node[destination][rail_index]),
                        rail_index=rail_index,
                    )
                )
    return plan


def evaluate_nhc_output(
    returncode: int,
    stdout: str,
    stderr: str,
    timed_out: bool,
    *,
    installation_source: str | None = None,
) -> dict[str, Any]:
    combined = f"{stdout}\n{stderr}"
    result_marker = re.search(r"^\[CHECK RESULT\]:\s*(.*?)\s*$", combined, re.M)
    result_text = result_marker.group(1).strip() if result_marker else None
    show_installation_source = False
    if timed_out:
        status, reason, message = "NOT_VERIFIED", "NHC_CHECK_TIMEOUT", "NHC command timed out"
    elif result_text and result_text not in {"PASSED", "PASS"}:
        status, reason, message = "FAIL", "NHC_CHECK_FAILED", "NHC check reported failures"
    elif returncode == 0 and (result_text in {"PASSED", "PASS"} or re.search(r"^PASSED\s*$", combined, re.M)):
        status, reason, message = "PASS", "NHC_CHECK_PASSED", "NHC check passed"
    elif returncode == 127:
        status, reason = "NOT_VERIFIED", "NHC_COMMAND_NOT_FOUND"
        message = "run_nhc is not available on the node PATH; install or repair the host command"
        show_installation_source = installation_source is not None
    elif returncode == 125:
        status, reason = "NOT_VERIFIED", "NHC_EXECUTION_ERROR"
        message = "NHC supervisor reported an execution error; verify the host run_nhc installation"
        show_installation_source = installation_source is not None
    elif returncode != 0:
        status, reason = "NOT_VERIFIED", "NHC_EXECUTION_FAILED"
        message = f"run_nhc could not produce a health result (rc={returncode}); verify the host command"
        show_installation_source = installation_source is not None
    else:
        status, reason = "NOT_VERIFIED", "NHC_RESULT_MARKER_MISSING"
        message = "run_nhc output did not contain a parseable result marker; verify the host command"
        show_installation_source = installation_source is not None
    result = {
        "status": status,
        "reason_code": reason,
        "message": message,
        "returncode": returncode,
        "timed_out": timed_out,
    }
    if show_installation_source:
        result["installation_source"] = installation_source
    return result


def _natural_key(value: str) -> list[Any]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", value)]


def parse_ibstat_output(text: str) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    current_device: dict[str, Any] | None = None
    current_port: dict[str, Any] | None = None
    for raw_line in text.splitlines():
        ca_match = re.match(r"^\s*CA\s+'([^']+)'", raw_line)
        if ca_match:
            current_device = {"name": ca_match.group(1), "ports": []}
            devices.append(current_device)
            current_port = None
            continue
        if current_device is None:
            continue
        port_match = re.match(r"^\s*Port\s+(\d+):", raw_line)
        if port_match:
            current_port = {"port": int(port_match.group(1))}
            current_device["ports"].append(current_port)
            continue
        if current_port is None:
            continue
        field_match = re.match(
            r"^\s*(State|Physical state|Rate|Link layer):\s*(.*?)\s*$",
            raw_line,
        )
        if field_match:
            key = field_match.group(1).lower().replace(" ", "_")
            current_port[key] = field_match.group(2)
    return devices


def evaluate_ib_state_output(
    returncode: int,
    stdout: str,
    stderr: str,
    timed_out: bool,
) -> dict[str, Any]:
    devices = parse_ibstat_output(stdout)
    supported_hcas = sorted(
        (
            device["name"]
            for device in devices
            if re.match(r"^(?:mlx|shca)", device["name"])
            and "bond" not in device["name"].lower()
            and _HCA_RE.fullmatch(device["name"])
        ),
        key=_natural_key,
    )
    incomplete_ports: list[str] = []
    inactive_ports: list[str] = []
    for device in devices:
        if not device["ports"]:
            incomplete_ports.append(f"{device['name']}:no-port")
            continue
        for port in device["ports"]:
            label = f"{device['name']}/{port['port']}"
            state = port.get("state")
            physical_state = port.get("physical_state")
            if not state or not physical_state:
                incomplete_ports.append(label)
            elif state != "Active" or physical_state != "LinkUp":
                inactive_ports.append(
                    f"{label}:state={state},physical_state={physical_state}"
                )
    if timed_out:
        status, reason, message = (
            "NOT_VERIFIED",
            "IB_STATE_TIMEOUT",
            "ibstat command timed out",
        )
    elif returncode == 127:
        status, reason, message = (
            "NOT_VERIFIED",
            "IBSTAT_COMMAND_NOT_FOUND",
            "ibstat is not available on the node",
        )
    elif returncode != 0:
        status, reason, message = (
            "NOT_VERIFIED",
            "IBSTAT_EXECUTION_FAILED",
            f"ibstat exited with rc={returncode}",
        )
    elif not devices:
        status, reason, message = (
            "FAIL",
            "IB_HCA_NOT_FOUND",
            "ibstat did not report any HCA",
        )
    elif not supported_hcas:
        status, reason, message = (
            "FAIL",
            "SUPPORTED_IB_HCA_NOT_FOUND",
            "ibstat did not report any mlx* or shca* HCA",
        )
    elif inactive_ports:
        status, reason, message = (
            "FAIL",
            "IB_PORT_NOT_ACTIVE",
            "inactive IB ports: " + ", ".join(inactive_ports),
        )
    elif incomplete_ports:
        status, reason, message = (
            "NOT_VERIFIED",
            "IB_STATE_EVIDENCE_INCOMPLETE",
            "incomplete ibstat port evidence: " + ", ".join(incomplete_ports),
        )
    else:
        status, reason, message = (
            "PASS",
            "IB_STATE_PASSED",
            f"all {len(supported_hcas)} supported HCA ports are Active/LinkUp",
        )
    return {
        "status": status,
        "reason_code": reason,
        "message": message,
        "returncode": returncode,
        "timed_out": timed_out,
        "devices": devices,
        "supported_hcas": supported_hcas,
    }


def evaluate_ib_write_bw_output(
    returncode: int,
    stdout: str,
    stderr: str,
    timed_out: bool,
    *,
    minimum_gbps: float | None = None,
) -> dict[str, Any]:
    combined = f"{stdout}\n{stderr}"
    average = SlurmActiveCheckRunner._parse_verbs_average_gbps(combined)
    metadata = SlurmActiveCheckRunner._parse_verbs_endpoint_metadata(combined)
    if timed_out:
        status, reason, message = "NOT_VERIFIED", "IB_WRITE_BW_TIMEOUT", "ib_write_bw test timed out"
    elif returncode != 0:
        status, reason, message = "FAIL", "IB_WRITE_BW_FAILED", f"ib_write_bw exited with rc={returncode}"
    elif average is None:
        status, reason, message = "NOT_VERIFIED", "IB_WRITE_BW_METRIC_MISSING", "ib_write_bw output did not contain a bandwidth result row"
    elif minimum_gbps is not None and average < minimum_gbps:
        status, reason, message = "FAIL", "IB_BANDWIDTH_BELOW_THRESHOLD", f"average bandwidth {average} Gbit/s is below {minimum_gbps}"
    else:
        status, reason, message = "PASS", "IB_WRITE_BW_PASSED", "ib_write_bw connectivity and bandwidth smoke test passed"
    return {
        "status": status,
        "reason_code": reason,
        "message": message,
        "average_gbps": average,
        "endpoint_metadata": metadata,
        "returncode": returncode,
        "timed_out": timed_out,
    }


def _set_record_status(record: dict[str, Any], status: str) -> None:
    current = record.get("status")
    if status == "FAIL":
        record["status"] = "BLOCKED"
    elif status == "NOT_VERIFIED" and current == "READY":
        record["status"] = "INCOMPLETE"


def _add_check_to_record(record: dict[str, Any], check: dict[str, Any]) -> None:
    record.setdefault("checks", []).append(check)
    if check["status"] == "PASS":
        return
    severity = "FAIL" if check["status"] == "FAIL" else "UNKNOWN"
    record.setdefault("findings", []).append(
        _result_finding(severity, check["reason_code"], check["message"])
    )
    _set_record_status(record, check["status"])


def _aggregate_status(items: Sequence[dict[str, Any]]) -> str:
    statuses = [item.get("status") for item in items]
    if not statuses:
        return "NOT_VERIFIED"
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    if any(status == "NOT_VERIFIED" for status in statuses):
        return "NOT_VERIFIED"
    return "PASS"


def _execution_with_timeout(
    execution_config: BaremetalExecutionConfig,
    *,
    output_root: Path,
    timeout_seconds: float,
) -> BaremetalExecutionConfig:
    return BaremetalExecutionConfig(
        output_root=output_root,
        transport=execution_config.transport,
        concurrency=execution_config.concurrency,
        connect_timeout_seconds=execution_config.connect_timeout_seconds,
        command_timeout_seconds=timeout_seconds,
        ssh_user=execution_config.ssh_user,
        ssh_port=execution_config.ssh_port,
        identity_file=execution_config.identity_file,
        ssh_config_file=execution_config.ssh_config_file,
        known_hosts_file=execution_config.known_hosts_file,
        strict_host_key_checking=execution_config.strict_host_key_checking,
        clush_executable=execution_config.clush_executable,
        ssh_executable=execution_config.ssh_executable,
        max_stdout_bytes=execution_config.max_stdout_bytes,
        max_stderr_bytes=execution_config.max_stderr_bytes,
    )


def _run_simple_node_checks(
    *,
    nodes: Sequence[str],
    records_by_node: dict[str, dict[str, Any]],
    execution_config: BaremetalExecutionConfig,
    output_root: Path,
    check_name: str,
    command_name: str,
    command: Sequence[str],
    timeout_seconds: float,
    evaluator: Callable[[int, str, str, bool], dict[str, Any]],
    runner: RunText,
    which: Which,
) -> dict[str, Any]:
    started_at = _utc_now()
    executor = BaremetalClusterExecutor(
        nodes,
        _execution_with_timeout(
            execution_config,
            output_root=output_root,
            timeout_seconds=timeout_seconds,
        ),
        runner=runner,
        which=which,
    )
    node_results: dict[str, dict[str, Any]] = {}

    def consume(result: BaremetalNodeResult) -> None:
        check = {
            "name": check_name,
            **evaluator(
                result.returncode,
                result.stdout,
                result.stderr,
                result.timed_out,
            ),
            "command": _command_summary(result),
        }
        node_results[result.node] = check
        record = records_by_node.get(result.node)
        if record is not None:
            _add_check_to_record(record, check)

    raw = executor.execute(
        command_name,
        list(command),
        result_handler=consume,
        release_output=True,
    )
    for node, result in raw.nodes.items():
        if node not in node_results:
            consume(result)
    checks = [node_results[node] | {"node": node} for node in nodes if node in node_results]
    return {
        "enabled": True,
        "status": _aggregate_status(checks),
        "transport": raw.transport,
        "command": list(command),
        "evidence_dir": raw.run_dir,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "nodes": checks,
    }


def _run_ib_state_checks(
    *,
    nodes: Sequence[str],
    records_by_node: dict[str, dict[str, Any]],
    execution_config: BaremetalExecutionConfig,
    config: IBStateCheckConfig,
    output_root: Path,
    runner: RunText,
    which: Which,
    implicit_for_bandwidth: bool = False,
) -> dict[str, Any]:
    if not config.enabled:
        return {"enabled": False, "status": "NOT_REQUESTED", "nodes": []}
    result = _run_simple_node_checks(
        nodes=nodes,
        records_by_node=records_by_node,
        execution_config=execution_config,
        output_root=output_root,
        check_name="ib_state",
        command_name="ib-state",
        command=config.command,
        timeout_seconds=config.timeout_seconds,
        evaluator=evaluate_ib_state_output,
        runner=runner,
        which=which,
    )
    result["implicit_for_bandwidth"] = implicit_for_bandwidth
    return result


def _build_nhc_command(config: NHCCheckConfig) -> list[str]:
    argv = config.argv()
    environment = _safe_env(config.environment)
    if not environment:
        return argv
    assignments = [f"{name}={value}" for name, value in sorted(environment.items())]
    return ["env", *assignments, *argv]


def _run_nhc_checks(
    *,
    nodes: Sequence[str],
    records_by_node: dict[str, dict[str, Any]],
    execution_config: BaremetalExecutionConfig,
    config: NHCCheckConfig,
    output_root: Path,
    runner: RunText,
    which: Which,
) -> dict[str, Any]:
    if not config.enabled:
        return {"enabled": False, "status": "NOT_REQUESTED", "nodes": []}
    started_at = _utc_now()
    nhc_execution = _execution_with_timeout(
        execution_config,
        output_root=output_root,
        timeout_seconds=config.timeout_seconds,
    )
    executor = BaremetalClusterExecutor(nodes, nhc_execution, runner=runner, which=which)
    command = _build_nhc_command(config)
    node_results: dict[str, dict[str, Any]] = {}

    def consume(result: BaremetalNodeResult) -> None:
        evaluation = evaluate_nhc_output(
            result.returncode,
            result.stdout,
            result.stderr,
            result.timed_out,
            installation_source=config.installation_source,
        )
        check = {
            "name": "run_nhc",
            **evaluation,
            "command": _command_summary(result),
        }
        node_results[result.node] = check
        record = records_by_node.get(result.node)
        if record is not None:
            _add_check_to_record(record, check)

    raw = executor.execute("run-nhc", command, result_handler=consume, release_output=True)
    for node, result in raw.nodes.items():
        if node not in node_results:
            consume(result)
    checks = [node_results[node] | {"node": node} for node in nodes if node in node_results]
    return {
        "enabled": True,
        "status": _aggregate_status(checks),
        "transport": raw.transport,
        "command": list(config.command),
        "installation_source": config.installation_source,
        "evidence_dir": raw.run_dir,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "nodes": checks,
    }


def _ssh_base(config: BaremetalExecutionConfig) -> list[str]:
    executable = config.ssh_executable or "ssh"
    argv = [
        executable,
        "-n",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectionAttempts=1",
        "-o",
        f"ConnectTimeout={max(1, math.ceil(config.connect_timeout_seconds))}",
        "-o",
        f"StrictHostKeyChecking={config.strict_host_key_checking}",
        "-p",
        str(config.ssh_port),
    ]
    if config.identity_file:
        argv.extend(["-i", str(config.identity_file)])
    if config.ssh_config_file:
        argv.extend(["-F", str(config.ssh_config_file)])
    if config.known_hosts_file:
        argv.extend(["-o", f"UserKnownHostsFile={config.known_hosts_file}"])
    return argv


def _ssh_destination(node: str, config: BaremetalExecutionConfig) -> str:
    return f"{config.ssh_user}@{node}" if config.ssh_user else node


def _remote_shell(command: Sequence[str], sentinel: str) -> str:
    command_text = shlex.join(command)
    wrapper = (
        f"{command_text}\n"
        "rc=$?\n"
        f"printf '\n{sentinel}=%s\n' \"$rc\"\n"
        "exit \"$rc\""
    )
    return "sh -c " + shlex.quote(wrapper)


def _extract_sentinel(stdout: str, sentinel: str) -> tuple[str, int | None]:
    kept: list[str] = []
    remote_rc: int | None = None
    prefix = f"{sentinel}="
    for line in stdout.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        if stripped.startswith(prefix):
            match = _SSH_SENTINEL_RE.fullmatch(stripped)
            if match:
                remote_rc = int(match.group(1))
                continue
        kept.append(line)
    return "".join(kept), remote_rc


def _run_remote_command(
    *,
    node: str,
    command: Sequence[str],
    config: BaremetalExecutionConfig,
    timeout_seconds: float,
    runner: RunText,
) -> dict[str, Any]:
    token = f"{time.monotonic_ns():x}"
    sentinel = f"__HCU_ENVCHECK_IB_RC_{token}__"
    argv = _ssh_base(config) + [_ssh_destination(node, config), _remote_shell(command, sentinel)]
    started = time.monotonic()
    try:
        completed = runner(
            argv,
            stdin=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=config.connect_timeout_seconds + timeout_seconds + 1.0,
            check=False,
        )
        stdout, remote_rc = _extract_sentinel(completed.stdout or "", sentinel)
        returncode = remote_rc if remote_rc is not None else completed.returncode
        return {
            "argv": argv,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": completed.stderr or "",
            "duration_seconds": time.monotonic() - started,
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": argv,
            "returncode": 124,
            "stdout": exc.stdout if isinstance(exc.stdout, str) else "",
            "stderr": exc.stderr if isinstance(exc.stderr, str) else "",
            "duration_seconds": time.monotonic() - started,
            "timed_out": True,
        }
    except OSError as exc:
        return {
            "argv": argv,
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "duration_seconds": time.monotonic() - started,
            "timed_out": False,
        }


def _command_result_summary(raw: dict[str, Any], *, name: str) -> dict[str, Any]:
    stdout = raw.get("stdout") or ""
    stderr = raw.get("stderr") or ""
    return {
        "name": name,
        "argv": raw.get("argv") or [],
        "returncode": raw.get("returncode"),
        "duration_seconds": round(float(raw.get("duration_seconds") or 0.0), 3),
        "timed_out": bool(raw.get("timed_out")),
        "stdout_bytes": len(stdout.encode("utf-8", "replace")),
        "stderr_bytes": len(stderr.encode("utf-8", "replace")),
    }


def _run_ib_spec(
    *,
    spec: IBTestSpec,
    config: IBWriteBandwidthConfig,
    execution_config: BaremetalExecutionConfig,
    runner: RunText,
    control_port: int,
) -> dict[str, Any]:
    server_result: dict[str, Any] | None = None
    with ThreadPoolExecutor(max_workers=1) as pool:
        server_future = pool.submit(
            _run_remote_command,
            node=spec.destination,
            command=config.perftest_argv(
                device=spec.destination_hca,
                control_port=control_port,
            ),
            config=execution_config,
            timeout_seconds=config.timeout_seconds,
            runner=runner,
        )
        if config.startup_grace_seconds:
            time.sleep(config.startup_grace_seconds)
        client_raw = _run_remote_command(
            node=spec.source,
            command=config.perftest_argv(
                server=spec.destination,
                device=spec.source_hca,
                control_port=control_port,
            ),
            config=execution_config,
            timeout_seconds=config.timeout_seconds,
            runner=runner,
        )
        try:
            server_result = server_future.result(timeout=config.timeout_seconds + 5.0)
        except Exception as exc:
            server_result = {
                "argv": [],
                "returncode": 125,
                "stdout": "",
                "stderr": f"server collection failed: {exc}",
                "duration_seconds": 0.0,
                "timed_out": False,
            }
    server_raw = server_result
    combined_stdout = f"{server_raw['stdout']}\n{client_raw['stdout']}"
    combined_stderr = f"{server_raw['stderr']}\n{client_raw['stderr']}"
    returncode = int(client_raw["returncode"] or server_raw["returncode"] or 0)
    timed_out = bool(client_raw["timed_out"] or server_raw["timed_out"])
    evaluation = evaluate_ib_write_bw_output(
        returncode,
        combined_stdout,
        combined_stderr,
        timed_out,
        minimum_gbps=config.minimum_average_gbps,
    )
    return {
        "round": 1,
        "source": spec.source,
        "destination": spec.destination,
        "source_hca": spec.source_hca,
        "destination_hca": spec.destination_hca,
        "rail_index": spec.rail_index,
        "control_port": control_port,
        **evaluation,
        "commands": [
            _command_result_summary(server_raw, name="ib_write_bw-server"),
            _command_result_summary(client_raw, name="ib_write_bw-client"),
        ],
    }


def _ib_prerequisite_failure(
    *,
    nodes: Sequence[str],
    records_by_node: dict[str, dict[str, Any]],
    status: str,
    reason_code: str,
    message: str,
    started_at: str,
) -> dict[str, Any]:
    check = {
        "name": "ib_write_bw",
        "round": 1,
        "status": status,
        "reason_code": reason_code,
        "message": message,
    }
    for node in nodes:
        record = records_by_node.get(str(node))
        if record is not None:
            _add_check_to_record(record, check.copy())
    return {
        "enabled": True,
        "status": status,
        "reason_code": reason_code,
        "message": message,
        "rounds": 1,
        "pairs": [],
        "started_at": started_at,
        "finished_at": _utc_now(),
    }


def _run_ib_checks(
    *,
    nodes: Sequence[str],
    records_by_node: dict[str, dict[str, Any]],
    execution_config: BaremetalExecutionConfig,
    config: IBWriteBandwidthConfig,
    ib_state: dict[str, Any],
    output_root: Path,
    runner: RunText,
) -> dict[str, Any]:
    if not config.enabled:
        return {"enabled": False, "status": "NOT_REQUESTED", "pairs": []}
    config.validate()
    started_at = _utc_now()
    state_by_node = {
        str(item.get("node")): item
        for item in ib_state.get("nodes", [])
        if item.get("node")
    }
    if config.device:
        hcas_by_node = {str(node): [config.device] for node in nodes}
        missing_device = [
            str(node)
            for node in nodes
            if config.device not in state_by_node.get(str(node), {}).get("supported_hcas", [])
        ]
        if missing_device:
            message = (
                f"explicit HCA {config.device} was not reported by ibstat on: "
                + ", ".join(missing_device)
            )
            return _ib_prerequisite_failure(
                nodes=nodes,
                records_by_node=records_by_node,
                status="FAIL",
                reason_code="IB_DEVICE_NOT_FOUND",
                message=message,
                started_at=started_at,
            )
    else:
        hcas_by_node = {
            str(node): list(state_by_node.get(str(node), {}).get("supported_hcas", []))
            for node in nodes
        }
    failed_state_nodes = [
        str(node)
        for node in nodes
        if state_by_node.get(str(node), {}).get("status") != "PASS"
    ]
    if failed_state_nodes:
        return _ib_prerequisite_failure(
            nodes=nodes,
            records_by_node=records_by_node,
            status="NOT_VERIFIED",
            reason_code="IB_WRITE_BW_PREREQUISITE_FAILED",
            message=(
                "ib_write_bw was not started because IB state was not PASS on: "
                + ", ".join(failed_state_nodes)
            ),
            started_at=started_at,
        )
    try:
        plan = build_ib_test_plan(nodes, hcas_by_node)
    except ValueError as exc:
        return _ib_prerequisite_failure(
            nodes=nodes,
            records_by_node=records_by_node,
            status="FAIL",
            reason_code="IB_HCA_INVENTORY_MISMATCH",
            message=str(exc),
            started_at=started_at,
        )
    if not plan:
        return _ib_prerequisite_failure(
            nodes=nodes,
            records_by_node=records_by_node,
            status="NOT_VERIFIED",
            reason_code="IB_WRITE_BW_NOT_ENOUGH_NODES",
            message="ib_write_bw cluster smoke test requires at least two selected nodes",
            started_at=started_at,
        )
    if len(plan) > config.max_tests:
        return _ib_prerequisite_failure(
            nodes=nodes,
            records_by_node=records_by_node,
            status="NOT_VERIFIED",
            reason_code="IB_WRITE_BW_TEST_LIMIT_EXCEEDED",
            message=f"planned {len(plan)} tests exceeds configured limit {config.max_tests}",
            started_at=started_at,
        )
    ib_dir = output_root / "ib-write-bw"
    ib_dir.mkdir(parents=True, exist_ok=False)
    pair_results: list[dict[str, Any]] = []
    max_workers = min(config.concurrency, len(plan))
    port_span = 65535 - config.control_port + 1
    if len(plan) > port_span:
        raise ValueError("IB test plan has more concurrent directions than available control ports")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _run_ib_spec,
                spec=spec,
                config=config,
                execution_config=execution_config,
                runner=runner,
                control_port=config.control_port + index,
            ): spec
            for index, spec in enumerate(plan)
        }
        for future in as_completed(futures):
            spec = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "round": 1,
                    "source": spec.source,
                    "destination": spec.destination,
                    "source_hca": spec.source_hca,
                    "destination_hca": spec.destination_hca,
                    "rail_index": spec.rail_index,
                    "status": "NOT_VERIFIED",
                    "reason_code": "IB_WRITE_BW_EXECUTOR_ERROR",
                    "message": f"ib_write_bw executor error: {exc}",
                    "average_gbps": None,
                    "commands": [],
                }
            if result["status"] != "PASS":
                endpoint_path = (
                    f"{result['source']}:{result['source_hca']} -> "
                    f"{result['destination']}:{result['destination_hca']} "
                    f"(rail {result['rail_index']})"
                )
                result["message"] = f"{endpoint_path}: {result['message']}"
            pair_results.append(result)
            pair_file = ib_dir / (
                f"{len(pair_results):04d}-{spec.source}-{spec.source_hca}"
                f"-to-{spec.destination}-{spec.destination_hca}.json"
            )
            _write_json(pair_file, result)
            check = {
                "name": "ib_write_bw",
                "round": 1,
                "peer": result["destination"],
                "direction": f"{result['source']}->{result['destination']}",
                "source_hca": result["source_hca"],
                "destination_hca": result["destination_hca"],
                "rail_index": result["rail_index"],
                "status": result["status"],
                "reason_code": result["reason_code"],
                "message": result["message"],
                "average_gbps": result.get("average_gbps"),
                "commands": result.get("commands", []),
            }
            source_record = records_by_node.get(result["source"])
            destination_record = records_by_node.get(result["destination"])
            if source_record is not None:
                _add_check_to_record(source_record, check)
            if destination_record is not None:
                _add_check_to_record(
                    destination_record,
                    {
                        **check,
                        "peer": result["source"],
                        "direction": f"{result['source']}->{result['destination']}",
                    },
                )
    pair_results.sort(
        key=lambda item: (
            item["source"],
            item["destination"],
            item["rail_index"],
        )
    )
    _write_json(ib_dir / "ib-write-bw-result.json", {"pairs": pair_results})
    averages = [item["average_gbps"] for item in pair_results if item.get("average_gbps") is not None]
    return {
        "enabled": True,
        "status": _aggregate_status(pair_results),
        "tool": config.tool,
        "protocol": config.protocol,
        "minimum_average_gbps": config.minimum_average_gbps,
        "concurrency": config.concurrency,
        "transport": "ssh",
        "rounds": 1,
        "hcas_by_node": hcas_by_node,
        "evidence_dir": str(ib_dir),
        "started_at": started_at,
        "finished_at": _utc_now(),
        "summary": {
            "planned_tests": len(plan),
            "passed_pairs": sum(item["status"] == "PASS" for item in pair_results),
            "failed_pairs": sum(item["status"] == "FAIL" for item in pair_results),
            "not_verified_pairs": sum(item["status"] == "NOT_VERIFIED" for item in pair_results),
            "minimum_average_gbps_observed": min(averages) if averages else None,
        },
        "pairs": pair_results,
    }


def run_cluster_extra_checks(
    *,
    nodes: Sequence[str],
    records: list[dict[str, Any]],
    execution_config: BaremetalExecutionConfig,
    config: ClusterExtraCheckConfig,
    output_root: Path,
    runner: RunText = subprocess.run,
    which: Which | None = None,
) -> dict[str, Any]:
    config.validate()
    records_by_node = {record["node"]: record for record in records}
    output_root.mkdir(parents=True, exist_ok=True)
    resolved_which = which or shutil.which
    implicit_ib_state = config.ib.enabled and not config.ib_state.enabled
    ib_state_config = (
        IBStateCheckConfig(
            enabled=True,
            command=config.ib_state.command,
            timeout_seconds=config.ib_state.timeout_seconds,
        )
        if implicit_ib_state
        else config.ib_state
    )
    ib_state = _run_ib_state_checks(
        nodes=nodes,
        records_by_node=records_by_node,
        execution_config=execution_config,
        config=ib_state_config,
        output_root=output_root,
        runner=runner,
        which=resolved_which,
        implicit_for_bandwidth=implicit_ib_state,
    )
    nhc = _run_nhc_checks(
        nodes=nodes,
        records_by_node=records_by_node,
        execution_config=execution_config,
        config=config.nhc,
        output_root=output_root,
        runner=runner,
        which=resolved_which,
    )
    ib = _run_ib_checks(
        nodes=nodes,
        records_by_node=records_by_node,
        execution_config=execution_config,
        config=config.ib,
        ib_state=ib_state,
        output_root=output_root,
        runner=runner,
    )
    enabled_results = [
        item
        for item in (ib_state, nhc, ib)
        if item.get("enabled")
    ]
    result = {
        "enabled": bool(enabled_results),
        "status": _aggregate_status(enabled_results),
        "rounds": 1,
        "taint_mutation": False,
        "ib_state": ib_state,
        "nhc": nhc,
        "ib_write_bw": ib,
    }
    _write_json(output_root / "cluster-extra-checks.json", result)
    return result
