# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .environment import collect_environment
from .k8s import (
    K8S_NODE_BLOCKING_CONDITIONS,
    K8S_STANDARD_HEALTH_TAINT_KEYS,
    KubernetesPodExecutor,
)
from .models import CommandResult, DeviceMetrics, Finding, ProbeResult
from .output import atomic_write_text_exclusive
from .parsers import ParseError, RocmAgent, parse_hy_smi_samples, parse_rocminfo
from .roce_health import normalize_roce_policy


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_environment_profile(
    *,
    include_environment: bool,
    require_compiler: bool,
    require_rdma: bool,
    minimum_rdma_devices: int,
    expected_rdma_protocol: str,
    require_rccl: bool,
    require_ucx: bool,
    rdma_policy: dict[str, Any] | None = None,
) -> None:
    """Reject API profiles that would silently skip requested environment checks."""

    if expected_rdma_protocol not in {"auto", "ib", "roce"}:
        raise ValueError("expected_rdma_protocol must be auto, ib, or roce")
    if minimum_rdma_devices < 0:
        raise ValueError("minimum_rdma_devices cannot be negative")
    if rdma_policy is not None:
        normalize_roce_policy(rdma_policy)
        if expected_rdma_protocol == "ib":
            raise ValueError("a RoCE policy conflicts with expected_rdma_protocol=ib")
    explicit_environment_profile = any(
        (
            require_compiler,
            require_rdma,
            minimum_rdma_devices > 0,
            expected_rdma_protocol != "auto",
            rdma_policy is not None,
            require_rccl,
            require_ucx,
        )
    )
    if not include_environment and explicit_environment_profile:
        raise ValueError(
            "include_environment=False cannot be combined with compiler/RDMA/RCCL/UCX profile checks"
        )


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _command_finding(result: CommandResult) -> Finding | None:
    text = f"{result.stdout}\n{result.stderr}".lower()
    if "undefined symbol" in text and ("hcu_smi" in text or "hcusmi" in text):
        return Finding(
            "FAIL",
            "HCUSMI_LIBRARY_ABI_MISMATCH",
            f"{result.name}: HCUSMI/HYHAL native library symbol mismatch",
        )
    if "permission denied" in text or "unable to open /dev/kfd" in text:
        return Finding(
            "FAIL",
            "HCU_PERMISSION_DENIED",
            f"{result.name}: device access denied",
        )
    if "no hcu driver loaded" in text or "no hycu driver loaded" in text or "driver not loaded" in text:
        return Finding("FAIL", "HCU_DRIVER_NOT_LOADED", f"{result.name}: HCU driver is not loaded")
    if "no devices found" in text or "no hcu devices" in text:
        return Finding("FAIL", "HCU_NOT_VISIBLE", f"{result.name}: no HCU devices are visible")
    if result.timed_out:
        return Finding("UNKNOWN", "COLLECTION_TIMEOUT", f"{result.name} timed out")
    if result.returncode != 0:
        return Finding(
            "UNKNOWN",
            "COMMAND_FAILED",
            f"{result.name} rc={result.returncode}",
        )
    return None


def _output_truncation_finding(result: CommandResult) -> Finding | None:
    if not result.output_truncated:
        return None
    streams: list[str] = []
    if result.stdout_truncated:
        streams.append(f"stdout={result.stdout_total_bytes} bytes")
    if result.stderr_truncated:
        streams.append(f"stderr={result.stderr_total_bytes} bytes")
    return Finding(
        "UNKNOWN",
        "KUBECTL_OUTPUT_TRUNCATED",
        f"{result.name}: bounded kubectl capture truncated " + ", ".join(streams),
    )


def _k8s_node_health_findings(identity: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    if identity.get("ready") != "True":
        findings.append(
            Finding("FAIL", "K8S_NODE_NOT_READY", f"node Ready={identity.get('ready')}")
        )
    for condition in identity.get("conditions") or []:
        condition_type = condition.get("type")
        status = condition.get("status")
        if condition_type in K8S_NODE_BLOCKING_CONDITIONS and status != "False":
            findings.append(
                Finding(
                    "FAIL",
                    "K8S_NODE_CONDITION_UNHEALTHY",
                    f"node {condition_type}={status or 'MISSING'}",
                )
            )
    for taint in identity.get("taints") or []:
        key = taint.get("key")
        effect = taint.get("effect")
        if key in K8S_STANDARD_HEALTH_TAINT_KEYS and effect in {"NoSchedule", "NoExecute"}:
            findings.append(
                Finding(
                    "FAIL",
                    "K8S_NODE_HEALTH_TAINT",
                    f"node taint {key}:{effect}",
                )
            )
    if identity.get("unschedulable"):
        findings.append(
            Finding(
                "FAIL",
                "K8S_NODE_UNSCHEDULABLE",
                "node spec.unschedulable=true",
            )
        )
    return findings


def _write_evidence(directory: Path, results: list[CommandResult]) -> list[dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=False)
    directory.chmod(0o700)
    summaries: list[dict[str, Any]] = []
    for index, result in enumerate(results, start=1):
        base = f"{index:02d}-{result.name}"
        stdout_path = directory / f"{base}.stdout"
        stderr_path = directory / f"{base}.stderr"
        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")
        stdout_path.chmod(0o600)
        stderr_path.chmod(0o600)
        summary = result.summary()
        summary.update(
            {
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "stdout_sha256": hashlib.sha256(result.stdout.encode("utf-8", "replace")).hexdigest(),
                "stderr_sha256": hashlib.sha256(result.stderr.encode("utf-8", "replace")).hexdigest(),
            }
        )
        summaries.append(summary)
    return summaries


def _match_agents(
    hy_cards: dict[int, dict[str, Any]], roc_agents: list[RocmAgent]
) -> tuple[dict[int, RocmAgent | None], list[Finding]]:
    findings: list[Finding] = []
    hy_bdfs = [str(item.get("bdf")) for item in hy_cards.values() if item.get("bdf")]
    roc_bdfs = [str(agent.bdf) for agent in roc_agents if agent.bdf]
    if len(hy_bdfs) != len(set(hy_bdfs)) or len(roc_bdfs) != len(set(roc_bdfs)):
        findings.append(
            Finding(
                "FAIL",
                "HCU_BDF_MAPPING_MISMATCH",
                "duplicate PCI BDF reported by rocminfo or hy-smi",
            )
        )
    by_bdf = {agent.bdf: agent for agent in roc_agents if agent.bdf}
    ordered = sorted(roc_agents, key=lambda item: item.agent_id)
    mapping: dict[int, RocmAgent | None] = {}
    used_agents: set[int] = set()
    for position, device_id in enumerate(sorted(hy_cards)):
        bdf = hy_cards[device_id].get("bdf")
        mapping[device_id] = by_bdf.get(bdf) if bdf else None
        if mapping[device_id] is not None:
            used_agents.add(mapping[device_id].agent_id)
            continue
        # If both tools provided BDFs, an explicit mismatch is evidence of an
        # enumeration/topology problem. Do not hide it with positional pairing.
        if bdf and roc_bdfs:
            findings.append(
                Finding(
                    "FAIL",
                    "HCU_BDF_MAPPING_MISMATCH",
                    f"hy-smi card{device_id} BDF={bdf} has no rocminfo match",
                    device_id,
                )
            )
            continue
        remaining = [agent for agent in ordered if agent.agent_id not in used_agents]
        if remaining:
            mapping[device_id] = remaining[0]
            used_agents.add(remaining[0].agent_id)
    return mapping, findings


def evaluate_metrics(
    target: dict[str, Any],
    hy_cards: dict[int, dict[str, Any]],
    roc_agents: list[RocmAgent],
    expected_devices: int | None,
    max_vram_used_percent: float,
    max_hcu_util_percent: float,
    busy_sample_quorum: int = 1,
    capacity_tolerance_mib: float = 1.0,
    accounting_tolerance_mib: float = 512.0,
) -> tuple[list[DeviceMetrics], list[Finding], str]:
    findings: list[Finding] = []
    requested_devices = _int_or_none(target.get("device_request"))
    limited_devices = _int_or_none(target.get("device_limit"))
    allocatable_devices = _int_or_none(target.get("node_device_allocatable"))
    if expected_devices is not None and len(hy_cards) != expected_devices:
        findings.append(
            Finding(
                "FAIL",
                "HCU_DEVICE_COUNT_MISMATCH",
                f"hy-smi found {len(hy_cards)} devices; expected {expected_devices}",
            )
        )
    if len(roc_agents) != len(hy_cards):
        findings.append(
            Finding(
                "FAIL",
                "ROCINFO_HYSMI_DEVICE_COUNT_MISMATCH",
                f"rocminfo found {len(roc_agents)} HCU agents; hy-smi found {len(hy_cards)} cards",
            )
        )
    if requested_devices is not None and len(hy_cards) < requested_devices:
        findings.append(
            Finding(
                "FAIL",
                "CONTAINER_DEVICE_NOT_PASSED",
                f"container requested {requested_devices} devices but hy-smi sees {len(hy_cards)}",
            )
        )
    if limited_devices is not None and len(hy_cards) > limited_devices:
        findings.append(
            Finding(
                "FAIL",
                "CONTAINER_DEVICE_ISOLATION_MISMATCH",
                f"container limit is {limited_devices} devices but hy-smi sees {len(hy_cards)}",
            )
        )
    if allocatable_devices is not None and expected_devices is not None:
        if allocatable_devices < expected_devices:
            findings.append(
                Finding(
                    "FAIL",
                    "K8S_HCU_ALLOCATABLE_INSUFFICIENT",
                    f"node allocatable={allocatable_devices}, expected={expected_devices}",
                )
            )

    mapping, mapping_findings = _match_agents(hy_cards, roc_agents)
    findings.extend(mapping_findings)
    devices: list[DeviceMetrics] = []
    for device_id in sorted(hy_cards):
        raw = hy_cards[device_id]
        agent = mapping.get(device_id)
        total = raw.get("total_mib")
        used = raw.get("used_mib")
        available = raw.get("available_mib")
        reported_percent = raw.get("memory_used_percent_reported")
        utilization = raw.get("hcu_util_percent")
        used_samples = [float(value) for value in raw.get("used_mib_samples", [])]
        calculated_samples = (
            [value / total * 100.0 for value in used_samples]
            if total not in (None, 0)
            else []
        )
        utilization_samples = [float(value) for value in raw.get("hcu_util_percent_samples", [])]
        total_samples = [float(value) for value in raw.get("total_mib_samples", [])]
        available_samples = [float(value) for value in raw.get("available_mib_samples", [])]
        reported_percent_samples = [
            float(value) for value in raw.get("memory_used_percent_reported_samples", [])
        ]
        expected_sample_count = int(raw.get("sample_count", 0))
        calculated_percent = max(calculated_samples) if calculated_samples else None
        memory_exceed_count = sum(value > max_vram_used_percent for value in calculated_samples)
        utilization_exceed_count = sum(value > max_hcu_util_percent for value in utilization_samples)
        reserved = (
            total - used - available
            if total is not None and used is not None and available is not None
            else None
        )

        device_findings: list[Finding] = []
        sample_counts = {
            "total": len(total_samples),
            "used": len(used_samples),
            "available": len(available_samples),
            "memory_percent": len(reported_percent_samples),
            "utilization": len(utilization_samples),
        }
        incomplete_metrics = [
            f"{name}={count}/{expected_sample_count}"
            for name, count in sample_counts.items()
            if count != expected_sample_count
        ]
        if expected_sample_count < 1 or incomplete_metrics:
            device_findings.append(
                Finding(
                    "UNKNOWN",
                    "HCU_SAMPLE_INCOMPLETE",
                    "incomplete samples: " + ", ".join(incomplete_metrics or ["sample_count=0"]),
                    device_id,
                )
            )

        invalid_metrics: list[str] = []
        if any(value <= 0 for value in total_samples):
            invalid_metrics.append("total_mib<=0")
        if total_samples and max(total_samples) - min(total_samples) > capacity_tolerance_mib:
            device_findings.append(
                Finding(
                    "FAIL",
                    "VRAM_CAPACITY_UNSTABLE",
                    f"total VRAM changed across samples: {total_samples}",
                    device_id,
                )
            )
        if total is not None:
            if any(value < 0 or value > total + accounting_tolerance_mib for value in used_samples):
                invalid_metrics.append("used_mib_out_of_range")
            if any(value < 0 or value > total + accounting_tolerance_mib for value in available_samples):
                invalid_metrics.append("available_mib_out_of_range")
        if any(value < 0 or value > 100 for value in reported_percent_samples):
            invalid_metrics.append("memory_percent_out_of_range")
        if any(value < 0 or value > 100 for value in utilization_samples):
            invalid_metrics.append("utilization_out_of_range")
        if invalid_metrics:
            device_findings.append(
                Finding(
                    "UNKNOWN",
                    "HCU_METRIC_OUT_OF_RANGE",
                    "invalid metrics: " + ", ".join(invalid_metrics),
                    device_id,
                )
            )
        required = {
            "hy_smi_total_mib": total,
            "used_mib": used,
            "available_mib": available,
            "hcu_util_percent": utilization,
            "rocminfo_total_mib": agent.total_mib if agent else None,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            device_findings.append(
                Finding(
                    "UNKNOWN",
                    "HCU_METRIC_MISSING",
                    f"missing required metrics: {', '.join(missing)}",
                    device_id,
                )
            )

        if total is not None and agent and agent.total_mib is not None:
            if abs(total - agent.total_mib) > capacity_tolerance_mib:
                device_findings.append(
                    Finding(
                        "FAIL",
                        "VRAM_CAPACITY_SOURCE_MISMATCH",
                        f"hy-smi total={total:.0f} MiB, rocminfo total={agent.total_mib:.0f} MiB",
                        device_id,
                    )
                )
        if memory_exceed_count >= busy_sample_quorum:
            device_findings.append(
                Finding(
                    "FAIL",
                    "VRAM_IN_USE",
                    f"VRAM exceeded {max_vram_used_percent:.1f}% in "
                    f"{memory_exceed_count}/{len(calculated_samples)} samples; peak={calculated_percent:.1f}%",
                    device_id,
                )
            )
        elif memory_exceed_count:
            device_findings.append(
                Finding(
                    "WARN",
                    "TRANSIENT_VRAM_ACTIVITY",
                    f"VRAM exceeded {max_vram_used_percent:.1f}% in "
                    f"{memory_exceed_count}/{len(calculated_samples)} samples",
                    device_id,
                )
            )
        if utilization_exceed_count >= busy_sample_quorum:
            device_findings.append(
                Finding(
                    "FAIL",
                    "HCU_BUSY",
                    f"HCU utilization exceeded {max_hcu_util_percent:.1f}% in "
                    f"{utilization_exceed_count}/{len(utilization_samples)} samples; peak={utilization:.1f}%",
                    device_id,
                )
            )
        elif utilization_exceed_count:
            device_findings.append(
                Finding(
                    "WARN",
                    "TRANSIENT_HCU_ACTIVITY",
                    f"HCU utilization exceeded {max_hcu_util_percent:.1f}% in "
                    f"{utilization_exceed_count}/{len(utilization_samples)} samples",
                    device_id,
                )
            )
        if reserved is not None and (reserved < -accounting_tolerance_mib or reserved > accounting_tolerance_mib):
            device_findings.append(
                Finding(
                    "WARN",
                    "VRAM_ACCOUNTING_GAP",
                    f"total-used-available={reserved:.1f} MiB",
                    device_id,
                )
            )
        if calculated_percent is not None and reported_percent is not None:
            if abs(calculated_percent - reported_percent) > 2.0:
                device_findings.append(
                    Finding(
                        "WARN",
                        "VRAM_PERCENT_SOURCE_MISMATCH",
                        f"calculated={calculated_percent:.1f}%, hy-smi={reported_percent:.1f}%",
                        device_id,
                    )
                )

        reason_codes = [finding.reason_code for finding in device_findings]
        if any(finding.severity == "FAIL" for finding in device_findings):
            device_status = "FAIL"
        elif any(finding.severity == "UNKNOWN" for finding in device_findings):
            device_status = "UNKNOWN"
        elif any(finding.severity == "WARN" for finding in device_findings):
            device_status = "WARN"
        else:
            device_status = "PASS"

        devices.append(
            DeviceMetrics(
                device_id=device_id,
                bdf=raw.get("bdf") or (agent.bdf if agent else None),
                model=agent.model if agent else None,
                architecture=agent.architecture if agent else None,
                rocminfo_agent=agent.agent_id if agent else None,
                rocminfo_total_mib=agent.total_mib if agent else None,
                hy_smi_total_mib=total,
                used_mib=used,
                available_mib=available,
                reserved_mib=reserved,
                memory_used_percent=calculated_percent,
                memory_used_percent_reported=reported_percent,
                hcu_util_percent=utilization,
                memory_used_percent_samples=calculated_samples,
                hcu_util_percent_samples=utilization_samples,
                memory_exceed_count=memory_exceed_count,
                utilization_exceed_count=utilization_exceed_count,
                sample_count=int(raw.get("sample_count", 0)),
                status=device_status,
                reason_codes=reason_codes,
            )
        )
        findings.extend(device_findings)

    if any(finding.severity == "FAIL" for finding in findings):
        status = "BLOCKED"
    elif any(finding.severity == "UNKNOWN" for finding in findings):
        status = "INCOMPLETE"
    else:
        status = "READY"
    return devices, findings, status


def run_k8s_hcu_preflight(
    executor: KubernetesPodExecutor,
    expected_devices: int | None,
    max_vram_used_percent: float,
    max_hcu_util_percent: float,
    samples: int,
    busy_sample_quorum: int,
    sample_interval_seconds: float,
    evidence_dir: Path,
    include_environment: bool = True,
    require_compiler: bool = False,
    require_rdma: bool = False,
    minimum_rdma_devices: int = 0,
    expected_rdma_protocol: str = "auto",
    require_rccl: bool = False,
    require_ucx: bool = False,
    rdma_policy: dict[str, Any] | None = None,
    rdma_counter_interval_seconds: int = 5,
) -> ProbeResult:
    validate_environment_profile(
        include_environment=include_environment,
        require_compiler=require_compiler,
        require_rdma=require_rdma,
        minimum_rdma_devices=minimum_rdma_devices,
        expected_rdma_protocol=expected_rdma_protocol,
        require_rccl=require_rccl,
        require_ucx=require_ucx,
        rdma_policy=rdma_policy,
    )
    if rdma_counter_interval_seconds != 0 and not 1 <= rdma_counter_interval_seconds <= 60:
        raise ValueError(
            "rdma_counter_interval_seconds must be 0 or between 1 and 60"
        )
    started_at = _utc_now()
    command_results: list[CommandResult] = []
    identity, identity_command = executor.pod_identity()
    command_results.append(identity_command)
    effective_expected_devices = expected_devices
    if effective_expected_devices is None:
        effective_expected_devices = _int_or_none(identity.get("device_limit"))
    if effective_expected_devices is None:
        effective_expected_devices = _int_or_none(identity.get("device_request"))

    early_findings: list[Finding] = []
    if identity.get("phase") != "Running":
        early_findings.append(
            Finding("UNKNOWN", "K8S_POD_NOT_RUNNING", f"pod phase={identity.get('phase')}")
        )
    if not identity.get("node"):
        early_findings.append(
            Finding("UNKNOWN", "K8S_POD_NOT_SCHEDULED", "pod has no assigned node")
        )
    if identity.get("container_state") != "running" or identity.get("container_ready") is not True:
        reason = identity.get("container_state_reason") or identity.get("container_state")
        early_findings.append(
            Finding(
                "UNKNOWN",
                "K8S_CONTAINER_NOT_READY",
                f"container state={reason}, ready={identity.get('container_ready')}",
            )
        )
    if early_findings:
        command_summaries = _write_evidence(evidence_dir, command_results)
        return ProbeResult(
            status="INCOMPLETE",
            target=identity,
            thresholds={
                "max_vram_used_percent": max_vram_used_percent,
                "max_hcu_util_percent": max_hcu_util_percent,
                "busy_sample_quorum": busy_sample_quorum,
            },
            device_count=0,
            expected_device_count=effective_expected_devices,
            devices=[],
            findings=early_findings,
            commands=command_summaries,
            started_at=started_at,
            finished_at=_utc_now(),
            evidence_dir=str(evidence_dir),
        )

    node_identity, node_command = executor.node_identity(identity["node"])
    command_results.append(node_command)
    identity.update(
        {
            "node_uid": node_identity.get("node_uid"),
            "node_ready": node_identity.get("ready"),
            "node_unschedulable": node_identity.get("unschedulable"),
            "node_conditions": node_identity.get("conditions", []),
            "node_taints": node_identity.get("taints", []),
            "node_device_capacity": node_identity.get("device_capacity"),
            "node_device_allocatable": node_identity.get("device_allocatable"),
            "node_cpu_capacity": node_identity.get("cpu_capacity"),
            "node_memory_capacity": node_identity.get("memory_capacity"),
            "node_kernel_version": node_identity.get("kernel_version"),
            "node_os_image": node_identity.get("os_image"),
            "node_container_runtime_version": node_identity.get("container_runtime_version"),
            "node_kubelet_version": node_identity.get("kubelet_version"),
        }
    )
    early_findings.extend(_k8s_node_health_findings(node_identity))

    requested = _int_or_none(identity.get("device_request"))
    limited = _int_or_none(identity.get("device_limit"))
    if requested is None or limited is None:
        early_findings.append(
            Finding(
                "FAIL",
                "K8S_HCU_RESOURCE_NOT_DECLARED",
                "container must declare both HCU request and limit",
            )
        )
    elif requested != limited:
        early_findings.append(
            Finding(
                "FAIL",
                "K8S_HCU_REQUEST_LIMIT_MISMATCH",
                f"request={requested}, limit={limited}",
            )
        )
    if effective_expected_devices is not None and limited is not None and limited != effective_expected_devices:
        early_findings.append(
            Finding(
                "FAIL",
                "K8S_HCU_EXPECTED_LIMIT_MISMATCH",
                f"expected={effective_expected_devices}, limit={limited}",
            )
        )

    hy_smi, hy_smi_resolve = executor.resolve_tool("hy-smi", "Hy-smi")
    command_results.append(hy_smi_resolve)
    rocminfo, rocminfo_resolve = executor.resolve_tool("rocminfo")
    command_results.append(rocminfo_resolve)
    identity["hy_smi_path"] = hy_smi
    identity["rocminfo_path"] = rocminfo

    for resolver_result in (hy_smi_resolve, rocminfo_resolve):
        truncation_finding = _output_truncation_finding(resolver_result)
        if truncation_finding is not None:
            early_findings.append(truncation_finding)
    if not hy_smi and not hy_smi_resolve.output_truncated:
        early_findings.append(Finding("FAIL", "HY_SMI_NOT_FOUND", "hy-smi not found in container"))
    if not rocminfo and not rocminfo_resolve.output_truncated:
        early_findings.append(Finding("FAIL", "ROCMINFO_NOT_FOUND", "rocminfo not found in container"))
    if any(item.severity in {"FAIL", "UNKNOWN"} for item in early_findings):
        command_summaries = _write_evidence(evidence_dir, command_results)
        return ProbeResult(
            status=("BLOCKED" if any(item.severity == "FAIL" for item in early_findings) else "INCOMPLETE"),
            target=identity,
            thresholds={
                "max_vram_used_percent": max_vram_used_percent,
                "max_hcu_util_percent": max_hcu_util_percent,
                "busy_sample_quorum": busy_sample_quorum,
            },
            device_count=0,
            expected_device_count=effective_expected_devices,
            devices=[],
            findings=early_findings,
            commands=command_summaries,
            started_at=started_at,
            finished_at=_utc_now(),
            evidence_dir=str(evidence_dir),
        )

    version_result = executor.exec("hy_smi_version", [hy_smi, "--version"])
    help_result = executor.exec("hy_smi_help", [hy_smi, "--help"])
    roc_result = executor.exec("rocminfo", [rocminfo], timeout=60)
    bus_result = executor.exec("hy_smi_showbus", [hy_smi, "--showbus", "--json"])
    command_results.extend([version_result, help_result, roc_result, bus_result])

    memory_outputs: list[str] = []
    available_outputs: list[str] = []
    memory_percent_outputs: list[str] = []
    utilization_outputs: list[str] = []
    for sample_index in range(samples):
        commands = (
            ("memory", [hy_smi, "--showmeminfo", "vram", "--json"]),
            ("available", [hy_smi, "--showmemavailable", "--json"]),
            ("memory_percent", [hy_smi, "--showmemuse", "--json"]),
            ("utilization", [hy_smi, "--showuse", "--json"]),
        )
        sample_results: dict[str, CommandResult] = {}
        for metric_name, argv in commands:
            result = executor.exec(f"sample_{sample_index + 1}_{metric_name}", argv)
            command_results.append(result)
            sample_results[metric_name] = result
        memory_outputs.append(sample_results["memory"].stdout)
        available_outputs.append(sample_results["available"].stdout)
        memory_percent_outputs.append(sample_results["memory_percent"].stdout)
        utilization_outputs.append(sample_results["utilization"].stdout)
        if sample_index + 1 < samples:
            time.sleep(sample_interval_seconds)

    environment: dict[str, Any] = {}
    environment_findings: list[Finding] = []
    if include_environment:
        environment, environment_result, environment_findings = collect_environment(
            executor,
            expected_device_count=effective_expected_devices,
            require_compiler=require_compiler,
            require_rdma=require_rdma,
            minimum_rdma_devices=minimum_rdma_devices,
            expected_rdma_protocol=expected_rdma_protocol,
            require_rccl=require_rccl,
            require_ucx=require_ucx,
            network_host_scope_verified=(
                identity.get("host_network") is True
                and identity.get("container_privileged") is True
            ),
            rdma_policy=rdma_policy,
            rdma_counter_interval_seconds=rdma_counter_interval_seconds,
        )
        command_results.append(environment_result)

    command_findings = [
        finding
        for result in command_results
        if result.name not in {
            "pod_identity",
            "node_identity",
            "resolve_hy-smi",
            "resolve_rocminfo",
            "hy_smi_version",
            "hy_smi_help",
            "environment_inventory",
        }
        for finding in [_command_finding(result)]
        if finding is not None
    ]
    parse_findings: list[Finding] = []
    try:
        hy_cards = parse_hy_smi_samples(
            memory_outputs,
            available_outputs,
            memory_percent_outputs,
            utilization_outputs,
            bus_result.stdout if bus_result.returncode == 0 else None,
        )
    except ParseError as exc:
        hy_cards = {}
        parse_findings.append(Finding("UNKNOWN", "HY_SMI_PARSE_FAILED", str(exc)))
    try:
        roc_agents = parse_rocminfo(roc_result.stdout)
    except ParseError as exc:
        roc_agents = []
        parse_findings.append(Finding("UNKNOWN", "ROCMINFO_PARSE_FAILED", str(exc)))

    parse_findings.extend(command_findings)
    parse_findings.extend(
        finding
        for result in command_results
        for finding in [_output_truncation_finding(result)]
        if finding is not None
    )
    parse_findings.extend(environment_findings)

    devices: list[DeviceMetrics] = []
    findings = list(early_findings) + list(parse_findings)
    status = "INCOMPLETE" if findings else "READY"
    if hy_cards and roc_agents:
        devices, evaluated_findings, evaluated_status = evaluate_metrics(
            identity,
            hy_cards,
            roc_agents,
            effective_expected_devices,
            max_vram_used_percent,
            max_hcu_util_percent,
            busy_sample_quorum,
        )
        findings.extend(evaluated_findings)
        if any(finding.severity == "FAIL" for finding in findings):
            status = "BLOCKED"
        elif any(finding.severity == "UNKNOWN" for finding in findings):
            status = "INCOMPLETE"
        else:
            status = evaluated_status

    command_summaries = _write_evidence(evidence_dir, command_results)
    return ProbeResult(
        status=status,
        target=identity,
        thresholds={
            "max_vram_used_percent": max_vram_used_percent,
            "max_hcu_util_percent": max_hcu_util_percent,
            "busy_sample_quorum": busy_sample_quorum,
        },
        device_count=len(devices),
        expected_device_count=effective_expected_devices,
        devices=devices,
        findings=findings,
        commands=command_summaries,
        started_at=started_at,
        finished_at=_utc_now(),
        evidence_dir=str(evidence_dir),
        environment=environment,
    )


def save_result(result: ProbeResult, path: Path) -> None:
    atomic_write_text_exclusive(
        path,
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2) + "\n",
    )
