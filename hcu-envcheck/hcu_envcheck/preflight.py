# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from .models import DeviceMetrics, Finding
from .parsers import ParseError, RocmAgent, parse_hy_smi_samples, parse_rocminfo
from .roce_health import normalize_roce_policy


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
