# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Sequence

from .k8s import KubernetesPodExecutor
from .models import CommandResult, Finding
from .rdma import evaluate_rdma_network


def _first_line(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().splitlines()[0].strip() or None


def _command_stdout(tool: dict[str, Any], field: str = "version") -> str | None:
    command = tool.get(field, {})
    if command.get("rc") != 0:
        return None
    return command.get("stdout")


def _extract(pattern: str, text: str | None) -> str | None:
    match = re.search(pattern, text or "", re.I)
    return match.group(1).strip() if match else None


def _normal_pci_id(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    return normalized or None


def _canonical_python_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _adapter_inventory(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group adapters by stable PCI identity, vendor label and driver.

    Interface names and BDFs remain in raw evidence and intentionally do not
    enter the cross-node folding key.
    """
    groups: dict[str, dict[str, Any]] = {}
    for item in items:
        vendor_id = _normal_pci_id(item.get("pci_vendor_id") or item.get("pci_vendor"))
        device_id = _normal_pci_id(item.get("pci_device_id") or item.get("pci_device"))
        subsystem_vendor_id = _normal_pci_id(
            item.get("pci_subsystem_vendor_id") or item.get("pci_subsystem_vendor")
        )
        subsystem_device_id = _normal_pci_id(
            item.get("pci_subsystem_device_id") or item.get("pci_subsystem_device")
        )
        speed_mbps = item.get("speed_mbps")
        if speed_mbps is not None and str(speed_mbps).strip() in {"", "-1"}:
            speed_mbps = None
        pci_model = str(item.get("pci_device_name") or "").strip()
        if pci_model.lower() in {"", "device", "unknown"}:
            pci_model = ""
        entry = {
            "vendor": item.get("pci_vendor_name") or "UNKNOWN",
            "model": item.get("hardware_model") or pci_model or "UNNAMED",
            "pci_id": f"{vendor_id or '????'}:{device_id or '????'}",
            "subsystem_pci_id": (
                f"{subsystem_vendor_id}:{subsystem_device_id}"
                if subsystem_vendor_id and subsystem_device_id
                else None
            ),
            "driver": item.get("driver") or "UNKNOWN",
            "driver_version": item.get("driver_version"),
            "firmware_version": item.get("firmware_version"),
            "class": item.get("pci_class_name") or "UNKNOWN",
            "local_link": item.get("local_link_status") or "UNKNOWN",
            "speed_mbps": speed_mbps,
            "mtu": item.get("mtu"),
        }
        signature = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(signature, {**entry, "count": 0})
        group["count"] += 1
    return sorted(
        groups.values(),
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )


def _adapter_hardware_profile(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Stable cross-node identity; excludes BDFs, names and live link values."""
    fields = (
        "pci_id",
        "subsystem_pci_id",
        "driver",
        "driver_version",
        "firmware_version",
    )
    return _project_adapter_profile(inventory, fields)


def _adapter_link_profile(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ("pci_id", "local_link", "speed_mbps", "mtu")
    return _project_adapter_profile(inventory, fields)


def _project_adapter_profile(
    inventory: list[dict[str, Any]],
    fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Project then re-aggregate so omitted fields cannot split the profile."""
    groups: dict[str, dict[str, Any]] = {}
    for item in inventory:
        projected = {field: item.get(field) for field in fields}
        signature = json.dumps(projected, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(signature, {**projected, "count": 0})
        group["count"] += int(item.get("count") or 0)
    return sorted(
        groups.values(),
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )


def evaluate_environment(
    payload: dict[str, Any],
    *,
    expected_device_count: int | None,
    require_compiler: bool,
    require_rdma: bool,
    minimum_rdma_devices: int,
    require_rccl: bool,
    require_ucx: bool,
    network_host_scope_verified: bool = True,
    expected_rdma_protocol: str = "auto",
    rdma_policy: dict[str, Any] | None = None,
    software_payload: dict[str, Any] | None = None,
    required_python_packages: Sequence[str] = ("torch",),
) -> tuple[list[Finding], dict[str, Any], list[dict[str, Any]]]:
    """Evaluate node hardware and the selected training software separately.

    ``payload`` is always the host/node inventory.  When ``software_payload``
    is supplied, DTK/compiler, RCCL/UCX and Python/Torch checks use only that
    selected Docker/Conda target.  An empty mapping deliberately cannot fall
    back to host software evidence.
    """

    findings: list[Finding] = []
    checks: list[dict[str, Any]] = []

    def check(
        check_id: str,
        ok: bool,
        message: str,
        *,
        required: bool = True,
        absent_reason: str,
    ) -> None:
        status = "PASS" if ok else ("FAIL" if required else "WARN")
        checks.append({"check_id": check_id, "status": status, "message": message})
        if not ok:
            findings.append(Finding(status, absent_reason, message))

    host_dtk = payload.get("dtk", {})
    host_tools = host_dtk.get("tools", {})
    selected_software = payload if software_payload is None else software_payload
    dtk = selected_software.get("dtk", {})
    tools = dtk.get("tools", {})
    version_file = dtk.get("version_file") or {}
    dtk_version = _first_line(version_file.get("value"))
    check(
        "DTK_VERSION",
        bool(dtk_version),
        f"DTK version={dtk_version or 'unavailable'}",
        required=False,
        absent_reason="DTK_VERSION_UNAVAILABLE",
    )
    hipcc_text = _command_stdout(tools.get("hipcc", {}))
    hipcc_version = _first_line(hipcc_text)
    check(
        "HIP_COMPILER",
        bool(tools.get("hipcc", {}).get("path") and hipcc_version),
        f"hipcc={hipcc_version or 'unavailable'}",
        required=require_compiler,
        absent_reason="HIPCC_NOT_AVAILABLE",
    )

    # Driver/device health remains host-scoped in Docker/Conda mode.
    hy_smi_tool = host_tools.get("hy-smi", {})
    driver_text = _command_stdout(hy_smi_tool, "driver_version")
    driver_version = _extract(r"Driver\s+Version:\s*([^\r\n]+)", driver_text)
    check(
        "DRIVER_VERSION",
        bool(driver_version),
        f"driver={driver_version or 'unavailable'}",
        required=False,
        absent_reason="DRIVER_VERSION_UNAVAILABLE",
    )
    driver_modules = payload.get("driver", {}).get("modules", [])
    driver_loaded = any(re.match(r"hycu\s", line) for line in driver_modules)
    check(
        "HYCU_DRIVER_MODULE",
        driver_loaded,
        "hycu kernel module loaded" if driver_loaded else "hycu kernel module not visible",
        required=False,
        absent_reason="HYCU_DRIVER_NOT_LOADED",
    )
    device_nodes = payload.get("driver", {}).get("device_nodes", [])
    check(
        "HCU_DEVICE_NODE",
        "/dev/kfd" in device_nodes,
        "/dev/kfd visible" if "/dev/kfd" in device_nodes else "/dev/kfd not visible",
        absent_reason="HCU_DEVICE_NODE_MISSING",
    )

    library_paths = selected_software.get("libraries", {}).get("paths", [])
    rccl_paths = [path for path in library_paths if "librccl.so" in path or "libnccl.so" in path]
    torch_info = selected_software.get("torch", {})
    python_packages = selected_software.get("python", {}).get("packages", {})
    available_python_packages = {
        _canonical_python_package_name(str(name)): version
        for name, version in python_packages.items()
    }
    required_packages = tuple(
        dict.fromkeys(
            _canonical_python_package_name(str(name))
            for name in required_python_packages
        )
    )
    for package_name in required_packages:
        if package_name == "torch":
            continue
        check_id = "PYTHON_PACKAGE_" + re.sub(
            r"[^A-Z0-9]+",
            "_",
            package_name.upper(),
        ).strip("_")
        version = available_python_packages.get(package_name)
        check(
            check_id,
            version is not None,
            f"Python package {package_name}={version or 'not installed'}",
            absent_reason="PYTHON_PACKAGE_NOT_FOUND",
        )
    rccl_backend_available = torch_info.get("distributed_nccl_available") is True
    check(
        "RCCL_LIBRARY",
        bool(rccl_paths) or rccl_backend_available,
        f"RCCL libraries={rccl_paths or 'none'}, torch_nccl_backend={rccl_backend_available}",
        required=require_rccl,
        absent_reason="RCCL_LIBRARY_NOT_FOUND",
    )

    ucx_text = _command_stdout(tools.get("ucx_info", {}))
    ucx_version = _extract(r"Library version:\s*([^\r\n]+)", ucx_text)
    check(
        "UCX",
        bool(ucx_version),
        f"UCX={ucx_version or 'unavailable'}",
        required=require_ucx,
        absent_reason="UCX_NOT_AVAILABLE",
    )

    rdma_devices = payload.get("network", {}).get("rdma_devices", [])
    def rdma_state(value: Any) -> str:
        return str(value or "").split(":", 1)[-1].strip().upper()

    active_ports = [
        (device.get("name"), port.get("port"), port.get("rate"))
        for device in rdma_devices
        for port in device.get("ports", [])
        if rdma_state(port.get("state")) == "ACTIVE"
        and rdma_state(port.get("phys_state")) == "LINKUP"
    ]
    active_device_names = sorted({item[0] for item in active_ports if item[0]})
    rdma_required = (
        require_rdma
        or minimum_rdma_devices > 0
        or expected_rdma_protocol != "auto"
        or rdma_policy is not None
    )
    minimum = max(minimum_rdma_devices, 1 if rdma_required else 0)
    if network_host_scope_verified:
        rdma_count_ok = len(rdma_devices) >= minimum if rdma_required else bool(rdma_devices)
        check(
            "RDMA_DEVICE_COUNT",
            rdma_count_ok,
            f"RDMA devices={len(rdma_devices)}, required={minimum if rdma_required else 'optional'}",
            required=rdma_required,
            absent_reason="RDMA_DEVICE_COUNT_INSUFFICIENT",
        )
        active_device_ok = (
            len(active_device_names) >= minimum if rdma_required else bool(active_device_names)
        )
        check(
            "RDMA_ACTIVE_DEVICE_COUNT",
            active_device_ok,
            f"RDMA devices with Active+LinkUp port={len(active_device_names)}, "
            f"active ports={len(active_ports)}, required devices={minimum if rdma_required else 'optional'}",
            required=rdma_required,
            absent_reason="RDMA_DEVICE_NOT_ACTIVE",
        )
        rdma_findings, rdma_checks, rdma_summary = evaluate_rdma_network(
            payload.get("network", {}),
            expected_protocol=expected_rdma_protocol,
            required=rdma_required,
            rdma_policy=rdma_policy,
        )
        findings.extend(rdma_findings)
        checks.extend(rdma_checks)
    else:
        severity = "UNKNOWN" if rdma_required else "WARN"
        message = "pod hostNetwork/privileged capability is insufficient to verify host NIC/RDMA"
        checks.append(
            {"check_id": "NETWORK_HOST_SCOPE", "status": severity, "message": message}
        )
        findings.append(Finding(severity, "NETWORK_HOST_SCOPE_UNVERIFIED", message))
        rdma_summary = {
            "rdma_current_protocol": "UNVERIFIED",
            "rdma_protocol_status": "UNKNOWN",
            "rdma_expected_protocol": expected_rdma_protocol,
            "rdma_hardware_protocol_capability": "UNKNOWN_NO_HOST_ACCESS",
            "rdma_runtime_transport_verified": False,
            "rdma_userspace": {
                "status": "UNKNOWN",
                "check_status": "UNKNOWN",
                "reason_code": "NETWORK_HOST_SCOPE_UNVERIFIED",
                "message": "host network scope is not verified",
                "required": rdma_required,
                "sysfs_devices": [],
                "enumerated_devices": [],
                "missing_enumerated_devices": [],
                "device_open_checks": [],
            },
            "rdma_fabric_profile": None,
            "rdma_protocol_profile": None,
            "ib_endpoint": {
                "status": "UNKNOWN",
                "reason": "host network scope is not verified",
            },
            "roce_endpoint": {
                "status": "UNKNOWN",
                "reason": "host network scope is not verified",
            },
            "roce_configuration_health": {
                "status": "UNKNOWN",
                "policy_applied": rdma_policy is not None,
                "summary": {"unknown_checks": ["NETWORK_HOST_SCOPE_UNVERIFIED"]},
            },
        }

    if "torch" in required_packages:
        torch_importable = torch_info.get("importable") is True
        if torch_importable:
            check(
                "TORCH_IMPORT",
                True,
                f"torch={torch_info.get('version')}",
                absent_reason="TORCH_IMPORT_FAILED",
            )
            check(
                "TORCH_HIP_BUILD",
                bool(torch_info.get("hip_version")),
                f"torch HIP={torch_info.get('hip_version') or 'unavailable'}",
                required=False,
                absent_reason="TORCH_NOT_HIP_BUILD",
            )
            check(
                "TORCH_HCU_AVAILABLE",
                torch_info.get("hcu_available") is True,
                f"torch.cuda.is_available={torch_info.get('hcu_available')}",
                absent_reason="TORCH_HCU_UNAVAILABLE",
            )
            if expected_device_count is not None:
                check(
                    "TORCH_DEVICE_COUNT",
                    torch_info.get("device_count") == expected_device_count,
                    f"torch device_count={torch_info.get('device_count')}, expected={expected_device_count}",
                    absent_reason="TORCH_DEVICE_COUNT_MISMATCH",
                )
        else:
            error = f"{torch_info.get('error_type')}: {torch_info.get('error')}"
            lowered = error.lower()
            if "undefined symbol" in lowered and ("hcu_smi" in lowered or "hcusmi" in lowered):
                reason_code = "HCUSMI_LIBRARY_ABI_MISMATCH"
            elif "cannot open shared object file" in lowered or "no such file or directory" in lowered:
                reason_code = "TORCH_NATIVE_DEPENDENCY_MISSING"
            else:
                reason_code = "TORCH_IMPORT_FAILED"
            checks.append({"check_id": "TORCH_IMPORT", "status": "FAIL", "message": error})
            findings.append(Finding("FAIL", reason_code, f"torch import failed: {error}"))

    hy_smi_version = _first_line(_command_stdout(hy_smi_tool))
    smi_library_version = _extract(
        r"ROCM-SMI-LIB\s+version:\s*([^\r\n]+)",
        _command_stdout(hy_smi_tool, "library_version"),
    )
    mpi_version = _first_line(_command_stdout(tools.get("mpirun", {})))
    hsw_versions = sorted(
        set(re.findall(r"FW\s+Version:\s*([^\s\r\n]+)", _command_stdout(hy_smi_tool, "hsw_firmware") or ""))
    )
    vbios_payload: dict[str, Any] = {}
    vbios_text = _command_stdout(hy_smi_tool, "vbios")
    if vbios_text:
        try:
            vbios_payload = json.loads(vbios_text)
        except json.JSONDecodeError:
            vbios_payload = {}
    vbios_versions = sorted(
        {
            str(value)
            for fields in vbios_payload.values()
            if isinstance(fields, dict)
            for key, value in fields.items()
            if "vbios" in key.lower()
        }
    )
    interfaces = payload.get("network", {}).get("interfaces", []) if network_host_scope_verified else []
    if not network_host_scope_verified:
        rdma_devices = []
        active_ports = []
        active_device_names = []
    nic_inventory = _adapter_inventory(interfaces)
    rdma_nic_inventory = _adapter_inventory(rdma_devices)
    nic_link_summary = {
        state: sum(item.get("local_link_status") == state for item in interfaces)
        for state in ("UP", "DOWN", "UNKNOWN")
    } if network_host_scope_verified else {state: None for state in ("UP", "DOWN", "UNKNOWN")}
    summary = {
        # This field is consumed by the host hardware grouping/report.  A
        # Docker image OS must never masquerade as the bare-metal node OS.
        "container_os": payload.get("system", {}).get("os_release", {}).get("PRETTY_NAME"),
        "software_target_os": selected_software.get("system", {})
        .get("os_release", {})
        .get("PRETTY_NAME"),
        "kernel": payload.get("system", {}).get("kernel"),
        "cpu_logical_count": payload.get("system", {}).get("cpu_logical_count"),
        "cpu_affinity_count": payload.get("system", {}).get("cpu_affinity_count"),
        "cpu_models": payload.get("system", {}).get("cpu_models", []),
        "mem_total": payload.get("system", {}).get("meminfo", {}).get("MemTotal"),
        "dtk_version": dtk_version,
        "driver_version": driver_version,
        "hy_smi_version": hy_smi_version,
        "smi_library_version": smi_library_version,
        "hipcc_version": hipcc_version,
        "rccl_paths": rccl_paths,
        "ucx_version": ucx_version,
        "mpi_version": mpi_version,
        "vbios_versions": vbios_versions,
        "hsw_firmware_versions": hsw_versions,
        "network_scope": "HOST_VERIFIED" if network_host_scope_verified else "UNVERIFIED_POD_SCOPE",
        "physical_nic_count": len(interfaces) if network_host_scope_verified else None,
        "nic_drivers": sorted({item.get("driver") for item in interfaces if item.get("driver")}),
        "nic_inventory": nic_inventory,
        "nic_hardware_profile": (
            _adapter_hardware_profile(nic_inventory) if network_host_scope_verified else None
        ),
        "nic_link_profile": (
            _adapter_link_profile(nic_inventory) if network_host_scope_verified else None
        ),
        "nic_link_summary": nic_link_summary,
        "rdma_nic_inventory": rdma_nic_inventory,
        "rdma_hardware_profile": (
            _adapter_hardware_profile(rdma_nic_inventory) if network_host_scope_verified else None
        ),
        "pci_name_source": payload.get("network", {}).get("pci_name_source"),
        "rdma_device_count": len(rdma_devices) if network_host_scope_verified else None,
        "rdma_active_device_count": (
            len(active_device_names) if network_host_scope_verified else None
        ),
        "rdma_active_port_count": len(active_ports) if network_host_scope_verified else None,
        "rdma_rates": (
            sorted({item[2] for item in active_ports if item[2]})
            if network_host_scope_verified
            else None
        ),
        "torch_version": torch_info.get("version"),
        "torch_hip_version": torch_info.get("hip_version"),
        "python_version": selected_software.get("python", {}).get("version"),
        "python_packages": python_packages,
        "required_python_packages": list(required_packages),
        "torch_device_count": torch_info.get("device_count"),
        "torch_hcu_available": torch_info.get("hcu_available"),
        "torch_distributed_available": torch_info.get("distributed_available"),
        "torch_nccl_backend_available": torch_info.get("distributed_nccl_available"),
        "torch_nccl_version": torch_info.get("nccl_version"),
        "dev_shm": payload.get("system", {}).get("dev_shm"),
        "cgroup_memory_max": payload.get("system", {}).get("cgroup_memory_max"),
        "cgroup_cpu_max": payload.get("system", {}).get("cgroup_cpu_max"),
        "runtime_env": selected_software.get("runtime_env", {}),
        "software_evidence_scope": (
            "HOST" if software_payload is None else "SELECTED_TRAINING_TARGET"
        ),
        **rdma_summary,
    }
    packages = summary["python_packages"]
    summary["core_python_packages"] = {
        name: packages[name]
        for name in ("torch", "torchvision", "triton", "flash-attn", "numpy", "hcusmi")
        if name in packages
    }
    return findings, summary, checks


def collect_environment(
    executor: KubernetesPodExecutor,
    *,
    expected_device_count: int | None,
    require_compiler: bool,
    require_rdma: bool,
    minimum_rdma_devices: int,
    require_rccl: bool,
    require_ucx: bool,
    network_host_scope_verified: bool,
    expected_rdma_protocol: str = "auto",
    rdma_policy: dict[str, Any] | None = None,
    rdma_counter_interval_seconds: int = 5,
) -> tuple[dict[str, Any], CommandResult, list[Finding]]:
    if rdma_counter_interval_seconds != 0 and not 1 <= rdma_counter_interval_seconds <= 60:
        raise ValueError(
            "rdma_counter_interval_seconds must be 0 or between 1 and 60"
        )
    script_path = Path(__file__).with_name("pod_probe.py")
    script = script_path.read_text(encoding="utf-8")
    result = executor.exec_stdin(
        "environment_inventory",
        [
            "env",
            "PYTHONDONTWRITEBYTECODE=1",
            f"HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS={rdma_counter_interval_seconds}",
            "python3",
            "-",
        ],
        script,
        timeout=90,
    )
    if result.returncode != 0:
        reason_code = (
            "ENVIRONMENT_PROBE_OOM"
            if result.returncode in {137, -9} or "exit code 137" in result.stderr.lower()
            else "ENVIRONMENT_INVENTORY_FAILED"
        )
        finding = Finding(
            "UNKNOWN",
            reason_code,
            f"container environment inventory rc={result.returncode}: {result.stderr[:512]}",
        )
        return {}, result, [finding]
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        finding = Finding(
            "UNKNOWN",
            "ENVIRONMENT_INVENTORY_PARSE_FAILED",
            f"cannot parse environment inventory JSON: {exc}",
        )
        return {}, result, [finding]
    findings, summary, checks = evaluate_environment(
        payload,
        expected_device_count=expected_device_count,
        require_compiler=require_compiler,
        require_rdma=require_rdma,
        minimum_rdma_devices=minimum_rdma_devices,
        require_rccl=require_rccl,
        require_ucx=require_ucx,
        network_host_scope_verified=network_host_scope_verified,
        expected_rdma_protocol=expected_rdma_protocol,
        rdma_policy=rdma_policy,
    )
    payload["summary"] = summary
    payload["checks"] = checks
    payload["coverage"] = {
        "host_hardware": (
            "CHECKED_FROM_HOSTNETWORK_PRIVILEGED_POD_AND_K8S_API"
            if network_host_scope_verified
            else "UNVERIFIED_POD_SCOPE"
        ),
        "container_environment": "CHECKED",
        "switch_management": "NOT_CHECKED_NO_SWITCH_CREDENTIALS",
        "torch_device_execution": "RUNTIME_VISIBILITY_ONLY_NO_TENSOR_NO_COLLECTIVE",
        "rdma_port_configuration": summary.get("rdma_protocol_status"),
        "rdma_runtime_transport": "NOT_VERIFIED_BY_PREFLIGHT",
    }
    return payload, result, findings
