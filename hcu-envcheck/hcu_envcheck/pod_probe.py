# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import glob
import ipaddress
import importlib.metadata
import json
import math
import os
import pathlib
import platform
import re
import shlex
import shutil
import subprocess
import time
from typing import Any


def read(path: str | pathlib.Path, limit: int = 8192) -> str | None:
    try:
        return pathlib.Path(path).read_text(errors="replace")[:limit].strip()
    except (OSError, UnicodeError):
        return None


def run(argv: list[str], timeout: int = 10, limit: int = 8192) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            argv,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        return {
            "rc": completed.returncode,
            "stdout": completed.stdout[:limit].strip(),
            "stderr": completed.stderr[:limit].strip(),
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "rc": 124 if isinstance(exc, subprocess.TimeoutExpired) else 127,
            "error": str(exc),
        }


def first_existing(paths: list[str]) -> dict[str, str] | None:
    for path in paths:
        value = read(path)
        if value:
            return {"path": path, "value": value}
    return None


def sys_value(path: pathlib.Path) -> str | None:
    value = read(path, 512)
    return value if value not in {"", None} else None


def collect_tools() -> dict[str, Any]:
    tool_specs = {
        "hipcc": ["--version"],
        "hy-smi": ["--version"],
        "rocminfo": None,
        "ucx_info": ["-v"],
        "mpirun": ["--version"],
        "ibv_devinfo": None,
        "ibv_devices": None,
        "rdma": ["-V"],
        "dcb": ["-V"],
        "devlink": ["-V"],
        "ip": ["-V"],
        "ethtool": ["--version"],
        "lspci": ["--version"],
    }
    tools: dict[str, Any] = {}
    for name, version_args in tool_specs.items():
        candidates = (name, "Hy-smi") if name == "hy-smi" else (name,)
        path = next((shutil.which(item) for item in candidates if shutil.which(item)), None)
        if not path:
            continue
        item: dict[str, Any] = {"path": path}
        if version_args is not None:
            item["version"] = run([path] + version_args, limit=2048)
        tools[name] = item
    hy_smi = tools.get("hy-smi", {}).get("path")
    if hy_smi:
        tools["hy-smi"]["library_version"] = run([hy_smi, "--libversion"], limit=2048)
        tools["hy-smi"]["driver_version"] = run([hy_smi, "--showdriverversion"], limit=2048)
        tools["hy-smi"]["vbios"] = run([hy_smi, "--showvbios", "--json"], limit=8192)
        tools["hy-smi"]["hsw_firmware"] = run([hy_smi, "--showhswfw"], limit=4096)
    return tools


def collect_system() -> dict[str, Any]:
    os_release: dict[str, str] = {}
    for line in (read("/etc/os-release") or "").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            os_release[key] = value.strip('"')
    cpu_models = sorted(
        {
            line.split(":", 1)[1].strip()
            for line in (read("/proc/cpuinfo", 1024 * 1024) or "").splitlines()
            if line.lower().startswith("model name") and ":" in line
        }
    )
    meminfo: dict[str, str] = {}
    selected = {
        "MemTotal",
        "MemAvailable",
        "SwapTotal",
        "SwapFree",
        "HugePages_Total",
        "HugePages_Free",
        "Hugepagesize",
    }
    for line in (read("/proc/meminfo") or "").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            if key in selected:
                meminfo[key] = value.strip()
    try:
        cpu_affinity_count = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        cpu_affinity_count = None
    shm = None
    try:
        stat = os.statvfs("/dev/shm")
        shm = {
            "total_bytes": stat.f_blocks * stat.f_frsize,
            "available_bytes": stat.f_bavail * stat.f_frsize,
        }
    except OSError:
        pass
    return {
        "hostname": platform.node(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "os_release": os_release,
        "cpu_logical_count": os.cpu_count(),
        "cpu_affinity_count": cpu_affinity_count,
        "cpu_models": cpu_models,
        "meminfo": meminfo,
        "cgroup_memory_max": first_existing(
            [
                "/sys/fs/cgroup/memory.max",
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            ]
        ),
        "cgroup_memory_current": first_existing(
            [
                "/sys/fs/cgroup/memory.current",
                "/sys/fs/cgroup/memory/memory.usage_in_bytes",
            ]
        ),
        "cgroup_cpu_max": first_existing(["/sys/fs/cgroup/cpu.max"]),
        "cgroup_cpuset_effective": first_existing(
            [
                "/sys/fs/cgroup/cpuset.cpus.effective",
                "/sys/fs/cgroup/cpuset/cpuset.cpus",
            ]
        ),
        "dev_shm": shm,
    }


def collect_driver() -> dict[str, Any]:
    modules = []
    for line in (read("/proc/modules", 1024 * 1024) or "").splitlines():
        name = line.split()[0] if line.split() else ""
        if re.search(r"hycu|hyhcu|amdgpu|hsa|ib_|rdma|mlx|shca|bnxt|hns", name, re.I):
            modules.append(line)
    device_nodes = sorted(
        path
        for pattern in (
            "/dev/kfd",
            "/dev/dri/*",
            "/dev/infiniband/*",
            "/dev/hy*",
            "/dev/hcu*",
        )
        for path in glob.glob(pattern)
    )[:256]
    return {
        "modules": modules,
        "device_nodes": device_nodes,
    }


def _pci_label_parts(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    match = re.search(r"\[([0-9a-fA-F]{4})\]\s*$", value)
    pci_id = match.group(1).lower() if match else None
    name = value[: match.start()].strip() if match else value.strip()
    return name or None, pci_id


def parse_lspci_machine_readable(text: str) -> dict[str, dict[str, str | None]]:
    """Parse `lspci -Dmmnn` without depending on one NIC vendor.

    PCI numeric IDs remain the stable identity.  Human-readable names are
    enrichment from the image's pci.ids database and may be absent or generic.
    """
    devices: dict[str, dict[str, str | None]] = {}
    for line in text.splitlines():
        try:
            fields = shlex.split(line)
        except ValueError:
            continue
        if len(fields) < 4 or not re.fullmatch(
            r"[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]", fields[0]
        ):
            continue
        class_name, class_id = _pci_label_parts(fields[1])
        vendor_name, vendor_id = _pci_label_parts(fields[2])
        device_name, device_id = _pci_label_parts(fields[3])
        trailing = [
            field
            for field in fields[4:]
            if not re.fullmatch(r"-[rp][0-9a-fA-F]{2}", field)
        ]
        subsystem_vendor_name, subsystem_vendor_id = _pci_label_parts(
            trailing[0] if len(trailing) > 0 else None
        )
        subsystem_device_name, subsystem_device_id = _pci_label_parts(
            trailing[1] if len(trailing) > 1 else None
        )
        devices[fields[0].lower()] = {
            "pci_class_name": class_name,
            "pci_class_id": class_id,
            "pci_vendor_name": vendor_name,
            "pci_vendor_id": vendor_id,
            "pci_device_name": device_name,
            "pci_device_id": device_id,
            "pci_subsystem_vendor_name": subsystem_vendor_name,
            "pci_subsystem_vendor_id": subsystem_vendor_id,
            "pci_subsystem_device_name": subsystem_device_name,
            "pci_subsystem_device_id": subsystem_device_id,
        }
    return devices


def parse_key_value_lines(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip().lower().replace("-", "_")] = value.strip()
    return values


def parse_ibv_devinfo_ports(text: str) -> dict[str, dict[str, str]]:
    """Extract portable port facts used when a driver omits sysfs attributes."""
    ports: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for line in text.splitlines():
        port_match = re.match(r"^\s*port:\s*(\d+)\s*$", line)
        if port_match:
            current = ports.setdefault(port_match.group(1), {})
            continue
        if current is None or ":" not in line:
            continue
        key, value = line.strip().split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "state":
            match = re.fullmatch(r"PORT_([A-Z_]+)\s*\((\d+)\)", value, re.I)
            if match:
                value = f"{match.group(2)}: {match.group(1).replace('_', '')}"
        field = {
            "state": "state",
            "max_mtu": "max_mtu",
            "active_mtu": "active_mtu",
            "sm_lid": "sm_lid",
            "port_lid": "lid",
            "port_lmc": "lmc",
            "link_layer": "link_layer",
        }.get(key)
        if field:
            current[field] = value
    return ports


def _is_nonzero_gid(value: str | None) -> bool:
    try:
        parsed = ipaddress.IPv6Address(str(value or "").strip())
    except ipaddress.AddressValueError:
        return False
    return int(parsed) != 0


def _indexed_files(path: pathlib.Path) -> list[pathlib.Path]:
    def key(item: pathlib.Path) -> tuple[int, str]:
        return (int(item.name), "") if item.name.isdigit() else (2**31 - 1, item.name)

    return sorted((item for item in path.glob("*") if item.is_file()), key=key)


def collect_gid_table_evidence(
    port: pathlib.Path,
) -> tuple[list[dict[str, Any]] | None, str, str, str]:
    gids_path = port / "gids"
    if not gids_path.is_dir():
        return None, "UNAVAILABLE", "NOT_COLLECTED", "NOT_COLLECTED"
    entries: list[dict[str, Any]] = []
    read_errors = 0
    type_read_errors = 0
    ndev_read_errors = 0
    for gid_path in _indexed_files(gids_path):
        gid = sys_value(gid_path)
        if gid is None:
            read_errors += 1
            continue
        if not _is_nonzero_gid(gid):
            continue
        type_path = port / "gid_attrs" / "types" / gid_path.name
        ndev_path = port / "gid_attrs" / "ndevs" / gid_path.name
        gid_type_raw = read(type_path, 512) if type_path.is_file() else None
        netdev_raw = read(ndev_path, 512) if ndev_path.is_file() else None
        type_readable = type_path.is_file() and gid_type_raw is not None
        ndev_readable = ndev_path.is_file() and netdev_raw is not None
        gid_type = gid_type_raw or None
        netdev = netdev_raw or None
        if not type_readable:
            type_read_errors += 1
        if not ndev_readable:
            ndev_read_errors += 1
        entries.append(
            {
                "index": int(gid_path.name) if gid_path.name.isdigit() else gid_path.name,
                "gid": gid,
                "type": gid_type,
                "netdev": netdev,
            }
        )
    status = "COMPLETE" if read_errors == 0 else "PARTIAL_READ_ERROR"
    type_status = "COMPLETE" if type_read_errors == 0 else "PARTIAL_READ_ERROR"
    ndev_status = "COMPLETE" if ndev_read_errors == 0 else "PARTIAL_READ_ERROR"
    return entries, status, type_status, ndev_status


def collect_gid_table(port: pathlib.Path) -> list[dict[str, Any]] | None:
    return collect_gid_table_evidence(port)[0]


def gid_subnet_prefix(entries: list[dict[str, Any]] | None) -> str | None:
    for entry in entries or []:
        try:
            gid = ipaddress.IPv6Address(str(entry.get("gid") or "").strip())
        except ipaddress.AddressValueError:
            continue
        if int(gid) != 0:
            return f"0x{(int(gid) >> 64):016x}"
    return None


def collect_pkey_table_evidence(
    port: pathlib.Path,
) -> tuple[list[dict[str, Any]] | None, str]:
    pkeys_path = port / "pkeys"
    if not pkeys_path.is_dir():
        return None, "UNAVAILABLE"
    entries: list[dict[str, Any]] = []
    read_errors = 0
    for pkey_path in _indexed_files(pkeys_path):
        value = sys_value(pkey_path)
        try:
            numeric = int(str(value), 0)
        except (TypeError, ValueError):
            read_errors += 1
            continue
        if numeric & 0x7FFF:
            entries.append(
                {
                    "index": int(pkey_path.name) if pkey_path.name.isdigit() else pkey_path.name,
                    "value": value,
                }
            )
    status = "COMPLETE" if read_errors == 0 else "PARTIAL_READ_ERROR"
    return entries, status


def collect_pkey_table(port: pathlib.Path) -> list[dict[str, Any]] | None:
    return collect_pkey_table_evidence(port)[0]


IB_PORT_COUNTER_NAMES = (
    "excessive_buffer_overrun_errors",
    "link_downed",
    "link_error_recovery",
    "local_link_integrity_errors",
    "port_rcv_constraint_errors",
    "port_rcv_errors",
    "port_rcv_remote_physical_errors",
    "port_rcv_switch_relay_errors",
    "port_xmit_constraint_errors",
    "port_xmit_discards",
    "port_xmit_wait",
    "symbol_error",
    "VL15_dropped",
)
RDMA_COUNTER_INTERVAL_ENV = "HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS"


def collect_ib_port_counter_evidence(
    port: pathlib.Path,
) -> tuple[dict[str, str] | None, str]:
    counter_dir = port / "counters"
    if not counter_dir.is_dir():
        return None, "UNAVAILABLE"
    values: dict[str, str] = {}
    read_errors = 0
    for name in IB_PORT_COUNTER_NAMES:
        value = sys_value(counter_dir / name)
        if value is not None:
            values[name] = value
        else:
            read_errors += 1
    if not values:
        return None, "UNAVAILABLE"
    return values, "COMPLETE" if read_errors == 0 else "PARTIAL_READ_ERROR"


def collect_ib_port_counters(port: pathlib.Path) -> dict[str, str] | None:
    return collect_ib_port_counter_evidence(port)[0]


def collect_ib_port_hw_counter_evidence(
    port: pathlib.Path,
) -> tuple[dict[str, str] | None, str]:
    """Collect driver-specific counters without assigning generic thresholds."""

    counter_dir = port / "hw_counters"
    if not counter_dir.is_dir():
        return None, "UNAVAILABLE"
    paths = sorted(counter_dir.glob("*"), key=lambda path: path.name)
    if not paths:
        return None, "EMPTY"
    values: dict[str, str] = {}
    read_errors = 0
    for path in paths:
        value = sys_value(path)
        if value is not None:
            values[path.name] = value
        else:
            read_errors += 1
    if not values:
        return None, "UNAVAILABLE"
    return values, "COMPLETE" if read_errors == 0 else "PARTIAL_READ_ERROR"


def _rdma_counter_sampling_configuration(
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    environment = os.environ if environ is None else environ
    raw_value = str(environment.get(RDMA_COUNTER_INTERVAL_ENV, "5")).strip()
    try:
        interval = float(raw_value)
    except ValueError:
        interval = None
    if interval is None or not math.isfinite(interval):
        status = "INVALID_INTERVAL"
    elif interval == 0:
        status = "DISABLED"
    elif 1 <= interval <= 60:
        status = "ENABLED"
    else:
        status = "INVALID_INTERVAL"
    return {
        "status": status,
        "environment_variable": RDMA_COUNTER_INTERVAL_ENV,
        "configured_value": raw_value,
        "interval_seconds": interval if status in {"ENABLED", "DISABLED"} else None,
    }


def _collect_ib_counter_snapshot(port: pathlib.Path) -> dict[str, Any]:
    monotonic_started_ns = time.monotonic_ns()
    sampled_at_unix_ns = time.time_ns()
    counters, counter_status = collect_ib_port_counter_evidence(port)
    hw_counters, hw_counter_status = collect_ib_port_hw_counter_evidence(port)
    monotonic_finished_ns = time.monotonic_ns()
    return {
        "sampled_at_unix_ns": sampled_at_unix_ns,
        "monotonic_started_ns": monotonic_started_ns,
        "monotonic_finished_ns": monotonic_finished_ns,
        "monotonic_ns": (monotonic_started_ns + monotonic_finished_ns) // 2,
        "counter_status": counter_status,
        "counters": counters,
        "hw_counter_status": hw_counter_status,
        "hw_counters": hw_counters,
    }


def _counter_window_status(before: dict[str, Any], after: dict[str, Any]) -> str:
    statuses = {before.get("counter_status"), after.get("counter_status")}
    if statuses == {"COMPLETE"}:
        return "COMPLETE"
    if statuses <= {"UNAVAILABLE"}:
        return "UNAVAILABLE"
    return "PARTIAL"


def _collect_ib_counter_windows(
    port_records: list[tuple[pathlib.Path, dict[str, Any]]],
    *,
    environ: dict[str, str] | None = None,
    sleep_fn: Any = None,
) -> dict[str, Any]:
    """Take a node-local pair of snapshots for every enumerated RDMA port."""

    config = _rdma_counter_sampling_configuration(environ)
    if not port_records:
        return {**config, "status": "NOT_APPLICABLE", "ports": 0}

    if config["status"] != "ENABLED":
        for port_path, payload in port_records:
            snapshot = _collect_ib_counter_snapshot(port_path)
            payload["counters"] = snapshot.get("counters")
            payload["counter_collection_status"] = snapshot.get("counter_status")
            payload["hw_counters"] = snapshot.get("hw_counters")
            payload["hw_counter_collection_status"] = snapshot.get(
                "hw_counter_status"
            )
            payload["counter_window"] = {
                "status": config["status"],
                "configured_value": config["configured_value"],
                "configured_interval_seconds": config.get("interval_seconds"),
                "interval_seconds": None,
                "before": None,
                "after": snapshot,
            }
        return {**config, "ports": len(port_records)}

    before_by_path = {
        str(port_path): _collect_ib_counter_snapshot(port_path)
        for port_path, _payload in port_records
    }
    (sleep_fn or time.sleep)(float(config["interval_seconds"]))
    window_statuses: list[str] = []
    actual_intervals: list[float] = []
    for port_path, payload in port_records:
        before = before_by_path[str(port_path)]
        after = _collect_ib_counter_snapshot(port_path)
        actual_interval = max(
            0.0,
            (int(after["monotonic_ns"]) - int(before["monotonic_ns"]))
            / 1_000_000_000,
        )
        window_status = _counter_window_status(before, after)
        window_statuses.append(window_status)
        actual_intervals.append(actual_interval)
        payload["counters"] = after.get("counters")
        payload["counter_collection_status"] = after.get("counter_status")
        payload["hw_counters"] = after.get("hw_counters")
        payload["hw_counter_collection_status"] = after.get("hw_counter_status")
        payload["counter_window"] = {
            "status": window_status,
            "configured_value": config["configured_value"],
            "configured_interval_seconds": config["interval_seconds"],
            "interval_seconds": actual_interval,
            "before": before,
            "after": after,
        }

    if all(status == "COMPLETE" for status in window_statuses):
        status = "COMPLETE"
    elif all(status == "UNAVAILABLE" for status in window_statuses):
        status = "UNAVAILABLE"
    else:
        status = "PARTIAL"
    return {
        **config,
        "status": status,
        "ports": len(port_records),
        "actual_interval_seconds_min": min(actual_intervals),
        "actual_interval_seconds_max": max(actual_intervals),
    }


def parse_lspci_device_names(text: str) -> dict[str, str]:
    """Return firmware/BIOS supplied ``DeviceName`` labels keyed by PCI BDF."""
    names: dict[str, str] = {}
    current_bdf: str | None = None
    for line in text.splitlines():
        device = re.match(
            r"^([0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7])\s+",
            line,
        )
        if device:
            current_bdf = device.group(1).lower()
            continue
        label = re.match(r"^\s+DeviceName:\s*(\S.*)$", line)
        if current_bdf and label:
            names[current_bdf] = label.group(1).strip()
    return names


def _local_link_status(operstate: str | None, carrier: str | None) -> str:
    if operstate == "up" and carrier == "1":
        return "UP"
    if operstate in {"down", "lowerlayerdown"} or carrier == "0":
        return "DOWN"
    return "UNKNOWN"


def _bond_attributes(base: pathlib.Path) -> dict[str, Any] | None:
    bond = base / "bonding"
    if not bond.is_dir():
        return None
    return {
        "mode": sys_value(bond / "mode"),
        "active_slave": sys_value(bond / "active_slave"),
        "mii_status": sys_value(bond / "mii_status"),
        "lacp_rate": sys_value(bond / "lacp_rate"),
        "xmit_hash_policy": sys_value(bond / "xmit_hash_policy"),
        "min_links": sys_value(bond / "min_links"),
    }


def _bond_slave_attributes(base: pathlib.Path) -> dict[str, Any] | None:
    slave = base / "bonding_slave"
    if not slave.is_dir():
        return None
    return {
        name: sys_value(slave / name)
        for name in ("state", "mii_status", "link_failure_count", "queue_id")
    }


def _interface_relationships(base: pathlib.Path) -> dict[str, Any]:
    lower_interfaces = sorted(
        path.name.removeprefix("lower_") for path in base.glob("lower_*")
    )
    bond_slaves_value = sys_value(base / "bonding" / "slaves")
    bond_slaves = sorted(str(bond_slaves_value or "").split())
    master = None
    try:
        if (base / "master").exists():
            master = (base / "master").resolve().name
    except OSError:
        master = None
    ifindex = sys_value(base / "ifindex")
    iflink = sys_value(base / "iflink")
    if not lower_interfaces and ifindex and iflink and ifindex != iflink:
        for candidate in base.parent.glob("*"):
            if sys_value(candidate / "ifindex") == iflink:
                lower_interfaces = [candidate.name]
                break
    return {
        "ifindex": ifindex,
        "iflink": iflink,
        "master": master,
        "lower_interfaces": lower_interfaces,
        "bond_slaves": bond_slaves,
        "bond": _bond_attributes(base),
        "bond_slave": _bond_slave_attributes(base),
    }


def parse_ip_address_payload(text: str) -> list[dict[str, Any]]:
    payload = json.loads(text or "[]")
    if not isinstance(payload, list):
        raise ValueError("ip address JSON must be an array")
    addresses: list[dict[str, Any]] = []
    for link in payload:
        if not isinstance(link, dict) or not isinstance(link.get("addr_info", []), list):
            raise ValueError("ip address JSON contains an invalid link record")
        for entry in link.get("addr_info", []):
            if not isinstance(entry, dict):
                raise ValueError("ip address JSON contains an invalid address record")
            addresses.append(
                {
                    "family": entry.get("family"),
                    "local": entry.get("local"),
                    "prefixlen": entry.get("prefixlen"),
                    "scope": entry.get("scope"),
                }
            )
    return addresses


def collect_ip_address_evidence(name: str, ip_path: str | None) -> dict[str, Any]:
    if not ip_path:
        return {"status": "TOOL_UNAVAILABLE", "addresses": [], "command": None}
    command = run([ip_path, "-j", "address", "show", "dev", name])
    if command.get("rc") != 0:
        return {"status": "COMMAND_FAILED", "addresses": [], "command": command}
    try:
        addresses = parse_ip_address_payload(str(command.get("stdout") or ""))
    except (json.JSONDecodeError, TypeError, ValueError):
        return {"status": "PARSE_FAILED", "addresses": [], "command": command}
    return {"status": "COMPLETE", "addresses": addresses, "command": command}


def parse_ip_link_payload(text: str, name: str) -> dict[str, Any]:
    payload = json.loads(text or "[]")
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise ValueError("ip link JSON must contain exactly one link record")
    link = payload[0]
    linkinfo = link.get("linkinfo") if isinstance(link.get("linkinfo"), dict) else {}
    info_kind = str(linkinfo.get("info_kind") or "").strip().lower()
    info_data = linkinfo.get("info_data") if isinstance(linkinfo.get("info_data"), dict) else {}
    if info_kind == "vlan":
        try:
            vlan_id = int(info_data.get("id"))
        except (TypeError, ValueError) as exc:
            raise ValueError("VLAN interface has no valid VLAN ID") from exc
        vlan_protocol = str(info_data.get("protocol") or "802.1Q")
        vlan_kind = "TAGGED"
    else:
        vlan_id = 0
        vlan_protocol = "untagged"
        vlan_kind = "UNTAGGED"
    return {
        "name": str(link.get("ifname") or name),
        "parent": link.get("link"),
        "link_kind": info_kind or None,
        "vlan_id": vlan_id,
        "vlan_protocol": vlan_protocol,
        "vlan_kind": vlan_kind,
    }


def collect_ip_link_evidence(name: str, ip_path: str | None) -> dict[str, Any]:
    if not ip_path:
        return {"status": "TOOL_UNAVAILABLE", "details": None, "command": None}
    command = run([ip_path, "-d", "-j", "link", "show", "dev", name])
    if command.get("rc") != 0:
        return {"status": "COMMAND_FAILED", "details": None, "command": command}
    try:
        details = parse_ip_link_payload(str(command.get("stdout") or ""), name)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {"status": "PARSE_FAILED", "details": None, "command": command}
    return {"status": "COMPLETE", "details": details, "command": command}


def _interface_leaf_topology(name: str) -> dict[str, Any]:
    root = pathlib.Path("/sys/class/net")
    paths: list[list[str]] = []
    missing: set[str] = set()
    cycles: list[list[str]] = []

    def walk(current: str, path: list[str]) -> None:
        if current in path:
            cycles.append(path[path.index(current):] + [current])
            return
        base = root / current
        if not base.exists():
            missing.add(current)
            return
        relationships = _interface_relationships(base)
        children = sorted(
            set(relationships["lower_interfaces"] + relationships["bond_slaves"])
        )
        if not children:
            paths.append(path + [current])
            return
        for child in children:
            walk(child, path + [current])

    walk(name, [])
    leaves = sorted({path[-1] for path in paths if path})
    leaf_evidence: dict[str, Any] = {}
    for leaf in leaves:
        base = root / leaf
        operstate = sys_value(base / "operstate")
        carrier = sys_value(base / "carrier")
        leaf_evidence[leaf] = {
            "local_link_status": _local_link_status(operstate, carrier),
            "operstate": operstate,
            "carrier": carrier,
            "mtu": sys_value(base / "mtu"),
            "speed_mbps": sys_value(base / "speed"),
            "bond_slave": _bond_slave_attributes(base),
        }
    return {
        "status": "COMPLETE" if not missing and not cycles else "PARTIAL",
        "paths": paths,
        "leaf_interfaces": leaves,
        "missing_interfaces": sorted(missing),
        "cycles": cycles,
        "leaf_evidence": leaf_evidence,
    }


def _dcb_leaf_interfaces(name: str) -> list[str]:
    topology = _interface_leaf_topology(name)
    return topology["leaf_interfaces"] or [name]

MAX_RDMA_PROVIDER_CONFIG_FILES = 64
MAX_RDMA_PROVIDER_CONFIG_BYTES = 4096
MAX_RDMA_LIBRARY_DIRS = 48
MAX_RDMA_LIBRARY_PATHS = 256


def parse_ibv_devices(text: str) -> list[str]:
    """Return device names from the stable, whitespace-delimited ibv_devices table."""
    devices: set[str] = set()
    for line in text.splitlines():
        fields = line.strip().split()
        if not fields:
            continue
        name = fields[0]
        if name.lower() in {"device", "------"} or set(name) == {"-"}:
            continue
        if re.fullmatch(r"[A-Za-z0-9_.:-]+", name):
            devices.add(name)
    return sorted(devices)


def _read_limited_text(path: pathlib.Path, limit: int) -> dict[str, Any]:
    """Read at most limit+1 characters; never load an unbounded provider file."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            value = handle.read(limit + 1)
    except (OSError, UnicodeError) as exc:
        return {"error": f"{type(exc).__name__}: {exc}"[:512]}
    return {
        "content": value[:limit].strip(),
        "truncated": len(value) > limit,
    }


def _collect_rdma_provider_configs() -> dict[str, Any]:
    directories = ("/etc/libibverbs.d", "/usr/share/libibverbs.d")
    files: list[dict[str, Any]] = []
    directory_status: list[dict[str, Any]] = []
    truncated = False
    for directory_name in directories:
        directory = pathlib.Path(directory_name)
        if not directory.is_dir():
            directory_status.append({"path": directory_name, "status": "NOT_FOUND"})
            continue
        try:
            candidates = sorted(directory.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            directory_status.append(
                {
                    "path": directory_name,
                    "status": "READ_FAILED",
                    "error": f"{type(exc).__name__}: {exc}"[:512],
                }
            )
            continue
        directory_status.append({"path": directory_name, "status": "COLLECTED"})
        for candidate in candidates:
            if len(files) >= MAX_RDMA_PROVIDER_CONFIG_FILES:
                truncated = True
                break
            if candidate.name.startswith("."):
                continue
            try:
                is_config = candidate.is_file() or candidate.is_symlink()
            except OSError:
                is_config = False
            if not is_config:
                continue
            evidence = _read_limited_text(candidate, MAX_RDMA_PROVIDER_CONFIG_BYTES)
            files.append(
                {
                    "path": str(candidate),
                    "realpath": os.path.realpath(str(candidate)),
                    **evidence,
                }
            )
        if truncated:
            break
    return {
        "directories": directory_status,
        "files": files,
        "truncated": truncated,
        "limits": {
            "max_files": MAX_RDMA_PROVIDER_CONFIG_FILES,
            "max_bytes_per_file": MAX_RDMA_PROVIDER_CONFIG_BYTES,
        },
    }


def _rdma_library_kind(name: str) -> str:
    lowered = name.lower()
    if lowered.startswith(("librccl-net", "libnccl-net")):
        return "RCCL_NET_PLUGIN"
    if lowered.startswith("libibverbs"):
        return "LIBIBVERBS"
    return "VERBS_PROVIDER"


def _collect_rdma_userspace_libraries() -> dict[str, Any]:
    standard_directories = [
        "/usr/lib64",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib",
        "/lib64",
        "/lib/x86_64-linux-gnu",
        "/lib",
        "/opt/dtk/lib",
        "/opt/dtk/lib64",
        "/opt/rocm/lib",
        "/opt/rocm/lib64",
        "/opt/hyhal/lib",
        "/opt/ucx/lib",
    ]
    standard_directories.extend(sorted(glob.glob("/usr/lib/*-linux-gnu"))[:8])
    explicit_directories: list[str] = []
    ignored_ld_library_path_entries: list[str] = []
    ld_library_path_truncated = False
    for index, raw_entry in enumerate(
        os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep)
    ):
        if index >= 64:
            ld_library_path_truncated = True
            break
        entry = raw_entry.strip()
        if not entry:
            continue
        if not os.path.isabs(entry):
            if len(ignored_ld_library_path_entries) < 32:
                ignored_ld_library_path_entries.append(entry[:256])
            continue
        real_entry = os.path.realpath(entry)
        if real_entry not in explicit_directories:
            explicit_directories.append(real_entry)
        if len(explicit_directories) >= 32:
            ld_library_path_truncated = True
            break

    sources: list[tuple[str, str]] = []
    for directory in standard_directories:
        sources.append((directory, "STANDARD"))
    for directory in explicit_directories:
        sources.append((directory, "LD_LIBRARY_PATH"))
    expanded_sources: list[tuple[str, str]] = []
    seen_directories: set[str] = set()
    for directory, source in sources:
        for candidate in (directory, os.path.join(directory, "libibverbs")):
            normalized = os.path.realpath(candidate)
            if normalized in seen_directories:
                continue
            seen_directories.add(normalized)
            expanded_sources.append((normalized, source))
            if len(expanded_sources) >= MAX_RDMA_LIBRARY_DIRS:
                break
        if len(expanded_sources) >= MAX_RDMA_LIBRARY_DIRS:
            break

    patterns = (
        "libibverbs.so*",
        "lib*-rdmav*.so*",
        "libmlx5.so*",
        "libshca.so*",
        "libhns.so*",
        "libbnxt_re.so*",
        "libirdma.so*",
        "libefa.so*",
        "librccl-net*.so*",
        "libnccl-net*.so*",
    )
    libraries: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    truncated = False
    for directory, source in expanded_sources:
        if not os.path.isdir(directory):
            continue
        for pattern in patterns:
            for path in sorted(glob.glob(os.path.join(directory, pattern))):
                if path in seen_paths:
                    continue
                seen_paths.add(path)
                try:
                    size_bytes = os.stat(path).st_size
                except OSError:
                    size_bytes = None
                libraries.append(
                    {
                        "kind": _rdma_library_kind(os.path.basename(path)),
                        "path": path,
                        "realpath": os.path.realpath(path),
                        "directory_source": source,
                        "size_bytes": size_bytes,
                    }
                )
                if len(libraries) >= MAX_RDMA_LIBRARY_PATHS:
                    truncated = True
                    break
            if truncated:
                break
        if truncated:
            break
    return {
        "search_directories": [
            {"path": path, "source": source}
            for path, source in expanded_sources
            if os.path.isdir(path)
        ],
        "libraries": libraries,
        "explicit_ld_library_path_directories": explicit_directories,
        "ignored_ld_library_path_entries": ignored_ld_library_path_entries,
        "ld_library_path_truncated": ld_library_path_truncated,
        "truncated": truncated,
        "limits": {
            "max_directories": MAX_RDMA_LIBRARY_DIRS,
            "max_paths": MAX_RDMA_LIBRARY_PATHS,
        },
    }


def collect_rdma_userspace_evidence(
    rdma_devices: list[dict[str, Any]],
    *,
    ibv_devices_path: str | None = None,
    ibv_devinfo_path: str | None = None,
) -> dict[str, Any]:
    """Collect bounded, read-only evidence for the userspace verbs stack."""
    if ibv_devices_path is None:
        ibv_devices_path = shutil.which("ibv_devices")
    if ibv_devinfo_path is None:
        ibv_devinfo_path = shutil.which("ibv_devinfo")
    devices_result = (
        run([ibv_devices_path], timeout=5, limit=64 * 1024)
        if ibv_devices_path
        else None
    )
    enumerated_devices = (
        parse_ibv_devices(devices_result.get("stdout", ""))
        if devices_result and devices_result.get("rc") == 0
        else []
    )
    return {
        "target_sysfs_devices": sorted(
            str(item.get("name")) for item in rdma_devices if item.get("name")
        ),
        "ibv_devices": {
            "tool_path": ibv_devices_path,
            "command": devices_result,
            "enumerated_devices": enumerated_devices,
        },
        "ibv_devinfo_tool_path": ibv_devinfo_path,
        "provider_configs": _collect_rdma_provider_configs(),
        "libraries": _collect_rdma_userspace_libraries(),
    }

def collect_network() -> dict[str, Any]:
    lspci_path = shutil.which("lspci")
    ethtool_path = shutil.which("ethtool")
    ibv_devinfo_path = shutil.which("ibv_devinfo")
    ibv_devices_path = shutil.which("ibv_devices")
    dcb_path = shutil.which("dcb")
    ip_path = shutil.which("ip")
    lspci_result = run([lspci_path, "-Dmmnn"], limit=256 * 1024) if lspci_path else None
    pci_labels = (
        parse_lspci_machine_readable(lspci_result.get("stdout", ""))
        if lspci_result and lspci_result.get("rc") == 0
        else {}
    )
    lspci_detail_result = run([lspci_path, "-Dnnk"], limit=256 * 1024) if lspci_path else None
    pci_device_names = (
        parse_lspci_device_names(lspci_detail_result.get("stdout", ""))
        if lspci_detail_result and lspci_detail_result.get("rc") == 0
        else {}
    )
    rdma_hardware_models: dict[str, str] = {}
    for rdma_base in sorted(pathlib.Path("/sys/class/infiniband").glob("*")):
        try:
            rdma_bdf = (rdma_base / "device").resolve().name.lower()
        except OSError:
            continue
        hardware_model = sys_value(rdma_base / "hca_type")
        if hardware_model:
            rdma_hardware_models[rdma_bdf] = hardware_model
    interfaces = []
    for base in sorted(pathlib.Path("/sys/class/net").glob("*")):
        device_link = base / "device"
        if not device_link.exists():
            continue
        try:
            device_path = device_link.resolve()
        except OSError:
            device_path = device_link
        try:
            driver = (device_link / "driver").resolve().name
        except OSError:
            driver = None
        operstate = sys_value(base / "operstate")
        carrier = sys_value(base / "carrier")
        driver_info: dict[str, str] = {}
        if ethtool_path:
            ethtool_result = run([ethtool_path, "-i", base.name], limit=4096)
            if ethtool_result.get("rc") == 0:
                driver_info = parse_key_value_lines(ethtool_result.get("stdout", ""))
        interface = {
            "name": base.name,
            "operstate": operstate,
            "carrier": carrier,
            "local_link_status": _local_link_status(operstate, carrier),
            "mtu": sys_value(base / "mtu"),
            "speed_mbps": sys_value(base / "speed"),
            "duplex": sys_value(base / "duplex"),
            "pci_bdf": device_path.name,
            "pci_vendor": sys_value(device_link / "vendor"),
            "pci_device": sys_value(device_link / "device"),
            "pci_subsystem_vendor": sys_value(device_link / "subsystem_vendor"),
            "pci_subsystem_device": sys_value(device_link / "subsystem_device"),
            "driver": driver or driver_info.get("driver"),
            "driver_version": (
                sys_value(pathlib.Path("/sys/module") / driver / "version") if driver else None
            )
            or driver_info.get("version"),
            "firmware_version": driver_info.get("firmware_version"),
            "hardware_model": (
                rdma_hardware_models.get(device_path.name.lower())
                or pci_device_names.get(device_path.name.lower())
            ),
            "numa_node": sys_value(device_link / "numa_node"),
        }
        interface.update(_interface_relationships(base))
        interface.update(pci_labels.get(device_path.name.lower(), {}))
        interfaces.append(interface)
    rdma_devices = []
    rdma_port_records: list[tuple[pathlib.Path, dict[str, Any]]] = []
    for base in sorted(pathlib.Path("/sys/class/infiniband").glob("*")):
        rdma_device_link = base / "device"
        item: dict[str, Any] = {
            "name": base.name,
            "node_guid": sys_value(base / "node_guid"),
            "node_type": sys_value(base / "node_type"),
            "firmware_version": sys_value(base / "fw_ver"),
            "hardware_model": sys_value(base / "hca_type"),
            "board_id": sys_value(base / "board_id"),
            "pci_vendor": sys_value(rdma_device_link / "vendor"),
            "pci_device": sys_value(rdma_device_link / "device"),
            "pci_subsystem_vendor": sys_value(rdma_device_link / "subsystem_vendor"),
            "pci_subsystem_device": sys_value(rdma_device_link / "subsystem_device"),
            "netdevs": sorted(path.name for path in (rdma_device_link / "net").glob("*")),
            "ports": [],
        }
        try:
            item["pci_bdf"] = rdma_device_link.resolve().name
        except OSError:
            item["pci_bdf"] = None
        try:
            item["driver"] = (rdma_device_link / "driver").resolve().name
        except OSError:
            item["driver"] = None
        item["driver_version"] = (
            sys_value(pathlib.Path("/sys/module") / item["driver"] / "version")
            if item.get("driver")
            else None
        )
        item["numa_node"] = sys_value(rdma_device_link / "numa_node")
        if item.get("pci_bdf"):
            item.update(pci_labels.get(str(item["pci_bdf"]).lower(), {}))
            item["hardware_model"] = (
                item.get("hardware_model")
                or pci_device_names.get(str(item["pci_bdf"]).lower())
            )
        ibv_result = (
            run([ibv_devinfo_path, "-d", base.name], timeout=10, limit=64 * 1024)
            if ibv_devinfo_path
            else None
        )
        ibv_ports = (
            parse_ibv_devinfo_ports(ibv_result.get("stdout", ""))
            if ibv_result and ibv_result.get("rc") == 0
            else {}
        )
        item["ibv_devinfo"] = ibv_result
        for port in sorted((base / "ports").glob("*")):
            fallback = ibv_ports.get(port.name, {})
            (
                gids,
                gid_collection_status,
                gid_type_collection_status,
                gid_ndev_collection_status,
            ) = collect_gid_table_evidence(port)
            pkeys, pkey_collection_status = collect_pkey_table_evidence(port)
            subnet_prefix = sys_value(port / "subnet_prefix") or gid_subnet_prefix(gids)
            port_payload = {
                    "port": port.name,
                    "state": sys_value(port / "state") or fallback.get("state"),
                    "phys_state": sys_value(port / "phys_state"),
                    "rate": sys_value(port / "rate"),
                    "link_layer": sys_value(port / "link_layer") or fallback.get("link_layer"),
                    "max_mtu": sys_value(port / "max_mtu") or fallback.get("max_mtu"),
                    "active_mtu": sys_value(port / "active_mtu") or fallback.get("active_mtu"),
                    "lid": sys_value(port / "lid") or fallback.get("lid"),
                    "sm_lid": sys_value(port / "sm_lid") or fallback.get("sm_lid"),
                    "lmc": sys_value(port / "lmc") or fallback.get("lmc"),
                    "sm_sl": sys_value(port / "sm_sl"),
                    "subnet_prefix": subnet_prefix,
                    "gids": gids,
                    "gid_collection_status": gid_collection_status,
                    "gid_type_collection_status": gid_type_collection_status,
                    "gid_ndev_collection_status": gid_ndev_collection_status,
                    "pkeys": pkeys,
                    "pkey_collection_status": pkey_collection_status,
                }
            item["ports"].append(port_payload)
            rdma_port_records.append((port, port_payload))
        rdma_devices.append(item)

    rdma_counter_sampling = _collect_ib_counter_windows(rdma_port_records)

    roce_netdevs: set[str] = set()
    roce_candidate_netdevs: set[str] = set()
    for device in rdma_devices:
        ethernet_ports = [
            port
            for port in device.get("ports", [])
            if str(port.get("link_layer") or "").strip().lower() == "ethernet"
        ]
        if not ethernet_ports:
            continue
        roce_candidate_netdevs.update(str(name) for name in device.get("netdevs", []))
        for port in ethernet_ports:
            for entry in port.get("gids") or []:
                gid_type = str(entry.get("type") or "").lower()
                if _is_nonzero_gid(entry.get("gid")) and "roce" in gid_type and entry.get("netdev"):
                    roce_netdevs.add(str(entry["netdev"]))
    roce_candidate_netdevs.update(roce_netdevs)

    interfaces_by_name = {str(item.get("name")): item for item in interfaces}
    pending_interfaces = list(sorted(roce_candidate_netdevs))
    visited_interfaces: set[str] = set()
    while pending_interfaces:
        name = pending_interfaces.pop(0)
        if name in visited_interfaces:
            continue
        visited_interfaces.add(name)
        base = pathlib.Path("/sys/class/net") / name
        if not base.exists():
            continue
        if name not in interfaces_by_name:
            operstate = sys_value(base / "operstate")
            carrier = sys_value(base / "carrier")
            logical_interface = {
                "name": name,
                "operstate": operstate,
                "carrier": carrier,
                "local_link_status": _local_link_status(operstate, carrier),
                "mtu": sys_value(base / "mtu"),
                "speed_mbps": sys_value(base / "speed"),
                "duplex": sys_value(base / "duplex"),
                "pci_bdf": None,
                "pci_vendor": None,
                "pci_device": None,
                "pci_subsystem_vendor": None,
                "pci_subsystem_device": None,
                "driver": None,
                "driver_version": None,
                "firmware_version": None,
                "hardware_model": None,
                "numa_node": None,
                "logical_interface": True,
            }
            logical_interface.update(_interface_relationships(base))
            interfaces.append(logical_interface)
            interfaces_by_name[name] = logical_interface
        relationships = _interface_relationships(base)
        children = sorted(
            set(relationships["lower_interfaces"] + relationships["bond_slaves"])
        )
        pending_interfaces.extend(
            child for child in children if child not in visited_interfaces
        )

    for interface in interfaces:
        if interface.get("name") not in roce_candidate_netdevs:
            continue
        name = str(interface["name"])
        address_evidence = collect_ip_address_evidence(name, ip_path)
        link_evidence = collect_ip_link_evidence(name, ip_path)
        link_details = link_evidence.get("details") or {}
        if link_evidence["status"] == "COMPLETE":
            interface.update(
                {
                    "vlan_id": link_details.get("vlan_id"),
                    "vlan_protocol": link_details.get("vlan_protocol"),
                    "vlan_kind": link_details.get("vlan_kind"),
                    "link_kind": link_details.get("link_kind"),
                }
            )
            parent = link_details.get("parent")
            if parent and parent not in interface.get("lower_interfaces", []):
                interface["lower_interfaces"] = sorted(
                    set(interface.get("lower_interfaces", [])) | {str(parent)}
                )
        topology = _interface_leaf_topology(name)
        targets = topology["leaf_interfaces"] or [name]
        dcb_targets = (
            {
                target: {
                    section: run(
                        [dcb_path, section, "show", "dev", target],
                        limit=16 * 1024,
                    )
                    for section in ("pfc", "ets", "app", "buffer", "dcbx")
                }
                for target in targets
            }
            if dcb_path
            else {}
        )
        pause_targets = {
            target: run([ethtool_path, "--show-pause", target], limit=4096)
            for target in targets
        } if ethtool_path else {}
        fec_targets = {
            target: run([ethtool_path, "--show-fec", target], limit=4096)
            for target in targets
        } if ethtool_path else {}
        interface["roce_configuration"] = {
            "ip_addresses": address_evidence["addresses"],
            "ip_address_collection_status": address_evidence["status"],
            "ip_address_command": address_evidence["command"],
            "vlan_id": link_details.get("vlan_id"),
            "vlan_protocol": link_details.get("vlan_protocol"),
            "vlan_kind": link_details.get("vlan_kind"),
            "vlan_collection_status": link_evidence["status"],
            "ip_link_command": link_evidence["command"],
            "topology": topology,
            "dcb_targets": dcb_targets,
            "dcb_target_collection_status": (
                "COMPLETE" if dcb_path else "TOOL_UNAVAILABLE"
            ),
            "pause_targets": pause_targets,
            "pause_target_collection_status": (
                "COMPLETE" if ethtool_path else "TOOL_UNAVAILABLE"
            ),
            "fec_targets": fec_targets,
            "fec_target_collection_status": (
                "COMPLETE" if ethtool_path else "TOOL_UNAVAILABLE"
            ),
            # Preserve the original single-netdev fields for old JSON consumers.
            "pause": pause_targets.get(name),
            "fec": fec_targets.get(name),
        }
    rdma_userspace = collect_rdma_userspace_evidence(
        rdma_devices,
        ibv_devices_path=ibv_devices_path,
        ibv_devinfo_path=ibv_devinfo_path,
    )
    return {
        "interfaces": interfaces,
        "rdma_devices": rdma_devices,
        "rdma_userspace": rdma_userspace,
        "rdma_counter_sampling": rdma_counter_sampling,
        "roce_candidate_netdevs": sorted(roce_candidate_netdevs),
        "pci_name_source": "lspci-pci.ids" if pci_labels else "numeric-sysfs-only",
        "lspci": lspci_result,
        "lspci_detail": lspci_detail_result,
    }


def collect_python() -> tuple[dict[str, Any], dict[str, Any]]:
    default_package_names = [
        "torch",
        "torchvision",
        "torchaudio",
        "triton",
        "flash-attn",
        "deepspeed",
        "transformers",
        "accelerate",
        "megatron-core",
        "mpi4py",
        "ucx-py",
        "numpy",
        "hcusmi",
    ]
    required_setting = os.environ.get("HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES")
    if required_setting is None:
        # Preserve the historic K8s behavior. Bare-metal always supplies an
        # explicit list, including an empty list when no package was requested.
        required_package_names = ["torch"]
    else:
        try:
            parsed_required = json.loads(required_setting)
        except (TypeError, ValueError):
            parsed_required = []
        required_package_names = (
            [str(name) for name in parsed_required]
            if isinstance(parsed_required, list)
            else []
        )
    canonical_required = {
        re.sub(r"[-_.]+", "-", name).lower()
        for name in required_package_names
    }
    package_names = list(dict.fromkeys(default_package_names + required_package_names))
    packages: dict[str, str] = {}
    for name in package_names:
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            pass
    torch_info: dict[str, Any] = {
        "importable": None,
        "check_status": "NOT_REQUESTED",
    }
    if "torch" in canonical_required:
        try:
            import torch

            torch_info = {
                "importable": True,
                "check_status": "CHECKED",
                "version": getattr(torch, "__version__", None),
                "hip_version": getattr(getattr(torch, "version", None), "hip", None),
                "cuda_version_field": getattr(getattr(torch, "version", None), "cuda", None),
                "module_path": getattr(torch, "__file__", None),
            }
            try:
                torch_info["hcu_available"] = bool(torch.cuda.is_available())
                torch_info["device_count"] = int(torch.cuda.device_count())
            except Exception as exc:
                torch_info["hcu_available"] = None
                torch_info["device_count"] = None
                torch_info["device_query_error"] = f"{type(exc).__name__}: {exc}"[:2048]
            try:
                torch_info["distributed_available"] = bool(torch.distributed.is_available())
                torch_info["distributed_nccl_available"] = bool(torch.distributed.is_nccl_available())
            except Exception as exc:
                torch_info["distributed_query_error"] = f"{type(exc).__name__}: {exc}"[:2048]
            try:
                torch_info["nccl_version"] = torch.cuda.nccl.version()
            except Exception as exc:
                torch_info["nccl_version_error"] = f"{type(exc).__name__}: {exc}"[:2048]
        except Exception as exc:
            torch_info = {
                "importable": False,
                "check_status": "CHECKED",
                "error_type": type(exc).__name__,
                "error": str(exc)[:2048],
            }
    python_info = {
        "version": platform.python_version(),
        "executable": os.path.realpath(os.sys.executable),
        "packages": packages,
    }
    return python_info, torch_info


def _is_hcu_hip_runtime_library(path: str) -> bool:
    return os.path.basename(path).lower().startswith("libamdhip64.so")


def _public_library_inventory(paths: list[str]) -> dict[str, Any]:
    visible_paths: list[str] = []
    resolved: dict[str, str] = {}
    hcu_hip_runtime_detected = False
    for path in paths:
        realpath = os.path.realpath(path)
        if _is_hcu_hip_runtime_library(path) or _is_hcu_hip_runtime_library(realpath):
            hcu_hip_runtime_detected = True
            continue
        visible_paths.append(path)
        resolved[path] = realpath
    return {
        "paths": visible_paths,
        "resolved": resolved,
        "hcu_hip_runtime": {
            "component": "HCU HIP runtime",
            "detected": hcu_hip_runtime_detected,
        },
    }


def collect_libraries() -> dict[str, Any]:
    discovered_paths = sorted(
        {
            path
            for pattern in (
                "/opt/dtk/lib/librccl.so*",
                "/opt/dtk/lib/libnccl.so*",
                "/opt/dtk/lib/libamdhip64.so*",
                "/opt/dtk/.hyhal/lib/libhsa-runtime64.so*",
                "/opt/ucx/lib/libucp.so*",
                "/opt/ucx/lib/libuct.so*",
                "/opt/ucx/lib/libucs.so*",
            )
            for path in glob.glob(pattern)
        }
    )
    return _public_library_inventory(discovered_paths)


def main() -> None:
    python_info, torch_info = collect_python()
    payload = {
        "schema_version": "1.0",
        "system": collect_system(),
        "dtk": {
            "version_file": first_existing(
                [
                    "/opt/dtk/.dtk_version",
                    "/opt/dtk/.info/version",
                    "/opt/rocm/.info/version",
                ]
            ),
            "component_versions": {
                path: read(path)
                for path in (
                    "/opt/dtk/.info/rocm_version",
                    "/opt/dtk/.info/version-dev",
                    "/opt/dtk/.info/version-libs",
                    "/opt/dtk/.info/version-utils",
                )
                if read(path)
            },
            "tools": collect_tools(),
        },
        "driver": collect_driver(),
        "network": collect_network(),
        "libraries": collect_libraries(),
        "python": python_info,
        "torch": torch_info,
        "runtime_env": {
            name: os.environ.get(name)
            for name in (
                "PATH",
                "LD_LIBRARY_PATH",
                "PYTHONPATH",
                "ROCM_PATH",
                "HIP_PATH",
                "ROCR_VISIBLE_DEVICES",
                "HIP_VISIBLE_DEVICES",
                "CUDA_VISIBLE_DEVICES",
            )
            if os.environ.get(name) is not None
        },
    }
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
