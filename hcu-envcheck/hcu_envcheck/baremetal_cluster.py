# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
import re
import shlex
import uuid
import zlib
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from . import __version__
from .baremetal import (
    BaremetalClusterExecutor,
    BaremetalExecutionConfig,
    BaremetalNodeResult,
)
from .cluster_checks import ClusterExtraCheckConfig, run_cluster_extra_checks
from .conda_mode import (
    CondaStorageObservation,
    EnvironmentMode,
    EnvironmentSelection,
    plan_conda_collection,
    validate_environment_selection,
)
from .environment import (
    STATIC_DEVICE_FIELDS, STATIC_ENVIRONMENT_FIELDS, evaluate_environment,
    static_environment_view, static_configuration_sections, canonical_static_value,
)
from .models import Finding
from .output import atomic_write_text_exclusive, claim_labeled_run_directory
from .parsers import ParseError, parse_hy_smi_samples, parse_rocminfo
from .preflight import evaluate_metrics
from .roce_health import normalize_roce_policy


_NATURAL_PART_RE = re.compile(r"(\d+)")
_SOFTWARE_CHECK_IDS = {
    "RCCL_LIBRARY",
    "UCX",
    "TORCH_IMPORT",
    "TORCH_HIP_BUILD",
    "TORCH_HCU_AVAILABLE",
    "TORCH_DEVICE_COUNT",
}
_SOFTWARE_REASON_CODES = {
    "RCCL_LIBRARY_NOT_FOUND",
    "UCX_NOT_AVAILABLE",
    "TORCH_IMPORT_FAILED",
    "TORCH_NATIVE_DEPENDENCY_MISSING",
    "HCUSMI_LIBRARY_ABI_MISMATCH",
    "TORCH_NOT_HIP_BUILD",
    "TORCH_HCU_UNAVAILABLE",
    "TORCH_DEVICE_COUNT_MISMATCH",
}
_CONSISTENCY_FIELDS = (
    "dtk_version",
    "driver_version",
    "vbios_versions",
    "hsw_firmware_versions",
    "nic_hardware_profile",
    "rdma_hardware_profile",
    "rdma_current_protocol",
    "rdma_fabric_profile",
    "rdma_device_count",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _natural_key(value: str) -> list[tuple[int, Any]]:
    return [
        (1, int(part)) if part.isdigit() else (0, part.lower())
        for part in _NATURAL_PART_RE.split(value)
        if part
    ]


def _json_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _write_json(path: Path, payload: Any) -> None:
    atomic_write_text_exclusive(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


def _write_text(path: Path, text: str) -> None:
    atomic_write_text_exclusive(path, text)


@dataclass(frozen=True)
class BaremetalPreflightPolicy:
    expected_devices: int | None = None
    max_vram_used_percent: float = 5.0
    max_hcu_util_percent: float = 5.0
    samples: int = 3
    busy_sample_quorum: int = 2
    sample_interval_seconds: float = 1.0
    software_mode: str = "host-python"
    conda_prefix: str | None = None
    conda_storage: str | None = None
    docker_image: str | None = None
    container_python: str = "python3"
    required_python_packages: tuple[str, ...] = ()
    require_compiler: bool = False
    require_rdma: bool = False
    minimum_rdma_devices: int = 0
    expected_rdma_protocol: str = "auto"
    require_rccl: bool = False
    require_ucx: bool = False
    strict_hardware_consistency: bool = False
    # An acceptance target is optional.  The runner's ability to process a
    # ten-thousand-device inventory is not a target for each individual run.
    target_scale_devices: int | None = None
    rdma_policy: dict[str, Any] | None = None
    rdma_counter_interval_seconds: int = 5
    env_script: str | None = None
    execution_scope: str = "host"
    container_name: str | None = None
    container_shell: str = "bash"
    check_categories: tuple[str, ...] = ("platform", "resource")
    container_workdir: str | None = None
    run_token: str | None = None

    def validate(self) -> None:
        if not self.check_categories or set(self.check_categories) - {"platform", "resource"}:
            raise ValueError("check_categories must contain platform and/or resource")
        if self.expected_devices is not None and self.expected_devices < 1:
            raise ValueError("expected_devices must be at least 1")
        if self.samples < 1:
            raise ValueError("samples must be at least 1")
        if not 1 <= self.busy_sample_quorum <= self.samples:
            raise ValueError("busy_sample_quorum must be between 1 and samples")
        if self.sample_interval_seconds < 0:
            raise ValueError("sample_interval_seconds cannot be negative")
        if self.minimum_rdma_devices < 0:
            raise ValueError("minimum_rdma_devices cannot be negative")
        if self.expected_rdma_protocol not in {"auto", "ib", "roce"}:
            raise ValueError("expected_rdma_protocol must be auto, ib, or roce")
        if self.target_scale_devices is not None and self.target_scale_devices < 1:
            raise ValueError("target_scale_devices must be at least 1")
        if self.rdma_counter_interval_seconds != 0 and not 1 <= self.rdma_counter_interval_seconds <= 60:
            raise ValueError(
                "rdma_counter_interval_seconds must be 0 or between 1 and 60"
            )
        if self.execution_scope not in {"host", "container"}:
            raise ValueError("execution_scope must be host or container")
        if self.execution_scope == "container" and not self.container_name:
            raise ValueError(
                "container_name is required when execution_scope=container"
            )
        if self.env_script is not None:
            if not str(self.env_script).strip() or "\x00" in self.env_script:
                raise ValueError("env_script must be a non-empty safe path")
        if self.container_name is not None and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", self.container_name
        ):
            raise ValueError("container_name contains unsafe characters")
        if self.container_shell not in {"bash", "sh"}:
            raise ValueError("container_shell must be bash or sh")
        if self.container_workdir is not None:
            if not self.container_workdir.startswith("/") or any(ord(ch) < 32 for ch in self.container_workdir):
                raise ValueError("container_workdir must be an absolute container path without control characters")
        if self.run_token is not None and (
            not isinstance(self.run_token, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{15,95}", self.run_token)
        ):
            raise ValueError("run_token must be 16..96 letters, digits, underscores or hyphens")
        if isinstance(self.required_python_packages, (str, bytes)):
            raise ValueError("required_python_packages must be an argument sequence")
        for package_name in self.required_python_packages:
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", package_name):
                raise ValueError(
                    f"unsafe required Python package name: {package_name!r}"
                )
        if self.rdma_policy is not None:
            normalize_roce_policy(self.rdma_policy)
            if self.expected_rdma_protocol == "ib":
                raise ValueError("a RoCE policy conflicts with expected_rdma_protocol=ib")
        self.environment_selection()
        if (
            not self.container_python
            or self.container_python.startswith("-")
            or ".." in Path(self.container_python).parts
            or not re.fullmatch(r"[A-Za-z0-9_./-]+", self.container_python)
        ):
            raise ValueError("container_python must be a safe executable path")

    def environment_selection(self) -> EnvironmentSelection:
        return validate_environment_selection(
            env_mode=self.software_mode,
            conda_prefix=self.conda_prefix,
            conda_storage=self.conda_storage,
            image=self.docker_image,
        )


_SOFTWARE_PROBE_SOURCE = r"""
import shutil
import subprocess
import glob
import importlib.metadata
import json
import os
import pathlib
import platform
import re


def read_file(path, limit=65536):
    try:
        value = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")[:limit].strip()
    except (OSError, UnicodeError):
        return None
    return value or None


def capture(argv, timeout=15, limit=4096):
    try:
        completed = subprocess.run(
            argv, text=True, encoding="utf-8", errors="replace",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout, check=False,
        )
        return {
            "rc": completed.returncode,
            "stdout": (completed.stdout or "")[:limit].strip(),
            "stderr": (completed.stderr or "")[:limit].strip(),
        }
    except subprocess.TimeoutExpired as exc:
        return {"rc": 124, "stdout": "", "stderr": str(exc)[:limit], "timed_out": True}
    except OSError as exc:
        return {"rc": 127, "stdout": "", "stderr": str(exc)[:limit]}


def first_executable(name, candidates):
    discovered = shutil.which(name)
    ordered = ([discovered] if discovered else []) + list(candidates)
    for candidate in ordered:
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return os.path.realpath(candidate)
    return None


def is_hcu_hip_runtime_library(path):
    return os.path.basename(path).lower().startswith("libamdhip64.so")


def public_library_inventory(paths):
    visible_paths = []
    resolved = {}
    hcu_hip_runtime_detected = False
    for path in paths:
        realpath = os.path.realpath(path)
        if is_hcu_hip_runtime_library(path) or is_hcu_hip_runtime_library(realpath):
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


default_package_names = (
    "torch", "torchvision", "torchaudio", "triton", "flash-attn", "deepspeed",
    "transformers", "accelerate", "megatron-core", "mpi4py", "ucx-py", "numpy",
    "hcusmi",
)
try:
    required_package_names = json.loads(
        os.environ.get("HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES", "[]")
    )
except (TypeError, ValueError):
    required_package_names = []
if not isinstance(required_package_names, list):
    required_package_names = []
required_package_names = [str(name) for name in required_package_names]
canonical_required = {
    re.sub(r"[-_.]+", "-", name).lower()
    for name in required_package_names
}
package_names = tuple(dict.fromkeys(default_package_names + tuple(required_package_names)))
packages = {}
for package_name in package_names:
    try:
        packages[package_name] = importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        pass
python_info = {
    "version": platform.python_version(),
    "executable": os.path.realpath(os.sys.executable),
    "prefix": os.path.realpath(os.sys.prefix),
    "packages": packages,
}
torch_info = {"importable": None, "check_status": "NOT_REQUESTED"}
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
    except BaseException as exc:
        torch_info = {
            "importable": False,
            "check_status": "CHECKED",
            "error_type": type(exc).__name__,
            "error": str(exc)[:2048],
        }

# The shared collector definitions are prepended by build_remote_probe_command.
# Both host and selected Conda/Docker targets use the same environment contract.
dtk_inventory = collect_dtk()
public_libraries = collect_libraries()

os_release = {}
for line in (read_file("/etc/os-release") or "").splitlines():
    if "=" in line:
        key, value = line.split("=", 1)
        os_release[key] = value.strip().strip('"')

print(json.dumps({
    "schema_version": "1.0",
    "evidence_scope": "SELECTED_TRAINING_TARGET",
    "system": {"os_release": os_release},
    "dtk": dtk_inventory,
    "python": python_info,
    "torch": torch_info,
    "libraries": public_libraries,
    "runtime_env": collect_runtime_env(),
}, ensure_ascii=False, separators=(",", ":")))
"""


def build_remote_probe_command(
    policy: BaremetalPreflightPolicy,
    remote_python: str,
    *,
    env_script: str | None = None,
    execution_scope: str | None = None,
    container_name: str | None = None,
    container_shell: str | None = None,
    container_workdir: str | None = None,
) -> list[str]:
    """Build one compressed, dependency-free Python probe for every target node.

    The embedded inventory is the the shared ``pod_probe.py`` inventory collector.
    Parsing and policy decisions remain on the controller so every node runs
    read-only collection only.
    """

    policy.validate()
    selection = policy.environment_selection()
    probe_token = uuid.uuid4().hex
    probe_source = Path(__file__).with_name("pod_probe.py").read_text(encoding="utf-8")
    # The embedded module must define its collectors without executing its CLI
    # entry point; the wrapper below emits the sole JSON document on stdout.
    probe_source = re.split(
        r"(?m)^if __name__ == [\"']__main__[\"']:\s*$", probe_source, maxsplit=1
    )[0]
    required_packages_json = json.dumps(
        list(policy.required_python_packages),
        ensure_ascii=True,
        separators=(",", ":"),
    )
    software_probe_source = (
        probe_source + "\nimport os as _hcu_os\n"
        f"_hcu_os.environ['HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES'] = "
        f"{required_packages_json!r}\n"
        + _SOFTWARE_PROBE_SOURCE
    )
    wrapper = f"""
import contextlib as _contextlib
import io as _io
import json as _json
import shutil as _shutil
import subprocess as _subprocess
import time as _time

os.environ["HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS"] = str({policy.rdma_counter_interval_seconds})
os.environ["HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES"] = {required_packages_json!r}

_software_mode = {selection.env_mode.value!r}
_software_probe_source = {software_probe_source!r}
if _software_mode != "host-python":
    def _collect_host_without_training_python():
        return ({{
            "version": platform.python_version(),
            "executable": os.path.realpath(os.sys.executable),
            "packages": {{}},
            "check_status": "COLLECTED_SEPARATELY",
        }}, {{
            "importable": None,
            "check_status": "COLLECTED_SEPARATELY",
        }})
    collect_python = _collect_host_without_training_python

_environment_buffer = _io.StringIO()
with _contextlib.redirect_stdout(_environment_buffer):
    main({policy.check_categories!r})
_environment_lines = [line for line in _environment_buffer.getvalue().splitlines() if line.strip()]
_environment = _json.loads(_environment_lines[-1])

def _capture(argv, timeout, limit=4194304):
    if not argv or not argv[0]:
        return {{"rc": 127, "stdout": "", "stderr": "tool not found", "timed_out": False}}
    try:
        completed = _subprocess.run(
            argv,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=_subprocess.PIPE,
            stderr=_subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        return {{
            "rc": completed.returncode,
            "stdout": stdout[:limit],
            "stderr": stderr[:65536],
            "timed_out": False,
            "stdout_truncated": len(stdout) > limit,
        }}
    except _subprocess.TimeoutExpired as exc:
        return {{
            "rc": 124,
            "stdout": (exc.stdout or "")[:limit] if isinstance(exc.stdout, str) else "",
            "stderr": (exc.stderr or "")[:65536] if isinstance(exc.stderr, str) else str(exc),
            "timed_out": True,
        }}
    except OSError as exc:
        return {{"rc": 127, "stdout": "", "stderr": str(exc), "timed_out": False}}


def _last_json(text):
    for line in reversed((text or "").splitlines()):
        try:
            payload = _json.loads(line)
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("schema_version") == "1.0":
            return payload
    return None


def _command_metadata(result):
    stdout = result.get("stdout") or ""
    return {{
        "rc": result.get("rc"),
        "timed_out": bool(result.get("timed_out")),
        "stdout_bytes": len(stdout.encode("utf-8", "replace")),
        "stdout_truncated": bool(result.get("stdout_truncated")),
        "stderr": str(result.get("stderr") or "")[:1024],
    }}


def _apply_software_result(result, target):
    parsed = _last_json(result.get("stdout"))
    if result.get("rc") == 0 and not result.get("stdout_truncated") and parsed:
        inventory = parsed
        target["status"] = "SUCCESS"
    else:
        detail = str(result.get("stderr") or "software probe returned invalid output")[:2048]
        inventory = {{
            "schema_version": "1.0",
            "evidence_scope": "SELECTED_TRAINING_TARGET",
            "dtk": {{"version_file": None, "component_versions": {{}}, "tools": {{}}}},
            "python": {{"version": None, "packages": {{}}}},
            "torch": {{
                "importable": False,
                "error_type": "SoftwareTargetProbeError",
                "error": detail,
            }},
            "libraries": {{"paths": []}},
            "runtime_env": {{}},
        }}
        target["status"] = "ERROR"
        target.setdefault("reason_code", "SOFTWARE_TARGET_PROBE_FAILED")
    target["inventory"] = inventory
    # Preserve historic fields for output compatibility.  Policy evaluation
    # consumes target["inventory"] and never falls back to host DTK evidence.
    _environment["python"] = inventory.get("python", {{}})
    _environment["torch"] = inventory.get("torch", {{}})
    _environment["libraries"] = inventory.get("libraries", {{"paths": []}})
    target["command"] = _command_metadata(result)

_software_target = {{"mode": _software_mode, "status": "SUCCESS"}}
if _software_mode == "conda" and "platform" in {policy.check_categories!r}:
    _conda_prefix = {selection.conda_prefix!r}
    _conda_python = os.path.join(_conda_prefix, "bin", "python")
    _mount_identity = None
    try:
        _mount_lines = pathlib.Path("/proc/self/mountinfo").read_text(
            encoding="utf-8", errors="replace"
        ).splitlines()
    except OSError:
        _mount_lines = []
    for _mount_line in _mount_lines:
        _fields = _mount_line.split()
        try:
            _separator = _fields.index("-")
            _mount_point = _fields[4].replace("\\\\040", " ").replace("\\\\134", "\\\\")
            _fs_type = _fields[_separator + 1].lower()
            _mount_source = _fields[_separator + 2].replace("\\\\040", " ").replace("\\\\134", "\\\\")
        except (ValueError, IndexError):
            continue
        if _conda_prefix == _mount_point or _conda_prefix.startswith(_mount_point.rstrip("/") + "/"):
            if _mount_identity is None or len(_mount_point) > len(_mount_identity[0]):
                _mount_identity = (_mount_point, _fs_type, _mount_source)
    try:
        _prefix_stat = os.stat(_conda_prefix)
        _fingerprint = f"{{_prefix_stat.st_dev}}:{{_prefix_stat.st_ino}}"
    except OSError:
        _fingerprint = None
    _shared_filesystems = {{
        "nfs", "nfs4", "lustre", "gpfs", "beegfs", "ceph", "cephfs",
        "glusterfs", "cifs", "smb3", "panfs", "wekafs", "fuse.sshfs",
    }}
    _observed_fs = _mount_identity[1] if _mount_identity else None
    _software_target["conda_storage_observation"] = {{
        "prefix": _conda_prefix,
        "prefix_exists": os.path.isdir(_conda_prefix),
        "python_executable": os.path.isfile(_conda_python) and os.access(_conda_python, os.X_OK),
        "realpath": os.path.realpath(_conda_prefix) if os.path.exists(_conda_prefix) else None,
        "mount_source": _mount_identity[2] if _mount_identity else None,
        "fs_type": _observed_fs,
        "identity_fingerprint": _fingerprint,
        "collection_status": "SUCCESS",
        "shared_backend": (_observed_fs in _shared_filesystems) if _observed_fs else None,
    }}
    _software_result = _capture([_conda_python, "-c", _software_probe_source], 120)
    _apply_software_result(_software_result, _software_target)
elif _software_mode == "docker" and "platform" in {policy.check_categories!r}:
    _docker = _shutil.which("docker") or "docker"
    _image_inspect = _capture([_docker, "image", "inspect", {selection.image!r}], 30, 65536)
    _software_target.update({{
        "image": {selection.image!r},
        "image_inspect": _command_metadata(_image_inspect),
        "container_id": None,
        "cleanup_status": "NOT_CREATED",
        "cleanup_command": None,
        "runtime_mounts": [],
    }})
    if _image_inspect.get("rc") != 0:
        _software_target["reason_code"] = "DOCKER_IMAGE_NOT_PRESENT"
        _software_result = {{
            "rc": _image_inspect.get("rc"),
            "stdout": "",
            "stderr": "Docker image is not present locally; implicit pull is forbidden: "
                + str(_image_inspect.get("stderr") or "")[:1024],
            "timed_out": bool(_image_inspect.get("timed_out")),
            "stdout_truncated": False,
        }}
        _apply_software_result(_software_result, _software_target)
    else:
        _cidfile = "/tmp/hcu-envcheck-{probe_token}.cid"
        _container_id = None
        _cleanup_status = "NOT_CREATED"
        _cleanup_command = None
        _software_result = {{
            "rc": 125,
            "stdout": "",
            "stderr": "Docker target probe did not start",
            "timed_out": False,
            "stdout_truncated": False,
        }}
        try:
            try:
                pathlib.Path(_cidfile).unlink(missing_ok=True)
            except OSError:
                pass
            _docker_argv = [
                _docker, "run", "--pull=never", "--rm", "--cidfile", _cidfile,
                "--network=none", "--ipc=private", "--read-only", "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=256m",
                "--tmpfs", "/var/log/hylog:rw,nosuid,nodev,size=16m",
                "--env", "HOME=/tmp", "--env", "PYTHONDONTWRITEBYTECODE=1",
            ]
            # The host driver userspace is a runtime dependency, not an image
            # replacement.  Bind only the fixed, known Hygon runtime path and
            # keep it read-only; never grant --privileged or arbitrary mounts.
            if os.path.isdir("/opt/hyhal"):
                _docker_argv.extend([
                    "--mount", "type=bind,src=/opt/hyhal,dst=/opt/hyhal,readonly",
                ])
                _software_target["runtime_mounts"].append({{
                    "source": "/opt/hyhal",
                    "destination": "/opt/hyhal",
                    "read_only": True,
                    "purpose": "HOST_DRIVER_USERSPACE",
                }})
            _device_paths = ["/dev/kfd", "/dev/mkfd"]
            _device_paths.extend(sorted(glob.glob("/dev/dri/renderD*")))
            _device_paths.extend(sorted(glob.glob("/dev/dri/card*")))
            for _device_path in dict.fromkeys(_device_paths):
                if os.path.exists(_device_path):
                    _docker_argv.extend(["--device", f"{{_device_path}}:{{_device_path}}"])
            _docker_argv.extend([
                "--entrypoint", {policy.container_python!r}, {selection.image!r},
                "-c", _software_probe_source,
            ])
            _software_result = _capture(_docker_argv, 180)
        except BaseException as exc:
            _software_result = {{
                "rc": 125,
                "stdout": "",
                "stderr": f"Docker target probe setup failed: {{type(exc).__name__}}: {{exc}}"[:2048],
                "timed_out": False,
                "stdout_truncated": False,
            }}
        finally:
            try:
                _candidate_id = pathlib.Path(_cidfile).read_text(
                    encoding="ascii", errors="ignore"
                ).strip()[:128]
                if re.fullmatch(r"[0-9a-fA-F]{{12,64}}", _candidate_id):
                    _container_id = _candidate_id
            except OSError:
                pass
            if _container_id:
                _cleanup_result = _capture([_docker, "rm", "-f", _container_id], 30, 65536)
                _cleanup_command = _command_metadata(_cleanup_result)
                _cleanup_text = str(
                    _cleanup_result.get("stderr") or _cleanup_result.get("stdout") or ""
                ).lower()
                if _cleanup_result.get("rc") == 0:
                    _cleanup_status = "REMOVED"
                elif "no such container" in _cleanup_text:
                    _cleanup_status = "REMOVED_AUTOMATICALLY"
                else:
                    _cleanup_status = "FAILED"
            try:
                pathlib.Path(_cidfile).unlink(missing_ok=True)
            except OSError:
                pass
            _software_target.update({{
                "container_id": _container_id,
                "cleanup_status": _cleanup_status,
                "cleanup_command": _cleanup_command,
            }})
        _apply_software_result(_software_result, _software_target)
_environment["software_target"] = _software_target

_tools = _environment.get("dtk", {{}}).get("tools", {{}})
_hy_smi = _tools.get("hy-smi", {{}}).get("path") or (resolve_tool("hy-smi") or {{}}).get("path")
_rocminfo = _tools.get("rocminfo", {{}}).get("path") or (resolve_tool("rocminfo") or {{}}).get("path")
_metrics = {{
    "hy_smi_path": _hy_smi,
    "rocminfo_path": _rocminfo,
    "rocminfo": _capture([_rocminfo] if _rocminfo else [], 60),
    "bus": _capture([_hy_smi, "--showbus", "--json"] if _hy_smi else [], 20),
    "memory": [],
    "available": [],
    "memory_percent": [],
    "utilization": [],
}}
for _sample_index in range({policy.samples if 'resource' in policy.check_categories else 1}):
    _metrics["memory"].append(_capture([_hy_smi, "--showmeminfo", "vram", "--json"] if _hy_smi else [], 20))
    _metrics["available"].append(_capture([_hy_smi, "--showmemavailable", "--json"] if _hy_smi else [], 20))
    _metrics["memory_percent"].append(_capture([_hy_smi, "--showmemuse", "--json"] if _hy_smi else [], 20))
    _metrics["utilization"].append(_capture([_hy_smi, "--showuse", "--json"] if _hy_smi else [], 20))
    if _sample_index + 1 < {policy.samples if 'resource' in policy.check_categories else 1}:
        _time.sleep({policy.sample_interval_seconds!r})

print(_json.dumps({{
    "schema_version": "1.0",
    "environment": _environment,
    "metrics": _metrics,
}}, ensure_ascii=False, separators=(",", ":")))
"""
    source = probe_source + "\n" + wrapper
    encoded = base64.b64encode(zlib.compress(source.encode("utf-8"), 9)).decode("ascii")
    loader = (
        "import base64,zlib;"
        f"exec(compile(zlib.decompress(base64.b64decode('{encoded}')),'hcu-node-probe','exec'))"
    )
    command = [remote_python, "-c", loader]
    bootstrap = env_script if env_script is not None else policy.env_script
    scope = execution_scope or policy.execution_scope
    target_container = (
        container_name if container_name is not None else policy.container_name
    )
    target_shell = container_shell or policy.container_shell
    target_workdir = container_workdir if container_workdir is not None else policy.container_workdir
    # Validate overrides as well as policy fields before constructing the shell.
    replace(policy, env_script=bootstrap, execution_scope=scope, container_name=target_container,
            container_shell=target_shell, container_workdir=target_workdir).validate()
    shell = target_shell if scope == "container" else "bash"
    if bootstrap:
        from cluster_run.env import bootstrap_command
        command = bootstrap_command(bootstrap, command, shell=shell,
                                    workdir=target_workdir if scope == "container" else None)
    elif scope == "container":
        # Preserve shell selection even when no environment script is supplied.
        lines = ["set -e"]
        if target_workdir:
            lines.append(f"cd -- {shlex.quote(target_workdir)}")
        lines.append(f"exec {shlex.join(command)}")
        command = [shell, "-lc" if shell == "bash" else "-c", "\n".join(lines)]
    if policy.run_token is not None:
        # Register before shell initialization and source: bootstrap processes
        # must be cancellable even if module/Conda setup never returns.
        # Local import avoids the standalone probe's task-control import cycle.
        from cluster_run.task_control import managed_command
        command = managed_command(command, policy.run_token)
    if scope == "container":
        if not target_container:
            raise ValueError(
                "container_name is required for container execution scope"
            )
        docker = ["docker", "exec"]
        if target_workdir:
            docker.extend(["-w", target_workdir])
        docker.extend([target_container, *command])
        return ["bash", "-lc", "exec " + shlex.join(docker)]
    if scope != "host":
        raise ValueError("execution_scope must be host or container")
    return command


def evaluate_baremetal_environment(
    payload: dict[str, Any], policy: BaremetalPreflightPolicy
) -> tuple[list[Finding], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Apply hardware policy while requiring an explicit opt-in for Python/Torch."""

    selection = policy.environment_selection()
    target = payload.get("software_target") or {}
    software_payload = None
    if selection.env_mode is not EnvironmentMode.HOST_PYTHON:
        target_inventory = target.get("inventory")
        # Missing/invalid target evidence is an explicit empty scope.  It must
        # never fall back to a healthy host Python or host DTK installation.
        software_payload = target_inventory if isinstance(target_inventory, dict) else {}
    findings, summary, checks = evaluate_environment(
        payload,
        expected_device_count=policy.expected_devices,
        require_compiler=policy.require_compiler,
        require_rdma=policy.require_rdma,
        minimum_rdma_devices=policy.minimum_rdma_devices,
        expected_rdma_protocol=policy.expected_rdma_protocol,
        require_rccl=policy.require_rccl,
        require_ucx=policy.require_ucx,
        network_host_scope_verified=True,
        rdma_policy=policy.rdma_policy,
        software_payload=software_payload,
        required_python_packages=policy.required_python_packages,
    )
    software = {
        **selection.to_dict(),
        "mode": selection.env_mode.value.upper().replace("-", "_"),
        "status": "CHECKED",
        "message": f"training software checked in explicit {selection.env_mode.value} mode",
        "required_python_packages": list(policy.required_python_packages),
    }
    if selection.env_mode is not EnvironmentMode.HOST_PYTHON:
        target_status = target.get("status")
        software["target_status"] = target_status or "MISSING"
        if target_status != "SUCCESS":
            detail = (
                target.get("command", {}).get("stderr")
                or "selected software target did not return complete evidence"
            )
            findings.append(
                Finding("FAIL", "SOFTWARE_TARGET_PROBE_FAILED", str(detail)[:2048])
            )
            checks.append(
                {
                    "check_id": "SOFTWARE_TARGET",
                    "status": "FAIL",
                    "message": str(detail)[:2048],
                }
            )
        else:
            checks.append(
                {
                    "check_id": "SOFTWARE_TARGET",
                    "status": "PASS",
                    "message": f"{selection.env_mode.value} target probe completed",
                }
            )
    if selection.env_mode is EnvironmentMode.DOCKER:
        cleanup_status = target.get("cleanup_status")
        software["cleanup_status"] = cleanup_status or "UNKNOWN"
        if cleanup_status not in {"REMOVED", "REMOVED_AUTOMATICALLY", "NOT_CREATED"}:
            message = f"temporary Docker probe cleanup status={cleanup_status or 'UNKNOWN'}"
            findings.append(Finding("UNKNOWN", "DOCKER_PROBE_CLEANUP_FAILED", message))
            checks.append(
                {"check_id": "DOCKER_PROBE_CLEANUP", "status": "UNKNOWN", "message": message}
            )
        else:
            checks.append(
                {
                    "check_id": "DOCKER_PROBE_CLEANUP",
                    "status": "PASS",
                    "message": f"temporary probe container cleanup={cleanup_status}",
                }
            )
    return findings, summary, checks, software


def _result_finding(severity: str, reason_code: str, message: str) -> dict[str, Any]:
    return asdict(Finding(severity, reason_code, message))


def _transport_incomplete(node: str, result: BaremetalNodeResult) -> dict[str, Any]:
    reason = result.error_kind or "NODE_PROBE_FAILED"
    detail = (result.stderr or result.stdout or f"returncode={result.returncode}").strip()[:1024]
    reachable = result.error_kind == "REMOTE_COMMAND_FAILED"
    return {
        "node": node,
        "status": "INCOMPLETE",
        "reachable": reachable,
        "device_count": None,
        "devices": [],
        "metric_summary": {"max_vram_used_percent": None, "max_hcu_util_percent": None},
        "findings": [_result_finding("UNKNOWN", reason, detail)],
        "checks": [],
        "environment": {},
        "software_environment": {
            "mode": "NOT_SELECTED",
            "status": "NOT_CHECKED",
            "message": (
                "远端探针执行失败，未选择也未检查训练软件环境"
                if reachable
                else "节点不可达，未选择也未检查训练软件环境"
            ),
        },
        "probe_transport": result.metadata(),
    }


def evaluate_node_result(
    node: str,
    transport_result: BaremetalNodeResult,
    policy: BaremetalPreflightPolicy,
) -> dict[str, Any]:
    if not transport_result.success:
        return _transport_incomplete(node, transport_result)
    try:
        payload = None
        for line in reversed(transport_result.stdout.splitlines()):
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and candidate.get("schema_version") == "1.0":
                payload = candidate
                break
        if payload is None:
            raise json.JSONDecodeError("no probe JSON payload line", transport_result.stdout, 0)
        environment_payload = payload["environment"]
        metrics = payload["metrics"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        record = _transport_incomplete(node, transport_result)
        record["reachable"] = True
        record["findings"] = [
            _result_finding("UNKNOWN", "NODE_PROBE_OUTPUT_INVALID", f"cannot parse probe JSON: {exc}")
        ]
        return record

    if "platform" in policy.check_categories:
        env_findings, summary, checks, software = evaluate_baremetal_environment(
            environment_payload, policy
        )
    else:
        env_findings, checks = [], []
        system = environment_payload.get("system") or {}
        summary = {"mem_total": (system.get("meminfo") or {}).get("MemTotal"),
                   **{field: system.get(field) for field in ("kernel", "cpu_logical_count", "cpu_affinity_count", "cpu_models", "dev_shm", "cgroup_memory_max", "cgroup_cpu_max")},
                   "container_os": (system.get("os_release") or {}).get("PRETTY_NAME")}
        software = {"mode": "NOT_SELECTED", "status": "NOT_CHECKED",
                    "message": "platform category was not requested"}
    findings = list(env_findings) if "platform" in policy.check_categories else []
    resource_findings: list[Finding] = []
    devices = []
    metrics_complete = False

    hy_smi_path = metrics.get("hy_smi_path")
    rocminfo_path = metrics.get("rocminfo_path")
    if not hy_smi_path:
        resource_findings.append(Finding("FAIL", "HY_SMI_NOT_FOUND", "hy-smi/Hy-smi not found on node"))
    if not rocminfo_path:
        resource_findings.append(Finding("FAIL", "ROCMINFO_NOT_FOUND", "rocminfo not found on node"))

    metric_keys = ("rocminfo", "bus", "memory", "available", "memory_percent", "utilization")
    failed_commands: list[str] = []
    for key in metric_keys:
        values = metrics.get(key, [])
        if isinstance(values, dict):
            values = [values]
        for index, item in enumerate(values):
            if not isinstance(item, dict) or item.get("rc") != 0:
                failed_commands.append(f"{key}[{index}] rc={item.get('rc') if isinstance(item, dict) else 'missing'}")
            elif item.get("stdout_truncated"):
                failed_commands.append(f"{key}[{index}] stdout truncated")
    if failed_commands:
        resource_findings.append(
            Finding(
                "UNKNOWN",
                "HCU_METRIC_COMMAND_FAILED",
                "; ".join(failed_commands[:32]),
            )
        )

    if hy_smi_path and rocminfo_path and not failed_commands:
        try:
            hy_cards = parse_hy_smi_samples(
                [item.get("stdout", "") for item in metrics.get("memory", [])],
                [item.get("stdout", "") for item in metrics.get("available", [])],
                [item.get("stdout", "") for item in metrics.get("memory_percent", [])],
                [item.get("stdout", "") for item in metrics.get("utilization", [])],
                (metrics.get("bus") or {}).get("stdout"),
            )
            roc_agents = parse_rocminfo((metrics.get("rocminfo") or {}).get("stdout", ""))
            devices, metric_findings, _ = evaluate_metrics(
                {},
                hy_cards,
                roc_agents,
                policy.expected_devices,
                policy.max_vram_used_percent,
                policy.max_hcu_util_percent,
                policy.busy_sample_quorum,
            )
            resource_findings.extend(metric_findings)
            metrics_complete = True
        except (ParseError, TypeError, ValueError) as exc:
            resource_findings.append(Finding("UNKNOWN", "HCU_METRIC_PARSE_FAILED", str(exc)))

    if "resource" in policy.check_categories:
        findings.extend(resource_findings)

    if any(item.severity == "FAIL" for item in findings):
        status = "BLOCKED"
    elif any(item.severity == "UNKNOWN" for item in findings):
        status = "INCOMPLETE"
    else:
        status = "READY"
    memory_values = [
        item.memory_used_percent for item in devices if item.memory_used_percent is not None
    ]
    utilization_values = [
        item.hcu_util_percent for item in devices if item.hcu_util_percent is not None
    ]
    return {
        "node": node,
        "status": status,
        "reachable": True,
        "device_count": len(devices) if metrics_complete else None,
        "devices": [asdict(item) for item in devices],
        "metric_summary": {
            "max_vram_used_percent": max(memory_values) if memory_values else None,
            "max_hcu_util_percent": max(utilization_values) if utilization_values else None,
        },
        "findings": [asdict(item) for item in findings],
        "checks": checks if "platform" in policy.check_categories else [],
        "environment": summary,
        "software_environment": software,
        "software_target": environment_payload.get("software_target") or {},
        "probe_transport": transport_result.metadata(),
    }


def apply_conda_collection_plan(
    records: list[dict[str, Any]], policy: BaremetalPreflightPolicy
) -> dict[str, Any] | None:
    """Validate declared Conda storage without projecting one node runtime to peers."""

    selection = policy.environment_selection()
    if selection.env_mode is not EnvironmentMode.CONDA:
        return None
    observations: list[CondaStorageObservation] = []
    for record in records:
        evidence = (record.get("software_target") or {}).get(
            "conda_storage_observation"
        )
        if not isinstance(evidence, dict):
            continue
        observations.append(
            CondaStorageObservation(
                node=record["node"],
                prefix=str(evidence.get("prefix") or selection.conda_prefix),
                prefix_exists=bool(evidence.get("prefix_exists")),
                python_executable=bool(evidence.get("python_executable")),
                realpath=evidence.get("realpath"),
                mount_source=evidence.get("mount_source"),
                fs_type=evidence.get("fs_type"),
                identity_fingerprint=evidence.get("identity_fingerprint"),
                collection_status=str(evidence.get("collection_status") or "ERROR"),
                reason_code=evidence.get("reason_code"),
                shared_backend=evidence.get("shared_backend"),
            )
        )
    plan = plan_conda_collection(
        selection,
        expected_nodes=[record["node"] for record in records],
        observations=observations,
    )
    records_by_node = {record["node"]: record for record in records}
    for finding in plan.findings:
        for node in finding.nodes:
            record = records_by_node[node]
            record.setdefault("findings", []).append(
                _result_finding(finding.severity, finding.reason_code, finding.message)
            )
            if finding.severity == "FAIL":
                record["status"] = "BLOCKED"
            elif finding.severity == "UNKNOWN" and record.get("status") == "READY":
                record["status"] = "INCOMPLETE"
    return plan.to_dict()


def _group_node_results(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        reason_codes = sorted({item["reason_code"] for item in record.get("findings", [])})
        metrics = record.get("metric_summary") or {}
        visible = {
            "status": record["status"],
            "reachable": record["reachable"],
            "device_count": record["device_count"],
            "reason_codes": reason_codes,
            "software_status": record.get("software_environment", {}).get("status"),
            "max_vram_used_percent": _percent(metrics.get("max_vram_used_percent")),
            "max_hcu_util_percent": _percent(metrics.get("max_hcu_util_percent")),
        }
        key = _json_key(visible)
        group = groups.setdefault(key, {**visible, "nodes": []})
        group["nodes"].append(record["node"])
    output = list(groups.values())
    for group in output:
        group["nodes"].sort(key=_natural_key)
    return sorted(output, key=lambda item: _natural_key(item["nodes"][0]))


def _device_profile(record: dict[str, Any]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for device in record.get("devices", []):
        item = {
            "model": device.get("model"),
            "architecture": device.get("architecture"),
            "total_mib": device.get("hy_smi_total_mib"),
        }
        key = _json_key(item)
        group = groups.setdefault(key, {**item, "count": 0})
        group["count"] += 1
    return sorted(groups.values(), key=_json_key)


def _normalized_mem_total(value: Any) -> Any:
    """Fold insignificant /proc/meminfo variation without hiding capacity drift."""
    if not isinstance(value, str):
        return value
    match = re.fullmatch(r"\s*(\d+)\s+kB\s*", value, flags=re.IGNORECASE)
    if match is None:
        return value
    gib = int(match.group(1)) / (1024 * 1024)
    return f"{round(gib)} GiB"


def _hardware_groups(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    fields = set().union(*(STATIC_ENVIRONMENT_FIELDS[category] for category in
                           ("hardware_devices", "system", "driver_dtk", "network_rdma")))
    fields.discard("software_target_os")  # This legacy view describes the host.
    for record in records:
        if not record.get("reachable") or not record.get("environment"):
            continue
        env = canonical_static_value(static_environment_view(record["environment"]))
        visible = {field: env.get(field) for field in fields}
        visible["mem_total"] = _normalized_mem_total(env.get("mem_total"))
        visible["hcu_profile"] = _device_profile(record)
        key = _json_key(visible)
        group = groups.setdefault(key, {**visible, "nodes": []})
        group["nodes"].append(record["node"])
    output = list(groups.values())
    for group in output:
        group["nodes"].sort(key=_natural_key)
    return sorted(output, key=lambda item: _natural_key(item["nodes"][0]))


def _consistency_findings(
    records: list[dict[str, Any]], *, strict: bool,
    categories: tuple[str, ...] = ("platform", "resource"),
) -> list[dict[str, Any]]:
    usable = [record for record in records if record.get("reachable") and record.get("environment")]
    if len(usable) < 2:
        return []
    findings: list[dict[str, Any]] = []
    observed = set().union(*(set(static_environment_view(record["environment"])) for record in usable))
    fields = observed | set(_CONSISTENCY_FIELDS) if "platform" in categories else observed & {
        "mem_total", "cgroup_memory_max", "cgroup_cpu_max", "dev_shm", "cpu_logical_count", "cpu_models", "cpu_affinity_count",
    }
    for field in sorted(fields):
        values: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for record in usable:
            environment = canonical_static_value(static_environment_view(record["environment"]))
            if field not in environment or environment.get(field) is None:
                missing.append(record["node"])
                continue
            value = environment[field]
            if field == "rdma_current_protocol" and value not in {
                "NATIVE_INFINIBAND",
                "ROCE",
                "MIXED",
            }:
                missing.append(record["node"])
                continue
            key = _json_key(value)
            values.setdefault(key, {"value": value, "nodes": []})["nodes"].append(record["node"])
        if missing:
            missing_reason = {
                "rdma_current_protocol": "RDMA_PROTOCOL_EVIDENCE_MISSING",
                "rdma_fabric_profile": "RDMA_FABRIC_PROFILE_EVIDENCE_MISSING",
            }.get(field, "HARDWARE_EVIDENCE_MISSING")
            findings.append(
                {
                    "severity": "UNKNOWN",
                    "reason_code": missing_reason,
                    "field": field,
                    "nodes": sorted(missing, key=_natural_key),
                    "values": [],
                }
            )
        if len(values) > 1:
            rendered = []
            for item in values.values():
                item["nodes"].sort(key=_natural_key)
                rendered.append(item)
            rendered.sort(key=lambda item: _natural_key(item["nodes"][0]))
            mandatory_rdma = field in {"rdma_current_protocol", "rdma_fabric_profile"}
            reason_code = {
                "rdma_current_protocol": "RDMA_PROTOCOL_CLUSTER_MIXED",
                "rdma_fabric_profile": "RDMA_FABRIC_PROFILE_INCONSISTENT",
            }.get(field, "HARDWARE_PROFILE_INCONSISTENT")
            findings.append(
                {
                    "severity": "FAIL" if mandatory_rdma or strict else "WARN",
                    "reason_code": reason_code,
                    "field": field,
                    "nodes": sorted(
                        [record["node"] for record in usable], key=_natural_key
                    ),
                    "values": rendered,
                }
            )
    return findings


def _fmt(value: Any) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _percent(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "-"


def _md_cell(value: Any) -> str:
    return _fmt(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _scale_assessment(
    records: list[dict[str, Any]], *, target_devices: int | None
) -> dict[str, Any]:
    completed = [item for item in records if item.get("device_count") is not None]
    checked_devices = sum(int(item.get("device_count") or 0) for item in completed)
    ready = [item for item in completed if item.get("status") == "READY"]
    blocked_nodes = sorted(
        [item["node"] for item in records if item.get("status") == "BLOCKED"],
        key=_natural_key,
    )
    incomplete_nodes = sorted(
        [item["node"] for item in records if item.get("status") == "INCOMPLETE"],
        key=_natural_key,
    )
    device_count_distribution: dict[str, int] = {}
    for item in completed:
        count = str(int(item.get("device_count") or 0))
        device_count_distribution[count] = device_count_distribution.get(count, 0) + 1
    if blocked_nodes:
        status = "NOT_READY"
        conclusion = "本次检测节点存在阻断项；不据此推断未检测节点状态。"
    elif incomplete_nodes:
        status = "NOT_VERIFIED"
        conclusion = "本次检测节点证据不完整；不能判断这些节点是否就绪。"
    elif target_devices is None:
        status = "SAMPLE_READY_RUNTIME_UNVERIFIED"
        conclusion = "本次检测节点静态检查通过；未验证跨节点通信、训练或万卡规模运行。"
    elif checked_devices < target_devices:
        status = "SAMPLE_READY_FULL_SCALE_UNVERIFIED"
        conclusion = "本次检测节点静态检查通过；用户指定的目标规模、训练和通信仍未验证。"
    else:
        status = "FULL_SCALE_STATIC_PREFLIGHT_PASSED_RUNTIME_UNVERIFIED"
        conclusion = "目标卡数完成静态检查；训练和通信数据面仍未验证。"
    return {
        "status": status,
        "target_devices": target_devices,
        "checked_nodes": len(completed),
        "checked_devices": checked_devices,
        "device_count_distribution": device_count_distribution,
        "ready_nodes": len(ready),
        "ready_devices": sum(int(item.get("device_count") or 0) for item in ready),
        "blocking_nodes": blocked_nodes,
        "incomplete_nodes": incomplete_nodes,
        "coverage_percent": (
            round(min(100.0, checked_devices * 100.0 / target_devices), 3)
            if target_devices is not None else None
        ),
        "conclusion": conclusion,
        "is_training_validation": False,
    }


def _format_adapter(item: dict[str, Any]) -> str:
    return (
        f"{item.get('count', 0)}x {item.get('vendor') or 'UNKNOWN'} "
        f"{item.get('model') or 'UNNAMED'}; PCI={item.get('pci_id') or '-'}; "
        f"driver={item.get('driver') or '-'} {item.get('driver_version') or ''}; "
        f"firmware={item.get('firmware_version') or '-'}; "
        f"link={item.get('local_link') or '-'}; speed={item.get('speed_mbps') or '-'}Mbps"
    ).strip()


def _group_findings(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        for finding in record.get("findings", []):
            visible = {
                "status": record.get("status"),
                "severity": finding.get("severity"),
                "reason_code": finding.get("reason_code"),
                "message": finding.get("message"),
                "device_id": finding.get("device_id"),
            }
            key = _json_key(visible)
            group = groups.setdefault(key, {**visible, "nodes": []})
            group["nodes"].append(record["node"])
    output = list(groups.values())
    for group in output:
        group["nodes"].sort(key=_natural_key)
    return sorted(
        output,
        key=lambda item: (
            _natural_key(item["nodes"][0]),
            str(item.get("reason_code") or ""),
        ),
    )


def render_baremetal_markdown(report: dict[str, Any]) -> str:
    def fmt_percent(value: Any) -> str:
        if value is None:
            return "-"
        return f"{float(value):.3f}" if abs(float(value)) < 0.1 else f"{float(value):.1f}"

    def fmt_mib(value: Any) -> str:
        return "-" if value is None else f"{float(value):.0f}"

    def field_text(value: Any) -> str:
        if value is None:
            return "未采集"
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value).replace("|", "\\|").replace("\n", " ")

    summary = report.get("summary") or {}
    scale = report.get("scale_assessment") or {}
    records = sorted(report.get("nodes") or [], key=lambda item: _natural_key(item["node"]))
    classified = build_node_first_result(report)
    execution = report.get("execution") or {}
    consistency = report.get("consistency_summary") or {}
    scope = consistency.get("scope") or "legacy-preflight"
    lines = [
        "# HCU 集群环境检查报告",
        "",
        f"- 结论：`{report.get('status', '-')}`；范围：`{scope}`；场景：`{execution.get('scenario', '-')}`；执行位置：`{execution.get('scope', '-')}`",
        f"- 节点：{summary.get('node_count', len(records))}；可达={summary.get('reachable_nodes', '-')}；READY={summary.get('ready_nodes', '-')}；BLOCKED={summary.get('blocked_nodes', '-')}；INCOMPLETE={summary.get('incomplete_nodes', '-')}",
        f"- HCU：识别={summary.get('detected_devices', '-')}；预期={summary.get('expected_devices_total') if summary.get('expected_devices_total') is not None else '未设置'}；传输={report.get('transport', '-')}",
        f"- 采集时间：{report.get('started_at', '-')} → {report.get('finished_at', '-')}；工具版本={report.get('tool_version', '-')}",
        "- 说明：静态配置与瞬时资源状态分开展示；READY 节点仍可能有设备 WARN。完整采样、命令和证据路径见 JSON。",
        "",
        "## 节点显卡与资源状态",
        "",
        "| 节点 | 状态 | 可达 | HCU | 设备 PASS/WARN/FAIL | 已用显存 MiB（范围） | 最大显存占用% | 最大利用率% | 原因码 |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    container_status = report.get("container_status") or {}
    if container_status:
        lines[8:8] = [
            "## 容器状态预检",
            "",
            f"- 状态：`{container_status.get('status', '-')}`；可检测节点={len(container_status.get('ready_nodes') or [])}/{summary.get('node_count', len(records))}；目标镜像={container_status.get('image_reference') or '未指定'}。",
            "- 容器异常节点未执行环境探针；其余节点继续检测。容器状态不代表 DCU 空闲或 env.sh 正常。",
            *[
                f"- `{issue.get('node', '-')}`：`{issue.get('code', '-')}`；{issue.get('message', '')}"
                for issue in container_status.get("issues") or []
            ],
            "",
        ]
    for record in records:
        devices = record.get("devices") or []
        counts = {status: sum(device.get("status") == status for device in devices) for status in ("PASS", "WARN", "FAIL")}
        used = [float(device["used_mib"]) for device in devices if device.get("used_mib") is not None]
        used_range = f"{fmt_mib(min(used))}–{fmt_mib(max(used))}" if used else "-"
        reasons = sorted({reason for device in devices for reason in device.get("reason_codes") or []} | {finding.get("reason_code") for finding in record.get("findings") or [] if finding.get("reason_code")})
        metrics = record.get("metric_summary") or {}
        lines.append(
            f"| {record['node']} | {record.get('status', '-')} | {'是' if record.get('reachable') else '否'} | {record.get('device_count') if record.get('device_count') is not None else '-'} | "
            f"{counts['PASS']}/{counts['WARN']}/{counts['FAIL']} | {used_range} | {fmt_percent(metrics.get('max_vram_used_percent'))} | {fmt_percent(metrics.get('max_hcu_util_percent'))} | {', '.join(reasons) or '-'} |"
        )

    lines.extend(["", "## 设备异常与告警", ""])
    abnormal = False
    for record in records:
        groups: dict[tuple[str, tuple[str, ...]], list[Any]] = {}
        for device in record.get("devices") or []:
            if device.get("status") not in {"FAIL", "WARN"}:
                continue
            key = (device["status"], tuple(device.get("reason_codes") or []))
            groups.setdefault(key, []).append(device.get("device_id"))
        for (status, reasons), ids in groups.items():
            abnormal = True
            lines.append(f"- {record['node']}：`{status}`；设备={','.join(map(str, ids))}（{len(ids)} 张）；原因={', '.join(reasons) or '-'}")
    if not abnormal:
        lines.append("- 已采集设备无 FAIL/WARN。")

    lines.extend(["", "## 静态配置及跨节点差异", "", "| 类别 | 配置组 | 节点 | 关键配置 |", "|---|---:|---|---|"])
    static_categories = (
        ("hardware_devices", "硬件设备", ("device_count", "cpu_models")),
        ("system", "系统/内存", ("container_os", "kernel", "mem_total")),
        ("driver_dtk", "驱动/DTK", ("driver_version", "dtk_version", "hipcc_version")),
        ("software_components", "软件组件", ("python_version", "python_packages", "torch_version", "torch_nccl_version", "ucx_version", "mpi_version")),
        ("network_rdma", "网络/RDMA 配置", ("rdma_current_protocol", "rdma_device_count", "rdma_hardware_protocol_capability")),
    )
    for category, title, keys in static_categories:
        groups = (classified.get(category) or {}).get("configuration_nodes") or {}
        for label, payload in groups.items():
            key_values = "；".join(f"{key}={field_text(payload.get(key))}" for key in keys)
            if category == "hardware_devices":
                profiles = payload.get("device_profiles") or []
                key_values += "；型号=" + ",".join(f"{field_text(item.get('model'))}×{item.get('count', 0)}" for item in profiles)
            lines.append(f"| {title} | {len(groups)} | {label}（{len(payload['members'])} 节点） | {key_values} |")
    differing = [title for category, title, _ in static_categories if len((classified.get(category) or {}).get("configuration_nodes") or {}) > 1]
    no_difference = "已采集的上述类别中未发现跨节点差异。"
    if consistency.get("configuration_unknown_nodes") or not any(record.get("environment") for record in records):
        no_difference = "配置证据不足，不能确认节点配置一致。"
    lines.append("\n- 静态配置差异：" + ("、".join(differing) if differing else no_difference))
    if differing:
        lines.extend(["", "| 类别.差异字段 | 节点组 | 实际值 |", "|---|---|---|"])
        for category, title, _ in static_categories:
            groups = (classified.get(category) or {}).get("configuration_nodes") or {}
            if len(groups) < 2:
                continue
            fields = set().union(*(set(payload) for payload in groups.values())) - {"members", "node_count"}
            for field in sorted(fields):
                values = {json.dumps(payload.get(field), ensure_ascii=False, sort_keys=True, default=str) for payload in groups.values()}
                if len(values) < 2:
                    continue
                for label, payload in groups.items():
                    value = field_text(payload.get(field))
                    lines.append(f"| {title}.{field} | {label} | {value[:240]}{'…' if len(value) > 240 else ''} |")
    lines.append("- 空值表示未采集或未要求，不等于检测失败；具体检查项状态见 JSON。")

    lines.extend(["", "## 网络健康（动态）", ""])
    for label, payload in (classified.get("network_health") or {}).get("nodes", {}).items():
        rdma = payload.get("rdma_userspace") or {}
        ib = payload.get("ib_counter_health") or {}
        roce = payload.get("roce_counter_health") or {}
        lines.append(f"- {label}：活跃 RDMA 端口={field_text(payload.get('rdma_active_port_count'))}；IB 端点={field_text(payload.get('ib_endpoint_status'))}；RDMA userspace={field_text(rdma.get('check_status') or rdma.get('status'))}；IB counters={field_text(ib.get('status'))}；RoCE counters={field_text(roce.get('status'))}")
    lines.append("- 端口/计数器原始采样及 RDMA 命令输出见 `network_health.samples_by_node`；不计入静态配置差异。")

    extra = report.get("cluster_extra_checks") or {}
    if extra.get("enabled"):
        lines.append("")
        ib_state = extra.get("ib_state") or {}
        nhc = extra.get("nhc") or {}
        ib_write_bw = extra.get("ib_write_bw") or {}
        lines.extend(["## Cluster Extra Checks", ""])
        lines.append(
            f"- rounds: `{extra.get('rounds', 1)}`; "
            f"taint_mutation: `{extra.get('taint_mutation', False)}`"
        )
        if ib_state.get("enabled"):
            ib_nodes = ib_state.get("nodes") or []
            lines.append(
                f"- IB state: `{ib_state.get('status', 'NOT_VERIFIED')}`; "
                f"pass={sum(item.get('status') == 'PASS' for item in ib_nodes)}/"
                f"{len(ib_nodes)}; transport={_fmt(ib_state.get('transport'))}"
            )
        if nhc.get("enabled"):
            nhc_nodes = nhc.get("nodes") or []
            nhc_summary = (
                f"- NHC: `{nhc.get('status', 'NOT_VERIFIED')}`; "
                f"pass={sum(item.get('status') == 'PASS' for item in nhc_nodes)}/"
                f"{len(nhc_nodes)}; transport={_fmt(nhc.get('transport'))}"
            )
            execution_reason_codes = {
                "NHC_COMMAND_NOT_FOUND",
                "NHC_EXECUTION_ERROR",
                "NHC_EXECUTION_FAILED",
                "NHC_RESULT_MARKER_MISSING",
            }
            if nhc.get("installation_source") and any(
                item.get("reason_code") in execution_reason_codes
                for item in nhc_nodes
            ):
                nhc_summary += (
                    f"; installation_source={_fmt(nhc.get('installation_source'))}"
                )
            lines.append(nhc_summary)
        if ib_write_bw.get("enabled"):
            summary = ib_write_bw.get("summary") or {}
            lines.append(
                f"- ib_write_bw: `{ib_write_bw.get('status', 'NOT_VERIFIED')}`; "
                f"rounds={_fmt(ib_write_bw.get('rounds'))}; "
                f"planned_tests={_fmt(summary.get('planned_tests'))}; "
                f"pass={_fmt(summary.get('passed_pairs'))}; "
                f"fail={_fmt(summary.get('failed_pairs'))}; "
                f"not_verified={_fmt(summary.get('not_verified_pairs'))}; "
                f"min_avg_gbps={_fmt(summary.get('minimum_average_gbps_observed'))}"
            )
            for pair in ib_write_bw.get("pairs") or []:
                if pair.get("reason_code") != "IB_BANDWIDTH_BELOW_THRESHOLD":
                    continue
                lines.append(
                    f"  - Low bandwidth HCA path: "
                    f"`{_fmt(pair.get('source'))}:{_fmt(pair.get('source_hca'))} -> "
                    f"{_fmt(pair.get('destination'))}:{_fmt(pair.get('destination_hca'))}`; "
                    f"rail={_fmt(pair.get('rail_index'))}; "
                    f"average={_fmt(pair.get('average_gbps'))} Gbit/s; "
                    f"threshold={_fmt(ib_write_bw.get('minimum_average_gbps'))} Gbit/s"
                )
        lines.append("")

    lines.extend(["## 集群汇总与一致性", ""])
    if consistency:
        lines.append(
            f"- 范围：`{scope}`；通过={consistency.get('passed_node_count', '-')}；未通过={consistency.get('failed_node_count', '-')}；证据不完整={consistency.get('incomplete_node_count', '-')}。"
        )
        if scope == "resource-only":
            lines.append("- resource 一致性按内存容量、设备型号/数量和显存容量分组；瞬时占用和利用率不拆配置组。")
        elif scope == "platform-only":
            lines.append("- 本次仅执行 platform；resource 未检查，不能据此判断显卡空闲。")
        lines.append(f"- 本范围分组数：{consistency.get('configuration_group_count', '-')}；逐组字段和差异见 JSON `cluster.consistency_summary`。")
        lines.append(f"- 比较结论：`{consistency.get('comparison_status', 'UNVERIFIED')}`；相同缺失值不代表配置一致。")
        for node, missing in (consistency.get("configuration_missing_by_node") or {}).items():
            lines.append(f"- `{node}` 配置证据不足：{', '.join(missing)}。")
    else:
        lines.append("- 未生成独立范围的一致性摘要；下方保留预检硬件一致性结论。")
    for finding in report.get("consistency_findings") or []:
        lines.append(f"- `{finding.get('severity', '-')}` {finding.get('reason_code', '-')}：字段={finding.get('field', '-')}；节点={', '.join(finding.get('nodes') or [])}")
    if not report.get("consistency_findings"):
        lines.append("- 仅比较已采集静态字段；未采集项无法确认一致，检测状态不代表配置证据完整。")

    lines.extend(["", "## 执行证据", "", "| 节点 | 返回码 | 耗时秒 | 错误 | 证据目录 |", "|---|---:|---:|---|---|"])
    for record in records:
        transport = record.get("probe_transport") or {}
        lines.append(
            f"| {record['node']} | {field_text(transport.get('returncode'))} | {field_text(transport.get('duration_seconds'))} | "
            f"{field_text(transport.get('error_kind')) if transport.get('error_kind') else '-'} | {field_text(transport.get('result_dir'))} |"
        )
    lines.append("\n- 每节点详细检查项、finding 与传输证据见 `cluster-result.json`；公共探测命令见 `run.probe_command`。")

    distribution = scale.get("device_count_distribution") or {}
    distribution_text = "、".join(
        f"{node_count} 个节点×{device_count} 卡"
        for device_count, node_count in sorted(distribution.items(), key=lambda item: int(item[0]))
    ) or "未识别"
    lines.extend([
        "", "## 本次节点样本静态评估", "",
        f"- 状态：`{scale.get('status', 'NOT_VERIFIED')}`；已识别={scale.get('checked_devices', 0)} 张 HCU；节点分布：{distribution_text}。",
        f"- 节点级 READY：{scale.get('ready_nodes', '-')} 个节点、{scale.get('ready_devices', '-')} 张 HCU；阻断节点={', '.join(scale.get('blocking_nodes') or []) or '无'}；证据不完整节点={', '.join(scale.get('incomplete_nodes') or []) or '无'}。",
    ])
    if scale.get("target_devices") is not None:
        lines.append(
            f"- 用户显式指定验收目标：{scale['target_devices']} 张 HCU；本次覆盖={scale.get('coverage_percent', '-')}%。"
        )
    lines.extend([
        f"- 结论：{scale.get('conclusion', '本项不是训练或 collective 实测。')}",
        "- 工具按万卡级集群规模设计；设计容量不是本次检测目标，也不作为覆盖率分母。",
        "- 静态评估不作为全局门禁；用户根据报告决定后续主动测试。",
    ])
    return "\n".join(lines).rstrip() + "\n"


def build_baremetal_report(
    *,
    records: list[dict[str, Any]],
    policy: BaremetalPreflightPolicy,
    transport: str,
    evidence_dir: str,
    started_at: str,
    finished_at: str,
    conda_collection_plan: dict[str, Any] | None = None,
    cluster_extra_checks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    records = sorted(records, key=lambda item: _natural_key(item["node"]))
    consistency = _consistency_findings(
        records, strict=policy.strict_hardware_consistency, categories=policy.check_categories
    )
    if any(record["status"] == "BLOCKED" for record in records) or any(
        item["severity"] == "FAIL" for item in consistency
    ):
        status = "BLOCKED"
    elif any(record["status"] == "INCOMPLETE" for record in records) or any(
        item["severity"] == "UNKNOWN" for item in consistency
    ):
        status = "INCOMPLETE"
    else:
        status = "READY"
    expected_total = (
        policy.expected_devices * len(records) if policy.expected_devices is not None else None
    )
    selection = policy.environment_selection()
    software = {
        **selection.to_dict(),
        "mode": selection.env_mode.value.upper().replace("-", "_"),
        "status": (
            "CHECKED"
            if all(record.get("software_environment", {}).get("status") == "CHECKED" for record in records)
            else "INCOMPLETE"
        ),
        "checked_nodes": sum(
            record.get("software_environment", {}).get("status") == "CHECKED"
            for record in records
        ),
        "expected_nodes": len(records),
        "required_python_packages": list(policy.required_python_packages),
        "message": f"explicit {selection.env_mode.value} training software target",
    }
    scale = _scale_assessment(records, target_devices=policy.target_scale_devices)
    return {
        "schema_version": "1.0",
        "tool_version": __version__,
        "kind": "baremetal_cluster_preflight",
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "transport": transport,
        "evidence_dir": evidence_dir,
        "policy": asdict(policy),
        "software_environment": software,
        "conda_collection_plan": conda_collection_plan,
        "cluster_extra_checks": cluster_extra_checks,
        "scale_assessment": scale,
        "summary": {
            "node_count": len(records),
            "reachable_nodes": sum(bool(item["reachable"]) for item in records),
            "unreachable_nodes": sum(not item["reachable"] for item in records),
            "ready_nodes": sum(item["status"] == "READY" for item in records),
            "blocked_nodes": sum(item["status"] == "BLOCKED" for item in records),
            "incomplete_nodes": sum(item["status"] == "INCOMPLETE" for item in records),
            "detected_devices": sum(int(item.get("device_count") or 0) for item in records),
            "expected_devices_total": expected_total,
        },
        "node_result_groups": _group_node_results(records),
        "hardware_groups": _hardware_groups(records),
        "consistency_findings": consistency,
        "finding_groups": _group_findings(records),
        "nodes": records,
    }


def build_node_first_result(report: dict[str, Any]) -> dict[str, Any]:
    """Build the classified, lossless on-disk view of node results.

    In-memory records keep their original shape for existing evaluators.  A
    category folds nodes only when the values shown in that category match;
    changing counters, resource use and evidence paths never split a static
    configuration group.
    """

    def node_label(members: list[str]) -> str:
        if len(members) == 1:
            return members[0]
        matches = [re.fullmatch(r"(.*?)(\d+)", node) for node in members]
        if not all(matches):
            return ",".join(members)
        prefix = matches[0].group(1)
        width = len(matches[0].group(2))
        if any(match.group(1) != prefix or len(match.group(2)) != width for match in matches):
            return ",".join(members)
        numbers = [int(match.group(2)) for match in matches]
        pieces: list[str] = []
        start = previous = numbers[0]
        for number in numbers[1:] + [None]:
            if number is not None and number == previous + 1:
                previous = number
                continue
            first = f"{start:0{width}d}"
            last = f"{previous:0{width}d}"
            pieces.append(first if start == previous else f"{first}-{last}")
            if number is not None:
                start = previous = number
        return f"{prefix}[{','.join(pieces)}]"

    def fold_devices(devices: list[dict[str, Any]], fields: set[str]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for device in devices:
            detail = {key: value for key, value in device.items() if key in fields}
            key = _json_key(detail)
            group = groups.setdefault(key, {"device_ids": [], **detail})
            group["device_ids"].append(device.get("device_id"))
        for group in groups.values():
            group["device_ids"].sort(key=lambda item: (item is None, item if item is not None else -1))
        return list(groups.values())

    def fold_findings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for finding in findings:
            detail = {key: value for key, value in finding.items() if key != "device_id"}
            key = _json_key(detail)
            group = groups.setdefault(key, {**detail, "device_ids": []})
            if finding.get("device_id") is not None:
                group["device_ids"].append(finding["device_id"])
        for group in groups.values():
            group["device_ids"].sort()
        return list(groups.values())

    def group_nodes(values: dict[str, dict[str, Any]]) -> dict[str, Any]:
        grouped: dict[str, dict[str, Any]] = {}
        for node in sorted(values, key=_natural_key):
            payload = values[node]
            key = _json_key(payload)
            entry = grouped.setdefault(key, {"members": [], "payload": payload})
            entry["members"].append(node)
        output: dict[str, Any] = {}
        for entry in grouped.values():
            members = entry["members"]
            output[node_label(members)] = {
                "members": members,
                "node_count": len(members),
                **entry["payload"],
            }
        return {"nodes": output}

    static_device_fields = set(STATIC_DEVICE_FIELDS)
    identity_device_fields = ("bdf", "rocminfo_agent")
    system_fields = STATIC_ENVIRONMENT_FIELDS["system"]
    hardware_fields = STATIC_ENVIRONMENT_FIELDS["hardware_devices"]
    driver_fields = STATIC_ENVIRONMENT_FIELDS["driver_dtk"]
    software_fields = STATIC_ENVIRONMENT_FIELDS["software_components"] | {
        "required_python_packages", "torch_device_count", "torch_hcu_available", "software_evidence_scope",
    }
    health_fields = {
        "rdma_userspace", "ib_counter_health", "roce_counter_health", "roce_configuration_health",
        "ib_endpoint", "roce_endpoint", "nic_link_profile", "nic_link_summary",
        "rdma_active_device_count", "rdma_active_port_count", "rdma_rates", "rdma_fabric_profile",
    }
    categories = (
        "node_status", "hardware_devices", "system", "driver_dtk",
        "software_components", "network_rdma", "resource_state",
        "network_health", "execution_evidence",
    )
    by_category: dict[str, dict[str, dict[str, Any]]] = {category: {} for category in categories}
    configurations: dict[str, dict[str, Any]] = {category: {} for category in STATIC_ENVIRONMENT_FIELDS}
    requested = set((report.get("policy") or {}).get("check_categories") or ("platform", "resource"))
    configuration_scope = (report.get("consistency_summary") or {}).get("scope") or (
        "platform-only" if requested == {"platform"} else "resource-only" if requested == {"resource"} else "platform-and-resource"
    )
    unclassified: dict[str, dict[str, Any]] = {}
    health_samples: dict[str, dict[str, Any]] = {}
    records = sorted(report.get("nodes") or [], key=lambda item: _natural_key(str(item.get("node", ""))))
    commands = [
        (record.get("probe_transport") or {}).get("command")
        for record in records
    ]
    shared_command = commands[0] if commands and all(command == commands[0] for command in commands) else None

    for record in records:
        node = str(record.get("node") or "").strip()
        if not node:
            continue
        for category, values in static_configuration_sections(record, scope=configuration_scope).items():
            configurations[category][node] = values
        env = record.get("environment") or {}
        devices = record.get("devices") or []
        sections: dict[str, dict[str, Any]] = {category: {} for category in categories}
        sections["node_status"] = {"status": record.get("status"), "reachable": record.get("reachable")}
        if record.get("container_status") is not None:
            sections["node_status"]["container_status"] = record["container_status"]
        sections["hardware_devices"] = {
            "device_count": record.get("device_count"),
            "device_profiles": fold_devices(devices, static_device_fields),
            "device_identity": {
                str(device.get("device_id")): {key: device.get(key) for key in identity_device_fields}
                for device in devices
            },
        }
        sections["system"] = {
            "dev_shm": {key: value for key, value in (env.get("dev_shm") or {}).items() if key != "available_bytes"}
        }
        sections["software_components"] = {
            "software_environment": record.get("software_environment") or {},
            "software_target": record.get("software_target") or {},
            "torch_check_status": (
                "NOT_CHECKED"
                if not any(str(check.get("check_id", "")).startswith("TORCH_") for check in record.get("checks") or [])
                else "CHECKED"
            ),
        }
        sections["resource_state"] = {
            "metric_summary": record.get("metric_summary") or {},
            "device_groups": fold_devices(
                devices,
                set().union(*(set(device) for device in devices)) - static_device_fields - set(identity_device_fields) - {"device_id"},
            ),
            "dev_shm_available_bytes": (env.get("dev_shm") or {}).get("available_bytes"),
        }
        health = {field: env[field] for field in health_fields if field in env}
        health_samples[node] = health
        sections["network_health"] = {
            "rdma_active_device_count": env.get("rdma_active_device_count"),
            "rdma_active_port_count": env.get("rdma_active_port_count"),
            "nic_link_summary": env.get("nic_link_summary"),
            "ib_endpoint_status": (env.get("ib_endpoint") or {}).get("status"),
            "roce_endpoint_status": (env.get("roce_endpoint") or {}).get("status"),
            "rdma_userspace": {
                key: (health.get("rdma_userspace") or {}).get(key)
                for key in ("status", "check_status", "reason_code", "sysfs_devices", "enumerated_devices")
            },
            "ib_counter_health": {
                key: (health.get("ib_counter_health") or {}).get(key)
                for key in ("status", "ports", "status_counts", "reason_codes")
            },
            "roce_counter_health": {
                key: (health.get("roce_counter_health") or {}).get(key)
                for key in ("status", "reason_codes")
            },
            "roce_configuration_health": {
                key: (health.get("roce_configuration_health") or {}).get(key)
                for key in ("status", "policy_applied")
            },
        }
        transport = dict(record.get("probe_transport") or {})
        transport.pop("node", None)
        if shared_command is not None:
            transport.pop("command", None)
        sections["execution_evidence"] = {"probe_transport": transport}
        if env.get("library_evidence"):
            sections["execution_evidence"]["library_evidence"] = env["library_evidence"]

        for field, value in env.items():
            if field == "library_evidence":
                continue
            if field in system_fields:
                category = "system"
            elif field in hardware_fields:
                category = "hardware_devices"
            elif field in driver_fields:
                category = "driver_dtk"
            elif field in software_fields:
                category = "software_components"
            elif field in health_fields or field == "dev_shm":
                continue
            elif field.startswith(("nic_", "rdma_", "ib_", "roce_")) or field in {"network_scope", "physical_nic_count", "pci_name_source"}:
                category = "network_rdma"
            else:
                unclassified.setdefault(node, {})[field] = value
                continue
            sections[category][field] = value

        for check in record.get("checks") or []:
            check_id = str(check.get("check_id") or "")
            if check_id in {"RDMA_USERSPACE", "RDMA_ACTIVE_DEVICE_COUNT", "IB_COUNTER_HEALTH", "ROCE_COUNTER_HEALTH", "IB_ENDPOINT", "ROCE_ENDPOINT"}:
                category = "network_health"
            elif check_id.startswith(("RDMA_", "IB_", "ROCE_", "NETWORK_")):
                category = "network_rdma"
            elif check_id in {"DRIVER_VERSION", "HYCU_DRIVER_MODULE", "DTK_VERSION", "HIP_COMPILER"}:
                category = "driver_dtk"
            elif check_id == "HCU_DEVICE_NODE":
                category = "hardware_devices"
            elif check_id.startswith(("TORCH_", "PYTHON_PACKAGE_", "RCCL_", "UCX", "SOFTWARE_", "DOCKER_")):
                category = "software_components"
            else:
                category = "node_status"
            sections[category].setdefault("checks", []).append(check)

        for finding in record.get("findings") or []:
            code = str(finding.get("reason_code") or "")
            if code.startswith(("VRAM_", "HCU_METRIC_")) or code in {"HCU_BUSY", "HY_SMI_NOT_FOUND", "ROCMINFO_NOT_FOUND"}:
                category = "resource_state"
            elif code.startswith(("RDMA_", "IB_", "ROCE_", "NETWORK_", "NIC_")):
                category = "network_health"
            elif code.startswith(("DTK_", "DRIVER_", "HYCU_", "HIPCC_", "HCU_DEVICE_NODE")):
                category = "driver_dtk"
            elif code.startswith(("TORCH_", "PYTHON_", "RCCL_", "UCX_", "SOFTWARE_", "DOCKER_", "HCUSMI_")):
                category = "software_components"
            elif code.startswith(("SSH_", "REMOTE_", "LOCAL_", "NODE_PROBE_")) or code == "COMMAND_TIMEOUT":
                category = "execution_evidence"
            else:
                category = "node_status"
            sections[category].setdefault("findings", []).append(finding)

        for category, payload in sections.items():
            if "findings" in payload:
                payload["findings"] = fold_findings(payload["findings"])
            by_category[category][node] = payload
        known = {"node", "status", "reachable", "checks", "device_count", "devices", "metric_summary", "findings", "environment", "software_environment", "software_target", "probe_transport", "container_status"}
        unclassified.setdefault(node, {}).update({key: value for key, value in record.items() if key not in known})

    run_fields = ("tool_version", "kind", "started_at", "finished_at", "transport", "evidence_dir", "policy", "execution")
    result: dict[str, Any] = {
        "schema_version": "2.0",
        "run": {key: report[key] for key in run_fields if key in report},
    }
    if shared_command is not None:
        result["run"]["probe_command"] = shared_command
    for category in categories:
        result[category] = group_nodes(by_category[category])
        if category in configurations:
            # Exact evidence folding (`nodes`) may differ due to checks/identity.
            # Static configuration folding is explicitly separate and is the
            # source for the Markdown configuration table.
            result[category]["configuration_nodes"] = group_nodes(configurations[category])["nodes"]
            result[category]["configuration_scope"] = configuration_scope
    result["network_health"]["samples_by_node"] = health_samples
    if any(unclassified.values()):
        result["unclassified"] = {"nodes": unclassified}
    result["cluster"] = {
        key: value for key, value in report.items()
        if key not in set(run_fields) | {"nodes", "schema_version"}
    }
    return result


def run_baremetal_cluster_preflight(
    *,
    nodes: Sequence[str],
    execution_config: BaremetalExecutionConfig,
    policy: BaremetalPreflightPolicy,
    output_dir: Path,
    run_label: str,
    remote_python: str = "python3",
    extra_checks: ClusterExtraCheckConfig | None = None,
    extra_command_wrapper: Callable[[Sequence[str], float], Sequence[str]] | None = None,
    excluded_records: dict[str, dict[str, Any]] | None = None,
    progress: Callable[[str, str], None] | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    policy.validate()
    if not nodes:
        raise ValueError("nodes cannot be empty")
    run_dir = claim_labeled_run_directory(output_dir, run_label)
    execution_config = replace(
        execution_config,
        output_root=run_dir / "evidence",
    )
    started_at = _utc_now()
    excluded = excluded_records or {}
    unknown = set(excluded) - set(nodes)
    if unknown:
        raise ValueError(f"excluded records contain unknown nodes: {sorted(unknown)}")
    probe_nodes = [node for node in nodes if node not in excluded]
    records_by_node: dict[str, dict[str, Any]] = dict(excluded)

    def consume_node_result(result: BaremetalNodeResult) -> None:
        record = evaluate_node_result(result.node, result, policy)
        records_by_node[result.node] = record
        if progress is not None:
            progress(result.node, str(record.get("status") or "UNKNOWN"))

    raw = None
    if probe_nodes:
        executor = BaremetalClusterExecutor(probe_nodes, execution_config)
        command = build_remote_probe_command(
            policy,
            remote_python,
            env_script=policy.env_script,
            execution_scope=policy.execution_scope,
            container_name=policy.container_name,
            container_shell=policy.container_shell,
            container_workdir=policy.container_workdir,
        )
        raw = executor.execute(
            "baremetal-preflight",
            command,
            result_handler=consume_node_result,
            release_output=True,
        )
    records = []
    selection = policy.environment_selection()
    for node in nodes:
        record = records_by_node.get(node)
        if record is None:
            assert raw is not None
            record = evaluate_node_result(node, raw.nodes[node], policy)
        if record.get("software_environment", {}).get("mode") == "NOT_SELECTED":
            record["software_environment"] = {
                **selection.to_dict(),
                "mode": selection.env_mode.value.upper().replace("-", "_"),
                "status": "NOT_CHECKED",
                "message": "node probe did not produce selected software evidence",
            }
        records.append(record)
    conda_plan = apply_conda_collection_plan(records, policy)
    cluster_extra_checks = None
    if probe_nodes and extra_checks is not None and (
        extra_checks.ib_state.enabled
        or extra_checks.nhc.enabled
        or extra_checks.ib.enabled
    ):
        cluster_extra_checks = run_cluster_extra_checks(
            nodes=probe_nodes,
            records=[record for record in records if record["node"] in probe_nodes],
            execution_config=execution_config,
            config=extra_checks,
            output_root=execution_config.output_root / "cluster-extra",
            command_wrapper=extra_command_wrapper,
            cancel_event=execution_config.cancel_event,
        )
    report = build_baremetal_report(
        records=records,
        policy=policy,
        transport=raw.transport if raw is not None else execution_config.transport,
        evidence_dir=raw.run_dir if raw is not None else str(execution_config.output_root),
        started_at=started_at,
        finished_at=_utc_now(),
        conda_collection_plan=conda_plan,
        cluster_extra_checks=cluster_extra_checks,
    )
    json_path = run_dir / "cluster-result.json"
    md_path = run_dir / "cluster-summary.md"
    _write_json(json_path, build_node_first_result(report))
    _write_text(md_path, render_baremetal_markdown(report))
    return report, json_path, md_path
