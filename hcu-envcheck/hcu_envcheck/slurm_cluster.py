# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
import re
import shutil
import subprocess
import uuid
import zlib
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from . import __version__
from .baremetal import (
    BaremetalClusterExecutor,
    BaremetalExecutionConfig,
    BaremetalNodeResult,
    parse_nodes_file,
)
from .cluster_checks import ClusterExtraCheckConfig, run_cluster_extra_checks
from .conda_mode import (
    CondaStorageObservation,
    EnvironmentMode,
    EnvironmentSelection,
    plan_conda_collection,
    validate_environment_selection,
)
from .environment import evaluate_environment
from .models import Finding
from .output import atomic_write_text_exclusive, claim_nodes_check_run_directory
from .parsers import ParseError, parse_hy_smi_samples, parse_rocminfo
from .preflight import evaluate_metrics
from .roce_health import normalize_roce_policy


_SLURM_JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
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
    "rdma_active_device_count",
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
    target_scale_devices: int = 10_000
    rdma_policy: dict[str, Any] | None = None
    rdma_counter_interval_seconds: int = 5

    def validate(self) -> None:
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
        if self.target_scale_devices < 1:
            raise ValueError("target_scale_devices must be at least 1")
        if self.rdma_counter_interval_seconds != 0 and not 1 <= self.rdma_counter_interval_seconds <= 60:
            raise ValueError(
                "rdma_counter_interval_seconds must be 0 or between 1 and 60"
            )
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


RunText = Callable[..., subprocess.CompletedProcess[str]]


def _run_controller_command(argv: Sequence[str], *, runner: RunText) -> list[str]:
    try:
        result = runner(
            list(argv),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"cannot run {' '.join(argv)}: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown error").strip()
        raise RuntimeError(f"{' '.join(argv)} failed: {detail}")
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def discover_slurm_nodes(
    *,
    job_id: str | None = None,
    nodelist: str | None = None,
    runner: RunText = subprocess.run,
) -> list[str]:
    """Resolve one Slurm allocation without starting a job or touching nodes."""

    if (job_id is None) == (nodelist is None):
        raise ValueError("provide exactly one of job_id or nodelist")
    expression = nodelist
    if job_id is not None:
        if not _SLURM_JOB_ID_RE.fullmatch(job_id):
            raise ValueError(f"unsafe Slurm job id: {job_id!r}")
        lines = _run_controller_command(
            ["squeue", "-h", "-j", job_id, "-o", "%N"], runner=runner
        )
        expressions = [line for line in lines if line not in {"(null)", "n/a"}]
        if not expressions:
            raise RuntimeError(f"Slurm job {job_id} has no assigned nodes")
        expression = ",".join(expressions)
    assert expression is not None
    nodes = _run_controller_command(["scontrol", "show", "hostnames", expression], runner=runner)
    if not nodes:
        raise RuntimeError(f"Slurm nodelist {expression!r} expanded to no nodes")
    # BaremetalClusterExecutor performs the final hostname safety validation.
    return list(dict.fromkeys(nodes))


def collect_slurm_node_states(
    nodes: Sequence[str], *, runner: RunText = subprocess.run
) -> dict[str, dict[str, Any]]:
    """Collect scheduler availability and drain reasons for the selected nodes."""

    if not nodes:
        return {}
    lines = _run_controller_command(
        ["sinfo", "-N", "-h", "-n", ",".join(nodes), "-o", "%N|%T|%E"],
        runner=runner,
    )
    states: dict[str, dict[str, Any]] = {}
    unavailable = ("drain", "down", "fail", "maint", "power")
    for line in lines:
        fields = line.split("|", 2)
        if len(fields) < 2:
            continue
        node = fields[0].strip()
        state = fields[1].strip()
        reason = fields[2].strip() if len(fields) > 2 else ""
        reason = None if reason in {"", "none", "(null)", "n/a"} else reason
        candidate = {"state": state, "reason": reason, "raw": line}
        previous = states.get(node)
        if previous is None or (
            any(token in state.lower() for token in unavailable)
            and not any(token in str(previous.get("state", "")).lower() for token in unavailable)
        ):
            states[node] = candidate
    return states


def resolve_baremetal_nodes(
    *,
    nodes: Iterable[str] | None = None,
    nodes_file: Path | None = None,
    slurm_job_id: str | None = None,
    slurm_nodelist: str | None = None,
    runner: RunText = subprocess.run,
) -> list[str]:
    selected = sum(
        value is not None
        for value in (nodes, nodes_file, slurm_job_id, slurm_nodelist)
    )
    if selected != 1:
        raise ValueError(
            "select exactly one node source: nodes, nodes_file, slurm_job_id or slurm_nodelist"
        )
    if nodes is not None:
        result = list(nodes)
    elif nodes_file is not None:
        result = parse_nodes_file(nodes_file)
    else:
        result = discover_slurm_nodes(
            job_id=slurm_job_id,
            nodelist=slurm_nodelist,
            runner=runner,
        )
    if not result:
        raise ValueError("node source contains no nodes")
    return result


_SOFTWARE_PROBE_SOURCE = r"""
import glob
import importlib.metadata
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess


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

root_candidates = []
for variable in ("DTK_PATH", "HIP_PATH", "ROCM_PATH"):
    value = os.environ.get(variable)
    if value:
        root_candidates.append(os.path.realpath(value))
root_candidates.extend((os.path.realpath(os.sys.prefix), "/opt/dtk", "/opt/rocm"))
root_candidates = list(dict.fromkeys(root_candidates))
hipcc = first_executable(
    "hipcc",
    [os.path.join(root, relative) for root in root_candidates for relative in ("bin/hipcc", "hip/bin/hipcc")],
)
ucx_info = first_executable(
    "ucx_info",
    [os.path.join(root, "bin", "ucx_info") for root in (os.path.realpath(os.sys.prefix), "/opt/ucx", "/opt/dtk")],
)
mpirun = first_executable(
    "mpirun",
    [os.path.join(root, "bin", "mpirun") for root in (os.path.realpath(os.sys.prefix), "/opt/mpi", "/opt/dtk")],
)
tools = {}
for name, path, version_args in (
    ("hipcc", hipcc, ["--version"]),
    ("ucx_info", ucx_info, ["-v"]),
    ("mpirun", mpirun, ["--version"]),
):
    if path:
        tools[name] = {"path": path, "version": capture([path] + version_args)}

version_file = None
version_paths = []
for root in root_candidates:
    version_paths.extend((os.path.join(root, ".dtk_version"), os.path.join(root, ".info", "version")))
for candidate in dict.fromkeys(version_paths):
    value = read_file(candidate)
    if value:
        version_file = {"path": candidate, "value": value}
        break
component_versions = {}
for root in root_candidates:
    for name in ("rocm_version", "version-dev", "version-libs", "version-utils"):
        candidate = os.path.join(root, ".info", name)
        value = read_file(candidate)
        if value:
            component_versions[candidate] = value

library_patterns = []
for root in root_candidates:
    library_patterns.extend((
        os.path.join(root, "lib", "librccl.so*"),
        os.path.join(root, "lib", "libnccl.so*"),
        os.path.join(root, "lib", "libamdhip64.so*"),
    ))
library_patterns.extend(("/opt/ucx/lib/libucp.so*", "/opt/ucx/lib/libuct.so*"))
library_paths = sorted({path for pattern in library_patterns for path in glob.glob(pattern)})[:256]

os_release = {}
for line in (read_file("/etc/os-release") or "").splitlines():
    if "=" in line:
        key, value = line.split("=", 1)
        os_release[key] = value.strip().strip('"')

print(json.dumps({
    "schema_version": "1.0",
    "evidence_scope": "SELECTED_TRAINING_TARGET",
    "system": {"os_release": os_release},
    "dtk": {"version_file": version_file, "component_versions": component_versions, "tools": tools},
    "python": python_info,
    "torch": torch_info,
    "libraries": {"paths": library_paths, "resolved": {path: os.path.realpath(path) for path in library_paths}},
    "runtime_env": {
        name: os.environ.get(name)
        for name in ("PATH", "LD_LIBRARY_PATH", "PYTHONPATH", "DTK_PATH", "ROCM_PATH", "HIP_PATH")
    },
}, ensure_ascii=False, separators=(",", ":")))
"""


def build_remote_probe_command(policy: BaremetalPreflightPolicy, remote_python: str) -> list[str]:
    """Build one compressed, dependency-free Python probe for every target node.

    The embedded inventory is the same ``pod_probe.py`` used by the K8s path.
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
        "import os as _hcu_os\n"
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

_default_tool_paths = (
    "/opt/hyhal/bin", "/opt/dtk/bin", "/opt/dtk/hip/bin",
    "/opt/ucx/bin", "/opt/mpi/bin",
)
_current_path = os.environ.get("PATH", "")
os.environ["HCU_ENVCHECK_RDMA_COUNTER_INTERVAL_SECONDS"] = str({policy.rdma_counter_interval_seconds})
os.environ["HCU_ENVCHECK_REQUIRED_PYTHON_PACKAGES"] = {required_packages_json!r}

os.environ["PATH"] = ":".join(
    [path for path in _default_tool_paths if os.path.isdir(path)] + [_current_path]
)

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
    main()
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
if _software_mode == "conda":
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
elif _software_mode == "docker":
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
_hy_smi = _tools.get("hy-smi", {{}}).get("path") or _shutil.which("hy-smi") or _shutil.which("Hy-smi")
_rocminfo = _tools.get("rocminfo", {{}}).get("path") or _shutil.which("rocminfo")
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
for _sample_index in range({policy.samples}):
    _metrics["memory"].append(_capture([_hy_smi, "--showmeminfo", "vram", "--json"] if _hy_smi else [], 20))
    _metrics["available"].append(_capture([_hy_smi, "--showmemavailable", "--json"] if _hy_smi else [], 20))
    _metrics["memory_percent"].append(_capture([_hy_smi, "--showmemuse", "--json"] if _hy_smi else [], 20))
    _metrics["utilization"].append(_capture([_hy_smi, "--showuse", "--json"] if _hy_smi else [], 20))
    if _sample_index + 1 < {policy.samples}:
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
    return [remote_python, "-c", loader]


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

    env_findings, summary, checks, software = evaluate_baremetal_environment(
        environment_payload, policy
    )
    findings = list(env_findings)
    devices = []
    metrics_complete = False

    hy_smi_path = metrics.get("hy_smi_path")
    rocminfo_path = metrics.get("rocminfo_path")
    if not hy_smi_path:
        findings.append(Finding("FAIL", "HY_SMI_NOT_FOUND", "hy-smi/Hy-smi not found on node"))
    if not rocminfo_path:
        findings.append(Finding("FAIL", "ROCMINFO_NOT_FOUND", "rocminfo not found on node"))

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
        findings.append(
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
            findings.extend(metric_findings)
            metrics_complete = True
        except (ParseError, TypeError, ValueError) as exc:
            findings.append(Finding("UNKNOWN", "HCU_METRIC_PARSE_FAILED", str(exc)))

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
        "checks": checks,
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


def apply_slurm_state(
    record: dict[str, Any], state: dict[str, Any] | None, *, required: bool = False
) -> None:
    record["slurm"] = state or {"state": None, "reason": None}
    if not state:
        if required:
            record.setdefault("findings", []).append(
                _result_finding(
                    "UNKNOWN",
                    "SLURM_STATE_MISSING",
                    "selected Slurm node has no scheduler state evidence",
                )
            )
            if record.get("status") == "READY":
                record["status"] = "INCOMPLETE"
        return
    state_text = str(state.get("state") or "").rstrip("*~#$@%^")
    lowered = state_text.lower()
    unavailable = ("drain", "down", "fail", "maint", "power", "invalid")
    unknown = ("unknown", "no_respond", "reboot", "completing")
    if any(token in lowered for token in unknown):
        reason = state.get("reason") or "scheduler state is not stable"
        record.setdefault("findings", []).append(
            _result_finding(
                "UNKNOWN",
                "SLURM_NODE_STATE_UNKNOWN",
                f"Slurm state={state_text}, reason={reason}",
            )
        )
        if record.get("status") == "READY":
            record["status"] = "INCOMPLETE"
        return
    if not any(token in lowered for token in unavailable):
        return
    reason = state.get("reason") or "scheduler did not provide a reason"
    severity = "FAIL" if record.get("reachable") else "UNKNOWN"
    record.setdefault("findings", []).append(
        _result_finding(
            severity,
            "SLURM_NODE_UNAVAILABLE",
            f"Slurm state={state_text}, reason={reason}",
        )
    )
    if record.get("reachable"):
        record["status"] = "BLOCKED"


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
            "slurm_state": record.get("slurm", {}).get("state"),
            "slurm_reason": record.get("slurm", {}).get("reason"),
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
    fields = (
        "container_os",
        "kernel",
        "cpu_logical_count",
        "cpu_models",
        "mem_total",
        "dtk_version",
        "driver_version",
        "hy_smi_version",
        "smi_library_version",
        "nic_inventory",
        "rdma_nic_inventory",
        "rdma_device_count",
        "rdma_active_device_count",
        "rdma_active_port_count",
        "rdma_current_protocol",
        "rdma_protocol_status",
        "rdma_hardware_protocol_capability",
        "rdma_fabric_profile",
        "rdma_protocol_profile",
        "ib_endpoint",
        "roce_endpoint",
        "rdma_rates",
    )
    for record in records:
        if not record.get("reachable") or not record.get("environment"):
            continue
        env = record["environment"]
        visible = {field: env.get(field) for field in fields}
        counters = env.get("ib_counter_health") or {}
        sampling = counters.get("sampling") or {}
        visible["ib_counter_health"] = {
            key: counters.get(key)
            for key in ("status", "observed_status", "required", "ports", "status_counts", "reason_codes")
        }
        visible["ib_counter_health"]["sampling"] = {
            key: sampling.get(key)
            for key in ("status", "interval_seconds", "reason_code")
            if key in sampling
        }
        userspace = env.get("rdma_userspace") or {}
        libraries = userspace.get("libraries") or {}
        visible["rdma_userspace"] = {
            key: userspace.get(key)
            for key in ("status", "check_status", "reason_code", "sysfs_devices", "enumerated_devices", "missing_enumerated_devices")
        }
        visible["rdma_userspace"]["libraries"] = {
            key: libraries.get(key)
            for key in ("libibverbs", "providers", "rccl_net_plugins")
        }
        roce_health = env.get("roce_configuration_health") or {}
        visible["roce_configuration_health"] = {
            "status": roce_health.get("status"),
            "policy_applied": roce_health.get("policy_applied"),
            "normalized_policy": roce_health.get("normalized_policy"),
            "summary": roce_health.get("summary"),
        }
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
    records: list[dict[str, Any]], *, strict: bool
) -> list[dict[str, Any]]:
    usable = [record for record in records if record.get("reachable") and record.get("environment")]
    if len(usable) < 2:
        return []
    findings: list[dict[str, Any]] = []
    for field in _CONSISTENCY_FIELDS:
        values: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for record in usable:
            if field not in record["environment"] or record["environment"].get(field) is None:
                missing.append(record["node"])
                continue
            value = record["environment"][field]
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
    records: list[dict[str, Any]], *, target_devices: int
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
    if blocked_nodes:
        status = "NOT_READY"
        conclusion = "本次样本存在启动前阻断项，不具备扩大训练规模的放行条件。"
    elif incomplete_nodes:
        status = "NOT_VERIFIED"
        conclusion = "本次样本证据不完整，不能判断目标规模可用性。"
    elif checked_devices < target_devices:
        status = "SAMPLE_READY_FULL_SCALE_UNVERIFIED"
        conclusion = "本次样本静态检查通过；目标规模、训练和通信数据面仍未验证。"
    else:
        status = "FULL_SCALE_STATIC_PREFLIGHT_PASSED_RUNTIME_UNVERIFIED"
        conclusion = "目标卡数完成静态检查；训练和通信数据面仍未验证。"
    return {
        "status": status,
        "target_devices": target_devices,
        "checked_nodes": len(completed),
        "checked_devices": checked_devices,
        "ready_nodes": len(ready),
        "ready_devices": sum(int(item.get("device_count") or 0) for item in ready),
        "blocking_nodes": blocked_nodes,
        "incomplete_nodes": incomplete_nodes,
        "coverage_percent": round(min(100.0, checked_devices * 100.0 / target_devices), 3),
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
    scale = report.get("scale_assessment") or {}
    lines = [
        "# 裸金属 / Slurm HCU训练启动前环境检查",
        "",
        f"- 工具版本：`{report.get('tool_version', '-')}`",
        f"- 总体状态：`{report['status']}`",
        f"- 检查节点：{report['summary']['node_count']}；可达={report['summary']['reachable_nodes']}；不可达={report['summary']['unreachable_nodes']}",
        f"- HCU：已识别={report['summary']['detected_devices']}；预期={report['summary']['expected_devices_total']}；目标规模={report['policy']['target_scale_devices']}",
        f"- 远程执行：{report['transport']}（auto模式优先clush，不可用时回退SSH）",
        f"- 软件环境：{report['software_environment']['message']}",
        f"- 目标规模静态判断：`{scale.get('status', 'NOT_VERIFIED')}`（不是训练或collective实测）",
        "",
        "## 节点检查结果（相同结果折叠）",
        "",
        "| 节点 | 状态 | Slurm状态/原因 | 可达 | HCU数 | 最大显存% | 最大利用率% | 原因码 | 软件环境 |",
        "|---|---|---|---:|---:|---:|---:|---|---|",
    ]
    for group in report.get("node_result_groups", []):
        lines.append(
            "| "
            + ", ".join(group["nodes"])
            + f" | {group['status']} | {_md_cell(group.get('slurm_state'))}/{_md_cell(group.get('slurm_reason'))} | {'是' if group['reachable'] else '否'} | {_fmt(group['device_count'])} | {_fmt(group.get('max_vram_used_percent'))} | {_fmt(group.get('max_hcu_util_percent'))} | "
            + (", ".join(group["reason_codes"]) or "-")
            + f" | {group.get('software_status') or '-'} |"
        )

    lines.extend(["", "## 硬件、驱动与通信环境（相同结果折叠）", ""])
    for group in report.get("hardware_groups", []):
        lines.extend(
            [
                f"### {', '.join(group['nodes'])}",
                "",
                f"- OS / Kernel：{_fmt(group.get('container_os'))} / {_fmt(group.get('kernel'))}",
                f"- CPU / 内存：逻辑核={_fmt(group.get('cpu_logical_count'))}；型号={_fmt(group.get('cpu_models'))}；内存={_fmt(group.get('mem_total'))}",
                f"- DTK / 驱动：{_fmt(group.get('dtk_version'))} / {_fmt(group.get('driver_version'))}",
                f"- HCU：{_fmt(group.get('hcu_profile'))}",
            ]
        )
        nic_items = group.get("nic_inventory") or []
        rdma_items = group.get("rdma_nic_inventory") or []
        lines.append("- 网卡：" + ("；".join(_format_adapter(item) for item in nic_items) or "未采集"))
        lines.append("- RDMA HCA：" + ("；".join(_format_adapter(item) for item in rdma_items) or "未采集"))
        lines.append(
            f"- RDMA状态：设备={_fmt(group.get('rdma_device_count'))}；活跃设备={_fmt(group.get('rdma_active_device_count'))}；活跃端口={_fmt(group.get('rdma_active_port_count'))}；速率={_fmt(group.get('rdma_rates'))}"
        )
        ib = group.get("ib_endpoint") or {}
        roce = group.get("roce_endpoint") or {}
        ib_counters = group.get("ib_counter_health") or {}
        rdma_userspace = group.get("rdma_userspace") or {}
        roce_health = group.get("roce_configuration_health") or {}
        lines.append(
            f"- RDMA当前端口模式：{_fmt(group.get('rdma_current_protocol'))}；"
            f"协议检查={_fmt(group.get('rdma_protocol_status'))}；"
            f"硬件支持模式={_fmt(group.get('rdma_hardware_protocol_capability'))}"
        )
        lines.append(
            f"- RDMA userspace Verbs: {_fmt(rdma_userspace.get('check_status') or rdma_userspace.get('status'))}; "
            f"sysfs={_fmt(rdma_userspace.get('sysfs_devices'))}; "
            f"verbs={_fmt(rdma_userspace.get('enumerated_devices'))}; "
            f"reason={_fmt(rdma_userspace.get('reason_code'))}"
        )
        lines.append(
            f"- IB counter health: {_fmt(ib_counters.get('status'))}; "
            f"ports={_fmt(ib_counters.get('ports'))}; "
            f"counts={_fmt(ib_counters.get('status_counts'))}; "
            f"reasons={_fmt(ib_counters.get('reason_codes'))}"
        )
        lines.append(
            f"- IB端点：{_fmt(ib.get('status'))}，端口={_fmt(ib.get('ports'))}，"
            f"Active+LinkUp={_fmt(ib.get('active_linkup_ports'))}，"
            f"有效LID/SM-LID/GID/P_Key={_fmt(ib.get('valid_lid_ports'))}/"
            f"{_fmt(ib.get('valid_sm_lid_ports'))}/{_fmt(ib.get('valid_gid_ports'))}/"
            f"{_fmt(ib.get('valid_pkey_ports'))}，MTU={_fmt(ib.get('active_mtus'))}，"
            f"Subnet={_fmt(ib.get('subnet_prefixes'))}"
        )
        lines.append(
            f"- RoCE端点：{_fmt(roce.get('status'))}，端口={_fmt(roce.get('ports'))}，"
            f"版本={_fmt(roce.get('versions'))}，netdev={_fmt(roce.get('netdevs'))}，"
            f"MTU={_fmt(roce.get('mtus'))}"
        )
        lines.append(f"- RoCE configuration chain: {_fmt(roce_health.get('status'))}; policy_applied={_fmt(roce_health.get('policy_applied'))}; summary={_fmt(roce_health.get('summary'))}")
        lines.append(f"- RoCE主机QoS/DCB：{_fmt(roce.get('dcb_status'))}；交换机侧QoS：NOT_VERIFIED")
        lines.append("- 训练实际RDMA/TCP数据路径：NOT_VERIFIED_BY_PREFLIGHT")
        lines.append("")

    extra = report.get("cluster_extra_checks") or {}
    if extra.get("enabled"):
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

    lines.extend(
        [
            "## 目标规模静态适用性",
            "",
            f"- 状态：`{scale.get('status', 'NOT_VERIFIED')}`",
            f"- 目标：{scale.get('target_devices', '-')} 张HCU",
            f"- 完整识别：{scale.get('checked_nodes', 0)} 个节点、{scale.get('checked_devices', 0)} 张HCU；覆盖={scale.get('coverage_percent', 0)}%",
            f"- 节点级READY：{scale.get('ready_nodes', 0)} 个节点、{scale.get('ready_devices', 0)} 张HCU",
            f"- 阻断节点：{', '.join(scale.get('blocking_nodes', [])) or '无'}",
            f"- 证据不完整节点：{', '.join(scale.get('incomplete_nodes', [])) or '无'}",
            f"- 结论：{scale.get('conclusion', '证据不足。')}",
            "",
        ]
    )

    lines.extend(["## 跨节点硬件一致性", ""])
    if report.get("consistency_findings"):
        for finding in report["consistency_findings"]:
            lines.append(
                f"- `{finding['severity']}` {finding['reason_code']}：字段={finding['field']}；节点={', '.join(finding['nodes'])}"
            )
    else:
        lines.append("- 已采集字段未发现跨节点差异。")

    lines.extend(["", "## 节点异常明细", ""])
    abnormal = report.get("finding_groups", [])
    if not abnormal:
        lines.append("- 无阻断、未知或告警项。")
    for finding in abnormal:
        device_text = (
            f"；device={finding['device_id']}" if finding.get("device_id") is not None else ""
        )
        lines.append(
            f"- {', '.join(finding['nodes'])}：`{finding['severity']}` "
            f"`{finding['reason_code']}`{device_text}；{finding['message']}"
        )
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
        records, strict=policy.strict_hardware_consistency
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
        "kind": "baremetal_slurm_cluster_preflight",
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


def run_baremetal_cluster_preflight(
    *,
    nodes: Sequence[str],
    execution_config: BaremetalExecutionConfig,
    policy: BaremetalPreflightPolicy,
    output_dir: Path,
    remote_python: str = "python3",
    slurm_node_states: dict[str, dict[str, Any]] | None = None,
    extra_checks: ClusterExtraCheckConfig | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    policy.validate()
    if not nodes:
        raise ValueError("nodes cannot be empty")
    run_dir = claim_nodes_check_run_directory(output_dir)
    execution_config = replace(
        execution_config,
        output_root=run_dir / "evidence",
    )
    started_at = _utc_now()
    executor = BaremetalClusterExecutor(nodes, execution_config)
    command = build_remote_probe_command(policy, remote_python)
    records_by_node: dict[str, dict[str, Any]] = {}

    def consume_node_result(result: BaremetalNodeResult) -> None:
        records_by_node[result.node] = evaluate_node_result(result.node, result, policy)

    raw = executor.execute(
        "baremetal-preflight",
        command,
        result_handler=consume_node_result,
        release_output=True,
    )
    records = []
    selection = policy.environment_selection()
    for node in executor.nodes:
        record = records_by_node.get(node)
        if record is None:
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
    for record in records:
        apply_slurm_state(
            record,
            (slurm_node_states or {}).get(record["node"]),
            required=slurm_node_states is not None,
        )
    cluster_extra_checks = None
    if extra_checks is not None and (
        extra_checks.ib_state.enabled
        or extra_checks.nhc.enabled
        or extra_checks.ib.enabled
    ):
        cluster_extra_checks = run_cluster_extra_checks(
            nodes=executor.nodes,
            records=records,
            execution_config=execution_config,
            config=extra_checks,
            output_root=execution_config.output_root / "cluster-extra",
        )
    report = build_baremetal_report(
        records=records,
        policy=policy,
        transport=raw.transport,
        evidence_dir=raw.run_dir,
        started_at=started_at,
        finished_at=_utc_now(),
        conda_collection_plan=conda_plan,
        cluster_extra_checks=cluster_extra_checks,
    )
    json_path = run_dir / "cluster-result.json"
    md_path = run_dir / "cluster-summary.md"
    _write_json(json_path, report)
    _write_text(md_path, render_baremetal_markdown(report))
    return report, json_path, md_path
