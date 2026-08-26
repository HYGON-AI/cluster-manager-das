# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Conda execution-mode policy and collection planning.

This module intentionally has no transport or CLI dependency.  A caller can
validate a Docker/Conda selection, collect one :class:`CondaStorageObservation`
per expected node with its existing executor, and then use
``plan_conda_collection`` to decide where artifact and runtime probes run.

The important invariant is that a shared Conda artifact and a per-node runtime
are different evidence scopes.  Sharing may reduce artifact metadata reads; it
never turns one node's runtime result into an all-node result.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping, Sequence


class ConfigurationError(ValueError):
    """Raised before any remote connection when environment options conflict."""


class EnvironmentMode(str, Enum):
    HOST_PYTHON = "host-python"
    CONDA = "conda"
    DOCKER = "docker"


class CondaStorage(str, Enum):
    NODE_LOCAL = "node-local"
    SHARED = "shared"


class StorageScope(str, Enum):
    NODE_LOCAL = "node-local"
    SHARED = "shared"
    UNKNOWN = "unknown"


_COLLECTION_STATES = {
    "SUCCESS",
    "BLOCKED",
    "TIMEOUT",
    "ERROR",
    "UNSUPPORTED",
    "NOT_RUN",
}
_RESULT_STATES = {"PASS", "FAIL", "WARN", "UNKNOWN", "SKIP"}
_SHARED_FILESYSTEMS = frozenset(
    {
        "nfs",
        "nfs4",
        "lustre",
        "gpfs",
        "beegfs",
        "ceph",
        "cephfs",
        "glusterfs",
        "cifs",
        "smb3",
        "panfs",
        "wekafs",
        "fuse.sshfs",
    }
)
_LOCAL_FILESYSTEMS = frozenset(
    {
        "ext2",
        "ext3",
        "ext4",
        "xfs",
        "btrfs",
        "zfs",
        "f2fs",
        "tmpfs",
        "ramfs",
    }
)
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")


def _required_text(value: str | None, field: str) -> str:
    if value is None or not value.strip():
        raise ConfigurationError(f"{field} must not be empty")
    if _CONTROL_CHARACTER.search(value):
        raise ConfigurationError(f"{field} contains a control character")
    return value.strip()


def _normalise_prefix(value: str | None) -> str:
    raw = _required_text(value, "conda_prefix")
    path = PurePosixPath(raw)
    if not path.is_absolute() or path == PurePosixPath("/"):
        raise ConfigurationError("conda_prefix must be an absolute non-root POSIX path")
    if ".." in path.parts:
        raise ConfigurationError("conda_prefix must not contain '..'")
    return str(path)


def _normalise_image(value: str | None) -> str:
    image = _required_text(value, "image")
    if any(character.isspace() for character in image) or image.startswith("-"):
        raise ConfigurationError("image is not a safe container image reference")
    return image


@dataclass(frozen=True)
class EnvironmentSelection:
    """Validated mutually-exclusive environment selection."""

    env_mode: EnvironmentMode
    conda_prefix: str | None = None
    conda_storage: CondaStorage | None = None
    image: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "env_mode": self.env_mode.value,
            "conda_prefix": self.conda_prefix,
            "conda_storage": self.conda_storage.value if self.conda_storage else None,
            "image": self.image,
        }


def validate_environment_selection(
    *,
    env_mode: str,
    conda_prefix: str | None = None,
    conda_storage: str | None = None,
    image: str | None = None,
) -> EnvironmentSelection:
    """Validate Conda/Docker XOR semantics without touching a remote system.

    Conda storage is deliberately mandatory.  Guessing whether a prefix is
    shared changes collection and reporting semantics and is therefore unsafe.
    """

    try:
        mode = EnvironmentMode(env_mode)
    except ValueError as exc:
        raise ConfigurationError(
            "env_mode must be 'host-python', 'conda' or 'docker'"
        ) from exc

    if mode is EnvironmentMode.HOST_PYTHON:
        if conda_prefix is not None or conda_storage is not None or image is not None:
            raise ConfigurationError(
                "conda_prefix, conda_storage and image are forbidden "
                "when env_mode=host-python"
            )
        return EnvironmentSelection(mode)

    if mode is EnvironmentMode.CONDA:
        if image is not None:
            raise ConfigurationError("image is forbidden when env_mode=conda")
        prefix = _normalise_prefix(conda_prefix)
        if conda_storage is None:
            raise ConfigurationError(
                "conda_storage is required when env_mode=conda "
                "(node-local or shared)"
            )
        try:
            storage = CondaStorage(conda_storage)
        except ValueError as exc:
            raise ConfigurationError(
                "conda_storage must be 'node-local' or 'shared'"
            ) from exc
        return EnvironmentSelection(mode, prefix, storage, None)

    if conda_prefix is not None or conda_storage is not None:
        raise ConfigurationError(
            "conda_prefix and conda_storage are forbidden when env_mode=docker"
        )
    return EnvironmentSelection(mode, None, None, _normalise_image(image))


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if _CONTROL_CHARACTER.search(cleaned):
        raise ValueError("observation contains a control character")
    return cleaned


def infer_storage_scope(fs_type: str | None) -> StorageScope:
    """Return a conservative backing scope inferred from a filesystem type."""

    normalised = (fs_type or "").strip().lower()
    if normalised in _SHARED_FILESYSTEMS:
        return StorageScope.SHARED
    if normalised in _LOCAL_FILESYSTEMS:
        return StorageScope.NODE_LOCAL
    return StorageScope.UNKNOWN


@dataclass(frozen=True)
class MountIdentity:
    mount_point: str
    mount_source: str
    fs_type: str


def _mountinfo_unescape(value: str) -> str:
    replacements = {
        r"\040": " ",
        r"\011": "\t",
        r"\012": "\n",
        r"\134": "\\",
    }
    for escaped, unescaped in replacements.items():
        value = value.replace(escaped, unescaped)
    return value


def parse_mountinfo(mountinfo: str, path: str) -> MountIdentity | None:
    """Find the deepest Linux mountinfo entry containing *path*.

    Malformed lines are skipped.  This parser is data-only and does not invoke
    ``findmnt`` or a shell, which makes it suitable for a restricted probe.
    """

    target = posixpath.normpath(_normalise_prefix(path))
    matches: list[MountIdentity] = []
    for line in mountinfo.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
            mount_point = _mountinfo_unescape(fields[4])
            fs_type = fields[separator + 1]
            source = _mountinfo_unescape(fields[separator + 2])
        except (ValueError, IndexError):
            continue
        mount_point = posixpath.normpath(mount_point)
        if target == mount_point or target.startswith(mount_point.rstrip("/") + "/"):
            matches.append(MountIdentity(mount_point, source, fs_type.lower()))
    if not matches:
        return None
    return max(matches, key=lambda item: len(item.mount_point))


@dataclass(frozen=True)
class CondaStorageObservation:
    """Low-cost, per-node evidence about one requested Conda prefix."""

    node: str
    prefix: str
    prefix_exists: bool
    python_executable: bool
    realpath: str | None = None
    mount_source: str | None = None
    fs_type: str | None = None
    identity_fingerprint: str | None = None
    collection_status: str = "SUCCESS"
    reason_code: str | None = None
    shared_backend: bool | None = None

    def __post_init__(self) -> None:
        _required_text(self.node, "node")
        _normalise_prefix(self.prefix)
        if self.collection_status not in _COLLECTION_STATES:
            raise ValueError(f"unsupported collection_status={self.collection_status!r}")
        for value in (
            self.realpath,
            self.mount_source,
            self.fs_type,
            self.identity_fingerprint,
            self.reason_code,
        ):
            _clean_optional(value)

    @property
    def storage_scope(self) -> StorageScope:
        if self.shared_backend is True:
            return StorageScope.SHARED
        if self.shared_backend is False:
            return StorageScope.NODE_LOCAL
        return infer_storage_scope(self.fs_type)


@dataclass(frozen=True)
class CondaStorageCohort:
    cohort_id: str
    storage_scope: StorageScope
    nodes: tuple[str, ...]
    representative_node: str | None
    status: str
    reason_code: str
    mount_source: str | None
    fs_type: str | None
    realpath: str | None
    identity_fingerprint: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cohort_id": self.cohort_id,
            "storage_scope": self.storage_scope.value,
            "nodes": list(self.nodes),
            "representative_node": self.representative_node,
            "status": self.status,
            "reason_code": self.reason_code,
            "mount_source": self.mount_source,
            "fs_type": self.fs_type,
            "realpath": self.realpath,
            "identity_fingerprint": self.identity_fingerprint,
        }


@dataclass(frozen=True)
class CondaPlanFinding:
    severity: str
    reason_code: str
    message: str
    nodes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "reason_code": self.reason_code,
            "message": self.message,
            "nodes": list(self.nodes),
        }


@dataclass(frozen=True)
class CondaCollectionPlan:
    declared_storage_mode: CondaStorage
    observed_storage_mode: str
    expected_nodes: tuple[str, ...]
    storage_observed_nodes: tuple[str, ...]
    storage_cohorts: tuple[CondaStorageCohort, ...]
    artifact_probe_nodes: tuple[str, ...]
    runtime_target_nodes: tuple[str, ...]
    runtime_probe_nodes: tuple[str, ...]
    findings: tuple[CondaPlanFinding, ...]

    @property
    def artifact_cohort_count(self) -> int:
        return sum(1 for cohort in self.storage_cohorts if cohort.representative_node)

    def to_dict(self) -> dict[str, Any]:
        return {
            "declared_storage_mode": self.declared_storage_mode.value,
            "observed_storage_mode": self.observed_storage_mode,
            "expected_nodes": list(self.expected_nodes),
            "storage_observed_nodes": list(self.storage_observed_nodes),
            "storage_cohorts": [cohort.to_dict() for cohort in self.storage_cohorts],
            "artifact_probe_nodes": list(self.artifact_probe_nodes),
            "runtime_target_nodes": list(self.runtime_target_nodes),
            "runtime_probe_nodes": list(self.runtime_probe_nodes),
            "coverage": {
                "storage_identity": {
                    "covered": len(self.storage_observed_nodes),
                    "expected": len(self.expected_nodes),
                },
                "artifact_metadata": {
                    "planned": len(self.artifact_probe_nodes),
                    "storage_cohorts": self.artifact_cohort_count,
                    "unit": "storage_cohort",
                },
                "node_runtime": {
                    "planned": len(self.runtime_probe_nodes),
                    "expected": len(self.runtime_target_nodes),
                    "unit": "node",
                },
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def _cohort_digest(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "conda-storage-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def _observation_problem(
    observation: CondaStorageObservation,
    expected_prefix: str,
) -> tuple[str, str] | None:
    if observation.collection_status != "SUCCESS":
        return "UNKNOWN", observation.reason_code or "CONDA_STORAGE_COLLECTION_INCOMPLETE"
    if _normalise_prefix(observation.prefix) != expected_prefix:
        return "FAIL", "CONDA_PREFIX_MISMATCH"
    if not observation.prefix_exists:
        return "FAIL", "CONDA_PREFIX_UNREACHABLE"
    if not observation.python_executable:
        return "FAIL", "CONDA_PYTHON_NOT_EXECUTABLE"
    return None


def _identity_payload(
    observation: CondaStorageObservation,
    *,
    force_node_identity: bool,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "storage_scope": observation.storage_scope.value,
        "mount_source": _clean_optional(observation.mount_source),
        "fs_type": (_clean_optional(observation.fs_type) or "").lower() or None,
        "realpath": _clean_optional(observation.realpath),
        "identity_fingerprint": _clean_optional(observation.identity_fingerprint),
    }
    if force_node_identity:
        payload["node"] = observation.node
    return payload


def _storage_identity_complete(observation: CondaStorageObservation) -> bool:
    return all(
        _clean_optional(value)
        for value in (
            observation.mount_source,
            observation.fs_type,
            observation.realpath,
            observation.identity_fingerprint,
        )
    )


def _observed_mode(
    cohorts: Sequence[CondaStorageCohort],
    expected_nodes: Sequence[str],
    observed_nodes: Sequence[str],
) -> str:
    if set(observed_nodes) != set(expected_nodes):
        return "unknown"
    usable = [cohort for cohort in cohorts if cohort.representative_node is not None]
    if (
        any(cohort.status == "UNKNOWN" for cohort in cohorts)
        or any(cohort.representative_node is None for cohort in cohorts)
        or not usable
    ):
        return "unknown"
    if len(usable) == 1 and usable[0].storage_scope is StorageScope.SHARED:
        return CondaStorage.SHARED.value
    if usable and all(
        cohort.storage_scope is StorageScope.NODE_LOCAL and len(cohort.nodes) == 1
        for cohort in usable
    ):
        return CondaStorage.NODE_LOCAL.value
    return "split"


def plan_conda_collection(
    selection: EnvironmentSelection,
    *,
    expected_nodes: Sequence[str],
    observations: Iterable[CondaStorageObservation],
) -> CondaCollectionPlan:
    """Build a deterministic, evidence-scoped Conda collection plan.

    ``artifact_probe_nodes`` contains one node per observed shared artifact but
    every node for node-local environments. ``runtime_target_nodes`` always
    keeps the original denominator; ``runtime_probe_nodes`` contains only nodes
    whose prefix and Python are currently executable.
    """

    if selection.env_mode is not EnvironmentMode.CONDA:
        raise ConfigurationError("Conda collection planning requires env_mode=conda")
    if selection.conda_prefix is None or selection.conda_storage is None:
        raise ConfigurationError("selection is missing validated Conda options")

    expected = tuple(sorted(_required_text(node, "node") for node in expected_nodes))
    if not expected:
        raise ValueError("expected_nodes must not be empty")
    if len(set(expected)) != len(expected):
        raise ValueError("expected_nodes contains duplicates")

    by_node: dict[str, CondaStorageObservation] = {}
    for observation in observations:
        if observation.node not in expected:
            raise ValueError(f"observation for out-of-scope node {observation.node!r}")
        if observation.node in by_node:
            raise ValueError(f"duplicate observation for node {observation.node!r}")
        by_node[observation.node] = observation

    grouped: dict[str, dict[str, Any]] = {}
    findings: list[CondaPlanFinding] = []
    runtime_probe_nodes: list[str] = []

    for node in expected:
        observation = by_node.get(node)
        if observation is None:
            payload = {"storage_scope": "unknown", "node": node, "missing": True}
            key = json.dumps(payload, sort_keys=True)
            grouped[key] = {
                "payload": payload,
                "nodes": [node],
                "status": "UNKNOWN",
                "reason_code": "CONDA_STORAGE_OBSERVATION_MISSING",
                "representative": None,
            }
            findings.append(
                CondaPlanFinding(
                    "UNKNOWN",
                    "CONDA_STORAGE_OBSERVATION_MISSING",
                    "no Conda storage observation was collected",
                    (node,),
                )
            )
            continue

        problem = _observation_problem(observation, selection.conda_prefix)
        if problem is not None:
            status, reason_code = problem
            payload = {
                **_identity_payload(observation, force_node_identity=True),
                "problem": reason_code,
            }
            key = json.dumps(payload, sort_keys=True)
            grouped[key] = {
                "payload": payload,
                "nodes": [node],
                "status": status,
                "reason_code": reason_code,
                "representative": None,
            }
            findings.append(
                CondaPlanFinding(
                    status,
                    reason_code,
                    "Conda prefix cannot be used for a runtime probe on this node",
                    (node,),
                )
            )
            continue

        runtime_probe_nodes.append(node)
        scope = observation.storage_scope
        identity_complete = _storage_identity_complete(observation)

        # Local and unknown filesystems are never coalesced across nodes.  This
        # prevents identical local clones from masquerading as one shared env.
        force_node = scope is not StorageScope.SHARED or not identity_complete
        payload = _identity_payload(observation, force_node_identity=force_node)
        key = json.dumps(payload, sort_keys=True, separators=(",", ":"))

        if not identity_complete or scope is StorageScope.UNKNOWN:
            status = "UNKNOWN"
            reason_code = (
                "SHARED_IDENTITY_UNKNOWN"
                if selection.conda_storage is CondaStorage.SHARED
                else "NODE_LOCAL_IDENTITY_UNKNOWN"
            )
        elif scope.value != selection.conda_storage.value:
            status = "FAIL"
            reason_code = "CONDA_STORAGE_MODE_MISMATCH"
        else:
            status = "PASS"
            reason_code = (
                "SHARED_IDENTITY_MATCH"
                if scope is StorageScope.SHARED
                else "NODE_LOCAL_IDENTITY_CONFIRMED"
            )

        group = grouped.setdefault(
            key,
            {
                "payload": payload,
                "nodes": [],
                "status": status,
                "reason_code": reason_code,
                "representative": node,
            },
        )
        group["nodes"].append(node)
        if status != "PASS":
            findings.append(
                CondaPlanFinding(
                    status,
                    reason_code,
                    "observed Conda backing storage does not prove the declared mode",
                    (node,),
                )
            )

    cohorts: list[CondaStorageCohort] = []
    for group in grouped.values():
        payload = group["payload"]
        nodes = tuple(sorted(group["nodes"]))
        scope = StorageScope(payload.get("storage_scope", "unknown"))
        cohorts.append(
            CondaStorageCohort(
                cohort_id=_cohort_digest(payload),
                storage_scope=scope,
                nodes=nodes,
                representative_node=group["representative"],
                status=group["status"],
                reason_code=group["reason_code"],
                mount_source=payload.get("mount_source"),
                fs_type=payload.get("fs_type"),
                realpath=payload.get("realpath"),
                identity_fingerprint=payload.get("identity_fingerprint"),
            )
        )
    cohorts.sort(key=lambda item: (item.nodes, item.cohort_id))

    observed_nodes = tuple(
        sorted(
            node
            for node, observation in by_node.items()
            if _observation_problem(observation, selection.conda_prefix) is None
            and _storage_identity_complete(observation)
        )
    )
    observed_mode = _observed_mode(cohorts, expected, observed_nodes)
    if (
        selection.conda_storage is CondaStorage.SHARED
        and observed_mode == "split"
        and not any(item.reason_code == "SHARED_ENV_SPLIT" for item in findings)
    ):
        findings.append(
            CondaPlanFinding(
                "FAIL",
                "SHARED_ENV_SPLIT",
                "the declared shared Conda prefix resolves to multiple storage artifacts",
                expected,
            )
        )
    elif (
        observed_mode in {CondaStorage.NODE_LOCAL.value, CondaStorage.SHARED.value}
        and observed_mode != selection.conda_storage.value
        and not any(item.reason_code == "CONDA_STORAGE_MODE_MISMATCH" for item in findings)
    ):
        findings.append(
            CondaPlanFinding(
                "FAIL",
                "CONDA_STORAGE_MODE_MISMATCH",
                f"declared={selection.conda_storage.value}, observed={observed_mode}",
                expected,
            )
        )

    artifact_nodes = tuple(
        sorted(
            cohort.representative_node
            for cohort in cohorts
            if cohort.representative_node is not None
        )
    )
    return CondaCollectionPlan(
        declared_storage_mode=selection.conda_storage,
        observed_storage_mode=observed_mode,
        expected_nodes=expected,
        storage_observed_nodes=observed_nodes,
        storage_cohorts=tuple(cohorts),
        artifact_probe_nodes=artifact_nodes,
        runtime_target_nodes=expected,
        runtime_probe_nodes=tuple(sorted(runtime_probe_nodes)),
        findings=tuple(findings),
    )


def plan_deep_runtime_representatives(
    plan: CondaCollectionPlan,
    *,
    host_cohort_by_node: Mapping[str, str],
) -> tuple[str, ...]:
    """Choose one node per ``storage artifact × host`` runtime cohort.

    This is a sampling plan, not an all-node PASS.  Callers must report the
    resulting evidence as cohort-scoped unless every target node is executed.
    """

    expected = set(plan.runtime_target_nodes)
    supplied = set(host_cohort_by_node)
    if supplied != expected:
        missing = sorted(expected - supplied)
        extra = sorted(supplied - expected)
        raise ValueError(f"host cohort mapping mismatch: missing={missing}, extra={extra}")

    storage_by_node = {
        node: cohort.cohort_id
        for cohort in plan.storage_cohorts
        if cohort.representative_node is not None
        for node in cohort.nodes
    }
    groups: dict[tuple[str, str], list[str]] = {}
    for node in plan.runtime_probe_nodes:
        storage_cohort = storage_by_node.get(node)
        if storage_cohort is None:
            continue
        host_cohort = _required_text(host_cohort_by_node[node], "host_cohort")
        groups.setdefault((storage_cohort, host_cohort), []).append(node)
    return tuple(sorted(min(nodes) for nodes in groups.values()))


@dataclass(frozen=True)
class CondaRuntimeObservation:
    """One node's runtime result; artifact projection is intentionally absent."""

    node: str
    status: str
    reason_code: str
    normalized_result: Mapping[str, Any]

    def __post_init__(self) -> None:
        _required_text(self.node, "node")
        if self.status not in _RESULT_STATES:
            raise ValueError(f"unsupported runtime status={self.status!r}")
        _required_text(self.reason_code, "reason_code")
        _canonical_result(self.normalized_result)


@dataclass(frozen=True)
class CondaRuntimeGroup:
    status: str
    reason_code: str
    normalized_result: Mapping[str, Any]
    nodes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason_code": self.reason_code,
            "normalized_result": dict(self.normalized_result),
            "nodes": list(self.nodes),
        }


def _canonical_result(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("normalized_result must be JSON-serializable without NaN") from exc


def group_runtime_observations(
    observations: Iterable[CondaRuntimeObservation],
) -> tuple[CondaRuntimeGroup, ...]:
    """Fold nodes only when status, reason and normalized result are identical."""

    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen_nodes: set[str] = set()
    for observation in observations:
        if observation.node in seen_nodes:
            raise ValueError(f"duplicate runtime observation for {observation.node!r}")
        seen_nodes.add(observation.node)
        canonical = _canonical_result(observation.normalized_result)
        key = (observation.status, observation.reason_code, canonical)
        group = groups.setdefault(
            key,
            {
                "status": observation.status,
                "reason_code": observation.reason_code,
                "normalized_result": json.loads(canonical),
                "nodes": [],
            },
        )
        group["nodes"].append(observation.node)
    result = [
        CondaRuntimeGroup(
            status=group["status"],
            reason_code=group["reason_code"],
            normalized_result=group["normalized_result"],
            nodes=tuple(sorted(group["nodes"])),
        )
        for group in groups.values()
    ]
    return tuple(sorted(result, key=lambda item: (item.status, item.reason_code, item.nodes)))


@dataclass(frozen=True)
class CondaProbeCommand:
    name: str
    phase: str
    argv: tuple[str, ...]
    environment: tuple[tuple[str, str], ...]
    timeout_seconds: int
    evidence_scope: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase,
            "argv": list(self.argv),
            "environment": dict(self.environment),
            "timeout_seconds": self.timeout_seconds,
            "evidence_scope": self.evidence_scope,
        }


_PREFIX_IDENTITY_SCRIPT = """\
import json, os, sys
print(json.dumps({
    "executable": sys.executable,
    "prefix": sys.prefix,
    "base_prefix": sys.base_prefix,
    "python_version": sys.version.split()[0],
    "realpath": os.path.realpath(sys.prefix),
}, sort_keys=True))
"""

_ARTIFACT_METADATA_SCRIPT = """\
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]) / "conda-meta"
digest = hashlib.sha256()
files = sorted(root.glob("*.json")) if root.is_dir() else []
for path in files:
    digest.update(path.name.encode("utf-8", "surrogateescape"))
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
print(json.dumps({
    "conda_meta_count": len(files),
    "conda_meta_sha256": digest.hexdigest(),
}, sort_keys=True))
"""

_TORCH_RUNTIME_SCRIPT = """\
import json, sys
payload = {"python_version": sys.version.split()[0], "sys_prefix": sys.prefix}
try:
    import torch
    payload.update({
        "torch_importable": True,
        "torch_version": str(torch.__version__),
        "torch_path": str(torch.__file__),
        "torch_hip_version": str(getattr(torch.version, "hip", None)),
        "device_available": bool(torch.cuda.is_available()),
        "device_count": int(torch.cuda.device_count()),
    })
except BaseException as exc:
    payload.update({
        "torch_importable": False,
        "error_type": type(exc).__name__,
        "error": str(exc)[:4096],
    })
    print(json.dumps(payload, sort_keys=True))
    raise SystemExit(1)
print(json.dumps(payload, sort_keys=True))
"""


def build_conda_probe_commands(
    selection: EnvironmentSelection,
) -> tuple[CondaProbeCommand, ...]:
    """Return direct-argv, read-only probe commands for an executor.

    No command uses ``shell=True``, ``source`` or ``conda activate``.  The
    caller schedules ``artifact_metadata`` only on ``artifact_probe_nodes`` and
    the two runtime commands on ``runtime_probe_nodes``.
    """

    if selection.env_mode is not EnvironmentMode.CONDA or selection.conda_prefix is None:
        raise ConfigurationError("Conda probe commands require env_mode=conda")
    python = str(PurePosixPath(selection.conda_prefix) / "bin" / "python")
    common_environment = (("PYTHONDONTWRITEBYTECODE", "1"),)
    return (
        CondaProbeCommand(
            "prefix_identity",
            "node-runtime-light",
            (python, "-S", "-c", _PREFIX_IDENTITY_SCRIPT),
            common_environment,
            15,
            "node",
        ),
        CondaProbeCommand(
            "artifact_metadata",
            "artifact-metadata",
            (python, "-S", "-c", _ARTIFACT_METADATA_SCRIPT, selection.conda_prefix),
            common_environment,
            60,
            "storage-cohort",
        ),
        CondaProbeCommand(
            "torch_runtime",
            "node-runtime-deep",
            (python, "-c", _TORCH_RUNTIME_SCRIPT),
            common_environment,
            90,
            "node-or-joint-cohort",
        ),
    )
