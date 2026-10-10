# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import signal
import shlex
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from .models import CommandResult


_HOST_RE = re.compile(r"^(?!-)[A-Za-z0-9][A-Za-z0-9_.:%-]*$")
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_SENTINEL_RE = re.compile(r"^__HCU_ENVCHECK_RC_[0-9a-f]+__=(-?\d+)\r?$")
MAX_CLUSTER_CONCURRENCY = 128
DEFAULT_NODE_STDOUT_LIMIT_BYTES = 8 * 1024 * 1024
DEFAULT_NODE_STDERR_LIMIT_BYTES = 256 * 1024
_OUTPUT_READ_CHUNK_BYTES = 64 * 1024


class NodeFileError(ValueError):
    """Raised when a bare-metal node file is empty or unsafe."""


class BaremetalConfigurationError(ValueError):
    """Raised before any remote command is started."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _split_top_level_commas(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    for index, character in enumerate(value):
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
            if depth < 0:
                raise NodeFileError(f"unmatched closing bracket in node expression: {value!r}")
        elif character == "," and depth == 0:
            parts.append(value[start:index])
            start = index + 1
    if depth != 0:
        raise NodeFileError(f"unmatched opening bracket in node expression: {value!r}")
    parts.append(value[start:])
    return [part for part in parts if part]


def _expand_bracket_expression(expression: str, max_nodes: int) -> list[str]:
    opening = expression.find("[")
    if opening < 0:
        return [expression]
    closing = expression.find("]", opening + 1)
    if closing < 0:
        raise NodeFileError(f"unmatched opening bracket in node expression: {expression!r}")

    prefix = expression[:opening]
    choices = expression[opening + 1 : closing]
    suffix = expression[closing + 1 :]
    if not choices:
        raise NodeFileError(f"empty bracket range in node expression: {expression!r}")

    expanded_choices: list[str] = []
    for item in choices.split(","):
        range_match = re.fullmatch(r"(\d+)-(\d+)", item)
        if range_match:
            first_text, last_text = range_match.groups()
            first, last = int(first_text), int(last_text)
            if last < first:
                raise NodeFileError(f"descending range is not supported: {item!r}")
            width = max(len(first_text), len(last_text))
            expanded_choices.extend(f"{value:0{width}d}" for value in range(first, last + 1))
        elif re.fullmatch(r"[A-Za-z0-9_.%-]+", item):
            expanded_choices.append(item)
        else:
            raise NodeFileError(f"unsupported bracket item {item!r} in {expression!r}")
        if len(expanded_choices) > max_nodes:
            raise NodeFileError(f"node expression expands past max_nodes={max_nodes}")

    suffix_values = _expand_bracket_expression(suffix, max_nodes)
    results = [f"{prefix}{choice}{tail}" for choice in expanded_choices for tail in suffix_values]
    if len(results) > max_nodes:
        raise NodeFileError(f"node expression expands past max_nodes={max_nodes}")
    return results


def _normalize_node(node: str) -> str:
    node = node.strip()
    if node.startswith("[") and node.endswith("]") and ":" in node:
        node = node[1:-1]
    if not node or len(node) > 253 or not _HOST_RE.fullmatch(node):
        raise NodeFileError(f"unsafe or invalid node name: {node!r}")
    return node


def parse_nodes_file(path: str | Path, *, max_nodes: int = 100_000) -> list[str]:
    """Parse plain, OpenMPI-style, comma-list and simple bracket-range host files.

    Examples accepted by this parser include ``node01``, ``node01 slots=8``,
    ``node01,node02`` and ``node[01-04,08]``.  Duplicate nodes are removed while
    preserving their first occurrence.  Shell metacharacters and user prefixes
    are rejected because the SSH user is an explicit executor setting.
    """

    if max_nodes < 1:
        raise NodeFileError("max_nodes must be at least 1")
    source = Path(path)
    try:
        content = source.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise NodeFileError(f"cannot read node file {source}: {exc}") from exc

    nodes: list[str] = []
    seen: set[str] = set()
    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        try:
            fields = shlex.split(raw_line, comments=True, posix=True)
        except ValueError as exc:
            raise NodeFileError(f"{source}:{line_number}: {exc}") from exc
        if not fields:
            continue
        if len(fields) > 1 and any("=" not in option for option in fields[1:]):
            raise NodeFileError(
                f"{source}:{line_number}: expected one node expression; "
                "additional hostfile fields must be key=value"
            )
        for top_level in _split_top_level_commas(fields[0]):
            for expanded in _expand_bracket_expression(top_level, max_nodes):
                node = _normalize_node(expanded)
                if node in seen:
                    continue
                seen.add(node)
                nodes.append(node)
                if len(nodes) > max_nodes:
                    raise NodeFileError(f"node file contains more than max_nodes={max_nodes}")
    if not nodes:
        raise NodeFileError(f"node file {source} contains no nodes")
    return nodes


def _safe_name(value: str) -> str:
    slug = _SAFE_NAME_RE.sub("_", value).strip("._-") or "item"
    digest = hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()[:10]
    return f"{slug[:80]}-{digest}"


def _chmod(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except OSError:
        # Windows and some shared filesystems do not implement POSIX modes.
        pass


def _write_text(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")
    _chmod(path, 0o600)


def _text_from_timeout(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value or ""


class _BoundedTextCapture:
    """Retain a bounded head and tail while recording the real byte count."""

    def __init__(self, limit_bytes: int):
        self.limit_bytes = limit_bytes
        self.head_limit = limit_bytes // 2
        self.tail_limit = limit_bytes - self.head_limit
        self.head = bytearray()
        self.tail = bytearray()
        self.total_bytes = 0

    def feed(self, value: str | bytes | None) -> None:
        if isinstance(value, str):
            chunk = value.encode("utf-8", "replace")
        else:
            chunk = value or b""
        if not chunk:
            return
        self.total_bytes += len(chunk)
        needed = self.head_limit - len(self.head)
        if needed > 0:
            self.head.extend(chunk[:needed])
            chunk = chunk[needed:]
        if chunk:
            self.tail.extend(chunk)
            overflow = len(self.tail) - self.tail_limit
            if overflow > 0:
                del self.tail[:overflow]

    @property
    def truncated(self) -> bool:
        return self.total_bytes > self.limit_bytes

    def render(self) -> str:
        head = bytes(self.head).decode("utf-8", "replace")
        tail = bytes(self.tail).decode("utf-8", "replace")
        if not self.truncated:
            return head + tail
        omitted = self.total_bytes - len(self.head) - len(self.tail)
        return f"{head}\n...[HCU_ENVCHECK omitted {omitted} output bytes]...\n{tail}"
    def drain(self, pipe: Any) -> None:
        try:
            while True:
                chunk = pipe.read(_OUTPUT_READ_CHUNK_BYTES)
                if not chunk:
                    break
                self.feed(chunk)
        finally:
            try:
                pipe.close()
            except OSError:
                pass


def _bounded_text(value: str | bytes | None, limit_bytes: int) -> tuple[str, int, bool]:
    capture = _BoundedTextCapture(limit_bytes)
    capture.feed(value)
    return capture.render(), capture.total_bytes, capture.truncated


def _extract_remote_rc(stdout: str, sentinel: str) -> tuple[str, int | None]:
    kept: list[str] = []
    remote_rc: int | None = None
    prefix = f"{sentinel}="
    for line in stdout.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        if stripped.startswith(prefix):
            match = _SENTINEL_RE.fullmatch(stripped)
            if match:
                remote_rc = int(match.group(1))
                continue
        kept.append(line)
    return "".join(kept), remote_rc


@dataclass(frozen=True)
class BaremetalExecutionConfig:
    output_root: Path
    transport: str = "ssh"
    concurrency: int = 32
    connect_timeout_seconds: float = 10.0
    command_timeout_seconds: float = 30.0
    ssh_user: str | None = None
    ssh_port: int = 22
    identity_file: Path | None = None
    ssh_config_file: Path | None = None
    known_hosts_file: Path | None = None
    strict_host_key_checking: str = "yes"
    clush_executable: str | None = None
    ssh_executable: str | None = None
    max_stdout_bytes: int = DEFAULT_NODE_STDOUT_LIMIT_BYTES
    max_stderr_bytes: int = DEFAULT_NODE_STDERR_LIMIT_BYTES
    cancel_event: threading.Event | None = field(default=None, compare=False, repr=False)

    def validate(self) -> None:
        if self.transport not in {"auto", "clush", "ssh"}:
            raise BaremetalConfigurationError("transport must be auto, clush or ssh")
        if not 1 <= self.concurrency <= MAX_CLUSTER_CONCURRENCY:
            raise BaremetalConfigurationError(
                f"concurrency must be between 1 and {MAX_CLUSTER_CONCURRENCY}"
            )
        if self.connect_timeout_seconds <= 0 or self.command_timeout_seconds <= 0:
            raise BaremetalConfigurationError("timeouts must be positive")
        if not 1 <= self.ssh_port <= 65535:
            raise BaremetalConfigurationError("ssh_port must be between 1 and 65535")
        if self.strict_host_key_checking not in {"yes", "accept-new"}:
            raise BaremetalConfigurationError(
                "strict_host_key_checking must be 'yes' or 'accept-new'"
            )
        if self.ssh_user and not re.fullmatch(r"[A-Za-z0-9_.-]+", self.ssh_user):
            raise BaremetalConfigurationError("ssh_user contains unsafe characters")
        if self.max_stdout_bytes < 1024:
            raise BaremetalConfigurationError("max_stdout_bytes must be at least 1024")
        if self.max_stderr_bytes < 1024:
            raise BaremetalConfigurationError("max_stderr_bytes must be at least 1024")


@dataclass
class BaremetalNodeResult:
    node: str
    transport: str
    command_name: str
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool = False
    error_kind: str | None = None
    result_dir: str | None = None
    stdout_total_bytes: int = 0
    stderr_total_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    @property
    def success(self) -> bool:
        return self.returncode == 0 and not self.timed_out and self.error_kind is None

    def to_command_result(self) -> CommandResult:
        return CommandResult(
            name=f"{self.command_name}@{self.node}",
            argv=list(self.command),
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
            duration_seconds=self.duration_seconds,
            timed_out=self.timed_out,
        )

    def release_output(self) -> None:
        """Drop captured text after evidence persistence and result parsing."""

        self.stdout = ""
        self.stderr = ""

    def metadata(self) -> dict[str, object]:
        payload = asdict(self)
        payload.pop("stdout", None)
        payload.pop("stderr", None)
        payload["success"] = self.success
        payload["stdout_bytes"] = self.stdout_total_bytes or len(
            self.stdout.encode("utf-8", "replace")
        )
        payload["stderr_bytes"] = self.stderr_total_bytes or len(
            self.stderr.encode("utf-8", "replace")
        )
        return payload


@dataclass
class BaremetalRunResult:
    command_name: str
    transport: str
    command: list[str]
    started_at: str
    finished_at: str
    run_dir: str
    nodes: dict[str, BaremetalNodeResult] = field(default_factory=dict)
    controller_warnings: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(result.error_kind == "CANCELLED" for result in self.nodes.values()):
            return "CANCELLED"
        succeeded = sum(result.success for result in self.nodes.values())
        if succeeded == len(self.nodes) and self.nodes:
            return "SUCCEEDED"
        if succeeded:
            return "PARTIAL"
        return "FAILED"

    def command_results(self) -> dict[str, CommandResult]:
        return {node: result.to_command_result() for node, result in self.nodes.items()}

    def metadata(self) -> dict[str, object]:
        return {
            "schema_version": "1.0",
            "command_name": self.command_name,
            "transport": self.transport,
            "command": self.command,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "run_dir": self.run_dir,
            "status": self.status,
            "controller_warnings": self.controller_warnings,
            "nodes": {node: result.metadata() for node, result in self.nodes.items()},
        }


Runner = Callable[..., subprocess.CompletedProcess[str]]
Popen = Callable[..., subprocess.Popen[bytes]]
Which = Callable[[str], str | None]


class BaremetalClusterExecutor:
    """Run read-only probes through clush or bounded, non-interactive SSH.

    The controller starts only ``clush`` or ``ssh`` locally.  The supplied probe
    command is always placed behind the remote transport and is never evaluated
    by a local shell.  This keeps compilation, customer workloads and diagnostic
    commands off the login node.
    """

    def __init__(
        self,
        nodes: Iterable[str],
        config: BaremetalExecutionConfig,
        *,
        runner: Runner = subprocess.run,
        popen: Popen = subprocess.Popen,
        which: Which = shutil.which,
    ):
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_node in nodes:
            node = _normalize_node(str(raw_node))
            if node not in seen:
                seen.add(node)
                normalized.append(node)
        if not normalized:
            raise BaremetalConfigurationError("at least one node is required")
        config.validate()
        self.nodes = normalized
        self.config = config
        self._runner = runner
        self._popen = popen
        self._which = which
        self._cancel_event = config.cancel_event if config.cancel_event is not None else threading.Event()
        self._process_lock = threading.Lock()
        self._processes: set[Any] = set()

    @classmethod
    def from_nodes_file(
        cls,
        path: str | Path,
        config: BaremetalExecutionConfig,
        *,
        max_nodes: int = 100_000,
        runner: Runner = subprocess.run,
        popen: Popen = subprocess.Popen,
        which: Which = shutil.which,
    ) -> "BaremetalClusterExecutor":
        return cls(
            parse_nodes_file(path, max_nodes=max_nodes),
            config,
            runner=runner,
            popen=popen,
            which=which,
        )

    def selected_transport(self) -> tuple[str, str]:
        clush = self.config.clush_executable or self._which("clush")
        ssh = self.config.ssh_executable or self._which("ssh")
        if self.config.transport == "clush":
            if not clush:
                raise BaremetalConfigurationError("clush was requested but is not available")
            self._validate_transport_executable(clush, "clush")
            return "clush", clush
        if self.config.transport == "ssh":
            if not ssh:
                raise BaremetalConfigurationError("ssh was requested but is not available")
            self._validate_transport_executable(ssh, "ssh")
            return "ssh", ssh
        if clush:
            self._validate_transport_executable(clush, "clush")
            return "clush", clush
        if ssh:
            self._validate_transport_executable(ssh, "ssh")
            return "ssh", ssh
        raise BaremetalConfigurationError("neither clush nor ssh is available")

    @staticmethod
    def _validate_transport_executable(executable: str, expected: str) -> None:
        basename = Path(executable).name.lower()
        if basename not in {expected, f"{expected}.exe"}:
            raise BaremetalConfigurationError(
                f"{expected}_executable must name the {expected} client, got {executable!r}"
            )

    @staticmethod
    def _clush_supports_file_output(executable: str) -> bool:
        """Check for the clush options used by the per-node evidence path.

        Several ClusterShell releases provide ``clush`` but do not implement
        the ``--outdir``/``--errdir`` options.  Treating those versions as a
        normal clush transport makes the command fail before it reaches any
        compute node, leaving every node as a misleading ``REMOTE_RESULT_MISSING``.
        """

        try:
            completed = subprocess.run(
                [executable, "--help"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        help_text = completed.stdout or ""
        return "--outdir" in help_text and "--errdir" in help_text

    def execute(
        self,
        command_name: str,
        command: Sequence[str],
        *,
        run_id: str | None = None,
        result_handler: Callable[[BaremetalNodeResult], None] | None = None,
        release_output: bool = False,
    ) -> BaremetalRunResult:
        if (
            isinstance(command, (str, bytes))
            or not command
            or any(not isinstance(item, str) or "\x00" in item for item in command)
        ):
            raise BaremetalConfigurationError("command must be a non-empty sequence of safe strings")
        if not command_name.strip():
            raise BaremetalConfigurationError("command_name cannot be empty")

        transport, executable = self.selected_transport()
        transport_warnings: list[str] = []
        clush_file_output = True
        # Older ClusterShell versions do not provide --outdir/--errdir.  They
        # are still usable: clush labels each output line with its node, which
        # is parsed by the compatibility path below.
        if (
            transport == "clush"
            and self._runner is subprocess.run
            and self._popen is subprocess.Popen
        ):
            clush_file_output = self._clush_supports_file_output(executable)
            if not clush_file_output:
                transport_warnings.append(
                    "clush lacks --outdir/--errdir; using labeled-output compatibility mode"
                )
        started_at = _utc_now()
        token = uuid.uuid4().hex
        run_label = run_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{token[:8]}"
        run_dir = self.config.output_root / f"{_safe_name(command_name)}-{_safe_name(run_label)}"
        run_dir.mkdir(parents=True, exist_ok=False)
        _chmod(run_dir, 0o700)
        sentinel = f"__HCU_ENVCHECK_RC_{token}__"
        command_list = list(command)

        if transport == "clush":
            results, warnings = self._execute_clush(
                executable,
                command_name,
                command_list,
                sentinel,
                run_dir,
                file_output=clush_file_output,
                result_handler=result_handler,
                release_output=release_output,
            )
        else:
            results, warnings = self._execute_ssh(
                executable,
                command_name,
                command_list,
                sentinel,
                run_dir,
                result_handler=result_handler,
                release_output=release_output,
            )
        warnings = transport_warnings + warnings

        finished_at = _utc_now()
        run_result = BaremetalRunResult(
            command_name=command_name,
            transport=transport,
            command=command_list,
            started_at=started_at,
            finished_at=finished_at,
            run_dir=str(run_dir),
            nodes={node: results[node] for node in self.nodes},
            controller_warnings=warnings,
        )
        _write_text(
            run_dir / "run.json",
            json.dumps(run_result.metadata(), ensure_ascii=False, indent=2) + "\n",
        )
        return run_result

    def _remote_command(self, command: Sequence[str], sentinel: str) -> str:
        command_text = shlex.join(command)
        wrapper = (
            f"{command_text}\n"
            "rc=$?\n"
            f"printf '\\n{sentinel}=%s\\n' \"$rc\"\n"
            'exit "$rc"'
        )
        return "sh -c " + shlex.quote(wrapper)

    def _ssh_base(self, executable: str) -> list[str]:
        argv = [
            executable,
            "-n",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectionAttempts=1",
            "-o",
            f"ConnectTimeout={max(1, math.ceil(self.config.connect_timeout_seconds))}",
            "-o",
            f"StrictHostKeyChecking={self.config.strict_host_key_checking}",
            "-p",
            str(self.config.ssh_port),
        ]
        if self.config.identity_file:
            argv.extend(["-i", str(self.config.identity_file)])
        if self.config.ssh_config_file:
            argv.extend(["-F", str(self.config.ssh_config_file)])
        if self.config.known_hosts_file:
            argv.extend(["-o", f"UserKnownHostsFile={self.config.known_hosts_file}"])
        return argv

    def cancel_local(self) -> None:
        """Unblock local waits; remote cleanup requires RemoteTaskSession.stop.

        Safe to request during scheduling: a task checks the event before
        spawning, and newly registered children poll it at least every 100 ms.
        """
        self._cancel_event.set()

    @staticmethod
    def _terminate_local_process(process: Any) -> None:
        """Reap only our transport's fresh process group, never application names."""
        def send(sig: int) -> None:
            try:
                if os.name == "posix" and getattr(process, "pid", None):
                    os.killpg(process.pid, sig)
                elif sig == signal.SIGTERM:
                    process.terminate()
                else:
                    process.kill()
            except (OSError, ProcessLookupError):
                pass

        send(signal.SIGTERM)
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
        # A parent can exit on TERM while its children ignore it. Kill our
        # isolated group too, then reap the local leader.
        send(getattr(signal, "SIGKILL", signal.SIGTERM))
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired as exc:
            raise OSError("local transport could not be reaped after cancellation") from exc

    def _run_bounded_process(self, argv: list[str], timeout: float) -> dict[str, Any]:
        """Drain transport pipes continuously so controller output cannot exhaust RAM."""

        with self._process_lock:
            if self._cancel_event.is_set():
                return {"returncode": 130, "stdout": "", "stderr": "cancelled before local launch",
                        "stdout_total_bytes": 0, "stderr_total_bytes": 0,
                        "stdout_truncated": False, "stderr_truncated": False,
                        "timed_out": False, "cancelled": True}
            process = self._popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                **({"start_new_session": True} if os.name == "posix" else {}),
            )
            self._processes.add(process)
        if process.stdout is None or process.stderr is None:
            self._terminate_local_process(process)
            with self._process_lock:
                self._processes.discard(process)
            raise OSError("SSH process did not expose stdout/stderr pipes")
        stdout_capture = _BoundedTextCapture(self.config.max_stdout_bytes)
        stderr_capture = _BoundedTextCapture(self.config.max_stderr_bytes)
        threads = [
            threading.Thread(target=stdout_capture.drain, args=(process.stdout,), daemon=True),
            threading.Thread(target=stderr_capture.drain, args=(process.stderr,), daemon=True),
        ]
        for thread in threads:
            thread.start()
        timed_out = False
        cancelled = False
        deadline = time.monotonic() + timeout
        try:
            while True:
                if self._cancel_event.is_set():
                    cancelled = True
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                try:
                    process.wait(timeout=min(0.1, remaining))
                    cancelled = self._cancel_event.is_set()
                    break
                except subprocess.TimeoutExpired:
                    continue
            if cancelled or timed_out:
                self._terminate_local_process(process)
        except BaseException:
            self._cancel_event.set()
            self._terminate_local_process(process)
            raise
        finally:
            with self._process_lock:
                self._processes.discard(process)
            for thread in threads:
                thread.join(timeout=1)
        return {
            "returncode": 130 if cancelled else int(process.returncode if process.returncode is not None else 124),
            "stdout": stdout_capture.render(),
            "stderr": stderr_capture.render(),
            "stdout_total_bytes": stdout_capture.total_bytes,
            "stderr_total_bytes": stderr_capture.total_bytes,
            "stdout_truncated": stdout_capture.truncated,
            "stderr_truncated": stderr_capture.truncated,
            "timed_out": timed_out,
            "cancelled": cancelled,
        }
    def _execute_ssh(
        self,
        executable: str,
        command_name: str,
        command: list[str],
        sentinel: str,
        run_dir: Path,
        *,
        result_handler: Callable[[BaremetalNodeResult], None] | None,
        release_output: bool,
    ) -> tuple[dict[str, BaremetalNodeResult], list[str]]:
        remote_command = self._remote_command(command, sentinel)
        base = self._ssh_base(executable)

        def run_one(node: str) -> BaremetalNodeResult:
            destination = f"{self.config.ssh_user}@{node}" if self.config.ssh_user else node
            argv = base + [destination, remote_command]
            started = time.monotonic()
            timeout = self.config.connect_timeout_seconds + self.config.command_timeout_seconds + 1.0
            try:
                if self._runner is subprocess.run or self._cancel_event.is_set():
                    captured = self._run_bounded_process(argv, timeout)
                    captured_stdout = captured["stdout"]
                    captured_stderr = captured["stderr"]
                    stdout_total = captured["stdout_total_bytes"]
                    stderr_total = captured["stderr_total_bytes"]
                    stdout_truncated = captured["stdout_truncated"]
                    stderr_truncated = captured["stderr_truncated"]
                    completed_returncode = captured["returncode"]
                    if captured["timed_out"]:
                        result = BaremetalNodeResult(
                            node=node,
                            transport="ssh",
                            command_name=command_name,
                            command=command,
                            returncode=124,
                            stdout=captured_stdout,
                            stderr=captured_stderr,
                            duration_seconds=time.monotonic() - started,
                            timed_out=True,
                            error_kind="COMMAND_TIMEOUT",
                            stdout_total_bytes=stdout_total,
                            stderr_total_bytes=stderr_total,
                            stdout_truncated=stdout_truncated,
                            stderr_truncated=stderr_truncated,
                        )
                        self._persist_node_result(run_dir, result)
                        return result
                else:
                    completed = self._runner(
                        argv,
                        stdin=subprocess.DEVNULL,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=timeout,
                        check=False,
                    )
                    captured_stdout, stdout_total, stdout_truncated = _bounded_text(
                        completed.stdout, self.config.max_stdout_bytes
                    )
                    captured_stderr, stderr_total, stderr_truncated = _bounded_text(
                        completed.stderr, self.config.max_stderr_bytes
                    )
                    completed_returncode = completed.returncode
                stdout, remote_rc = _extract_remote_rc(captured_stdout, sentinel)
                if self._cancel_event.is_set():
                    returncode = 130
                    error_kind = "CANCELLED"
                elif remote_rc is None:
                    returncode = completed_returncode if completed_returncode != 0 else 255
                    if completed_returncode == 255:
                        error_kind = "SSH_TRANSPORT_FAILED"
                    else:
                        error_kind = "REMOTE_RESULT_MISSING"
                else:
                    returncode = remote_rc
                    error_kind = None if remote_rc == 0 else "REMOTE_COMMAND_FAILED"
                result = BaremetalNodeResult(
                    node=node,
                    transport="ssh",
                    command_name=command_name,
                    command=command,
                    returncode=returncode,
                    stdout=stdout,
                    stderr=captured_stderr,
                    duration_seconds=time.monotonic() - started,
                    error_kind=error_kind,
                    stdout_total_bytes=stdout_total,
                    stderr_total_bytes=stderr_total,
                    stdout_truncated=stdout_truncated,
                    stderr_truncated=stderr_truncated,
                )
            except subprocess.TimeoutExpired as exc:
                captured_stdout, stdout_total, stdout_truncated = _bounded_text(
                    _text_from_timeout(exc.stdout), self.config.max_stdout_bytes
                )
                captured_stderr, stderr_total, stderr_truncated = _bounded_text(
                    _text_from_timeout(exc.stderr), self.config.max_stderr_bytes
                )
                result = BaremetalNodeResult(
                    node=node,
                    transport="ssh",
                    command_name=command_name,
                    command=command,
                    returncode=124,
                    stdout=captured_stdout,
                    stderr=captured_stderr,
                    duration_seconds=time.monotonic() - started,
                    timed_out=True,
                    error_kind="COMMAND_TIMEOUT",
                    stdout_total_bytes=stdout_total,
                    stderr_total_bytes=stderr_total,
                    stdout_truncated=stdout_truncated,
                    stderr_truncated=stderr_truncated,
                )
            except OSError as exc:
                result = BaremetalNodeResult(
                    node=node,
                    transport="ssh",
                    command_name=command_name,
                    command=command,
                    returncode=127,
                    stdout="",
                    stderr=str(exc),
                    duration_seconds=time.monotonic() - started,
                    error_kind="LOCAL_TRANSPORT_LAUNCH_FAILED",
                )
            self._persist_node_result(run_dir, result)
            return result

        results: dict[str, BaremetalNodeResult] = {}
        pool = ThreadPoolExecutor(max_workers=min(self.config.concurrency, len(self.nodes)))
        futures = {}
        pending_cancelled = False
        try:
            futures = {pool.submit(run_one, node): node for node in self.nodes}
            for future in as_completed(futures):
                node = futures[future]
                if self._cancel_event.is_set() and not pending_cancelled:
                    pending_cancelled = True
                    for pending in futures:
                        pending.cancel()
                try:
                    result = future.result()
                    if result_handler is not None:
                        result_handler(result)
                    if release_output:
                        result.release_output()
                    results[node] = result
                except Exception as exc:  # defensive isolation around a single node
                    result = BaremetalNodeResult(
                        node=node,
                        transport="ssh",
                        command_name=command_name,
                        command=command,
                        returncode=130 if self._cancel_event.is_set() else 70,
                        stdout="",
                        stderr="cancelled before execution" if self._cancel_event.is_set() else f"unexpected executor error: {exc}",
                        duration_seconds=0.0,
                        error_kind="CANCELLED" if self._cancel_event.is_set() else "EXECUTOR_INTERNAL_ERROR",
                    )
                    self._persist_node_result(run_dir, result)
                    results[node] = result
        except BaseException:
            self.cancel_local()
            for future in futures:
                future.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=self._cancel_event.is_set())
        return results, []

    def _execute_clush(
        self,
        executable: str,
        command_name: str,
        command: list[str],
        sentinel: str,
        run_dir: Path,
        *,
        file_output: bool,
        result_handler: Callable[[BaremetalNodeResult], None] | None,
        release_output: bool,
    ) -> tuple[dict[str, BaremetalNodeResult], list[str]]:
        clush_dir = run_dir / ".clush"
        stdout_dir = clush_dir / "stdout"
        stderr_dir = clush_dir / "stderr"
        stdout_dir.mkdir(parents=True)
        stderr_dir.mkdir(parents=True)
        _chmod(clush_dir, 0o700)
        _chmod(stdout_dir, 0o700)
        _chmod(stderr_dir, 0o700)

        remote_command = self._remote_command(command, sentinel)
        argv = [
            executable,
            "-S",
            "-n",
            "-f",
            str(self.config.concurrency),
            "-t",
            str(max(1, math.ceil(self.config.connect_timeout_seconds))),
            "-u",
            str(max(1, math.ceil(self.config.command_timeout_seconds))),
        ]
        if file_output:
            argv.extend(
                [
                    "--outdir",
                    str(stdout_dir),
                    "--errdir",
                    str(stderr_dir),
                ]
            )
        argv.extend(["-w", ",".join(self.nodes), "--", remote_command])
        waves = math.ceil(len(self.nodes) / self.config.concurrency)
        controller_timeout = waves * (
            self.config.connect_timeout_seconds + self.config.command_timeout_seconds
        ) + 10.0
        started = time.monotonic()
        controller_stdout = ""
        controller_stderr = ""
        controller_timed_out = False
        controller_cancelled = False
        controller_launch_error: str | None = None
        try:
            if self._runner is subprocess.run or self._cancel_event.is_set():
                captured = self._run_bounded_process(argv, controller_timeout)
                controller_stdout = captured["stdout"]
                controller_stderr = captured["stderr"]
                controller_timed_out = captured["timed_out"]
                controller_cancelled = captured.get("cancelled", False)
            else:
                completed = self._runner(
                    argv,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=controller_timeout,
                    check=False,
                )
                controller_stdout = completed.stdout or ""
                controller_stderr = completed.stderr or ""
        except subprocess.TimeoutExpired as exc:
            controller_timed_out = True
            controller_stdout = _text_from_timeout(exc.stdout)
            controller_stderr = _text_from_timeout(exc.stderr)
        except OSError as exc:
            controller_launch_error = str(exc)
            controller_stderr = str(exc)

        controller_stdout, _, _ = _bounded_text(
            controller_stdout, self.config.max_stdout_bytes
        )
        controller_stderr, _, _ = _bounded_text(
            controller_stderr, self.config.max_stderr_bytes
        )
        _write_text(run_dir / "controller.stdout", controller_stdout)
        _write_text(run_dir / "controller.stderr", controller_stderr)
        labeled_stdout = (
            {} if file_output else self._split_clush_labeled_output(controller_stdout)
        )
        labeled_stderr = (
            {} if file_output else self._split_clush_labeled_output(controller_stderr)
        )
        duration = time.monotonic() - started
        results: dict[str, BaremetalNodeResult] = {}
        for node in self.nodes:
            if file_output:
                stdout, stdout_total, stdout_truncated = self._read_clush_output(
                    stdout_dir, node, self.config.max_stdout_bytes
                )
                stderr, stderr_total, stderr_truncated = self._read_clush_output(
                    stderr_dir, node, self.config.max_stderr_bytes
                )
            else:
                stdout, stdout_total, stdout_truncated = _bounded_text(
                    labeled_stdout.get(node, ""), self.config.max_stdout_bytes
                )
                stderr, stderr_total, stderr_truncated = _bounded_text(
                    labeled_stderr.get(node, ""), self.config.max_stderr_bytes
                )
            stdout, remote_rc = _extract_remote_rc(stdout, sentinel)
            node_controller_error = self._clush_controller_error(controller_stderr, node)
            if node_controller_error and node_controller_error.strip() not in stderr:
                stderr = stderr + ("" if not stderr or stderr.endswith("\n") else "\n")
                stderr += node_controller_error
            shared_controller_error = controller_stderr if controller_launch_error else ""
            combined_error = f"{stderr}\n{shared_controller_error}".lower()
            if controller_cancelled or self._cancel_event.is_set():
                returncode = 130
                timed_out = False
                error_kind = "CANCELLED"
            elif remote_rc is not None:
                returncode = remote_rc
                timed_out = False
                error_kind = None if remote_rc == 0 else "REMOTE_COMMAND_FAILED"
            elif controller_launch_error:
                returncode = 127
                timed_out = False
                error_kind = "LOCAL_TRANSPORT_LAUNCH_FAILED"
            elif controller_timed_out or "timed out" in combined_error or "timeout" in combined_error:
                returncode = 124
                timed_out = True
                error_kind = "COMMAND_TIMEOUT"
            elif any(token in combined_error for token in ("ssh:", "unreachable", "connection refused")):
                returncode = 255
                timed_out = False
                error_kind = "SSH_TRANSPORT_FAILED"
            else:
                returncode = 255
                timed_out = False
                error_kind = "REMOTE_RESULT_MISSING"
            result = BaremetalNodeResult(
                node=node,
                transport="clush",
                command_name=command_name,
                command=command,
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
                duration_seconds=duration,
                timed_out=timed_out,
                error_kind=error_kind,
                stdout_total_bytes=stdout_total,
                stderr_total_bytes=stderr_total,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            )
            self._persist_node_result(run_dir, result)
            if result_handler is not None:
                result_handler(result)
            if release_output:
                result.release_output()
            results[node] = result

        warnings: list[str] = []
        if controller_launch_error:
            warnings.append(f"clush launch failed: {controller_launch_error}")
        elif controller_timed_out:
            warnings.append("clush controller exceeded its safety timeout")
        return results, warnings

    def _split_clush_labeled_output(self, value: str) -> dict[str, str]:
        """Split classic ``node: output`` clush streams into per-node text."""

        output: dict[str, list[str]] = {node: [] for node in self.nodes}
        prefixes = [(f"{node}: ", node) for node in sorted(self.nodes, key=len, reverse=True)]
        for line in value.splitlines(keepends=True):
            for prefix, node in prefixes:
                if line.startswith(prefix):
                    output[node].append(line[len(prefix) :])
                    break
        return {node: "".join(lines) for node, lines in output.items()}

    @staticmethod
    def _read_clush_output(
        directory: Path, node: str, limit_bytes: int
    ) -> tuple[str, int, bool]:
        candidates = [directory / node, directory / _safe_name(node)]
        for candidate in candidates:
            try:
                if not candidate.is_file():
                    continue
                size = candidate.stat().st_size
                capture = _BoundedTextCapture(limit_bytes)
                with candidate.open("rb") as stream:
                    while True:
                        chunk = stream.read(_OUTPUT_READ_CHUNK_BYTES)
                        if not chunk:
                            break
                        capture.feed(chunk)
                candidate.unlink(missing_ok=True)
                # stat() is kept as a sanity check; the byte counter is the
                # authoritative value if a writer completed just before read.
                return capture.render(), max(size, capture.total_bytes), capture.truncated
            except OSError:
                continue
        return "", 0, False

    @staticmethod
    def _clush_controller_error(controller_stderr: str, node: str) -> str:
        """Return only controller diagnostics that explicitly name this node."""

        pattern = re.compile(rf"(?:^|[\s:\[])({re.escape(node)})(?:$|[\s:\]])")
        lines = [line for line in controller_stderr.splitlines() if pattern.search(line)]
        return "\n".join(lines) + ("\n" if lines else "")

    @staticmethod
    def _persist_node_result(run_dir: Path, result: BaremetalNodeResult) -> None:
        node_dir = run_dir / "nodes" / _safe_name(result.node)
        node_dir.mkdir(parents=True, exist_ok=True)
        _chmod(node_dir.parent, 0o700)
        _chmod(node_dir, 0o700)
        result.result_dir = str(node_dir)
        _write_text(node_dir / "stdout.txt", result.stdout)
        _write_text(node_dir / "stderr.txt", result.stderr)
        _write_text(
            node_dir / "result.json",
            json.dumps(result.metadata(), ensure_ascii=False, indent=2) + "\n",
        )
