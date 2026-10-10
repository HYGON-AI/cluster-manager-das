# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Run-scoped remote cancellation, independent of the user's environment script.

Integration: create one session for ALL planned nodes before launching anything;
use its ``config`` for every executor, and wrap both the MPI leader and each
rank with ``managed_command(..., session.run_token)`` INSIDE the chosen scope.
Use ``with session:`` on the controller main thread around the whole run and
all worker pools. The signal handler only sets the shared event, allowing
cancel-aware workers to release pools. Context exit confirms remote cleanup
before raising ``TaskInterrupted`` (a KeyboardInterrupt subclass).
Do not report a successful cancellation unless ``report.confirmed`` is true.

The Linux guard needs Bash 4+, util-linux setsid/flock, and readable /proc.
Stop tombstones are deliberately retained; never reuse a token. Abrupt SIGKILL
of the controller or an unreachable node cannot be confirmed as cleaned up.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Iterator, Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig

_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{15,95}\Z")
_MARKER = "__HCU_TASK_STOP__="


def _guard_command(mode: str, run_token: str, args: Sequence[str] = ()) -> list[str]:
    if not isinstance(run_token, str) or not _TOKEN_RE.fullmatch(run_token):
        raise ValueError("run_token must be 16..96 letters, digits, underscores or hyphens")
    body = (Path(__file__).parent / "payloads" / "task_guard.sh").read_text(encoding="utf-8")
    # Bash otherwise reads BASH_ENV even with --norc. In particular stop must
    # not run a user startup hook that blocks or exits before installing STOP.
    return ["env", "-u", "BASH_ENV", "-u", "ENV", "bash", "--noprofile", "--norc",
            "-c", body, "hcu-task-guard", mode, run_token, *args]


def managed_command(command: Sequence[str], run_token: str) -> list[str]:
    """Inline guard: no shared project installation or remote Python required."""
    if isinstance(command, (str, bytes)) or not command or any(
        not isinstance(arg, str) or "\0" in arg for arg in command
    ):
        raise ValueError("command must be a nonempty sequence of strings without NUL")
    return _guard_command("run", run_token, ["--", *command])


@dataclass(frozen=True)
class NodeCancellation:
    node: str
    status: str  # CONFIRMED, FAILED (positive residual evidence), UNCONFIRMED
    reason: str
    returncode: int | None = None
    evidence_dir: str | None = None


@dataclass(frozen=True)
class CancellationReport:
    run_token: str
    nodes: dict[str, NodeCancellation]

    @property
    def confirmed(self) -> bool:
        return bool(self.nodes) and all(node.status == "CONFIRMED" for node in self.nodes.values())

    @property
    def status(self) -> str:
        return "CONFIRMED" if self.confirmed else "UNCONFIRMED"


class TaskInterrupted(KeyboardInterrupt):
    def __init__(self, signum: int, report: CancellationReport | None = None):
        self.signum = signum
        self.report = report
        super().__init__(f"task interrupted by signal {signum}")


class RemoteTaskSession:
    """Own a global token, local cancellation event and remote stop evidence.

    Use the same container user as rank launch when Docker's default user is
    overridden. Stop never sources env.sh and never stops/removes a container.
    ``executor_factory`` is injectable for transport/evidence tests.
    """

    def __init__(
        self,
        nodes: Sequence[str],
        config: BaremetalExecutionConfig,
        *,
        execution_scope: str = "host",
        container_name: str | None = None,
        container_user: str | None = None,
        run_token: str | None = None,
        grace_seconds: int = 3,
        verify_seconds: int = 10,
        executor_factory: Callable = BaremetalClusterExecutor,
    ):
        if not nodes or isinstance(nodes, (str, bytes)):
            raise ValueError("nodes must contain all planned target nodes")
        if execution_scope not in {"host", "container"}:
            raise ValueError("execution_scope must be host or container")
        if execution_scope == "container" and (
            not container_name or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", container_name)
        ):
            raise ValueError("a safe container_name is required for container scope")
        if not 0 <= grace_seconds <= 60 or not 1 <= verify_seconds <= 120:
            raise ValueError("invalid cancellation grace/verification timeout")
        self.nodes = tuple(dict.fromkeys(nodes))
        self.run_token = run_token or uuid.uuid4().hex
        _guard_command("stop", self.run_token)  # validate token and installed payload now
        self.cancel_event = config.cancel_event if config.cancel_event is not None else threading.Event()
        self.config = replace(config, cancel_event=self.cancel_event)
        self.execution_scope = execution_scope
        self.container_name = container_name
        self.container_user = container_user
        self.grace_seconds = grace_seconds
        self.verify_seconds = verify_seconds
        self._executor_factory = executor_factory
        self._cancel_lock = threading.Lock()
        self._local_lock = threading.Lock()
        self._local_processes: set[subprocess.Popen] = set()
        self.last_report: CancellationReport | None = None

    def runner(self, args, *, timeout=None, check=False, **kwargs) -> subprocess.CompletedProcess:
        """subprocess.run-compatible local runner with bounded cancellation waits.

        Intended for transport commands in additional checks. Remote commands
        still need managed_command wrapping. Return 130 on cancellation;
        timeouts retain subprocess.TimeoutExpired semantics.
        """
        input_value = kwargs.pop("input", None)
        capture_output = kwargs.pop("capture_output", False)
        if capture_output:
            if "stdout" in kwargs or "stderr" in kwargs:
                raise ValueError("stdout/stderr cannot be used with capture_output")
            kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if input_value is not None:
            if "stdin" in kwargs:
                raise ValueError("stdin and input arguments may not both be used")
            kwargs["stdin"] = subprocess.PIPE
        kwargs.setdefault("stdin", subprocess.DEVNULL)
        if os.name == "posix":
            kwargs["start_new_session"] = True
        text_mode = kwargs.get("text") or kwargs.get("universal_newlines") or kwargs.get("encoding")
        with self._local_lock:
            if self.cancel_event.is_set():
                empty = "" if text_mode else b""
                result = subprocess.CompletedProcess(args, 130,
                    empty if kwargs.get("stdout") == subprocess.PIPE else None,
                    empty if kwargs.get("stderr") == subprocess.PIPE else None)
                if check:
                    result.check_returncode()
                return result
            process = subprocess.Popen(args, **kwargs)
            self._local_processes.add(process)
        deadline = None if timeout is None else time.monotonic() + timeout
        first = True
        cancelled = False
        try:
            while True:
                cancelled = self.cancel_event.is_set()
                expired = deadline is not None and time.monotonic() >= deadline
                if cancelled or expired:
                    BaremetalClusterExecutor._terminate_local_process(process)
                    try:
                        stdout, stderr = process.communicate(timeout=2)
                    except subprocess.TimeoutExpired as exc:
                        stdout, stderr = exc.output, exc.stderr
                        if text_mode:
                            encoding, errors = kwargs.get("encoding") or "utf-8", kwargs.get("errors") or "replace"
                            if isinstance(stdout, bytes):
                                stdout = stdout.decode(encoding, errors)
                            if isinstance(stderr, bytes):
                                stderr = stderr.decode(encoding, errors)
                    if expired and not cancelled:
                        raise subprocess.TimeoutExpired(args, timeout, stdout, stderr)
                    break
                interval = 0.1 if deadline is None else max(0.001, min(0.1, deadline - time.monotonic()))
                try:
                    stdout, stderr = process.communicate(input=input_value if first else None, timeout=interval)
                    cancelled = self.cancel_event.is_set()
                    break
                except subprocess.TimeoutExpired:
                    first = False
            result = subprocess.CompletedProcess(args, 130 if cancelled else process.returncode, stdout, stderr)
            if check:
                result.check_returncode()
            return result
        except BaseException:
            if process.poll() is None:
                BaremetalClusterExecutor._terminate_local_process(process)
            raise
        finally:
            with self._local_lock:
                self._local_processes.discard(process)

    def cancel_and_wait(self) -> CancellationReport:
        """Stop every planned node in parallel; verify, never infer from SSH exit.

        Repeated calls retry unconfirmed nodes too. stop is idempotent, and a
        fresh transport executor is essential: its event MUST NOT be set.
        """
        self.cancel_event.set()
        with self._cancel_lock:
            command = _guard_command("stop", self.run_token, [str(self.grace_seconds), str(self.verify_seconds)])
            if self.execution_scope == "container":
                command = ["docker", "exec", *(["--user", self.container_user] if self.container_user else []),
                           self.container_name, *command]
            config = replace(
                self.config, cancel_event=None,
                output_root=self.config.output_root / "task-cancellation",
                command_timeout_seconds=max(30.0, self.grace_seconds + self.verify_seconds + 15.0),
            )
            reports: dict[str, NodeCancellation] = {}
            try:
                result = self._executor_factory(self.nodes, config).execute("task-stop", command)
                for node in self.nodes:
                    item = result.nodes.get(node)
                    if item is None:
                        reports[node] = NodeCancellation(node, "UNCONFIRMED", "remote stop result missing")
                        continue
                    payload = None
                    for line in item.stdout.splitlines():
                        if line.startswith(_MARKER):
                            try:
                                candidate = json.loads(line[len(_MARKER):])
                            except (ValueError, TypeError):
                                continue
                            if isinstance(candidate, dict) and candidate.get("token") == self.run_token:
                                payload = candidate
                    confirmed = bool(item.success and payload and payload.get("status") == "STOPPED"
                                     and payload.get("remaining") == 0 and payload.get("tombstone") is True
                                     and payload.get("uncertain", False) is False)
                    # Unreadable /proc is missing proof, not positive evidence
                    # of a surviving task. Keep that distinction per node.
                    remaining = payload.get("remaining") if payload else None
                    failed = bool(payload and payload.get("status") == "FAILED"
                                  and isinstance(remaining, int) and not isinstance(remaining, bool)
                                  and remaining > 0)
                    reports[node] = NodeCancellation(
                        node, "CONFIRMED" if confirmed else "FAILED" if failed else "UNCONFIRMED",
                        "stop tombstone installed; no live task processes" if confirmed else
                        (item.stderr.strip() or item.error_kind or "remote stop did not confirm task disappearance"),
                        item.returncode, item.result_dir,
                    )
            except Exception as exc:
                for node in self.nodes:
                    reports.setdefault(node, NodeCancellation(node, "UNCONFIRMED", f"stop transport error: {exc}"))
            self.last_report = CancellationReport(self.run_token, reports)
            return self.last_report

    @contextmanager
    def signal_handlers(self) -> Iterator["RemoteTaskSession"]:
        """Main-thread scope; cleanup completes before the interruption escapes.

        Repeated Ctrl+C/SIGTERM during cleanup sets the event but does not abort
        verification. Callers catch TaskInterrupted after this context and use
        last_report to display every unconfirmed/failed node.
        """
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("signal_handlers must be installed on the main thread")
        received: list[int] = []

        def interrupted(signum, _frame):
            self.cancel_event.set()
            if not received:
                received.append(signum)

        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            for sig in previous:
                signal.signal(sig, interrupted)
            try:
                yield self
            except BaseException as exc:
                # Also clean up failed scheduling/KeyboardInterrupt, not only
                # Python signal-handler delivery. Do not let a second signal
                # interrupt the first stop transport.
                received.append(0)
                self.cancel_and_wait()
                if isinstance(exc, TaskInterrupted):
                    exc.report = self.last_report
                raise
            finally:
                if self.cancel_event.is_set() and self.last_report is None:
                    self.cancel_and_wait()
            if received:
                raise TaskInterrupted(received[0], self.last_report)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    def __enter__(self) -> "RemoteTaskSession":
        self._signal_context = self.signal_handlers()
        return self._signal_context.__enter__()

    def __exit__(self, exc_type, exc, tb):
        return self._signal_context.__exit__(exc_type, exc, tb)
