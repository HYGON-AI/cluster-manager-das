# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, sentinel

from cluster_manager.executor import cmd_executor
from cluster_manager.executor.cmd_executor import CmdExecutor


def _install_process_group_compatibility(monkeypatch):
    # Production runs on Linux.  Use a stable stand-in for the Unix callback
    # so the subprocess contract can also be asserted on Windows CI.
    monkeypatch.setattr(
        cmd_executor.os, "setpgrp", sentinel.setpgrp, raising=False
    )


def test_execute_command_returns_decoded_output(monkeypatch):
    _install_process_group_compatibility(monkeypatch)
    run_mock = MagicMock(
        return_value=SimpleNamespace(returncode=0, stdout=b"test\n")
    )
    monkeypatch.setattr(cmd_executor.subprocess, "run", run_mock)

    error_code, result = CmdExecutor.execute_command(
        "echo test", capture_output=True
    )

    assert (error_code, result) == (0, "test")
    run_mock.assert_called_once_with(
        "echo test",
        shell=True,
        text=False,
        preexec_fn=sentinel.setpgrp,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
        check=False,
        cwd=None,
    )


def test_execute_command_preserves_nonzero_return_code(monkeypatch):
    _install_process_group_compatibility(monkeypatch)
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            returncode=127,
            stdout=b"/bin/sh: invalid_command: not found\n",
        )

    monkeypatch.setattr(cmd_executor.subprocess, "run", fake_run)

    error_code, result = CmdExecutor.execute_command(
        "invalid_command", capture_output=True
    )

    assert error_code == 127
    assert "invalid_command: not found" in result
    assert calls == [
        (
            ("invalid_command",),
            {
                "shell": True,
                "text": False,
                "preexec_fn": sentinel.setpgrp,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "timeout": 60,
                "check": False,
                "cwd": None,
            },
        )
    ]


def test_execute_command_forwards_work_dir_and_timeout(monkeypatch):
    _install_process_group_compatibility(monkeypatch)
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=b"ok")

    monkeypatch.setattr(cmd_executor.subprocess, "run", fake_run)

    assert CmdExecutor.execute_command(
        "pwd", work_dir="/workspace", capture_output=True, timeout=9
    ) == (0, "ok")
    assert calls[0][1]["cwd"] == os.fspath("/workspace")
    assert calls[0][1]["timeout"] == 9
