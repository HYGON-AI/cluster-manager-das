# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import os
import shutil
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from cluster_run.__main__ import entrypoint


ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash") or (str(Path("C:/Program Files/Git/bin/bash.exe"))
                               if Path("C:/Program Files/Git/bin/bash.exe").exists() else None)


def shell_path(path):
    path = Path(path).resolve()
    if os.name == "nt":
        return "/" + path.drive[0].lower() + path.as_posix()[2:]
    return str(path)


@unittest.skipUnless(BASH, "requires local Bash; no remote execution")
class ShellBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="entry contract ")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.capture = self.directory / "args.bin"
        self.python = self.directory / "selected python"
        self.python.write_text(
            '#!/bin/bash\n'
            'if [[ "$1" == -c ]]; then exit "${MOCK_VERSION_RC:-0}"; fi\n'
            'printf "%s\\0" "$@" > "$ENTRY_CAPTURE"\n'
            'printf "%s" "${CONTROLLER_MARK:-}" > "$ENTRY_MARK"\n', encoding="utf-8")
        self.python.chmod(0o755)
        self.env = dict(os.environ, HCU_ENVCHECK_PYTHON=shell_path(self.python),
                        ENTRY_CAPTURE=shell_path(self.capture),
                        ENTRY_MARK=shell_path(self.directory / "mark"),
                        PYTHONDONTWRITEBYTECODE="1")
        self.env.pop("MOCK_VERSION_RC", None)

    def run_entry(self, *args, env=None):
        return subprocess.run([BASH, shell_path(ROOT / "bin/hcu-cluster-run"), *args],
                              env=env or self.env, text=True, encoding="utf-8",
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)

    def test_help_and_version_do_not_require_or_execute_python(self):
        env = dict(self.env, HCU_ENVCHECK_PYTHON="/no/controller/python")
        for option in ("--help", "--version"):
            with self.subTest(option=option):
                result = self.run_entry(option, env=env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("hcu-cluster-run", result.stdout)
        self.assertFalse(self.capture.exists())

    def test_controller_environment_selects_python_without_default_python(self):
        script = self.directory / "controller env.sh"
        script.write_text(
            f"export HCU_ENVCHECK_PYTHON='{shell_path(self.python)}'\n"
            "export CONTROLLER_MARK=loaded\n", encoding="utf-8")
        result = self.run_entry("shared-conda", "resource", "-f", "nodes.txt",
                                "--controller-env-script", shell_path(script),
                                env=dict(self.env, HCU_ENVCHECK_PYTHON="/unavailable"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.directory / "mark").read_text(), "loaded")
        self.assertNotIn(b"controller-env-script", self.capture.read_bytes())

    def test_explicit_controller_python_overrides_legacy_environment(self):
        result = self.run_entry("--controller-python=" + shell_path(self.python),
                                "--help-full", env=dict(self.env, HCU_ENVCHECK_PYTHON="/missing"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.capture.read_bytes(), b"-m\x00cluster_run\x00--help\x00")

    def test_target_environment_is_never_sourced_and_payload_values_are_opaque(self):
        script = self.directory / "target env.sh"
        script.write_text("exit 91\n", encoding="utf-8")
        args = ["shared-conda", "script", "-f", "nodes.txt", "--env-script", shell_path(script),
                "--script", "/share/with spaces/check.py", "--script-arg", "--controller-python",
                "--script-arg", "--help"]
        result = self.run_entry(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.capture.read_bytes().decode().split("\0")[:-1],
                         ["-m", "cluster_run", *args])

    def test_controller_setup_failure_is_not_ignored(self):
        script = self.directory / "bad.sh"
        script.write_text("false\nexport CONTROLLER_MARK=should-not-run\n", encoding="utf-8")
        result = self.run_entry("--controller-env-script", shell_path(script), "shared-conda", "resource")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("controller environment setup failed", result.stderr)
        self.assertFalse(self.capture.exists())

    def test_invalid_bootstrap_options_have_actionable_errors(self):
        for args in (("--controller-python",), ("--controller-env-script", "relative.sh", "shared-conda")):
            with self.subTest(args=args):
                result = self.run_entry(*args)
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertIn("requires", result.stderr)
        result = self.run_entry("shared-conda", "resource", env=dict(self.env, MOCK_VERSION_RC="1"))
        self.assertEqual(result.returncode, 69)
        self.assertIn("--controller-env-script", result.stderr)

    @unittest.skipIf(os.name == "nt", "POSIX unlinked cwd semantics")
    def test_deleted_cwd_recovers_before_python_exec(self):
        child = self.directory / "gone"
        child.mkdir()
        result = subprocess.run(
            [BASH, "-c", 'cd "$1"; rmdir "$1"; exec bash "$2" --version',
             "test", str(child), str(ROOT / "bin/hcu-cluster-run")], env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("current directory no longer exists", result.stderr)


class EntrypointContractTests(unittest.TestCase):
    def test_shell_launcher_recovers_after_ftp_replaces_working_directory(self):
        launcher = Path(__file__).resolve().parents[1] / "bin" / "hcu-cluster-run"
        text = launcher.read_text(encoding="utf-8")
        self.assertIn("if ! pwd -P >/dev/null 2>&1; then", text)
        self.assertIn('CDPATH= cd -P -- "$ROOT"', text)

    def test_packaging_console_script_exposes_only_unified_entrypoint(self):
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        self.assertIn('hcu-cluster-run = "cluster_run.__main__:entrypoint"', text)
        self.assertNotIn('hcu_envcheck' + '.__main__', text)

    def test_unexpected_exception_is_tool_error_without_traceback(self):
        stderr = io.StringIO()
        with patch("cluster_run.__main__.main", side_effect=KeyError("injected")):
            with patch.dict(os.environ, {}, clear=True), redirect_stderr(stderr):
                code = entrypoint()

        self.assertEqual(code, 3)
        self.assertIn("RESULT        TOOL_ERROR", stderr.getvalue())
        self.assertIn("KeyError", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_debug_mode_includes_traceback(self):
        stderr = io.StringIO()
        with patch("cluster_run.__main__.main", side_effect=RuntimeError("injected")):
            with patch.dict(os.environ, {"HCU_ENVCHECK_DEBUG": "1"}, clear=True):
                with redirect_stderr(stderr):
                    code = entrypoint()

        self.assertEqual(code, 3)
        self.assertIn("Traceback", stderr.getvalue())

    def test_keyboard_interrupt_keeps_shell_interrupt_exit_code(self):
        stderr = io.StringIO()
        with patch("cluster_run.__main__.main", side_effect=KeyboardInterrupt):
            with redirect_stderr(stderr):
                code = entrypoint()

        self.assertEqual(code, 130)
        self.assertIn("interrupted by user", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
