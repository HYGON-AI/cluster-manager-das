# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import os
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from hcu_envcheck.__main__ import entrypoint


class EntrypointContractTests(unittest.TestCase):
    def test_packaging_console_script_uses_guarded_entrypoint(self):
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        self.assertIn(
            'hcu-envcheck = "hcu_envcheck.__main__:entrypoint"',
            text,
        )

    def test_unexpected_exception_is_tool_error_without_traceback(self):
        stderr = io.StringIO()
        with patch("hcu_envcheck.__main__.main", side_effect=KeyError("injected")):
            with patch.dict(os.environ, {}, clear=True), redirect_stderr(stderr):
                code = entrypoint()

        self.assertEqual(code, 3)
        self.assertIn("RESULT        TOOL_ERROR", stderr.getvalue())
        self.assertIn("KeyError", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_debug_mode_includes_traceback(self):
        stderr = io.StringIO()
        with patch("hcu_envcheck.__main__.main", side_effect=RuntimeError("injected")):
            with patch.dict(os.environ, {"HCU_ENVCHECK_DEBUG": "1"}, clear=True):
                with redirect_stderr(stderr):
                    code = entrypoint()

        self.assertEqual(code, 3)
        self.assertIn("Traceback", stderr.getvalue())

    def test_keyboard_interrupt_keeps_shell_interrupt_exit_code(self):
        stderr = io.StringIO()
        with patch("hcu_envcheck.__main__.main", side_effect=KeyboardInterrupt):
            with redirect_stderr(stderr):
                code = entrypoint()

        self.assertEqual(code, 130)
        self.assertIn("interrupted by user", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
