# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from cluster_manager.config.train_config import _safe_eval_arithmetic


def test_safe_eval_arithmetic_supports_basic_shell_math():
    values = {"TP": 8, "PP": 2}

    assert _safe_eval_arithmetic("TP * PP + 4", values) == 20
    assert _safe_eval_arithmetic("(TP + PP) // 2", values) == 5
    assert _safe_eval_arithmetic("$TP * 2", values) == 16


def test_safe_eval_arithmetic_rejects_code_and_unknown_names():
    values = {"TP": 8}

    assert _safe_eval_arithmetic("__import__('os').system('echo unsafe')", values) is None
    assert _safe_eval_arithmetic("TP.__class__", values) is None
    assert _safe_eval_arithmetic("UNKNOWN + 1", values) is None
    assert _safe_eval_arithmetic("2 ** 100", values) is None
