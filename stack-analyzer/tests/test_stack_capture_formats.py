# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.stack_capture import _parse_py_spy_json


def test_parse_py_spy_04_list_prefers_main_thread():
    payload = [
        {
            "pid": 10,
            "os_thread_id": 11,
            "thread_name": "QueueFeederThread",
            "active": True,
            "frames": [{"name": "_feed", "filename": "queues.py", "line": 1}],
        },
        {
            "pid": 10,
            "os_thread_id": 10,
            "thread_name": "MainThread",
            "active": False,
            "frames": [{"name": "train_step", "filename": "train.py", "line": 7}],
        },
    ]
    frames = _parse_py_spy_json(payload)
    assert [frame.function for frame in frames] == ["train_step"]


def test_parse_py_spy_03_dict_still_supported():
    payload = {
        "threads": [
            {
                "name": "MainThread",
                "frames": [{"name": "all_reduce", "filename": "dist.py", "line": 9}],
            }
        ]
    }
    frames = _parse_py_spy_json(payload)
    assert frames[0].function == "all_reduce"
