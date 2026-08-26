# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Test-only compatibility stubs for optional production dependencies."""

import sys
from types import ModuleType
from unittest.mock import MagicMock


def _install_optional_dependency_stubs():
    requests = ModuleType("requests")
    requests.post = MagicMock()
    adapters = ModuleType("requests.adapters")
    adapters.HTTPAdapter = MagicMock
    requests.adapters = adapters
    sys.modules.setdefault("requests", requests)
    sys.modules.setdefault("requests.adapters", adapters)
    sys.modules.setdefault("pytz", MagicMock())

    urllib3 = ModuleType("urllib3")
    urllib3_util = ModuleType("urllib3.util")
    urllib3_retry = ModuleType("urllib3.util.retry")
    urllib3_retry.Retry = MagicMock
    sys.modules.setdefault("urllib3", urllib3)
    sys.modules.setdefault("urllib3.util", urllib3_util)
    sys.modules.setdefault("urllib3.util.retry", urllib3_retry)

    fcntl = ModuleType("fcntl")
    fcntl.LOCK_EX = 1
    fcntl.LOCK_UN = 2
    fcntl.flock = MagicMock()
    sys.modules.setdefault("fcntl", fcntl)


_install_optional_dependency_stubs()
