#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

class BaseLauncher(ABC):

    @abstractmethod
    def start(self, nodes, slots):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def is_alive(self):
        pass