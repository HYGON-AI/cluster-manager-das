#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
from cluster_manager.launcher.mpirun_launcher import MPIRunLauncher

def create_launcher():
    mode = os.getenv("CLUSTER_LAUNCH_MODE", "mpi")
    if mode == "mpi":
        return MPIRunLauncher()
    raise ValueError(f"Unsupported launcher mode: {mode}")
