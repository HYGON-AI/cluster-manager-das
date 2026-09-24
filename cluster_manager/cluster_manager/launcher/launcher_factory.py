#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
from cluster_manager.launcher.docker_launcher import DockerLauncher
from cluster_manager.launcher.mpirun_launcher import MPIRunLauncher

def create_launcher():
    mode = os.getenv("CLUSTER_LAUNCH_MODE", "mpi").strip().lower()
    if mode in {"mpi", "mpi_docker", "mpi-docker"}:
        launcher = MPIRunLauncher()
        if mode in {"mpi_docker", "mpi-docker"}:
            launcher.docker_enabled = True
        return launcher
    if mode in {"docker", "docker_exec", "docker-exec"}:
        return DockerLauncher()
    raise ValueError(f"Unsupported launcher mode: {mode}")
