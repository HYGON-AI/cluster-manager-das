<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Third-Party Notices

This document identifies third-party source code included in or referenced by
the `hcu_cluster_manager` source distribution. The applicable license texts and
copyright notices remain controlling.

## DLRover

- Project: DLRover
- Upstream: https://github.com/intelligent-machine-learning/dlrover
- Local file: `cluster_manager/cluster_manager/utils/detection_utils.py`
- License: Apache License 2.0 (`Apache-2.0`)
- Copyright: Copyright 2023 The DLRover Authors
- Modifications: adapted for the HCU cluster manager integration

The original copyright and Apache-2.0 license header is retained in the source
file. Hygon modifications are also distributed under Apache-2.0.

## NVIDIA Resiliency Extension

- Project: NVIDIA Resiliency Extension
- Upstream: https://github.com/NVIDIA/nvidia-resiliency-ext
- Revision: `5eb5f7ec84e9aa1bf45c403b06d6ef766ea6784a`
- Local path: `hcu_resiliency_ext/nvidia_resiliency_ext`
- Distribution form: Git submodule
- License: Apache License 2.0 (`Apache-2.0`)

The submodule is a separate upstream work. Its own license and copyright files
apply to its contents. The superproject pins the revision listed above; release
builds must initialize that exact Gitlink revision and must not use
`git submodule update --remote`.

## NVIDIA profiling-derived implementation

- Local file: `hcu_resiliency_ext/hcu_resiliency_ext/shared_utils/profiling.py`
- License: Apache License 2.0 (`Apache-2.0`)
- Upstream copyright: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES
- Modifications: adapted by Hygon Information Technology Co., Ltd. for HCU
  environments

The NVIDIA copyright and license header is retained in the source file.

## External runtime dependencies

Python packages, system tools, container base images, and training frameworks
downloaded or installed separately are not bundled as source code in this
repository. They are governed by their own licenses and terms. Consult each
subproject's package metadata, deployment manifests, and documentation for the
applicable dependency list.
