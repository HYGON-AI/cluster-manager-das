# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Launcher-aware active-test workers.

The shell payloads in this directory are dispatched by the unified
``hcu-cluster-run`` entry point.  Python workers are deliberately small
and only run after the caller has sourced the user-provided ``env.sh``.
"""
