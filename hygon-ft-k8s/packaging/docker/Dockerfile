# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash procps coreutils grep util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY hygon-ft-k8s/packaging/docker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY hygon-ft-k8s/hygon_ft /app/hygon_ft
COPY hygon-ft-k8s/runtime /opt/hygon-ft
COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md /usr/share/licenses/hygon-ft-k8s/
RUN chmod +x /opt/hygon-ft/ft-launcher

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
