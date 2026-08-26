<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Security Policy

## Reporting a vulnerability

Do not publish credentials, cluster logs, host inventories, workload data, or
unpatched vulnerability details in a public issue. After this repository is
published on GitHub, use the repository's private security-advisory channel to
report vulnerabilities to the maintainers.

## Secrets and deployment data

- Inject webhook URLs and other credentials through environment variables or
  Kubernetes Secrets. Never commit them to source control.
- Treat hostfiles, blacklist state, logs, checkpoint metadata, and environment
  reports as potentially sensitive runtime data.
- Replace registry addresses, node names, mount paths, namespaces, and account
  names with documented examples before sharing diagnostics.
- Rotate a credential immediately if it has ever appeared in a commit, build
  artifact, log, review comment, or chat transcript.

## Operational safety

Several tools can terminate distributed workloads, taint Kubernetes nodes, or
run active RDMA/RCCL checks. Review the target cluster and use an isolated test
environment before enabling disruptive operations.
