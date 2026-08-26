<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to hcu_cluster_manager

Thank you for considering a contribution. By submitting a contribution, you
represent that you have the right to submit it and agree that it is provided
under the Apache License, Version 2.0.

## Before opening a change

- Open an issue for substantial behavior, API, deployment, or compatibility
  changes before implementation.
- Keep a change focused on one problem and avoid unrelated formatting changes.
- Do not submit credentials, customer information, personal paths, private
  addresses, proprietary datasets, model weights, or internal registry tokens.
- Do not copy third-party code unless its provenance and license are known and
  compatible with this repository.

## Development requirements

- Use a supported Python version documented by the affected subproject.
- Add or update tests for behavior changes.
- Run the affected unit tests locally. The repository-wide Python test entry is:

  ```bash
  python -m pytest
  ```

- Validate shell scripts with `bash -n` where applicable.
- Keep public examples generic and free of organization-specific paths,
  hostnames, accounts, and customer names.

Hardware-, Slurm-, MPI-, Kubernetes-, or HCU-dependent changes must describe
the environment and integration validation performed. Unit tests alone do not
establish production compatibility.

## Licensing and provenance

New original, commentable source files should carry:

```text
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
```

Use the appropriate comment syntax for the file type. Do not replace or remove
third-party copyright, patent, attribution, license, or NOTICE text. A modified
third-party file must retain its original notices and include a prominent Hygon
modification notice.

When adding a third-party component, update `THIRD_PARTY_NOTICES.md` with its
name, upstream URL, exact version or commit, local path, license, and whether it
was modified. Include any license or NOTICE files required by the upstream work.

## Commit certification

Contributions should be certified with the Developer Certificate of Origin by
adding a sign-off to each commit:

```text
Signed-off-by: Your Name <your.email@example.com>
```

Use `git commit -s` to add the line automatically. The sign-off certifies that
you have the right to submit the contribution under the project's license.

## Pull requests

A pull request should contain:

- a concise description of the problem and solution;
- affected modules and compatibility considerations;
- test commands and results;
- operational, security, and rollback impact where applicable;
- third-party provenance and license changes, if any.

Security vulnerabilities must follow `SECURITY.md` and must not be disclosed in
a public issue before a coordinated fix is available.
