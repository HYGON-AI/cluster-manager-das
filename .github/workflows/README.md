# PR CI

This repository runs path-aware unit tests for pull requests targeting `main`.
There is no nightly workflow and no custom PR planner.

## Module routing

| Workflow | Changed path | Test directory |
| --- | --- | --- |
| `PR Test (cluster_manager)` | `cluster_manager/**` | `cluster_manager/test/unit` |
| `PR Test (hcu_resiliency_ext)` | `hcu_resiliency_ext/**`, `.gitmodules` | `hcu_resiliency_ext/tests` |
| `PR Test (hcu-envcheck)` | `hcu-envcheck/**` | `hcu-envcheck/tests` |
| `PR Test (hygon-ft-k8s)` | `hygon-ft-k8s/**` | `hygon-ft-k8s/tests` |
| `PR Test (stack-analyzer)` | `stack-analyzer/**` | `stack-analyzer/tests` |

Each event-level `paths` filter starts only the corresponding workflow. A
change to the shared `_run-unit-tests.yml` implementation or
`.github/actionlint.yaml` starts all five workflows. Unrelated documentation
changes do not start any test workflow.

The PR gate uses `pull_request_target`, so the triggering workflow text always
comes from the target branch. Same-repository and fork PRs both run automatically
without an approval job. Checkout uses the PR head repository and immutable head
SHA from the event; changes to workflow files in the PR cannot change the trusted
controller for that run.
The checked-out PR source is mounted read-only at `/ci-source` and copied into
a writable, temporary `/workspace` inside the container. Tests can create
fixtures beside the source without modifying the runner checkout. `/tmp` and
`/workspace` allow executable test fixtures but disallow devices and setuid
executables. The container receives no GitHub token or network access. All
Linux capabilities are dropped except SETUID/SETGID, needed by cross-UID
process-control tests; privilege escalation is disabled.
The test container is limited to 2 CPUs, 2 GiB RAM, and 256 processes.

Existing failed runs retain the old target-branch workflow. After this change
reaches `main`, emit a fresh qualifying PR event (for example, a new commit or
closing and reopening the PR) to test an existing fork PR.

## Runner and image preparation

Authorize this repository for runner group `ci-general`. The runner must have
the labels `self-hosted`, `ci`, `nmz2`, and `bw1100`, plus a working Docker
daemon. This routes the jobs to `ci-nmz2`.

Load the reviewed image archive on every eligible runner before enabling the
workflows:

```bash
docker load --input /path/to/hygon-ft-controller-latest.tar
docker image inspect --format '{{.Id}}' hygon/ft-controller:latest
```

The expected image metadata is:

| Field | Value |
| --- | --- |
| Tag | `hygon/ft-controller:latest` |
| Image ID | `sha256:e2bab66a143d17a9526c14af34f6ba2bd6486d74c51b5c4deb74f33f9d98eb93` |
| Archive SHA-256 | `94c82bc110b2dbfb7e52b453bf2604e1088b8a3f8b45949a58c0d95a41202f0a` |
| Python | `3.11.16` |

The workflow rejects a matching tag with a different image ID. The supplied
image contains the runtime Kubernetes and requests dependencies but not the
test runner. A trusted setup container installs fixed versions of `pytest` and
`numpy` into a per-job Docker volume. The test container mounts that volume
read-only and runs with `--network none`.

Because these are new `pull_request_target` workflows, the first real run can
only occur after the reviewed workflows are present on the default branch and
a new qualifying pull-request event is emitted.
