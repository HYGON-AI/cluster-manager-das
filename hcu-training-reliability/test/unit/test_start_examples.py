# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import re
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[2]


def _active_python_command(script_path: Path) -> str:
    lines = script_path.read_text(encoding="utf-8").splitlines()
    command_pattern = re.compile(r"^(?:nohup\s+)?python3?\s+")
    for index, line in enumerate(lines):
        if not command_pattern.match(line.strip()):
            continue
        command_parts = []
        for command_line in lines[index:]:
            stripped = command_line.strip()
            continued = stripped.endswith("\\")
            command_parts.append(
                stripped[:-1].rstrip() if continued else stripped
            )
            if not continued:
                return " ".join(command_parts)
    raise AssertionError(f"No active Python command in {script_path}")


def _assert_common_training_mappings(command: str):
    for flag, variable in (
        ("--nodes_num", "NODES_NUM"),
        ("--slots", "SLOTS"),
        ("--exec", "RUN_PATH"),
        ("--hostfile", "HOSTFILE"),
    ):
        assert f'{flag} "${{{variable}}}"' in command


def test_start_sh_uses_exact_slurm_argument_mappings():
    command = _active_python_command(PROJECT_DIR / "start.sh")
    _assert_common_training_mappings(command)
    assert '--job_id "${JOB_ID}"' in command
    assert '--job_name "${JOB_NAME}"' in command
    assert '--sbatch_script "${SBATCH_SCRIPT}"' in command


def test_none_example_has_no_slurm_arguments():
    script = PROJECT_DIR / "examples" / "start_none.sh"
    content = script.read_text(encoding="utf-8")
    command = _active_python_command(script)

    assert re.search(r"(?m)^export CLUSTER_SCHEDULE=NONE\s*$", content)
    _assert_common_training_mappings(command)
    assert "--job_id" not in command
    assert "--sbatch_script" not in command
    assert '--job_name "${JOB_NAME}"' in command


def test_slurm_example_has_all_slurm_arguments():
    script = PROJECT_DIR / "examples" / "start_slurm.sh"
    content = script.read_text(encoding="utf-8")
    command = _active_python_command(script)

    assert re.search(r"(?m)^export CLUSTER_SCHEDULE=SLURM\s*$", content)
    _assert_common_training_mappings(command)
    assert '--job_id "${JOB_ID}"' in command
    assert '--job_name "${JOB_NAME}"' in command
    assert '--sbatch_script "${SBATCH_SCRIPT}"' in command
