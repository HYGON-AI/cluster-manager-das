# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

import pytest

import cluster_manager.config.global_config as global_config
from cluster_manager import main as main_module
from cluster_manager.controller.distributed_job_manager import DistributedJobManager
from cluster_manager.launcher.fault_detection import FaultDetection
from cluster_manager.monitor.nhc_monitor import NodePoolProxy


def make_args(**overrides):
    values = {
        "nodes_num": 2,
        "slots": 8,
        "exec": "/tmp/run.sh",
        "hostfile": "/tmp/hostfile",
        "job_name": None,
        "job_id": None,
        "sbatch_script": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_cli_does_not_accept_config_argument(monkeypatch):
    monkeypatch.setattr("sys.argv", ["hcu-cluster-inspect", "--config", "train.json"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.parse_args()

    assert exc_info.value.code == 2


def test_none_does_not_require_slurm_arguments(monkeypatch):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "NONE")

    assert main_module.validate_training_args(make_args()) == "NONE"


@pytest.mark.parametrize(
    ("field", "flag"),
    [
        ("job_id", "--job_id"),
        ("job_name", "--job_name"),
        ("sbatch_script", "--sbatch_script"),
    ],
)
def test_slurm_requires_slurm_arguments(monkeypatch, field, flag):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "SLURM")
    values = {
        "job_id": "1234",
        "job_name": "train-job",
        "sbatch_script": "/tmp/job.sbatch",
    }
    values[field] = None

    with pytest.raises(ValueError, match=flag):
        main_module.validate_training_args(make_args(**values))


def test_invalid_schedule_fails_fast(monkeypatch):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "UNKNOWN")

    with pytest.raises(ValueError, match="NONE or SLURM"):
        main_module.validate_training_args(make_args())


def test_build_runtime_args_maps_training_and_slurm_fields(monkeypatch):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "SLURM")
    args = make_args(
        job_id="1234",
        job_name="train-job",
        sbatch_script="/tmp/job.sbatch",
    )

    assert main_module.build_runtime_args(args) == {
        "required_nodes_num": 2,
        "slots_per_node": 8,
        "exec_path": "/tmp/run.sh",
        "hostfile": "/tmp/hostfile",
        "job_name": "train-job",
        "job_id": "1234",
        "sbatch_script": "/tmp/job.sbatch",
        "cluster_schedule": "SLURM",
    }


def test_missing_common_argument_fails_fast(monkeypatch):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "NONE")

    with pytest.raises(ValueError, match="--exec"):
        main_module.validate_training_args(make_args(exec=None))


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"nodes_num": 0}, "--nodes_num"),
        ({"slots": -1}, "--slots"),
    ],
)
def test_nodes_and_slots_must_be_positive(overrides, message):
    with pytest.raises(ValueError, match=message):
        main_module.validate_training_files(make_args(**overrides), "NONE")


def test_training_script_must_be_readable():
    with patch.object(main_module.os.path, "isfile", return_value=False):
        with pytest.raises(ValueError, match="Training script"):
            main_module.validate_training_files(make_args(), "NONE")


def test_slurm_file_precheck_does_not_require_hostfile():
    with patch.object(main_module.os.path, "isfile", return_value=True), patch.object(
        main_module.os, "access", return_value=True
    ), patch("builtins.open") as open_mock:
        main_module.validate_training_files(
            make_args(hostfile="/missing/hostfile"), "SLURM"
        )

    open_mock.assert_not_called()


def test_validate_sbatch_script_accepts_matching_job_and_nodes():
    content = "#!/usr/bin/env bash\n#SBATCH -J train-job\n#SBATCH -N 4\n"
    with patch.object(main_module.os.path, "exists", return_value=True), patch(
        "builtins.open", mock_open(read_data=content)
    ):
        assert main_module.validate_sbatch_script(
            "/tmp/job.sbatch", "train-job", 2
        )


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("#SBATCH -J other-job\n#SBATCH -N 2\n", "Job name mismatch"),
        ("#SBATCH -J train-job\n#SBATCH -N 1\n", "Insufficient nodes"),
    ],
)
def test_validate_sbatch_script_rejects_contract_mismatch(content, message):
    with patch.object(main_module.os.path, "exists", return_value=True), patch(
        "builtins.open", mock_open(read_data=content)
    ):
        with pytest.raises(ValueError, match=message):
            main_module.validate_sbatch_script(
                "/tmp/job.sbatch", "train-job", 2
            )


def _run_training_with_isolated_runtime(args, schedule):
    with patch.object(
        main_module, "validate_training_args", return_value=schedule
    ), patch.object(main_module, "validate_training_files"), patch.object(
        main_module, "validate_sbatch_script"
    ) as validate_sbatch, patch.object(
        main_module, "DistributedJobManager"
    ) as manager_cls, patch.object(
        main_module.os, "makedirs"
    ), patch.object(
        global_config, "WORK_DIR", "/tmp/work"
    ), patch.object(
        global_config, "MEGATRON_CONFIG", {}
    ), patch.object(
        global_config, "get_train_config", return_value={}
    ), patch.object(
        global_config, "get_megatron_config", return_value={}
    ):
        main_module.run_training(args)
    return validate_sbatch, manager_cls


def test_run_training_skips_sbatch_validation_in_none():
    args = make_args(job_name="display-only")

    validate_sbatch, manager_cls = _run_training_with_isolated_runtime(
        args, "NONE"
    )

    validate_sbatch.assert_not_called()
    manager_cls.return_value.run.assert_called_once_with()


def test_run_training_validates_sbatch_in_slurm():
    args = make_args(
        job_id="1234",
        job_name="train-job",
        sbatch_script="/tmp/job.sbatch",
    )

    validate_sbatch, manager_cls = _run_training_with_isolated_runtime(
        args, "SLURM"
    )

    validate_sbatch.assert_called_once_with(
        script_path=args.sbatch_script,
        expected_job_name="train-job",
        required_nodes_num=2,
    )
    manager_cls.return_value.run.assert_called_once_with()


def test_node_check_mode_does_not_validate_training_arguments():
    args = make_args()
    args.node_check = "node_check"

    with patch.object(main_module, "parse_args", return_value=args), patch.object(
        main_module, "run_node_check"
    ) as run_node_check, patch.object(main_module, "run_training") as run_training:
        main_module.main()

    run_node_check.assert_called_once_with(args)
    run_training.assert_not_called()


def test_none_validates_hostfile_node_count():
    with patch.object(main_module.os.path, "isfile", return_value=True), patch.object(
        main_module.os, "access", return_value=True
    ), patch("builtins.open", mock_open(read_data="node1 slots=8\nnode1 slots=8\n")):
        with pytest.raises(ValueError, match="1 unique nodes"):
            main_module.validate_training_files(make_args(), "NONE")


@patch("cluster_manager.controller.distributed_job_manager.Notify")
@patch("cluster_manager.controller.distributed_job_manager.create_launcher")
@patch("cluster_manager.controller.distributed_job_manager.EventBus")
@patch("cluster_manager.controller.distributed_job_manager.SlurmMgr")
def test_none_does_not_create_slurm_manager(
    slurm_mgr, event_bus, create_launcher, notify
):
    runtime_args = {
        "required_nodes_num": 2,
        "slots_per_node": 8,
        "exec_path": "/tmp/run.sh",
        "hostfile": "/tmp/hostfile",
        "job_name": None,
        "job_id": None,
        "sbatch_script": None,
        "cluster_schedule": "NONE",
    }

    manager = DistributedJobManager(runtime_args, "/tmp/workspace")

    assert manager.slurm_mgr is None
    slurm_mgr.assert_not_called()


@patch("cluster_manager.controller.distributed_job_manager.Notify")
@patch("cluster_manager.controller.distributed_job_manager.create_launcher")
@patch("cluster_manager.controller.distributed_job_manager.EventBus")
@patch("cluster_manager.controller.distributed_job_manager.SlurmMgr")
def test_slurm_constructs_manager(slurm_mgr, event_bus, create_launcher, notify):
    runtime_args = {
        "required_nodes_num": 2,
        "slots_per_node": 8,
        "exec_path": "/tmp/run.sh",
        "hostfile": "/tmp/hostfile",
        "job_name": "train-job",
        "job_id": "1234",
        "sbatch_script": "/tmp/job.sbatch",
        "cluster_schedule": "SLURM",
    }

    manager = DistributedJobManager(runtime_args, "/tmp/workspace")

    slurm_mgr.assert_called_once_with(
        "train-job", "/tmp/job.sbatch", "/tmp/hostfile", "1234"
    )
    assert manager.slurm_mgr is slurm_mgr.return_value


def test_none_run_skips_slurm_job_ensure():
    manager = DistributedJobManager.__new__(DistributedJobManager)
    manager.slurm_mgr = None
    manager.running = False
    manager._ensure_slurm_job = MagicMock()
    manager._init_components = MagicMock()
    manager._restore_state = MagicMock()

    manager.run()

    manager._ensure_slurm_job.assert_not_called()


def test_none_shortage_is_fatal_to_manager():
    manager = DistributedJobManager.__new__(DistributedJobManager)
    manager.cluster_schedule = "NONE"
    manager.job_name = None
    manager.runtime_args = {"required_nodes_num": 2, "slots_per_node": 8}
    manager.ctx = SimpleNamespace(
        node_pool_proxy=SimpleNamespace(
            apply_node_num_resources=lambda *_args: (None, None)
        )
    )

    with pytest.raises(RuntimeError, match="healthy nodes"):
        manager._start_training()


def test_none_allocation_never_queries_slurm():
    proxy = NodePoolProxy.__new__(NodePoolProxy)
    proxy._required_num = 0
    proxy._lock = MagicMock()
    proxy._lock.__enter__.return_value = None
    proxy._node_pool = MagicMock()
    proxy._node_pool.apply_node_num_resources.return_value = None
    proxy._node_pool.running_nodes = []
    proxy._slurm_mgr = None

    assert proxy.apply_node_num_resources(2, 8) == (None, None)


def test_none_skips_slurm_detection_commands(monkeypatch):
    monkeypatch.setattr(global_config, "CLUSTER_SCHEDULE", "NONE")
    detector = FaultDetection()

    with patch.object(detector, "_run_clush") as run_clush, patch(
        "cluster_manager.launcher.fault_detection.CmdExecutor.execute_command"
    ) as execute:
        assert detector.check_sinfo_R("/missing/hostfile") == {}
        assert detector.get_nodes_info("/missing/hostfile") == []
        assert detector.check_nodes_in_squeue(["node1"]) is False
        assert detector._run_scontrol("node1", 1) == ""

    run_clush.assert_not_called()
    execute.assert_not_called()


def test_start_script_maps_job_id_and_defaults_to_none():
    content = Path(main_module.__file__).parents[1].joinpath("start.sh").read_text(
        encoding="utf-8"
    )

    assert 'export CLUSTER_SCHEDULE="${CLUSTER_SCHEDULE:-NONE}"' in content
    assert '--job_id "${JOB_ID}"' in content
