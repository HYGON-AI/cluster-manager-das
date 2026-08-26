# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock


def _package(name):
    module = ModuleType(name)
    module.__path__ = []
    return module


def _install_profiling_dependencies(monkeypatch):
    class Config:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Provider:
        _instance = None

        def __init__(self):
            self.one_logger_ready = False
            self.config = None
            self.recorder = MagicMock()

        @classmethod
        def instance(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def with_base_config(self, config):
            self.config = config
            return self

        def with_exporter(self, _exporter):
            return self

        def configure_provider(self):
            return None

    class Attributes:
        def __init__(self):
            self.values = {}

        def add(self, key, value):
            self.values[key] = value

    modules = {
        "nv_one_logger": _package("nv_one_logger"),
        "nv_one_logger.api": _package("nv_one_logger.api"),
        "nv_one_logger.api.config": ModuleType("nv_one_logger.api.config"),
        "nv_one_logger.core": _package("nv_one_logger.core"),
        "nv_one_logger.core.attributes": ModuleType("nv_one_logger.core.attributes"),
        "nv_one_logger.core.event": ModuleType("nv_one_logger.core.event"),
        "nv_one_logger.core.span": ModuleType("nv_one_logger.core.span"),
        "nv_one_logger.exporter": _package("nv_one_logger.exporter"),
        "nv_one_logger.exporter.file_exporter": ModuleType(
            "nv_one_logger.exporter.file_exporter"
        ),
        "nv_one_logger.training_telemetry": _package(
            "nv_one_logger.training_telemetry"
        ),
        "nv_one_logger.training_telemetry.api": _package(
            "nv_one_logger.training_telemetry.api"
        ),
        "nv_one_logger.training_telemetry.api.training_telemetry_provider": ModuleType(
            "nv_one_logger.training_telemetry.api.training_telemetry_provider"
        ),
        "nvidia_resiliency_ext": _package("nvidia_resiliency_ext"),
        "nvidia_resiliency_ext.shared_utils": _package(
            "nvidia_resiliency_ext.shared_utils"
        ),
        "nvidia_resiliency_ext.shared_utils.log_manager": ModuleType(
            "nvidia_resiliency_ext.shared_utils.log_manager"
        ),
    }
    modules["nv_one_logger.api.config"].LoggerConfig = Config
    modules["nv_one_logger.api.config"].OneLoggerConfig = Config
    modules["nv_one_logger.core.attributes"].Attributes = Attributes
    modules["nv_one_logger.core.event"].Event = SimpleNamespace(
        create=lambda name, attrs: (name, attrs)
    )
    modules["nv_one_logger.core.span"].StandardSpanName = SimpleNamespace(
        APPLICATION="application"
    )
    modules["nv_one_logger.exporter.file_exporter"].FileExporter = Config
    modules[
        "nv_one_logger.training_telemetry.api.training_telemetry_provider"
    ].TrainingTelemetryProvider = Provider
    modules["nvidia_resiliency_ext.shared_utils.log_manager"].LogConfig = SimpleNamespace(
        name="nvrx"
    )

    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_profiler_records_cycle_and_stable_event_id(monkeypatch):
    _install_profiling_dependencies(monkeypatch)
    module_name = "hcu_resiliency_ext.hcu_resiliency_ext.shared_utils.profiling"
    sys.modules.pop(module_name, None)
    profiling = importlib.import_module(module_name)
    profiler = profiling.FaultToleranceProfiler()
    profiler._publish_metrics = MagicMock()
    monkeypatch.setattr(profiling.time, "time", lambda: 1_700_000_000.25)

    event_id = profiler.record_event(
        profiling.ProfilingEvent.FAILURE_DETECTED, node_id="node-a", rank=3
    )

    assert profiler._current_cycle == 1
    assert event_id == "failure_detected_1700000000.25_node-a_3"
    profiler._publish_metrics.assert_called_once()


def test_profiler_mpi_session_tag_uses_job_id(monkeypatch):
    _install_profiling_dependencies(monkeypatch)
    module_name = "hcu_resiliency_ext.hcu_resiliency_ext.shared_utils.profiling"
    sys.modules.pop(module_name, None)
    profiling = importlib.import_module(module_name)
    monkeypatch.setenv("MPI_JOB_ID", "mpi-job-42")

    profiler = profiling.FaultToleranceProfiler()

    assert profiler._get_mpi_session_tag() == "mpi-job-42"
    assert profiler._timestamp_to_utc_datetime(0) == "1970-01-01 00:00:00.000"
