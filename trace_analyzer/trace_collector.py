import json
import logging
import os

# Issue: [B403:blacklist] Consider possible security implications associated with pickle module.
# Severity: Low   Confidence: High
# CWE: CWE-502 (https://cwe.mitre.org/data/definitions/502.html)
# More Info: https://bandit.readthedocs.io/en/1.8.3/blacklists/blacklist_imports.html#b403-import-pickle
import pickle  # nosec
from abc import ABC, abstractmethod

try:
    import torch
except ModuleNotFoundError:
    torch = None

logger = logging.getLogger(__name__)


class TraceCollector(ABC):
    """
    Base class for trace analyzers that process runtime-collected traces.
    Derived classes must implement `collect()` to gather trace data at runtime.
    """

    def __init__(
        self,
        path: str,
    ):
        self.path = path

    @abstractmethod
    def collect(self):
        pass


class TorchFRTraceCollector(TraceCollector):
    """
    A utility class for dumping NCCL traces from PyTorch for collective analysis.

    Each rank writes its trace to a separate file at the specified path, allowing users to
    analyze collective communication patterns post-execution. The class manages an internal
    thread that periodically dumps traces after a specified timeout interval.

    ### Features:
    - Each rank writes its trace file independently.
    - Traces are stored at the user-specified path for collective analysis.

    ### Usage:
    1. Initialize the class with the desired output path.
    2. Call `collect()` to dump the trace data.

    This class is particularly useful for debugging and performance profiling of NCCL-based
    distributed training in PyTorch.
    """

    def __init__(
        self,
        path: str,
        json=True,
    ):
        if torch is None:
            raise RuntimeError("TorchFRTraceCollector requires PyTorch to be installed.")
        super().__init__(path)
        self.rank = torch.distributed.get_rank()
        self.trace = None
        self.stack_trace = None
        self.dump_fn = torch._C._distributed_c10d._dump_nccl_trace
        self.json = json
        logger.info(f"{self.rank} created TorchFRTraceCollector")

    def collect(self):
        """
        Dumps the collected trace data to a file.

        This method performs the following steps:
        - Creates a unique output path for the trace file
        - Opens the file in write mode
        - Writes the trace data to the file
        - Flushes the file to ensure all data is written
        """

        output_path = f"{self.path}/_dump_{self.rank}"
        self.trace = self.dump_fn(
            includeCollectives=True, includeStackTraces=False, onlyActive=True
        )
        mode = 'wb'
        if self.json:
            output_path = output_path + '.json'
            mode = 'w'
        with open(output_path, mode) as f:
            logger.info(f"{self.rank} is about to dump its trace to {output_path}")
            # Issue: [B301:blacklist] Pickle and modules that wrap it can be unsafe when used to deserialize untrusted data, possible security issue.
            # Severity: Medium   Confidence: High
            # CWE: CWE-502 (https://cwe.mitre.org/data/definitions/502.html)
            # More Info: https://bandit.readthedocs.io/en/1.8.3/blacklists/blacklist_calls.html#b301-pickle
            dumped_dict = pickle.loads(self.trace)  # nosec
            if self.json:
                json.dump(dumped_dict, f, indent=4)
            else:
                pickle.dump(dumped_dict, f)
            os.fsync(f.fileno())
