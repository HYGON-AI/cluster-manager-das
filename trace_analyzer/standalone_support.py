import argparse
import asyncio
import inspect
import logging
import os
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from enum import Enum, auto
from functools import partial
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar

T = TypeVar("T")
R = TypeVar("R")

logger = logging.getLogger(__name__)
LOG_VALUE_PREVIEW_CHARS = 16_384

_attribution_run_ctx: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
    "trace_analyzer_attribution_run_args",
    default=None,
)


def bounded_log_value(value: Any, *, limit: int = LOG_VALUE_PREVIEW_CHARS) -> str:
    if limit <= 0:
        return ""
    try:
        text = str(value)
    except Exception as e:
        return f"<unprintable {type(value).__name__}: {e}>"
    if len(text) <= limit:
        return text
    suffix = f"... [truncated; original {len(text)} chars]"
    preview_len = max(0, limit - len(suffix))
    return f"{text[:preview_len]}{suffix}"


def normalize_attribution_args(args: Any) -> Dict[str, Any]:
    if isinstance(args, argparse.Namespace):
        return dict(vars(args))
    if isinstance(args, Mapping):
        return dict(args)
    if isinstance(args, (list, tuple)) and len(args) == 2:
        return {"input_data": [args[0], args[1]]}
    if getattr(args, "__dict__", None) is not None:
        return dict(vars(args))
    raise TypeError(
        "run() / run_sync() args must be Namespace, mapping, length-2 list/tuple, "
        f"or simple object with __dict__, not {type(args).__name__}"
    )


def peek_attribution_run_args() -> Optional[Dict[str, Any]]:
    return _attribution_run_ctx.get()


def effective_run_or_init_config(init_config: Mapping[str, Any]) -> Dict[str, Any]:
    run = peek_attribution_run_args()
    if run is not None:
        return run
    return dict(init_config)


def _callable_arity(fn: Callable[..., Any]) -> int:
    return len(inspect.signature(fn).parameters)


class AttributionState(Enum):
    STOP = auto()
    CONTINUE = auto()


class NVRxAttribution(Generic[T, R]):
    _loop_local = threading.local()

    @classmethod
    def get_shared_loop(cls):
        loop = getattr(cls._loop_local, "loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            cls._loop_local.loop = loop
        return loop

    @classmethod
    def reset_thread_event_loop(cls) -> None:
        if not hasattr(cls._loop_local, "loop"):
            return
        loop = cls._loop_local.loop
        try:
            if not loop.is_closed():
                loop.close()
        finally:
            delattr(cls._loop_local, "loop")
            asyncio.set_event_loop(None)

    def __init__(
        self,
        preprocess_input: Callable[[Any], Any],
        attribution: Callable[[Any], R],
        output_handler: Callable[[R], None],
        thread_pool: Optional[ThreadPoolExecutor] = None,
    ):
        self._preprocess_input = preprocess_input
        self._attribution = attribution
        self._output_handler = output_handler
        self._thread_pool = thread_pool or ThreadPoolExecutor(max_workers=4)
        self._loop = self.get_shared_loop()

    async def _run_sync_in_thread(self, func: Callable, *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._thread_pool, partial(func, *args, **kwargs))

    async def _preprocess_input_inner(self, run_args: Dict[str, Any]) -> Any:
        fn = self._preprocess_input
        arity = _callable_arity(fn)
        if arity > 1:
            raise TypeError(f"preprocess_input must accept 0 or 1 arguments, not {arity}")
        if inspect.iscoroutinefunction(fn):
            if arity == 0:
                return await fn()
            return await fn(run_args)
        if arity == 0:
            return await self._run_sync_in_thread(fn)
        return await self._run_sync_in_thread(fn, run_args)

    async def do_attribution(self, preprocessed_data: Any) -> R:
        if inspect.iscoroutinefunction(self._attribution):
            return await self._attribution(preprocessed_data)
        return await self._run_sync_in_thread(self._attribution, preprocessed_data)

    async def output_handler(self, attribution_result: R):
        if inspect.iscoroutinefunction(self._output_handler):
            return await self._output_handler(attribution_result)
        return await self._run_sync_in_thread(self._output_handler, attribution_result)

    async def run(self, args: Any):
        run_args = normalize_attribution_args(args)
        token = _attribution_run_ctx.set(run_args)
        try:
            preprocessed_data = await self._preprocess_input_inner(run_args)
            attribution_result = await self.do_attribution(preprocessed_data)
            return await self.output_handler(attribution_result)
        finally:
            _attribution_run_ctx.reset(token)

    def run_sync(self, args: Any):
        loop = self.get_shared_loop()
        if loop is not self._loop:
            self._loop = loop
        return loop.run_until_complete(self.run(args))

    def __del__(self):
        if hasattr(self, "_thread_pool"):
            self._thread_pool.shutdown(wait=False)


def path_is_under_allowed_root(path: str, allowed_root: str) -> bool:
    try:
        resolved_path = os.path.realpath(os.path.abspath(path))
        resolved_root = os.path.realpath(os.path.abspath(allowed_root))
        return os.path.commonpath([resolved_path, resolved_root]) == resolved_root
    except (OSError, ValueError):
        return False
