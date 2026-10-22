# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Resolution of the thread-model and process-model fields of :class:`ServerOption`.

:func:`~trpc_sdk.server.create_server` first calls :func:`apply_process_model`,
which translates YAML ``global.processmodel`` onto
:class:`~trpc_sdk.server.ServerOption`: ``single_process`` becomes merge mode
(``io_num`` / ``handle_num`` become ``io_thread_num`` / ``handle_thread_num``)
and ``multi_process`` becomes separate or peer mode (``handle_num`` becomes
``worker_num``). ``0`` or ``''`` there means "not configured", and an empty
``processmodel.mode`` keeps the option defaults (single-loop merge).

:func:`resolve_server_option` then runs once to overwrite those fields, plus
the handler limits and ``ready_timeout`` copied from YAML ``server:``, with
the values the server runs with. An out-of-range count is logged as a warning
and replaced by a fallback, so a misconfigured service still starts.
Contradictions rather than typos fail startup with :class:`ValueError`: a
thread-model request under a multi-process mode, an ``io_handle_type`` that
disagrees with ``handle_thread_num``, or an ``io_handle_type`` that names no
thread model. Both checks matter for an option built by hand; one translated
from ``global.processmodel`` always passes them.

Thread model (merge mode only): ``handle_thread_num == 0`` runs
``io_thread_num`` serving threads that do I/O and run handlers (``merge``);
``handle_thread_num > 0`` runs ``io_thread_num`` I/O loops plus
``handle_thread_num`` handle threads (``separate``). Streaming RPC runs under
every thread model, so nothing here depends on the registered services.

Process model: ``worker_num`` is the rpc task count of ``separate`` and the
worker count of ``peer``; ``0`` resolves to
:func:`~trpc_sdk.config.default_worker_num` (the CPU count), read here, at
startup, rather than frozen into a module constant at import. ``merge`` does
not read it.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.config import ProcessModel
from trpc_sdk.config import ServerService
from trpc_sdk.config import default_worker_num
from trpc_sdk.log import logger
from trpc_sdk.runtime import IO_HANDLE_TYPE_MERGE
from trpc_sdk.runtime import IO_HANDLE_TYPE_SEPARATE
from trpc_sdk.runtime import MODE_MERGE
from trpc_sdk.runtime import MODE_PEER
from trpc_sdk.runtime import MODE_SEPARATE
from trpc_sdk.runtime import MODE_SPELLINGS
from trpc_sdk.runtime import PROCESS_MODEL_MULTI
from trpc_sdk.runtime import PROCESS_MODEL_SINGLE
from ._constants import DEFAULT_HANDLE_THREAD_NUM
from ._constants import DEFAULT_HANDLER_MAX_CONCURRENCY
from ._constants import DEFAULT_HANDLER_QUEUE_SIZE
from ._constants import DEFAULT_IO_HANDLE_TYPE
from ._constants import DEFAULT_IO_THREAD_NUM
from ._constants import DEFAULT_READY_TIMEOUT_MS
from ._constants import DEFAULT_TASK_NUM
from ._constants import DEFAULT_WORKER_NUM
from ._constants import THREADS_PER_CPU_WARN
from ._constants import WORKERS_PER_CPU_WARN

if TYPE_CHECKING:
    from ._option import ServerOption

# ``global.processmodel.io_handle_type`` spellings accepted under
# ``multi_process`` only: there, merging I/O and handlers means peer mode.
_MULTI_PROCESS_SPELLINGS = {MODE_PEER: IO_HANDLE_TYPE_MERGE}


def apply_process_model(option: ServerOption, model: Optional[ProcessModel]) -> None:
    """Translate YAML ``global.processmodel`` into the raw ``ServerOption`` fields.

    Runs before :func:`resolve_server_option`, which then validates the
    result as usual. ``task_num`` is always applied; the rest only when
    ``model.mode`` is set.

    Args:
        option: Server option to fill.
            ``task_num`` is overwritten; with ``model.mode`` set, ``mode``,
            ``io_handle_type``, ``io_thread_num``, ``handle_thread_num`` and
            ``worker_num`` are too.
        model: YAML ``global.processmodel``; ``None`` leaves ``option`` untouched.

    Raises:
        ValueError: If ``mode`` or ``io_handle_type`` names nothing, or
            ``single_process`` with ``separate`` has no ``handle_num``.
    """
    if model is None:
        return
    option.task_num = _resolve_task_num(model.task_num)
    if not model.mode:
        return
    if model.mode == PROCESS_MODEL_SINGLE:
        _apply_single_process(option, model)
    elif model.mode == PROCESS_MODEL_MULTI:
        _apply_multi_process(option, model)
    else:
        msg = (f"global.processmodel.mode={model.mode!r} is not "
               f"{PROCESS_MODEL_SINGLE!r} or {PROCESS_MODEL_MULTI!r}")
        logger.warning(msg)
        raise ValueError(msg)


def _resolve_task_num(value: int) -> int:
    """Resolve the default process count of a task that sets no ``task_num``.

    Args:
        value: YAML ``global.processmodel.task_num``; ``0`` means not configured.

    Returns:
        The default task process count, at least 1.
    """
    if value < 0:
        logger.warning("global.processmodel.task_num=%s is negative; using %s", value, DEFAULT_TASK_NUM)
        return DEFAULT_TASK_NUM
    return value or DEFAULT_TASK_NUM


def _process_model_io_handle_type(value: str, default: str, extra_spellings: Dict[str, str]) -> str:
    """Canonicalize ``global.processmodel.io_handle_type``.

    Args:
        value: YAML value; ``''`` means ``default``.
        default: Thread model used when ``value`` is empty.
        extra_spellings: Spellings accepted on top of :data:`MODE_SPELLINGS`.

    Returns:
        :data:`IO_HANDLE_TYPE_MERGE` or :data:`IO_HANDLE_TYPE_SEPARATE`.

    Raises:
        ValueError: If ``value`` names neither.
    """
    if not value:
        return default
    canonical = extra_spellings.get(value, _canonical_name(value))
    if canonical not in (IO_HANDLE_TYPE_MERGE, IO_HANDLE_TYPE_SEPARATE):
        msg = f"global.processmodel.io_handle_type={value!r} is not 'merge' or 'separate'"
        logger.warning(msg)
        raise ValueError(msg)
    return canonical


def _apply_single_process(option: ServerOption, model: ProcessModel) -> None:
    """Map ``single_process`` onto merge mode and its thread model.

    Under ``merge`` every serving thread does I/O and runs handlers, so
    ``io_num`` and ``handle_num`` describe the same threads: one of them
    alone is enough, and when they disagree ``handle_num`` wins.

    Args:
        option: Server option to overwrite.
        model: YAML ``global.processmodel`` with ``mode: single_process``.

    Raises:
        ValueError: If ``io_handle_type`` is invalid, or ``separate`` has no
            positive ``handle_num``.
    """
    io_handle_type = _process_model_io_handle_type(model.io_handle_type, IO_HANDLE_TYPE_MERGE, {})
    option.mode = MODE_MERGE
    option.io_handle_type = io_handle_type
    option.worker_num = DEFAULT_WORKER_NUM
    if io_handle_type == IO_HANDLE_TYPE_MERGE:
        thread_num = model.handle_num or model.io_num
        if model.io_num and model.handle_num and model.io_num != model.handle_num:
            logger.warning("global.processmodel: io_num=%s and handle_num=%s must be equal when io_handle_type is "
                           "merge; using handle_num=%s", model.io_num, model.handle_num, model.handle_num)
        option.io_thread_num = thread_num
        option.handle_thread_num = DEFAULT_HANDLE_THREAD_NUM
        return
    if model.handle_num <= 0:
        msg = (f"global.processmodel: io_handle_type 'separate' needs handle_num > 0, "
               f"got handle_num={model.handle_num}")
        logger.warning(msg)
        raise ValueError(msg)
    option.io_thread_num = model.io_num
    option.handle_thread_num = model.handle_num


def _apply_multi_process(option: ServerOption, model: ProcessModel) -> None:
    """Map ``multi_process`` onto separate mode or peer mode.

    Both run one loop per process, so the thread fields are reset and
    ``handle_num`` becomes ``worker_num``: the rpc task count of separate,
    the worker count of peer.

    Args:
        option: Server option to overwrite.
        model: YAML ``global.processmodel`` with ``mode: multi_process``.

    Raises:
        ValueError: If ``io_handle_type`` is invalid.
    """
    io_handle_type = _process_model_io_handle_type(model.io_handle_type, IO_HANDLE_TYPE_SEPARATE,
                                                   _MULTI_PROCESS_SPELLINGS)
    if io_handle_type == IO_HANDLE_TYPE_SEPARATE:
        option.mode = MODE_SEPARATE
        if model.io_num not in (0, 1):
            logger.warning("global.processmodel: io_num=%s, but multi_process separate runs a single I/O process; "
                           "using 1", model.io_num)
    else:
        option.mode = MODE_PEER
        if model.io_num:
            logger.warning("global.processmodel: io_num=%s has no effect in multi_process merge (peer); ignored",
                           model.io_num)
    option.io_handle_type = DEFAULT_IO_HANDLE_TYPE
    option.io_thread_num = DEFAULT_IO_THREAD_NUM
    option.handle_thread_num = DEFAULT_HANDLE_THREAD_NUM
    option.worker_num = model.handle_num


def resolve_server_option(option: ServerOption, services: List[ServerService]) -> None:
    """Validate the thread- and process-model fields, warn, and write the resolved values back.

    Args:
        option: Server option filled by :func:`apply_process_model` and from
            YAML ``server:``. ``mode``, ``io_handle_type``, ``io_thread_num``,
            ``handle_thread_num``, ``handler_max_concurrency``,
            ``handler_queue_size``, ``tcp_reuse_port`` and ``ready_timeout``
            are overwritten, plus ``worker_num`` in separate and peer mode.
        services: YAML ``server.service[]`` entries, read by the macOS
            separate-mode warning.

    Raises:
        ValueError: If threads are requested outside merge mode, or
            ``io_handle_type`` is invalid or disagrees with ``handle_thread_num``.
    """
    option.mode = _canonical_name(option.mode)
    _resolve_thread_options(option)
    if option.mode == MODE_SEPARATE:
        _resolve_separate_options(option, services)
    elif option.mode == MODE_PEER:
        _resolve_peer_options(option)
    elif option.worker_num != DEFAULT_WORKER_NUM:
        logger.warning("ServerOption.worker_num=%s is only read in separate and peer mode; ignored",
                       option.worker_num)


def _canonical_name(value: str) -> str:
    """Map an accepted spelling (``seperate``) to its canonical name; other values pass through."""
    return MODE_SPELLINGS.get(value, value)


def _resolve_thread_options(option: ServerOption) -> None:
    """Resolve the thread-model fields in place.

    Args:
        option: Server option holding the raw YAML values.

    Raises:
        ValueError: If ``option.mode`` is not merge and threads were requested,
            or ``io_handle_type`` is invalid or disagrees with ``handle_thread_num``.
    """
    _reject_threads_outside_merge(option.mode, option.handle_thread_num, option.io_thread_num)
    io_handle_type = _resolve_io_handle_type(option.io_handle_type, option.handle_thread_num)

    io_thread_num = _resolve_thread_count("io_num", option.io_thread_num)
    if io_handle_type == IO_HANDLE_TYPE_SEPARATE:
        _warn_if_oversized("handle_num", option.handle_thread_num)
        handle_thread_num = option.handle_thread_num
    else:
        if option.handle_thread_num < 0:
            logger.warning("global.processmodel.handle_num=%s is negative; using %s", option.handle_thread_num,
                           DEFAULT_HANDLE_THREAD_NUM)
        handle_thread_num = DEFAULT_HANDLE_THREAD_NUM

    threaded = io_handle_type == IO_HANDLE_TYPE_SEPARATE or io_thread_num > 1
    option.io_handle_type = io_handle_type
    option.io_thread_num = io_thread_num
    option.handle_thread_num = handle_thread_num
    option.handler_max_concurrency, option.handler_queue_size = _resolve_concurrency(
        option.handler_max_concurrency, option.handler_queue_size, io_handle_type)
    option.tcp_reuse_port = bool(option.tcp_reuse_port)
    # Separate and peer mode wait for their children against the same deadline.
    waits_for_startup = threaded or option.mode in (MODE_SEPARATE, MODE_PEER)
    option.ready_timeout = _resolve_ready_timeout(option.ready_timeout, waits_for_startup)


def _resolve_io_handle_type(value: str, handle_thread_num: int) -> str:
    """Pick the thread model from ``handle_thread_num``, checked against an explicit ``io_handle_type``.

    Args:
        value: ``ServerOption.io_handle_type``; ``''`` means infer it.
        handle_thread_num: ``ServerOption.handle_thread_num``.

    Returns:
        :data:`IO_HANDLE_TYPE_SEPARATE` when ``handle_thread_num > 0``,
        :data:`IO_HANDLE_TYPE_MERGE` otherwise.

    Raises:
        ValueError: If ``value`` names no thread model or disagrees with
            ``handle_thread_num``.
    """
    inferred = IO_HANDLE_TYPE_SEPARATE if handle_thread_num > 0 else IO_HANDLE_TYPE_MERGE
    if not value:
        return inferred
    value = _canonical_name(value)
    if value not in (IO_HANDLE_TYPE_MERGE, IO_HANDLE_TYPE_SEPARATE):
        msg = f"ServerOption.io_handle_type={value!r} is not 'merge' or 'separate'"
    elif value != inferred:
        msg = (f"ServerOption.io_handle_type={value!r} disagrees with handle_thread_num={handle_thread_num}: "
               "'separate' needs handle_thread_num > 0 and 'merge' needs it unset")
    else:
        return value
    logger.warning(msg)
    raise ValueError(msg)


def _reject_threads_outside_merge(mode: str, handle_thread_num: int, io_thread_num: int) -> None:
    """Fail startup when a multi-process mode is asked to run threads too.

    Separate and peer mode run one loop per process by design, so a
    thread-model request there is contradictory rather than tunable.

    Args:
        mode: ``ServerOption.mode``; ``''`` means merge.
        handle_thread_num: ``ServerOption.handle_thread_num``.
        io_thread_num: ``ServerOption.io_thread_num``.

    Raises:
        ValueError: If ``mode`` is not merge and threads were requested.
    """
    if not mode or mode == MODE_MERGE:
        return
    if handle_thread_num > 0 or io_thread_num > 1:
        msg = (f"ServerOption.mode={mode!r} runs a single thread per process; "
               f"handle_thread_num={handle_thread_num} / io_thread_num={io_thread_num} is only valid in merge mode")
        logger.warning(msg)
        raise ValueError(msg)


def _resolve_thread_count(field_name: str, value: int) -> int:
    """Resolve ``io_thread_num``, the same way for both thread models.

    Merge reads it as the number of serving threads, separate as the number
    of I/O loops (the main loop included).

    Args:
        field_name: ``global.processmodel`` field name, for the warnings.
        value: Thread count; ``0`` means not configured.

    Returns:
        The thread count to run with.
    """
    if value == 0:
        return DEFAULT_IO_THREAD_NUM
    if value < 0:
        logger.warning("global.processmodel.%s=%s is negative; using %s", field_name, value, DEFAULT_IO_THREAD_NUM)
        return DEFAULT_IO_THREAD_NUM
    _warn_if_oversized(field_name, value)
    return value


def _warn_if_oversized(field_name: str, value: int) -> None:
    """Warn when a thread count is far above the CPU count; the value is kept.

    Args:
        field_name: ``global.processmodel`` field name, for the warning.
        value: Requested thread count.
    """
    limit = THREADS_PER_CPU_WARN * (os.cpu_count() or 1)
    if value > limit:
        logger.warning("global.processmodel.%s=%s is more than %s times the CPU count (%s); keeping it, but it is likely too many",
                       field_name, value, THREADS_PER_CPU_WARN, os.cpu_count())


def _resolve_ready_timeout(value: int, waits_for_startup: bool) -> int:
    """Resolve how long thread or rpc task startup may take, in milliseconds.

    A single-loop merge server has nothing to wait for, so the value is not
    validated there and simply resolves to the default.

    Args:
        value: YAML ``server.ready_timeout``; ``0`` means not configured.
        waits_for_startup: Whether the resolved model starts threads, or is
            separate or peer mode waiting for its children.

    Returns:
        The timeout to run with.
    """
    if not waits_for_startup or value == 0:
        return DEFAULT_READY_TIMEOUT_MS
    if value < 0:
        logger.warning("server.ready_timeout=%s is negative; using %s", value, DEFAULT_READY_TIMEOUT_MS)
        return DEFAULT_READY_TIMEOUT_MS
    return value


def _resolve_concurrency(max_concurrency: int, queue_size: int, io_handle_type: str) -> Tuple[int, int]:
    """Resolve the in-flight limit and its overflow queue capacity.

    Only the separate thread model (``handle_thread_num > 0``) enforces them;
    merge ignores both.

    Args:
        max_concurrency: YAML ``server.handler_max_concurrency``.
        queue_size: YAML ``server.handler_queue_size``.
        io_handle_type: Resolved thread model.

    Returns:
        ``(handler_max_concurrency, handler_queue_size)``.
    """
    if max_concurrency < 0:
        logger.warning("server.handler_max_concurrency=%s is negative; using %s (unlimited)", max_concurrency,
                       DEFAULT_HANDLER_MAX_CONCURRENCY)
        max_concurrency = DEFAULT_HANDLER_MAX_CONCURRENCY
    if queue_size < 0:
        logger.warning("server.handler_queue_size=%s is negative; using %s", queue_size, DEFAULT_HANDLER_QUEUE_SIZE)
        queue_size = DEFAULT_HANDLER_QUEUE_SIZE

    if max_concurrency > 0 and io_handle_type != IO_HANDLE_TYPE_SEPARATE:
        logger.warning("server.handler_max_concurrency=%s is only enforced when handle_thread_num > 0; "
                       "ignored", max_concurrency)
    elif max_concurrency > 0 and queue_size == 0:
        logger.warning("server.handler_queue_size is 0: requests beyond handler_max_concurrency=%s are dropped",
                       max_concurrency)
    elif max_concurrency == 0 and queue_size != DEFAULT_HANDLER_QUEUE_SIZE:
        logger.warning("server.handler_queue_size=%s has no effect: handler_max_concurrency is 0 (unlimited)",
                       queue_size)
    return max_concurrency, queue_size


def _resolve_separate_options(option: ServerOption, services: List[ServerService]) -> None:
    """Resolve ``worker_num`` (rpc tasks) and warn about fields separate mode does not read.

    Args:
        option: Server option with ``mode: separate``, thread model resolved.
        services: YAML ``server.service[]`` entries.
    """
    option.worker_num = _resolve_worker_num(option.worker_num)
    _warn_single_rpc_task_on_macos(option.worker_num, services)
    if (option.handler_max_concurrency != DEFAULT_HANDLER_MAX_CONCURRENCY
            or option.handler_queue_size != DEFAULT_HANDLER_QUEUE_SIZE):
        logger.warning("server.handler_max_concurrency / handler_queue_size are not supported in separate mode yet "
                       "and are ignored (got %s / %s)", option.handler_max_concurrency, option.handler_queue_size)
    if option.tcp_reuse_port:
        logger.info("server.tcp_reuse_port is on: several processes may bind the same TCP port at once")


def _resolve_worker_num(value: int) -> int:
    """Resolve the process count of separate (rpc tasks) or peer (workers) mode; at least 1.

    Args:
        value: ``ServerOption.worker_num`` (YAML ``global.processmodel.handle_num``);
            ``0`` means the CPU count.

    Returns:
        The process count to run with.
    """
    default = default_worker_num()
    if value == 0:
        return default
    if value < 0:
        logger.warning("global.processmodel.handle_num=%s is negative; using %s", value, default)
        return default
    if value > WORKERS_PER_CPU_WARN * default:
        logger.warning("global.processmodel.handle_num=%s is more than %s times the CPU count (%s); keeping it, "
                       "but that many processes is likely too many", value, WORKERS_PER_CPU_WARN, default)
    return value


def _warn_single_rpc_task_on_macos(rpc_task_num: int, services: List[ServerService]) -> None:
    """Warn that macOS serves each self-bound address from one rpc task only.

    UDP, HTTP and user-protocol addresses are bound by every rpc task with
    ``SO_REUSEPORT``, which macOS accepts but does not balance: in practice
    one rpc task gets all the traffic. Only tRPC-over-TCP is spread by the
    I/O process.

    Args:
        rpc_task_num: Resolved rpc task count (``worker_num``).
        services: YAML ``server.service[]`` entries.
    """
    if sys.platform != "darwin" or rpc_task_num <= 1:
        return
    self_bound = [service.name for service in services
                  if "tcp" not in service.network or "http" in service.protocol or "sse" in service.protocol]
    if self_bound:
        logger.warning("global.processmodel.handle_num=%s on macOS: services %s are bound by each rpc task with SO_REUSEPORT, "
                       "which macOS does not balance, so one rpc task will handle them", rpc_task_num, self_bound)


def _resolve_peer_options(option: ServerOption) -> None:
    """Resolve ``worker_num`` (peer workers, worker-0 included).

    ``tcp_reuse_port`` is not warned about although it has no effect: every
    peer worker binds every endpoint with ``SO_REUSEPORT``. The handler
    limits are supported, so they are not warned about either, and
    ``reboot_child`` is left to the worker, which warns once for every mode.

    Args:
        option: Server option with ``mode: peer``.
    """
    option.worker_num = value = _resolve_worker_num(option.worker_num)
    if value == 1:
        logger.warning("global.processmodel.handle_num=1 in peer mode: a single worker, i.e. merge mode with a "
                       "supervisor around it")
    if sys.platform == "darwin" and value > 1:
        logger.warning("global.processmodel.handle_num=%s on macOS: SO_REUSEPORT does not balance load there, "
                       "so in practice one peer worker handles every request", value)
