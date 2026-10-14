# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Resolution of the thread-model and process-model fields of :class:`ServerOption`.

:func:`~trpc_sdk.server.create_server` copies the raw YAML ``server:`` values
onto :class:`~trpc_sdk.server.ServerOption`, where ``0`` or ``''`` means "not
configured", and then calls :func:`resolve_server_option` once to overwrite
them in place with the values the server runs with. An out-of-range count is
logged as a warning and replaced by a fallback, so a misconfigured service
still starts. Contradictions rather than typos fail startup with
:class:`ValueError`: a thread-model request under a multi-process
``server.mode``, an ``io_handle_type`` that disagrees with
``handle_thread_num``, or an ``io_handle_type`` that names no thread model.

``server.mode`` and ``server.io_handle_type`` accept the spellings in
:data:`~trpc_sdk.runtime.MODE_SPELLINGS` (``seperate``) and are written back
under their canonical name.

Thread model (merge mode only): ``handle_thread_num == 0`` runs
``io_thread_num`` serving threads that do I/O and run handlers (``merge``);
``handle_thread_num > 0`` runs ``io_thread_num`` I/O loops plus
``handle_thread_num`` handle threads (``separate``). ``io_handle_type`` is an
optional explicit form, kept for trpc-cpp configs; when set it must agree
with ``handle_thread_num``. Streaming RPC runs under every thread model, so
nothing here depends on the registered services.

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
from typing import List
from typing import Tuple

from trpc_sdk.config import ServerService
from trpc_sdk.config import default_worker_num
from trpc_sdk.log import logger
from trpc_sdk.runtime import IO_HANDLE_TYPE_MERGE
from trpc_sdk.runtime import IO_HANDLE_TYPE_SEPARATE
from trpc_sdk.runtime import MODE_MERGE
from trpc_sdk.runtime import MODE_PEER
from trpc_sdk.runtime import MODE_SEPARATE
from trpc_sdk.runtime import MODE_SPELLINGS
from ._constants import DEFAULT_HANDLE_THREAD_NUM
from ._constants import DEFAULT_HANDLER_MAX_CONCURRENCY
from ._constants import DEFAULT_HANDLER_QUEUE_SIZE
from ._constants import DEFAULT_IO_THREAD_NUM
from ._constants import DEFAULT_READY_TIMEOUT_MS
from ._constants import DEFAULT_WORKER_NUM
from ._constants import THREADS_PER_CPU_WARN
from ._constants import WORKERS_PER_CPU_WARN

if TYPE_CHECKING:
    from ._option import ServerOption


def resolve_server_option(option: ServerOption, services: List[ServerService]) -> None:
    """Validate the thread- and process-model fields, warn, and write the resolved values back.

    Args:
        option: Server option holding the raw YAML ``server:`` values.
            ``mode``, ``io_handle_type``, ``io_thread_num``,
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
        logger.warning("server.worker_num=%s is only read in separate and peer mode; ignored", option.worker_num)


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

    io_thread_num = _resolve_thread_count("io_thread_num", option.io_thread_num)
    if io_handle_type == IO_HANDLE_TYPE_SEPARATE:
        _warn_if_oversized("handle_thread_num", option.handle_thread_num)
        handle_thread_num = option.handle_thread_num
    else:
        if option.handle_thread_num < 0:
            logger.warning("server.handle_thread_num=%s is negative; using %s", option.handle_thread_num,
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
        value: YAML ``server.io_handle_type``; ``''`` means infer it.
        handle_thread_num: Raw YAML ``server.handle_thread_num``.

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
        msg = f"server.io_handle_type={value!r} is not 'merge' or 'separate'"
    elif value != inferred:
        msg = (f"server.io_handle_type={value!r} disagrees with handle_thread_num={handle_thread_num}: "
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
        mode: ``server.mode``; ``''`` means merge.
        handle_thread_num: Raw YAML ``server.handle_thread_num``.
        io_thread_num: Raw YAML ``server.io_thread_num``.

    Raises:
        ValueError: If ``mode`` is not merge and threads were requested.
    """
    if not mode or mode == MODE_MERGE:
        return
    if handle_thread_num > 0 or io_thread_num > 1:
        msg = (f"server.mode={mode!r} runs a single thread per process; "
               f"handle_thread_num={handle_thread_num} / io_thread_num={io_thread_num} is only valid in merge mode")
        logger.warning(msg)
        raise ValueError(msg)


def _resolve_thread_count(field_name: str, value: int) -> int:
    """Resolve ``io_thread_num``, the same way for both thread models.

    Merge reads it as the number of serving threads, separate as the number
    of I/O loops (the main loop included).

    Args:
        field_name: YAML field name, for the warnings.
        value: YAML value; ``0`` means not configured.

    Returns:
        The thread count to run with.
    """
    if value == 0:
        return DEFAULT_IO_THREAD_NUM
    if value < 0:
        logger.warning("server.%s=%s is negative; using %s", field_name, value, DEFAULT_IO_THREAD_NUM)
        return DEFAULT_IO_THREAD_NUM
    _warn_if_oversized(field_name, value)
    return value


def _warn_if_oversized(field_name: str, value: int) -> None:
    """Warn when a thread count is far above the CPU count; the value is kept.

    Args:
        field_name: YAML field name, for the warning.
        value: Requested thread count.
    """
    limit = THREADS_PER_CPU_WARN * (os.cpu_count() or 1)
    if value > limit:
        logger.warning("server.%s=%s is more than %s times the CPU count (%s); keeping it, but it is likely too many",
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
        value: YAML ``server.worker_num``; ``0`` means the CPU count.

    Returns:
        The process count to run with.
    """
    default = default_worker_num()
    if value == 0:
        return default
    if value < 0:
        logger.warning("server.worker_num=%s is negative; using %s", value, default)
        return default
    if value > WORKERS_PER_CPU_WARN * default:
        logger.warning("server.worker_num=%s is more than %s times the CPU count (%s); keeping it, "
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
        logger.warning("server.worker_num=%s on macOS: services %s are bound by each rpc task with SO_REUSEPORT, "
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
        logger.warning("server.worker_num=1 in peer mode: a single worker, i.e. merge mode with a supervisor "
                       "around it")
    if sys.platform == "darwin" and value > 1:
        logger.warning("server.worker_num=%s on macOS: SO_REUSEPORT does not balance load there, "
                       "so in practice one peer worker handles every request", value)
