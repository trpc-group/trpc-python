# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Translate retired YAML ``server:`` keys and values onto :class:`ServerOption`.

``server.worker_num`` used to size every process model. Merge mode now reads
``server.worker_thread_num`` and separate mode reads ``server.rpc_task_num``.
A config that only sets the old key must still produce a usable option, so
these helpers prefer the new key and fall back to ``worker_num`` (and then
the package default) when the new key is absent or zero.

``server.mode`` changed values as well as meaning, so :func:`compat_mode_name`
folds the retired names onto the two that ship today and warns, so the file
can be updated on its own schedule.

Only :func:`compat_mode_name` warns. A retired mode name can only appear
because the file wrote it, whereas ``worker_num`` is given the CPU core count
by :class:`ServerConfig` whether or not the file mentions it, so the count
helpers cannot tell a config that set it from one that said nothing. Warning
there would fire on every default config; do not add one without a schema
change that records which keys were present.

This module maps values only, never classes. Aliasing the retired process
classes onto the new workers would import cleanly and then fail at runtime:
``ExecutorProcess`` and ``DispatcherProcess`` were :class:`Process` subclasses
a master forked, while :class:`~trpc_sdk.server._merge_worker.MergeWorker` and
:class:`~trpc_sdk.server._separate_worker.SeparateWorker` *are* the server
process and have no ``start()``. An import error is the better failure.

The count helpers are called from :func:`create_server`, after
:meth:`TrpcConfig.load` and before :class:`Server` is constructed.
:func:`compat_mode_name` is called later, from
:mod:`~trpc_sdk.server._modes`, because the mode registry has to be consulted
before a name is folded.
"""

from __future__ import annotations

import warnings
from typing import Dict

from trpc_sdk.log import logger

from ._constants import DEFAULT_RPC_TASK_NUM
from ._constants import DEFAULT_WORKER_THREAD_NUM
from ._constants import MODE_MERGE
from ._constants import MODE_SEPARATE

# Retired ``server.mode`` value -> the mode that has its behaviour today.
#
# ``dispatcher`` named a master that forked one I/O child plus N handler
# children; ``separate`` is the same split without the master, the server
# process having taken the dispatcher's place.
#
# ``multi-listener`` named N processes that each bound the listen address and
# ran handlers themselves; ``merge`` keeps that "accept and handle in one
# place" shape, with event-loop threads instead of processes.
_RETIRED_MODES: Dict[str, str] = {
    "dispatcher": MODE_SEPARATE,
    "multi-listener": MODE_MERGE,
}


def compat_worker_thread_num(worker_num: int, worker_thread_num: int) -> int:
    """Return merge-mode event-loop threads from YAML, with ``worker_num`` fallback.

    Zero on the new key means "not set in this file", not "run with no
    threads". Merge mode needs at least one loop, so an absent new key
    reuses the retired ``worker_num`` (CPU-core default) rather than
    collapsing to :data:`DEFAULT_WORKER_THREAD_NUM` while ``worker_num``
    still carries a real size.

    Args:
        worker_num: Retired YAML ``server.worker_num``.
        worker_thread_num: YAML ``server.worker_thread_num``; ``0`` means unset.

    Returns:
        A positive thread count for :attr:`ServerOption.worker_thread_num`.
    """
    if worker_thread_num > 0:
        return worker_thread_num
    if worker_num > 0:
        return worker_num
    return DEFAULT_WORKER_THREAD_NUM


def compat_rpc_task_num(worker_num: int, rpc_task_num: int) -> int:
    """Return separate-mode rpc task processes from YAML, with ``worker_num`` fallback.

    Same rule as :func:`compat_worker_thread_num`: a missing
    ``server.rpc_task_num`` must not ignore a ``worker_num`` that already
    sized the old process model.

    Args:
        worker_num: Retired YAML ``server.worker_num``.
        rpc_task_num: YAML ``server.rpc_task_num``; ``0`` means unset.

    Returns:
        A positive process count for :attr:`ServerOption.rpc_task_num`.
    """
    if rpc_task_num > 0:
        return rpc_task_num
    if worker_num > 0:
        return worker_num
    return DEFAULT_RPC_TASK_NUM


def compat_mode_name(mode: str) -> str:
    """Return the modern equivalent of a ``server.mode`` value, warning once.

    Unlike the count helpers, this one is not total: a name it does not
    recognise is returned unchanged, leaving the caller to decide whether an
    unknown name is an error. :func:`~trpc_sdk.server._modes.resolve_worker_cls`
    treats it as one.

    Both a :class:`DeprecationWarning` and a log line are emitted, because the
    two reach different people: the warning surfaces in tests and CI, the log
    line in a running server where warnings are usually silenced.

    Args:
        mode: The ``server.mode`` value, as written in the config.

    Returns:
        The name in :data:`_RETIRED_MODES` this value folds onto, or ``mode``
        itself when it is not a retired name.
    """
    if mode not in _RETIRED_MODES:
        return mode

    replacement = _RETIRED_MODES[mode]
    message = (f"server.mode '{mode}' is deprecated; use '{replacement}'. The division of labour is the same "
               f"but the process topology is not: see the trpc_sdk.server module docstring for what changed.")
    warnings.warn(message, DeprecationWarning, stacklevel=2)
    logger.warning(message)
    return replacement
