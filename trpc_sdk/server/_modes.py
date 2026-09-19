# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Internal ``server.mode`` name -> worker class lookup.

Two modes ship with the framework: ``merge``, where one process accepts and
runs handlers, and ``separate``, where the process only does I/O and forks rpc
tasks to run handlers. Those are the public YAML values.
:func:`register_worker_mode` is not part of the public API; tests and private
forks use it to swap a class in the table.

:meth:`Server.serve` calls :func:`create_worker` after every ``Service.run``
has registered its listen endpoints on the engine, and before any child is
forked. The returned worker then owns this process: it forks the children and
only afterwards creates the event loop. Unknown mode names fail here rather
than in the worker constructor, so a typo in YAML is reported before any
socket is bound.

Resolution order is registry first, then the retired-name mapping in
:mod:`~trpc_sdk.server._compat`. That order is load-bearing: ``dispatcher`` is
both a legacy value that maps to ``separate`` and a name a private
registration may still use. Consulting the registry first means such a
registration wins; folding the name first would make it unreachable.
"""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Type

from ._compat import compat_mode_name
from ._constants import MODE_MERGE
from ._constants import MODE_SEPARATE
from ._merge_worker import MergeWorker
from ._separate_worker import SeparateWorker
from ._worker import BaseWorker

# Mode name -> the worker class that implements it. Mutable on purpose: this
# is the registry :func:`register_worker_mode` writes into.
_WORKER_MODES: Dict[str, Type[BaseWorker]] = {
    MODE_MERGE: MergeWorker,
    MODE_SEPARATE: SeparateWorker,
}


def register_worker_mode(mode: str, worker_cls: Type[BaseWorker]) -> None:
    """Bind a ``server.mode`` value to the worker class that implements it.

    Internal. Call this at import time of the package that defines
    ``worker_cls``, before :meth:`~trpc_sdk.server.Server.serve` reads the
    mode. Registering a name that already exists replaces it.

    Args:
        mode: Value that will appear as ``server.mode`` in the YAML config.
        worker_cls: Class to instantiate with the engine. Must accept
            ``(engine)`` and behave as a :class:`BaseWorker`: the server calls
            ``_spawn_children()`` and then ``run()`` on it.

    Raises:
        TypeError: ``worker_cls`` is not a :class:`BaseWorker` subclass.
    """
    if not (isinstance(worker_cls, type) and issubclass(worker_cls, BaseWorker)):
        raise TypeError(f"worker mode '{mode}' must be a BaseWorker subclass, got {worker_cls!r}")
    _WORKER_MODES[mode] = worker_cls


def registered_worker_modes() -> Dict[str, Type[BaseWorker]]:
    """Return a copy of the mode table, for diagnostics and tests."""
    return dict(_WORKER_MODES)


def resolve_worker_cls(mode: str) -> Type[BaseWorker]:
    """Find the worker class for a configured mode name.

    Args:
        mode: The ``server.mode`` value, as written in the config. Empty
            string is not expected here: :func:`create_server` leaves
            :data:`DEFAULT_MODE` in place when the YAML field is absent.

    Returns:
        The registered class, looked up under the configured name first and
        under its modern equivalent only if that missed.

    Raises:
        ValueError: The name is neither registered nor a known legacy value.
    """
    if mode in _WORKER_MODES:
        return _WORKER_MODES[mode]

    resolved = compat_mode_name(mode)
    if resolved in _WORKER_MODES:
        return _WORKER_MODES[resolved]

    raise ValueError(f"unknown server.mode: {mode!r}; registered modes are {sorted(_WORKER_MODES)}")


def create_worker(mode: str, engine: Any) -> BaseWorker:
    """Build the worker this process will become.

    Args:
        mode: The ``server.mode`` value, as written in the config.
        engine: :class:`TrpcServerEngine` holding the listen-endpoint tables
            filled during ``Service.run``, handed to the worker constructor.
            Typed as ``Any`` to avoid a cycle with :mod:`._engine`.

    Returns:
        A worker that has not forked anything and has no event loop yet.

    Raises:
        ValueError: If ``mode`` names no registered or legacy mode.
    """
    return resolve_worker_cls(mode)(engine)
