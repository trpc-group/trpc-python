# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""The worker as seen from a serving loop other than its own."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from .._worker import BaseWorker


class WorkerLoopView:
    """What a user protocol starter sees as ``worker`` on serving thread 0.

    Starters are written against ``starter(worker, index)`` and bind their
    socket with ``worker.loop``. On a threaded worker that has to be thread
    0's loop, not the main one, while every per-address table (``addresses``,
    ``handlers``, ``timeouts``, ``tran_opts``, ...) still comes from the
    worker. Writes go to the worker too, so whatever a starter hangs on
    ``worker`` is still there afterwards.
    """

    def __init__(self, worker: BaseWorker, loop: asyncio.AbstractEventLoop) -> None:
        object.__setattr__(self, "_worker", worker)
        object.__setattr__(self, "loop", loop)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._worker, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._worker, name, value)
