# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Abstract lifecycle contract for dedicated timer processes.

:class:`TimerHook` is registered in the parent through
:func:`trpc.utils._timer_process.register_timer_process` and carried into
the child on the :class:`~trpc.utils._types.TimerProcessInfo` record. All
three callbacks therefore run in the child process, not the parent.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod


class TimerHook(ABC):
    """Lifecycle hooks for a dedicated timer process.

    ``before`` runs in the child before the loop starts; ``after`` runs
    after SIGTERM/KeyboardInterrupt; ``signal_handle`` runs from the
    SIGTERM handler before tasks are cancelled.
    """

    @abstractmethod
    def before(self):
        """Called in the child before the executor starts."""

    @abstractmethod
    def after(self):
        """Called in the child after the loop is interrupted."""

    @abstractmethod
    def signal_handle(self):
        """Called from the SIGTERM handler before ``sys.exit``."""
