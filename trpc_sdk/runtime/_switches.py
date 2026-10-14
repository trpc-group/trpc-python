# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Runtime switches, read from ``TRPC_*`` environment variables, that select a
server implementation path.

Each switch picks between a pure-Python implementation and a native one. They
are read from the environment once, when this module is first imported, because
the choice must be stable for the whole lifetime of the process: the worker
decides which children to fork and how to wire the IPC pipes, and every child
must agree with that decision.

Read a switch through its accessor at the point of use rather than binding the
value at import time. A module-level ``FLAG = use_c_task()`` snapshot taken
while the package is still loading can be captured before tests or embedding
code have had a chance to change it.

The ``set_use_*`` functions exist for tests and for embedders that configure the
framework in code instead of through the environment; they must be called before
:meth:`~trpc_sdk.server.Server.serve` forks any child process.

Only the task path is switchable now. The C dispatcher and trpcio switches went
away with the process topology that read them: there is no dispatcher child to
choose an implementation for, since the server process accepts for itself. The
native data plane those switches selected is archived under ``trpc_sdk/old_server``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from ._constants import ENV_C_TASK
from ._constants import ENV_ENABLED


@dataclass
class ServerFeatures:
    """The set of implementation paths chosen for this process."""

    # Run task handlers in the native task driver instead of the Python one.
    c_task: bool = False

    @classmethod
    def from_env(cls) -> ServerFeatures:
        """Build the switch set from the ``TRPC_*`` environment variables."""
        return cls(c_task=os.environ.get(ENV_C_TASK) == ENV_ENABLED)


_FEATURES = ServerFeatures.from_env()


def use_c_task() -> bool:
    """Return whether task processes run the native task driver."""
    return _FEATURES.c_task


def set_use_c_task(enabled: bool) -> None:
    """Enable or disable the native task driver.

    Args:
        enabled: True to run task handlers in the native driver.
    """
    _FEATURES.c_task = enabled


def reload_from_env() -> None:
    """Re-read every switch from the environment, discarding code overrides."""
    global _FEATURES  # pylint: disable=global-statement
    _FEATURES = ServerFeatures.from_env()
