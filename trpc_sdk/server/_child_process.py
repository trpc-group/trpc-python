# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared startup sequence for the child processes forked by the master.

Every server child — worker, executor, dispatcher and task — comes up the same
way. It is created in the master, inherits that memory through ``fork``, and
then has to rebuild whatever cannot cross a fork before it can serve: a fresh
event loop, the ``fork_hook`` callbacks that let plugins repair inherited
state, plugin setup, and only then its own wiring (IPC pipes, listen sockets,
consumer threads). SIGTERM handling is registered last, because the master
shuts children down by signalling them and expects a clean exit.

:class:`BaseChildProcess` owns that sequence so each child only describes what
is different about it. Subclasses implement
:meth:`BaseChildProcess._prepare_to_serve` and override the exit hooks they
care about.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import threading
from multiprocessing import Process
from typing import Optional

from trpc_sdk.common import ProcessType
from trpc_sdk.common import execute_after_in_child
from trpc_sdk.common import execute_before_loop_run_in_child
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_task_plugins


class BaseChildProcess(Process):
    """A forked server child built around a single asyncio event loop.

    The object is constructed in the master **before fork**, so ``__init__``
    only records what the child will need later. Everything that cannot be
    inherited across a fork is created in :meth:`run`, which is the first code
    that executes inside the new process.

    Subclasses fill in the parts that differ:

    * :meth:`_prepare_to_serve` — prepare listeners, IPC, consumers and timers
      after plugins are initialized and before the event loop runs. Required.
    * :meth:`_init_plugins` — which plugin set to initialize. Business
      processes need every plugin, infrastructure processes only the
      non-business ones.
    * :meth:`_before_event_loop` — work that must happen before the loop
      exists.
    * :meth:`_on_sigterm_received`, :meth:`_on_graceful_exit`,
      :meth:`_on_unhandled_exception` — the exit lifecycle hooks.

    Attributes:
        type: Process role, used by the master for health checks and restarts.
        name: Process name shown in logs and process listings.
        loop: Event loop created in :meth:`run`; ``None`` until then.
        block: If False, :meth:`run` returns once setup is done instead of
            serving forever. Tests use this to exercise setup on its own.
    """

    def __init__(self, name: str, process_type: ProcessType) -> None:
        """Record the child's identity; no resource is created yet.

        Args:
            name: Process name shown in logs and process listings.
            process_type: Role this child plays in the process tree.
        """
        super().__init__()
        self.type = process_type
        self.name = name
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.block = True

    def run(self) -> None:
        """Child entry point: rebuild the runtime after fork, then serve."""
        self._before_event_loop()

        self.loop = get_or_create_event_loop()
        asyncio.set_event_loop(self.loop)

        execute_after_in_child()
        self._init_plugins()
        self._prepare_to_serve()
        execute_before_loop_run_in_child()

        self._serve()

    def _serve(self) -> None:
        """Take over SIGTERM and run the loop until the process exits."""
        try:
            if threading.current_thread() is threading.main_thread():
                # Only the main thread may install signal handlers; a child
                # started from another thread just runs its loop.
                self._register_signal_handlers()
            if self.block:
                self.loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            self.loop.stop()
            self._on_graceful_exit()
        except Exception as err:  # pylint: disable=broad-except
            self.loop.stop()
            self._on_unhandled_exception(err)

    def _before_event_loop(self) -> None:
        """Hook: run in the child before the event loop is created."""

    def _init_plugins(self) -> None:
        """Initialize the plugins this process needs.

        The default is the non-business set, which is what infrastructure
        processes such as the dispatcher and the task processes run. Processes
        that execute user handlers override this to initialize every plugin.
        """
        # TODO: Need to check
        # init_task_plugins()
        plugin.setup_task()

    def _prepare_to_serve(self) -> None:
        """Prepare listeners, IPC, consumers and timers for serving.

        Runs after plugin initialization and before the loop starts, so it may
        use ``loop.run_until_complete`` to finish connection setup.
        """
        raise NotImplementedError

    def _register_signal_handlers(self) -> None:
        """Install the signal handlers this process answers to."""
        self.loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self) -> None:
        """Run the exit callbacks, then leave the loop through ``SystemExit``."""
        self._on_sigterm_received()
        sys.exit()

    def _on_sigterm_received(self) -> None:
        """Hook: run inside the SIGTERM handler, before the process exits.

        Must not block: it runs on the event loop's signal-handling path.
        """
        logger.error("[%s] received SIGTERM, exiting", self.name)

    def _on_graceful_exit(self) -> None:
        """Hook: release resources after the loop stopped on a normal exit."""

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Handle an exception that escaped the event loop.

        The default re-raises, so the child dies with a traceback and a
        non-zero exit status and the master can restart it. Override to
        swallow the error instead.

        Args:
            err: The exception that terminated the loop.
        """
        logger.error("[%s] exited with error: %s", self.name, repr(err))
        raise err
