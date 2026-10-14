# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""The exit template every serving process shares.

Every role — the merge worker, the separate-mode I/O process, its rpc tasks,
and the task processes — leaves through the same coroutine,
:meth:`ExitTemplate._shutdown`. A role only describes what it has to do on
top, through four hooks; signals, re-entrancy, the drain and cutting it short,
and the parent dying are handled here once.

The sequence, started by the first stop signal:

1. ``exit_callbacks`` (naming deregistration).
2. :meth:`~ExitTemplate._stop_accepting`: close the listen sockets.
3. Wait :attr:`drain_seconds` while the loop keeps writing responses, unless
   a second stop signal cuts it short. Each cut also calls
   :meth:`~ExitTemplate._on_drain_cut`.
4. ``post_drain_callbacks`` (``server_engine.close``), whether or not the
   drain was cut short.
5. :meth:`~ExitTemplate._wait_handlers`: wait for handlers running
   elsewhere, i.e. the rpc tasks of a separate-mode I/O process.
6. ``master_exit_callbacks`` (the user server exit callback).
7. :meth:`~ExitTemplate._on_drained` (the user worker exit callbacks by
   default).
8. Leave the loop with ``SystemExit(_exit_code)``. ``BaseWorker._serve`` then
   runs ``_on_graceful_exit`` and :meth:`~ExitTemplate._stop_children`.

Controlled children. A child whose parent decides when it stops (an rpc task,
or a task forked by a separate-mode I/O process) sets ``_controlled``. It then
ignores SIGTERM and SIGINT, so a signal sent to the whole process group only
reaches its parent, and stops on SIGUSR1 instead. Every other process stops on
SIGTERM. That one flag is the only difference between the two kinds: the stop
signal, the parent-death signal and what :meth:`~ExitTemplate._stop_children`
sends all follow from it.

Parent death. A stop signal that arrives after the parent pid changed is the
kernel's ``PR_SET_PDEATHSIG``, not a request; so is the parent sentinel
becoming readable. Nobody is left to SIGKILL this process then, so both arm a
kill timer and skip the drain.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import threading
import time
from multiprocessing.connection import wait as wait_sentinels
from multiprocessing.process import BaseProcess
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.log import logger
from ._signals import run_worker_signal_handle
from .._constants import CHILD_EXIT_GRACE_SECONDS
from .._constants import CHILD_EXIT_POLL_INTERVAL
from .._constants import PARENT_EXIT_GRACE_SECONDS
from .._types import ChildRecord


def _run_callbacks(owner: str, kind: str, callbacks: List[Callable[[], None]]) -> None:
    """Run lifecycle callbacks in order, logging failures instead of raising.

    One failing callback must not stop the rest, nor keep the process alive
    (or keep it from becoming ready).

    Args:
        owner: Process name, for log messages.
        kind: Which list this is, for log messages.
        callbacks: Zero-argument callables to run.
    """
    for callback in callbacks:
        try:
            callback()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] %s callback failed: %s", owner, kind, repr(err))


class ExitTemplate:
    """Exit template and child stopping, inherited by ``BaseWorker``.

    Relies on attributes ``BaseWorker`` defines: ``name``, ``loop``,
    ``children``, ``reboot_child``, ``drain_seconds`` and the three exit
    callback lists.
    """

    # Declared by ``BaseWorker.__init__``; listed for readers of this class.
    name: str
    loop: Optional[asyncio.AbstractEventLoop]
    children: List[ChildRecord]
    reboot_child: bool
    drain_seconds: float
    exit_callbacks: List[Callable[[], None]]
    post_drain_callbacks: List[Callable[[], None]]
    master_exit_callbacks: List[Callable[[], None]]

    def __init__(self) -> None:
        """Set up the exit state; runs before fork for a forked role.

        ``super().__init__()`` without arguments, for the same reason as in
        ``BaseWorker.__init__``: it reaches ``Process.__init__`` for a forked
        role and ``object.__init__`` otherwise.
        """
        super().__init__()
        # True for a child its parent stops with SIGUSR1; it then ignores
        # SIGTERM and SIGINT. Set in the subclass constructor, before fork, so
        # the parent can read it to pick the signal it sends.
        self._controlled: bool = False
        # Pid of the process that built this object. For a forked role that
        # is the parent; a different ``getppid()`` later means it died.
        self._parent_pid: int = os.getpid()
        # True once shutdown started; a later stop signal cuts the drain short
        # and a dying child is no longer treated as a fault.
        self._exiting: bool = False
        # Set to cut the drain short. Created on the running loop, so it is
        # bound to it on every Python version; ``None`` when shutdown ran with
        # the loop stopped, which skips the drain anyway.
        self._drain_cut: Optional[asyncio.Event] = None
        # The stop signal arrived before the loop handler was installed.
        self._early_exit: bool = False
        # Status this process exits with. Set to 1 when it goes down because a
        # child died or never became ready, so a supervisor sees a failure.
        self._exit_code: int = 0
        # Armed by ``_arm_exit_killer`` once nobody is left to kill this
        # process should its shutdown hang.
        self._exit_killer: Optional[threading.Timer] = None

    def _stop_signal(self) -> int:
        """Signal that stops this process: SIGUSR1 when controlled, SIGTERM otherwise.

        Also the parent-death signal, and what a parent sends this child from
        :meth:`_stop_children`.
        """
        return signal.SIGUSR1 if self._controlled else signal.SIGTERM

    def _record_early_exit(self, signum: int, frame: Any) -> None:
        """``signal.signal`` handler: remember a stop signal that came before the loop handler.

        Installed by ``_before_event_loop`` on a controlled child, so a stop
        signal during startup does not kill it with the default action. Acted
        on once the loop handler is installed.

        Args:
            signum: Signal number, unused.
            frame: Interrupted frame, unused.
        """
        del signum, frame
        self._early_exit = True

    def _on_stop_signal(self) -> None:
        """Event callback for the stop signal: start shutdown, or cut its drain short.

        If the parent pid changed, the signal was ``PDEATHSIG``: the kill
        timer is armed and the drain is skipped.
        """
        if self._parent_changed():
            logger.error("[%s] parent process exited; shutting down now", self.name)
            self._arm_exit_killer()
            self._cut_now()
            return
        self._handle_sigterm()

    def _parent_changed(self) -> bool:
        """Whether the process that forked this one is gone.

        Always False in a process ``multiprocessing`` did not fork: it has no
        parent of ours to lose.
        """
        return isinstance(self, BaseProcess) and os.getppid() != self._parent_pid

    def _handle_sigterm(self) -> None:
        """Start the exit template, or cut its drain short if it already started.

        With the loop running, the template runs as a task and the process
        leaves the loop when it is done. With the loop stopped (``block``
        False, or a test) it runs to completion here, without the drain, and
        raises ``SystemExit``.

        A later call sets the drain cut and calls :meth:`_on_drain_cut` again,
        every time, so a parent can pass the cut on to its children.

        Raises:
            SystemExit: Only on the stopped-loop path.
        """
        if self._exiting:
            if self._drain_cut is not None and not self._drain_cut.is_set():
                if self.drain_seconds > 0:
                    logger.warning("[%s] shutdown requested again; cutting the drain short", self.name)
                self._drain_cut.set()
            self._on_drain_cut()
            return
        self._exiting = True

        loop = self.loop
        if loop is not None and loop.is_running():
            self._drain_cut = asyncio.Event()
            loop.create_task(self._shutdown()).add_done_callback(self._leave_loop)
            return

        own_loop = loop is None or loop.is_closed()
        if own_loop:
            loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._shutdown())
        finally:
            if own_loop:
                loop.close()
        sys.exit(self._exit_code)

    def _cut_now(self) -> None:
        """Start shutdown and cut its drain short at once.

        For when waiting would be wrong: the parent died, or the stop signal
        came during startup, when nothing is in flight yet.
        """
        started = not self._exiting
        self._handle_sigterm()
        if started:
            self._handle_sigterm()

    async def _shutdown(self) -> None:
        """Run the exit template; see the module docstring for the steps.

        Never raises: a failure is logged and makes the process exit with
        status 1, since the loop would otherwise swallow it and keep the
        process up after the stop signal.
        """
        try:
            logger.error("[%s] shutting down", self.name)
            _run_callbacks(self.name, "exit", self.exit_callbacks)
            try:
                self._stop_accepting()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[%s] failed to stop accepting: %s", self.name, repr(err))

            cut = self._drain_cut
            if self.drain_seconds > 0 and cut is not None and not cut.is_set():
                logger.warning("[%s] draining in-flight requests for %.3f s", self.name, self.drain_seconds)
                try:
                    await asyncio.wait_for(cut.wait(), self.drain_seconds)
                except asyncio.TimeoutError:
                    pass

            _run_callbacks(self.name, "post-drain", self.post_drain_callbacks)
            self.post_drain_callbacks = []
            await self._wait_handlers()
            _run_callbacks(self.name, "master-exit", self.master_exit_callbacks)
            self.master_exit_callbacks = []
            try:
                self._on_drained()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[%s] drained hook failed: %s", self.name, repr(err))
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] shutdown failed: %s", self.name, repr(err), exc_info=True)
            self._exit_code = 1

    def _leave_loop(self, task: asyncio.Task) -> None:
        """Done callback of the exit template: leave the loop with :attr:`_exit_code`.

        Args:
            task: The finished template; it never raises.

        Raises:
            SystemExit: Always; the loop lets it through ``run_forever``.
        """
        del task
        sys.exit(self._exit_code)

    def _exit_now(self, exit_code: int) -> None:
        """Stop every child and exit, skipping the template; startup cannot continue.

        Runs before ``ready_callbacks``, so nothing was registered with the
        naming service and there is nothing to drain.

        Args:
            exit_code: Status to exit with; 0 when a stop signal caused it.

        Raises:
            SystemExit: Always.
        """
        self._exiting = True
        self._exit_code = exit_code
        self._stop_children()
        sys.exit(exit_code)

    def _arm_exit_killer(self) -> None:
        """Kill this process with ``os._exit(1)`` after :data:`PARENT_EXIT_GRACE_SECONDS`.

        For when nobody is left to SIGKILL it if its shutdown hangs. A daemon
        timer thread, so it does not itself keep the process alive. Arming it
        again is a no-op.
        """
        if self._exit_killer is not None:
            return
        killer = threading.Timer(PARENT_EXIT_GRACE_SECONDS, os._exit, args=(1,))
        killer.daemon = True
        killer.start()
        self._exit_killer = killer

    def _stop_accepting(self) -> None:
        """Hook, template step 2: close the listen sockets; open connections stay up.

        No-op here. Processes that listen override it.
        """

    def _on_drain_cut(self) -> None:
        """Hook: a later stop signal cut the drain short; called on every such signal.

        No-op here. A process whose children drain on their own passes the
        cut on to them.
        """

    async def _wait_handlers(self) -> None:
        """Hook, template step 5: wait for handlers that run in other processes.

        No-op here, where every handler runs in this process.
        """

    def _on_drained(self) -> None:
        """Hook, template step 7: run the user worker exit callbacks.

        Runs after ``master_exit_callbacks``, once the drain is over, so the
        callbacks may release what in-flight requests were using. Processes
        that run no handlers override it with a no-op.
        """
        run_worker_signal_handle()

    def _child_alive(self, record: ChildRecord) -> bool:
        """Whether ``record``'s process still runs; judged by its sentinel, not ``is_alive()``.

        Args:
            record: The child.
        """
        if record.exited or record.process is None:
            return False
        return not wait_sentinels([record.process.sentinel], 0)

    def _signal(self, records: List[ChildRecord]) -> None:
        """Send each live child in ``records`` its own stop signal.

        Args:
            records: Children to signal.
        """
        for record in records:
            if not self._child_alive(record):
                continue
            try:
                os.kill(record.process.pid, record.process._stop_signal())  # pylint: disable=protected-access
            except ProcessLookupError:
                pass

    def _kill(self, records: List[ChildRecord]) -> None:
        """SIGKILL every child in ``records``.

        Args:
            records: Children that did not exit in time.
        """
        for record in records:
            logger.error("[%s] child %s (pid %s) did not exit in time; sending SIGKILL", self.name, record.name,
                         record.process.pid)
            try:
                os.kill(record.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    async def _wait_children(self, records: List[ChildRecord], timeout: float) -> None:
        """Wait for ``records`` to exit, SIGKILL whoever is left after ``timeout``.

        Polls the sentinels, so the loop keeps forwarding responses meanwhile.

        Args:
            records: Children to wait for.
            timeout: Seconds before the stragglers are killed.
        """
        pending = [record for record in records if self._child_alive(record)]
        deadline = time.monotonic() + timeout
        while pending and time.monotonic() < deadline:
            await asyncio.sleep(CHILD_EXIT_POLL_INTERVAL)
            pending = [record for record in pending if self._child_alive(record)]
        self._kill(pending)

    def _stop_children(self) -> None:
        """Stop every child still alive: stop signal twice, then SIGKILL after the grace period.

        The first signal starts a child's shutdown and the second cuts its
        drain short; the pause between them keeps the kernel from merging the
        two into one. Blocking waits, because the loop may not be running.
        Runs after ``reboot_child`` has stopped mattering, so a child dying
        here is expected rather than a fault. A no-op once every child exited.
        """
        self.reboot_child = False
        alive = [record for record in self.children if self._child_alive(record)]
        if not alive:
            return
        self._signal(alive)
        time.sleep(CHILD_EXIT_POLL_INTERVAL)
        self._signal(alive)
        deadline = time.monotonic() + CHILD_EXIT_GRACE_SECONDS
        while alive:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            wait_sentinels([record.process.sentinel for record in alive], remaining)
            alive = [record for record in alive if self._child_alive(record)]
        self._kill(alive)
