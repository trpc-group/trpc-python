# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Hand requests from I/O loops to handle threads under ``io_handle_type: separate``.

The loop that reads a request is not the one that runs it. One
:class:`SharedHandlerPool` per process is shared by every I/O loop; it picks
a handle thread for each unary request and enforces an optional per-thread
in-flight limit (``handler_max_concurrency``), with one global FIFO for the
overflow. The limit is off by default (``handler_max_concurrency: 0``).

Stream frames bypass the limit and the queue, through
:meth:`SharedHandlerPool.submit_stream`: a dropped FEEDBACK or CLOSE frame
would stall or leak the stream. Each stream is pinned to the handle thread
``(session_id + stream_id) % handle_thread_num``.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Deque
from typing import List
from typing import Sequence

from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import SchedulerParams
from trpc_sdk.transport import get_next_worker
from .._constants import SCHEDULE_TARGET_HANDLE_REQ

# Zero-argument factory returning the coroutine to run.
CoroutineFactory = Callable[[], Awaitable[None]]


class HandleSlot:
    """State of one handle thread, as seen by :class:`SharedHandlerPool`.

    ``in_flight`` and ``accepted`` are written only by the handle thread
    itself; I/O loops only read them, so a value they read may be stale, and
    the pool is built to tolerate that. ``pull_scheduled`` is set by an I/O
    loop and cleared by the handle thread.

    Attributes:
        index: Position among the handle threads.
        loop: The handle thread's loop.
        in_flight: Requests running on this thread.
        accepted: Requests posted to this thread directly that it has taken
            (started or queued); with the I/O loops' post counts this gives
            the requests still on their way to it.
        pull_scheduled: A wake-up to drain the global queue is already queued
            on ``loop``.
    """

    __slots__ = ("index", "loop", "in_flight", "accepted", "pull_scheduled")

    def __init__(self, index: int, loop: asyncio.AbstractEventLoop) -> None:
        self.index = index
        self.loop = loop
        self.in_flight = 0
        self.accepted = 0
        self.pull_scheduled = False


class SharedHandlerPool:
    """Hands unary requests from I/O loops to handle threads, under a per-thread limit.

    Without a limit (``max_concurrency == 0``, the default) each request goes
    to the handle thread :func:`~trpc_sdk.transport.get_next_worker` picks,
    with ``target="handle_req"``; nothing is counted or queued.

    With a limit, a request goes to the first handle thread with a free slot,
    starting from the one the selector picked. When every thread is full it
    waits in one global FIFO, and a handle thread that finishes a request
    takes the oldest one from there. Once something is queued, new requests
    queue behind it rather than overtake it.

    A thread counts as free when its running requests plus the ones posted
    to it but not yet taken are below the limit. Each I/O loop counts its own
    posts per thread, so a burst read in one ``data_received`` does not post
    more than the free slots; only posts racing in from other I/O loops can
    overshoot.

    Why no request is left in the queue with nobody to take it: an I/O loop
    always appends first and reads ``in_flight`` afterwards, while a handle
    thread always lowers ``in_flight`` (or clears ``pull_scheduled``) first
    and reads the queue afterwards. Whichever runs second sees the other's
    write. This relies on the GIL; free-threaded builds are not supported.

    The per-thread limit is exact, since only the handle thread counts its
    own requests. The queue bound is not: a request another I/O loop posted
    to a thread that filled up meanwhile is queued without a size check, so
    the queue can briefly exceed ``queue_size`` by those races. A request
    that does not fit is dropped and logged; the client sees a timeout, as
    no overload response is encoded on the I/O loop in this version.

    Attributes:
        slots: One per handle thread.
        max_concurrency: Requests allowed in flight per handle thread;
            ``0`` means unlimited.
        queue_size: Capacity of the global FIFO.
    """

    def __init__(self, slots: Sequence[HandleSlot], max_concurrency: int, queue_size: int) -> None:
        """Create an idle pool over already running handle threads.

        Args:
            slots: One per handle thread; must not be empty.
            max_concurrency: Per-thread in-flight limit; ``0`` is unlimited.
            queue_size: Global FIFO capacity; ignored without a limit.

        Raises:
            ValueError: ``slots`` is empty.
        """
        if not slots:
            raise ValueError("shared handler pool needs at least one handle thread")
        self.slots: List[HandleSlot] = list(slots)
        self.max_concurrency: int = max_concurrency
        self.queue_size: int = queue_size
        self._queue: Deque[CoroutineFactory] = deque()
        # One row per I/O loop thread: requests it posted to each slot. Each
        # row has a single writer; every I/O loop reads all rows.
        self._post_rows: List[List[int]] = []
        self._local = threading.local()

    @property
    def queued(self) -> int:
        """Requests waiting in the global FIFO."""
        return len(self._queue)

    def stream_target(self, session_id: int, stream_id: int) -> int:
        """Handle thread that serves every frame of one stream.

        The same rule :func:`~trpc_sdk.transport.get_next_worker` applies to
        stream frames, so each frame finds the thread on its own, without a
        table; hooks registered with ``register_dispatcher_worker_func`` are
        not consulted for streams there either. The hash ignores load.

        Args:
            session_id: Connection id from the TCP protocol.
            stream_id: Stream id from the frame head.
        """
        return (session_id + stream_id) % len(self.slots)

    def submit_stream(self, index: int, run: CoroutineFactory) -> None:
        """Hand one stream frame to the handle thread its stream is pinned to.

        Called on an I/O loop. Stream frames are not counted, queued or
        dropped: losing a FEEDBACK would leave the sender waiting for a
        window forever. Frames posted by one I/O loop to one handle thread
        start in the order they were posted.

        Args:
            index: :meth:`stream_target` of the frame's stream.
            run: Builds the coroutine that handles the frame.
        """
        self._post(self.slots[index], self._start, run)

    def post(self, index: int, callback: Callable[..., None], *args: Any) -> None:
        """Queue ``callback`` on one handle thread; dropped when that loop is closed.

        Args:
            index: Position among :attr:`slots`.
            callback: Runs on the handle thread's loop.
            *args: Passed to ``callback``.
        """
        self._post(self.slots[index], callback, *args)

    def submit(self, run: CoroutineFactory) -> None:
        """Hand one unary request to a handle thread, or queue or drop it.

        Called on an I/O loop.

        Args:
            run: Builds the coroutine that serves the request and writes the
                response back through its :class:`ConnWriter`.
        """
        slots = self.slots
        count = len(slots)
        start = get_next_worker(SchedulerParams(worker_conn_num=count, remote_ip="", session_id=0, stream_id=0,
                                                target=SCHEDULE_TARGET_HANDLE_REQ)) % count
        if self.max_concurrency <= 0:
            self._post(slots[start], self._start, run)
            return

        posted = self._my_post_row()
        if not self._queue:
            for offset in range(count):
                position = (start + offset) % count
                if self._is_free(position):
                    posted[position] += 1
                    self._post(slots[position], self._accept, slots[position], run)
                    return

        if len(self._queue) >= self.queue_size:
            report_attr("trpc_handler_queue_full", 1)
            logger.warning("[shared handler pool] queue full: %s queued; dropping request", len(self._queue))
            return
        self._queue.append(run)
        # Rescan after appending: a thread may have freed a slot between the
        # scan above and the append.
        for offset in range(count):
            position = (start + offset) % count
            slot = slots[position]
            if not slot.pull_scheduled and self._is_free(position):
                slot.pull_scheduled = True
                self._post(slot, self._pull, slot)
                return

    def _my_post_row(self) -> List[int]:
        """Return the calling I/O loop's post counts, creating them on first use."""
        row = getattr(self._local, "row", None)
        if row is None:
            row = [0] * len(self.slots)
            self._local.row = row
            self._post_rows.append(row)
        return row

    def _is_free(self, position: int) -> bool:
        """Whether a handle thread can take one more request, as far as this I/O loop can tell.

        Reads ``accepted`` before ``in_flight``: the handle thread raises
        ``in_flight`` before ``accepted``, so a request seen as taken is
        also seen as running and is never counted as neither.

        Args:
            position: Index into :attr:`slots`.
        """
        slot = self.slots[position]
        accepted = slot.accepted
        in_flight = slot.in_flight
        in_transit = sum(row[position] for row in self._post_rows) - accepted
        return in_flight + in_transit < self.max_concurrency

    @staticmethod
    def _post(slot: HandleSlot, callback: Callable[..., None], *args: Any) -> None:
        """Queue ``callback`` on a handle thread; dropped when that loop is closed.

        Args:
            slot: Target handle thread.
            callback: Runs on the handle thread's loop.
            *args: Passed to ``callback``.
        """
        try:
            slot.loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            # Handle threads stop before the I/O loops do at shutdown.
            report_attr("trpc_handler_post_drop", 1)

    @staticmethod
    def _start(run: CoroutineFactory) -> None:
        """Run one request without counting it; on the handle thread."""
        asyncio.get_running_loop().create_task(run())

    def _accept(self, slot: HandleSlot, run: CoroutineFactory) -> None:
        """Take one request posted to this thread; on the handle thread.

        The I/O loop chose this thread on counts it read earlier. When
        another I/O loop filled the last slot in between, the request goes to
        the global queue, without a size check, and this thread takes it
        itself once a slot frees.

        Args:
            slot: This thread's slot.
            run: The request.
        """
        if slot.in_flight < self.max_concurrency:
            slot.in_flight += 1
            slot.accepted += 1
            asyncio.get_running_loop().create_task(self._run(slot, run))
            return
        self._queue.append(run)
        slot.accepted += 1

    async def _run(self, slot: HandleSlot, run: CoroutineFactory) -> None:
        """Serve one request, then free its slot and drain the queue; on the handle thread.

        Args:
            slot: This thread's slot.
            run: The request.
        """
        try:
            await run()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[shared handler pool %s] request failed: %s", slot.index, repr(err), exc_info=True)
        finally:
            slot.in_flight -= 1
            self._pull(slot)

    def _pull(self, slot: HandleSlot) -> None:
        """Start queued requests while this thread has free slots; on the handle thread.

        Clears ``pull_scheduled`` before looking at the queue, so a request
        appended after an I/O loop saw the flag set is still seen here.

        Args:
            slot: This thread's slot.
        """
        slot.pull_scheduled = False
        loop = asyncio.get_running_loop()
        while slot.in_flight < self.max_concurrency:
            try:
                run = self._queue.popleft()
            except IndexError:
                # Empty, or another handle thread took the last one.
                break
            slot.in_flight += 1
            loop.create_task(self._run(slot, run))
