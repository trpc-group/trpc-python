# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Credit-based flow control for one stream.

:class:`SendControl` gates outgoing DATA until the peer's window has
room; :class:`ReceiveControl` counts incoming bytes and fires
``send_feedback`` (a FEEDBACK frame) once a quarter of the buffer has
been consumed. INIT window ``0`` means both sides leave these unset.
"""

from __future__ import annotations

from asyncio import Future
from typing import Awaitable
from typing import Callable
from typing import Optional


class SendControl:
    """Sender-side window: ``acquire_window`` blocks until credit is available.

    ``available_window`` is the remaining byte credit advertised by the
    peer. When it drops to ``<= 0``, :meth:`acquire_window` parks on
    ``window_waiter`` until :meth:`update_window` (a FEEDBACK frame) or
    :meth:`close_control` (peer CLOSE) wakes it.
    """

    def __init__(self, init_window: int) -> None:
        self.available_window = init_window
        self.window_waiter: Optional[Future] = None
        self.is_closed = False

    def close_control(self) -> None:
        """Mark the stream closed and wake any waiter in :meth:`acquire_window`."""
        self.is_closed = True
        if self.window_waiter:
            self.window_waiter.done()
            self.window_waiter.set_result(0)

    async def acquire_window(self, size: int) -> None:
        """Consume ``size`` bytes of send credit, waiting if needed.

        Args:
            size: Payload size of the DATA frame about to be written.
                Frame headers are not counted; only the body is.

        Raises:
            Exception: The stream was closed while waiting for credit.
        """
        while not self.is_closed:
            if self.available_window > 0:
                self.available_window -= size
                return

            self.window_waiter = Future()
            await self.window_waiter
            self.window_waiter = None
            if self.is_closed:
                raise Exception("stream is already closed")  # pylint: disable=broad-exception-raised

    def update_window(self, increment: int) -> None:
        """Add ``increment`` bytes of credit from a FEEDBACK frame.

        Args:
            increment: ``trpc_pb2.TrpcStreamFeedBackMeta.window_size_increment``.
        """
        previous_window = self.available_window
        self.available_window += increment
        if previous_window <= 0 and self.available_window > 0 and self.window_waiter:
            self.window_waiter.done()
            self.window_waiter.set_result(0)


class ReceiveControl:
    """Receiver-side window: count bytes and tell the peer to enlarge its send window.

    ``send_feedback`` is the coroutine that writes a FEEDBACK frame. It
    is invoked only after ``consumed_since_feedback`` reaches a quarter
    of ``window_size``, so a FEEDBACK is not sent for every DATA frame.
    """

    def __init__(self, window_size: int, send_feedback: Optional[Callable[[int], Awaitable]]) -> None:
        self.window_size = window_size
        self.remaining_window = window_size
        self.consumed_since_feedback = 0
        self.send_feedback = send_feedback

    def refresh_remaining_window(self) -> None:
        """Recompute remaining receive credit from ``window_size - consumed_since_feedback``."""
        self.remaining_window = self.window_size - self.consumed_since_feedback

    def get_remaining_window(self) -> int:
        """Return bytes the peer may still send before the next FEEDBACK.

        Returns:
            Remaining receive credit.
        """
        return self.remaining_window

    async def on_recv(self, size: int):
        """Account for ``size`` newly received payload bytes.

        Args:
            size: Length of the DATA body just popped from the recv queue.

        Returns:
            The awaitable produced by ``send_feedback``, or ``None`` when
            the quarter-buffer threshold has not been reached.
        """
        self.consumed_since_feedback += size
        if self.consumed_since_feedback >= self.window_size / 4:
            increment = self.consumed_since_feedback
            self.consumed_since_feedback = 0
            self.refresh_remaining_window()
            if self.send_feedback is not None:
                return await self.send_feedback(increment)
            return
        self.refresh_remaining_window()
