# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Async hand-off queue used by stream transports.

Unlike :class:`asyncio.Queue`, this type is a single-waiter rendezvous
plus a bounded deque. A waiting :meth:`Queue.pop` stores one
:class:`~asyncio.Future` on the instance; the next :meth:`Queue.push`
completes that future instead of enqueuing. When the deque is already
at ``max_count``, extra items are logged and dropped. After
:meth:`Queue.push` is called with ``close=True``, a later
:meth:`Queue.pop` that finds the deque empty raises :class:`Exception`.
"""

from __future__ import annotations

from asyncio import Future
from collections import deque
from typing import Any
from typing import Optional
from typing import Union


class Queue:
    """Bounded async queue with at most one waiting :meth:`pop`.

    Attributes:
        max_count: Maximum deque length; extras are discarded on push.
        queue: ``collections.deque`` buffer.
        future: Pending pop waiter, or ``None`` when nobody is waiting.
        is_closed: Set by :meth:`push` when ``close=True``.
    """

    def __init__(self, max_count: int = 10000) -> None:
        self.max_count = max_count
        self.queue = deque(maxlen=max_count)
        self.future: Optional[Future] = None
        self.is_closed = False

    def push(self, data: Any, close: bool = False) -> None:
        """Enqueue ``data``, or deliver it to a waiting :meth:`pop`.

        If ``close`` is True the queue is marked closed before the
        item is delivered. A waiter, if present, is completed even
        when the deque is full; only the no-waiter path discards.

        Args:
            data: Item to hand off.
            close: When True, subsequent empty :meth:`pop` calls raise.
        """
        if close:
            self.is_closed = close

        if self.future:
            if not self.future.done():
                self.future.set_result(data)
            self.future = None
            return

        if len(self.queue) >= self.max_count:
            from trpc_sdk.log import logger
            logger.error("the queue is full, discard")
            return

        self.queue.append(data)

    async def pop(self) -> Union[Any, Exception]:
        """Return the next item, waiting if the deque is empty.

        Returns:
            The oldest buffered item, or the value delivered by a
            later :meth:`push`.

        Raises:
            Exception: If the deque is empty and :attr:`is_closed` is True.
        """
        if len(self.queue) > 0:
            return self.queue.popleft()

        if self.is_closed:
            raise Exception("the queue is closed")

        if self.future:
            return await self.future

        self.future = Future()
        return await self.future
