# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Batch consumers running inside a task child process.

Requests and responses cross the worker/task boundary through two C queues.
This module drives them from Python: a dedicated thread drains the request
queue into the user handler, and a coroutine on the main loop drains the
response queue back onto the worker pipes.
"""

import asyncio
import inspect
import threading
from asyncio import new_event_loop

from ._cython import co_consume_batch_req
from ._cython import consume_batch_req
from ._cython import consume_batch_rsp


class TaskConsumer(threading.Thread):
    """Thread feeding batches from the request queue into the user handler.

    The handler runs off the main thread so a slow or blocking handler cannot
    stall the loop that pumps responses back to the workers. The thread owns
    its own event loop because the handler is allowed to be a coroutine.
    """

    def __init__(self, func, wait_interval, batch_size, daemon=True):
        """Configure the consumer; the thread is not started here.

        Args:
            func: User handler taking a list of requests and returning a list
                of responses of the same length. May be a coroutine function.
            wait_interval: Milliseconds to sleep when the queue is empty.
            batch_size: Maximum number of requests handed over in one call.
            daemon: Whether the thread should not keep the process alive.
        """
        threading.Thread.__init__(self)
        self.func = func
        self.wait_interval = wait_interval
        self.batch_size = batch_size
        self.daemon = daemon
        self._running = True

    def terminate(self):
        """Ask the consume loop to stop after the current batch."""
        self._running = False

    def run(self):
        """Run the consume loop on an event loop private to this thread."""
        loop = new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.consume())
        loop.close()

    async def consume(self):
        """Drain the request queue until :meth:`terminate` is called.

        The sync and async C entry points both call back into ``self`` for
        the handler and the batch size. A non-zero return means nothing was
        dequeued, which is the only case worth backing off on.
        """
        is_coroutine = inspect.iscoroutinefunction(self.func)
        while self._running:
            if is_coroutine:
                ret = await co_consume_batch_req(self)
            else:
                ret = consume_batch_req(self)
            if ret != 0:
                await asyncio.sleep(self.wait_interval / 1000)


async def consume_response(wait_interval):
    """Write finished responses back to the workers, forever.

    Runs on the task process main loop. After a productive round it yields
    only briefly, so a backlog is drained without returning to the idle
    ``wait_interval`` between every batch.

    Args:
        wait_interval: Milliseconds to sleep when the response queue is empty.
    """
    while True:
        rsp_num = consume_batch_rsp()
        if rsp_num > 0:
            await asyncio.sleep(0.005)
        else:
            await asyncio.sleep(wait_interval / 1000)
