# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols that run inside a task process.

:class:`TaskWorkerConnection` is the task-side end of each worker socketpair.
After fork it records the transport in :data:`TASK_CONN` so C task code can
write a response back through the same map.

:class:`TaskStreamConnection` accepts a stream connection from a worker (or
task client) and runs a task-stream service adapter on each complete frame.
The adapter type is deliberately left unannotated so this module does not
import :mod:`trpc_sdk.stream` at all (that package imports this package's
stream transport).
"""

from __future__ import annotations

import asyncio
import traceback
from typing import Dict

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import TaskStreamServerDecodeError
from trpc_sdk.codec import TaskStreamServerParser
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.task import receive_pkg_from_worker
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import now_ms

from ._c_ipc_codec import InnerStreamParser

# Remaining socketpair end per worker index. ``TaskWorkerConnection`` fills
# this after fork; C task code writes responses back through the same map.
TASK_CONN: Dict[int, object] = {}


class TaskWorkerConnection(asyncio.Protocol):
    """Task-side protocol for one worker req/rsp socketpair.

    ``connection_made`` stores the transport in :data:`TASK_CONN` keyed by
    worker index. Each complete inner-IPC frame is handed to
    :func:`receive_pkg_from_worker`.
    """

    def __init__(self, app, index):
        self.app = app
        self.index = index
        self.parser = InnerStreamParser()
        self.aio_connection = None

    def connection_made(self, transport) -> None:
        """Record this worker pipe so C task code can write a response.

        Args:
            transport: Task end of the worker↔task socketpair.
        """
        self.aio_connection = transport
        TASK_CONN[self.index] = transport

    def data_received(self, data: bytes) -> None:
        """Decode worker frames and dispatch them to the C task runtime.

        Args:
            data: Bytes just read from the worker IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_task_read_err", 1)
                logger.error(f"[task] failed to decode in conn, err: {err}\n, trace: {traceback.format_stack()}",
                             exc_info=True)
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    receive_pkg_from_worker(self.index, package)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_task_perform_err", 1)
                logger.error(f"[task] failed to perform task err: {err}\n, trace: {traceback.format_stack()}",
                             exc_info=True)

    def connection_lost(self, exc) -> None:
        """Close this worker pipe when the worker process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        if exc:
            logger.error(f"[task] connection lost, reason {exc}")
        report_attr("trpc_task_conn_lost", 1)
        logger.info("[task] connection lost")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class TaskStreamConnection(asyncio.Protocol):
    """Task-side protocol that serves a stream connection as a server.

    Each complete task-stream frame becomes a handler coroutine on the
    task process event loop. ``session_id`` is a wrapping 31-bit counter
    local to this process, not the client TCP session id used by the
    dispatcher.

    Args:
        service_adapter: Task-stream service adapter, actually a
            ``trpc_sdk.stream.TaskStreamServiceAdapter``. It is left
            unannotated because ``trpc_sdk.stream`` imports this package.
        loop: Task process event loop the handler coroutines run on.
    """

    _count: int = 0

    def __init__(self, service_adapter, loop: asyncio.BaseEventLoop) -> None:
        super().__init__()
        self.parser = TaskStreamServerParser()
        self._transport = None
        self._service_adapter = service_adapter
        self._loop: asyncio.BaseEventLoop = loop
        self._session_id = 0

    def connection_made(self, transport: asyncio.Transport) -> None:
        """Assign a process-local session id and keep the stream transport.

        Args:
            transport: Accepted stream socket (worker or task client).
        """
        self._transport = transport
        self._session_id = self._next_session_id() & 0x7fffffff

    @classmethod
    def _next_session_id(cls) -> int:
        """Return the previous counter value and increment it.

        Returns:
            The session id assigned to the connection that just called
            ``connection_made``.
        """
        last = cls._count
        cls._count += 1
        return last

    async def _handle_package(self, package: bytes) -> None:
        """Decode one stream frame and run the task-stream adapter.

        Args:
            package: A complete task-stream frame.
        """
        ctx = TrpcContext()
        ctx.recv_time = now_ms()
        ctx.timeout = self._service_adapter.service_opts.timeout or 1000
        ctx.with_meta_data("aio_transport", self._transport)
        ctx.with_meta_data("session_id", self._session_id)

        req_body = self._service_adapter.decode_header(ctx, package)
        await self._service_adapter.handle(ctx, req_body)

    def data_received(self, data: bytes) -> None:
        """Feed stream bytes into the parser and spawn a task per frame.

        Args:
            data: Bytes just read from the stream socket.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
                if len(packages) == 0:
                    return
                for package in packages:
                    self._loop.create_task(self._handle_package(package))
            except TaskStreamServerDecodeError as err:
                logger.error(f"[task stream] failed to decode, err: {err}", exc_info=True)
                return
            except Exception as err:  # pylint: disable=broad-except
                logger.error(f"[task stream] data_received, err: {err}", exc_info=True)

    def connection_lost(self, exc) -> None:
        """Close the stream socket when the peer drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        if exc:
            logger.error(f"[task stream] server connection lost with stream, err reason {exc}")
        if self._transport and not self._transport.is_closing():
            self._transport.close()
