# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols that run inside an executor process.

An executor listens for itself (multi-listener mode). The UDP path handles
upstream datagrams here; the TCP path uses :class:`TCPConnection` from
``trpc_sdk.protocol``. :class:`ExecutorTaskConnection` is the executor-side
end of the worker↔task reply pipe — same role as
:class:`WorkerTaskConnection`, but attached to an executor event loop.
"""

from __future__ import annotations

import traceback
from typing import List

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import get_message
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.task import inner_decode_task_rsp
from trpc_sdk.task import receive_from_task
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import set_addr_to_ctx_from_transport

from ._c_ipc_codec import InnerStreamParser
from ._connection import BaseConnection


class ExecutorUDPConnection(BaseConnection):
    """Executor-side protocol that handles upstream UDP requests.

    Bound to one listen socket in multi-listener mode. Each datagram is
    fed into the frame parser; every complete payload becomes a handler
    coroutine with a :class:`TrpcContext` already filled with timeout and
    client address. The response is written back with ``sendto``.
    """

    def __init__(self, app, checker, handle, timeout):
        super().__init__(app, checker.create())
        self.handle = handle
        self.timeout = timeout
        self.fd = 0
        self.session_id = 0

    def connection_made(self, transport) -> None:
        """Keep the datagram transport so responses can ``sendto``.

        Args:
            transport: asyncio DatagramTransport for the listen socket.
        """
        self.aio_connection = transport

    async def _handle_request(self, req_buf, conn, client_addr, ctx) -> None:
        """Run the handler and write the response to ``client_addr``.

        Args:
            req_buf: One complete request body.
            conn: Datagram transport used to ``sendto``.
            client_addr: Peer ``(host, port)`` of this datagram.
            ctx: Request context already filled by ``_spawn_request_tasks``.
        """
        try:
            ret = await self.handle(ctx, req_buf)

            conn.sendto(ret, client_addr)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_executor_udp_write_err", 1)
            logger.error("[executor] failed to send rsp to udp client, err: %s, trace: %s", repr(err),
                         traceback.format_exc())

    def _spawn_request_tasks(self, packages: List[bytes], addr) -> None:
        """Fill a context per frame and spawn a handler coroutine.

        Args:
            packages: Complete frames split out by the parser.
            addr: Peer ``(host, port)`` of the datagram that produced them.
        """
        loop = get_or_create_event_loop()
        for package in packages:
            ctx = TrpcContext()
            ctx.timeout = self.timeout
            get_message(ctx)
            set_addr_to_ctx_from_transport(ctx, self.aio_connection)
            ctx.remote_ip = addr[0]
            loop.create_task(self._handle_request(package, self.aio_connection, addr, ctx))

    def datagram_received(self, data: bytes, addr) -> None:
        """Feed one UDP datagram into the parser and handle complete frames.

        Args:
            data: Datagram payload.
            addr: Peer ``(host, port)``.
        """
        self.parser.feeds_data(data)

        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_executor_udp_read_err", 1)
                logger.error("[executor] failed to decode in client udp conn, err: %s, trace: %s", repr(err),
                             traceback.format_stack())
                return

            try:
                if packages is None or len(packages) == 0:
                    return
                self._spawn_request_tasks(packages, addr)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_executor_udp_process_err", 1)
                logger.error("[executor] failed to process udp package, err: %s, trace: %s", repr(err),
                             traceback.format_exc())


class ExecutorTaskConnection(BaseConnection):
    """Executor-side protocol that receives replies from a task process.

    Same wake-up path as :class:`WorkerTaskConnection`: decode
    ``(pkg_id, timeout, rsp_obj)`` and call :func:`receive_from_task`.
    """

    def __init__(self):
        super().__init__(None, InnerStreamParser())

    def connection_made(self, transport) -> None:
        """Keep the task pipe so replies can wake the parked coroutine.

        Args:
            transport: Executor end of the executor↔task socketpair.
        """
        self.aio_connection = transport

    def data_received(self, data: bytes) -> None:
        """Decode task replies and resume the matching user coroutine.

        Args:
            data: Bytes just read from the task IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_executor_task_read_err", 1)
                logger.error("[executor] failed to decode in conn, err: %s, trace: %s", repr(err),
                             traceback.format_stack())
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    pkg_id, timeout, rsp_obj = inner_decode_task_rsp(package)
                    receive_from_task(self.aio_connection, pkg_id, timeout, rsp_obj)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_executor_task_perform_err", 1)
                logger.error("[executor] failed to perform task, err: %s, trace: %s", repr(err),
                             traceback.format_stack())

    def connection_lost(self, ex) -> None:
        """Close the task pipe when the task process drops it.

        Args:
            ex: Close reason, or None on a clean close.
        """
        report_attr("trpc_executor_task_conn_lost", 1)
        logger.error("[executor] connection lost with task server")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()
