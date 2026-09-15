# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols for a process that runs handlers off an IPC pipe.

None of these owns a listen socket. They attach to the pipes their parent
created before fork:

- :class:`WorkerDispatcherConnection` — request/response frames from the
  process that does accept, which in ``separate`` mode is the worker itself
- :class:`WorkerTaskConnection` / :class:`WorkerCTaskConnection` — replies
  from a task process that wake a waiting user coroutine
- :class:`WorkerMessageConnection` — inter-worker broadcast pipe
"""

from __future__ import annotations

import asyncio
import json
import traceback
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import get_message
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import prepare_server_span_for_dispatch
from trpc_sdk.task import InnerParser as TaskParser
from trpc_sdk.task import inner_decode_task_rsp
from trpc_sdk.task import inner_decode_task_rsp_data
from trpc_sdk.task import receive_from_task
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import now_us
from trpc_sdk.utils import parse_addr_info_from_bytes
from ._c_ipc_codec import InnerStreamParser
from ._c_ipc_codec import decode_worker_msg_req
from ._c_ipc_codec import encode_worker_msg_req
from ._c_ipc_codec import inner_decode_req
from ._c_ipc_codec import inner_encode_rsp
from ._connection import BaseConnection


class WorkerDispatcherConnection(BaseConnection):
    """Worker-side protocol for the Python dispatcher req/rsp pipe.

    Each complete inner-IPC frame is decoded into a :class:`TrpcContext`, then
    a handler coroutine is spawned. The response is wrapped with
    ``inner_encode_rsp`` and written back on the same pipe.
    """

    def __init__(self, app, addresses, handlers, timeouts, tran_opts: Optional[BaseServerTransportOption] = None):
        super().__init__(app, InnerStreamParser())
        self.addresses = addresses
        self.handlers = handlers
        self.timeouts = timeouts
        self.loop: asyncio.BaseEventLoop = app.loop
        self.tran_opts = tran_opts

    def connection_made(self, transport) -> None:
        """Keep the dispatcher pipe so responses can be written back.

        Args:
            transport: Worker end of the dispatcher socketpair.
        """
        self.aio_connection = transport

    def _spawn_request_tasks(self, packages: List) -> None:
        """Decode each frame and spawn a handler coroutine.

        Args:
            packages: Complete inner-IPC frames from the dispatcher.
        """
        for package in packages:
            fd, port, session_id, recv_time, addr_info, data = inner_decode_req(package)
            local_addr, peer_addr = parse_addr_info_from_bytes(addr_info)
            index = self.addresses[port]
            ctx = TrpcContext()

            if self.tran_opts[index].socket_type == "unix":
                remote_ip = self.tran_opts[index].address
            else:
                remote_ip = peer_addr[0]

            ctx.timeout = self.timeouts[index]
            ctx.recv_time = recv_time
            ctx.server_post_sched_recv_msg_us = now_us()
            ctx.with_meta_data("fd", fd)
            ctx.with_meta_data("session_id", session_id)
            ctx.with_meta_data("aio_transport", self.aio_connection)
            ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(data))
            ctx.remote_ip = remote_ip
            ctx.remote_port = peer_addr[1]
            ctx.local_ip = local_addr[0]
            ctx.local_port = local_addr[1]

            msg = get_message(ctx)
            msg.set_local_addr(ctx.local_ip)
            msg.set_remote_addr(ctx.remote_ip)
            msg.set_session_id(session_id)

            self.loop.create_task(self._handle_request(ctx, index, data))

    def data_received(self, data: bytes) -> None:
        """Feed dispatcher-pipe bytes into the inner parser.

        Args:
            data: Bytes just read from the dispatcher IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_worker_read_err", 1)
                logger.error(f"[worker] failed to decode in worker conn, err: {err}", exc_info=1)
                return

            try:
                if len(packages) == 0:
                    return

                self._spawn_request_tasks(packages)
            except Exception as err:  # pylint: disable=broad-except
                # catch all Exceptions so that all packages can be handled
                report_attr("trpc_worker_process_err", 1)
                logger.error("[worker] failed to process package, err: %s, trace: %s", repr(err),
                             traceback.format_exc())

    def connection_lost(self, exc) -> None:
        """Close the dispatcher pipe when the dispatcher process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_conn_lost", 1)
        if exc:
            logger.error(f"[worker] connection lost with dispatcher {exc}")
        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()

    async def _handle_request(self, ctx, index, req_data) -> None:
        """Run the service handler and write the response back to the dispatcher.

        When RPCZ produced a span, it is sent on the write-only side pipe
        before the response so the dispatcher can finalize it after I/O.

        Args:
            ctx: Request context already filled by ``_spawn_request_tasks``.
            index: Service index selecting ``handlers[index]``.
            req_data: Decoded request body.
        """
        try:
            handle = self.handlers[index]
            ret = await handle(ctx, req_data)

            if not self.aio_connection.is_closing() and ret is not None and len(ret) > 0:
                ctx.server_pre_sched_send_msg_us = now_us()
                session_id = ctx.get_meta_data("session_id")
                conn_fd = ctx.get_meta_data("fd")
                rpcz_span = prepare_server_span_for_dispatch(ctx)

                if rpcz_span is not None and self.app.rpcz_pipe_conn is not None:
                    rpcz_payload = {
                        "fd": conn_fd,
                        "session_id": session_id,
                        "span": rpcz_span.to_dict(),
                    }
                    side_buf = encode_worker_msg_req(self.app.worker.id, json.dumps(rpcz_payload).encode("utf-8"))
                    self.app.rpcz_pipe_conn.send(side_buf)
                    logger.debug("[worker] rpcz side-pipe sent, worker_id=%s, fd=%s, session_id=%s, span_id=%s",
                                 self.app.worker.id, conn_fd, session_id, rpcz_span.span_id)

                session_id = ctx.pop_meta_data("session_id")
                conn_fd = ctx.pop_meta_data("fd")

                rsp_body_buf = inner_encode_rsp(conn_fd, session_id, 0, ret)

                ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(rsp_body_buf))
                self.aio_connection.write(rsp_body_buf)
                # dispatcher mode has no shared context across processes, record local post point.
                ctx.server_post_sched_send_msg_us = now_us()
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_worker_write_err", 1)
            logger.error("[worker] failed to send rsp to dispatcher (normal rpc), err: %s, trace: %s", repr(err),
                         traceback.format_stack())


class WorkerTaskConnection(asyncio.Protocol):
    """Worker-side protocol that receives replies from a task process.

    Each decoded ``(pkg_id, timeout, rsp_obj)`` wakes the user coroutine
    that :func:`receive_from_task` parked when the request was sent.
    """

    def __init__(self):
        self.aio_connection = None
        self.parser = InnerStreamParser()

    def connection_made(self, transport) -> None:
        """Keep the task pipe so connection_lost can close it.

        Args:
            transport: Worker end of the worker↔task socketpair.
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
                report_attr("trpc_worker_task_read_err", 1)
                logger.error(f"[worker] failed to decode in task conn, err: {err}", exc_info=1)
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    pkg_id, timeout, rsp_obj = inner_decode_task_rsp(package)
                    receive_from_task(self.aio_connection, pkg_id, timeout, rsp_obj)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_worker_task_perform_err", 1)
                logger.error("[worker] failed to perform task, err: %s, trace: %s", repr(err), traceback.format_stack())

    def connection_lost(self, exc) -> None:
        """Close the task pipe when the task process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_task_conn_lost", 1)
        logger.error(f"[worker] connection lost with task server {exc}")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class WorkerCTaskConnection(BaseConnection):
    """Worker-side protocol for replies from the native C task driver."""

    def __init__(self):
        super().__init__(None, TaskParser())

    def connection_made(self, transport) -> None:
        """Keep the C-task pipe so connection_lost can close it.

        Args:
            transport: Worker end of the worker↔C-task socketpair.
        """
        self.aio_connection = transport

    def data_received(self, data: bytes) -> None:
        """Decode C-task replies and resume the matching user coroutine.

        Args:
            data: Bytes just read from the C-task IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_worker_task_read_err", 1)
                logger.error("[worker] failed to decode in conn, err: %s, trace: %s", repr(err),
                             traceback.format_stack())
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    pkg_id, timeout, rsp_obj = inner_decode_task_rsp_data(package)
                    receive_from_task(self.aio_connection, pkg_id, timeout, rsp_obj)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_worker_task_perform_err", 1)
                logger.error("[worker] failed to perform task, err: %s, trace: %s", repr(err), traceback.format_stack())

    def connection_lost(self, exc) -> None:
        """Close the C-task pipe when the task process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_task_conn_lost", 1)
        logger.error(f"[worker] connection lost with task server {exc}")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class WorkerMessageConnection(asyncio.Protocol):
    """Worker-side protocol for the inter-worker broadcast pipe.

    Decoded payloads are handed to ``engine.on_worker_message``. The sender
    tagged the frame with its worker id; this end only dispatches.
    """

    def __init__(self, engine):
        self.aio_connection = None
        self.parser = InnerStreamParser()
        self.engine = engine

    def connection_made(self, transport) -> None:
        """Keep the broadcast pipe so connection_lost can close it.

        Args:
            transport: This worker's end of the message socketpair.
        """
        self.aio_connection = transport

    def data_received(self, data: bytes) -> None:
        """Decode inter-worker messages and hand them to the engine callback.

        Args:
            data: Bytes just read from the broadcast pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_worker_task_read_err", 1)
                logger.error("[worker] failed to decode in conn, err: %s, trace: %s", repr(err),
                             traceback.format_stack())
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    from_id, req_data = decode_worker_msg_req(package)
                    self.engine.on_worker_message(from_id, req_data)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_worker_task_perform_err", 1)
                logger.error("[worker] failed to perform task, err: %s, trace: %s", repr(err), traceback.format_stack())

    def connection_lost(self, exc) -> None:
        """Close the broadcast pipe when the peer drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_task_conn_lost", 1)
        logger.error(f"[worker] connection lost with task server {exc}")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()
