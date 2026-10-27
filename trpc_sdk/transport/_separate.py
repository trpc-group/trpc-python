# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols for both ends of the separate-mode IPC pipes.

The I/O process (``app`` is its :class:`~trpc_sdk.runtime.SeparateWorker`)
runs three connections on its event loop:

- :class:`SeparateClientConnection` accepts an upstream TCP client, splits
  complete frames, and writes them to an rpc task IPC pipe.
- :class:`SeparateRpcTaskConnection` reads rpc task responses and writes them
  back to the matching client TCP.
- :class:`SeparateRpczConnection` receives finished RPCZ spans over the
  side pipe so they can be finalized after the response write.

Each rpc task child (``app`` is its :class:`~trpc_sdk.runtime.RpcTaskProcess`)
runs :class:`RpcTaskConnection` on the other end of its pipe: it decodes
frames, runs the handler, and writes the response back.

Rpc tasks never hold a client socket. They only send ``fd`` and
``session_id`` back; the I/O process looks those up in
``app.client_connections``. Each forwarded frame carries the listen *address
index* in the frame head's port field, so an rpc task can pick the handler
even when two addresses share a port (two unix sockets, or one port on two
IPs). Pipes are bound by rpc task index, and a pipe that closes marks its rpc
task dead so no further frame is sent to it.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from typing import Any
from typing import List
from typing import Optional

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import get_message
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import Span
from trpc_sdk.rpcz import prepare_server_span_for_dispatch
from trpc_sdk.rpcz import submit_server_span_on_dispatcher_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import make_addr_info_to_bytes
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import parse_addr_info_from_bytes
from ._cython import InnerStreamParser
from ._cython import decode_worker_msg_req
from ._cython import encode_worker_msg_req
from ._cython import inner_decode_req
from ._cython import inner_decode_rsp
from ._cython import inner_encode_req
from ._cython import inner_encode_rsp
from ._scheduler import select_rpc_task
from ._types import ClientConnectionInfo
from ._types import SchedulerParams
from ._server import IpcStreamWriter


def _peer_host(peername: Any) -> Optional[str]:
    """Return the host or Unix path from an asyncio ``peername``.

    An inet peer is ``(host, port)``. A Unix client that bound its own path
    reports that path. A Unix client that did not bind reports ``""``, which
    is not a host.

    Args:
        peername: ``transport.get_extra_info("peername")``.

    Returns:
        The host or path, or ``None`` when the peer has none.
    """
    if isinstance(peername, str):
        return peername or None
    if isinstance(peername, (tuple, list)) and peername:
        return peername[0]
    return None


class SeparateClientConnection(asyncio.Protocol):
    """I/O-process protocol for one accepted client TCP.

    asyncio calls ``connection_made`` after ``accept()``. The TCP object
    stays on this process; workers later see ``fd`` / ``session_id`` /
    ``addr_info`` via IPC. ``session_id`` exists because TCP fds are reused:
    a late worker response tagged ``(fd, old_session)`` must not be written
    to a new client that inherited the same fd.
    """

    def __init__(self, app, address, parser, index: int, schedule_target: str = ""):
        """Bind one accepted client to its listen address.

        Args:
            app: The I/O process's worker; owns the client and rpc task tables.
            address: Listen address the client connected to, for log messages.
            parser: Parser factory of that address; cuts the byte stream.
            index: Position of the address in the worker's tables. Sent in the
                frame head's port field, so the rpc task picks the handler by
                index rather than by port.
            schedule_target: ``SchedulerParams.target`` for every frame.
        """
        self.app = app
        self.parser = parser.create()
        self.aio_connection = None
        self.address = address
        self.index = index
        self.schedule_target = schedule_target
        self.fd = 0
        self.session_id = 0
        self.remote_ip = b''
        self.addr_info = b''
        self.last_recv_time = 0
        self.check_idle_timeout = TrpcConfig.server_config().check_idle_timeout
        self.check_task = None

    async def _watch_idle_timeout(self) -> None:
        """Close the client if no bytes arrive within ``check_idle_timeout``.

        ``data_received`` refreshes ``last_recv_time``. The task exits when
        the connection is already gone, already closing, or this check
        itself closes it.
        """
        try:
            while True:
                await asyncio.sleep(self.check_idle_timeout / 1000)
                client_conn_info: ClientConnectionInfo = self.app.client_connections.get(self.fd, None)
                if not client_conn_info:
                    break

                if client_conn_info.conn.is_closing():
                    logger.debug("[dispatcher] connection is closing ")
                    break

                if (time.time() - self.last_recv_time) * 1000 > self.check_idle_timeout:
                    report_attr("conn_idle_check_timeout_err", 1)
                    logger.error(
                        "[dispatcher] conn_idle_check time %s, last_recv_time %s, diff_time  %s, idle_time: %s, fd: %s,"
                        "session_id: %s, address: %s", time.time(), self.last_recv_time,
                        (time.time() - self.last_recv_time) * 1000, self.check_idle_timeout, self.fd, self.session_id,
                        self.address)
                    self.close_client_conn(self.fd, self.session_id)
                    break
        except asyncio.exceptions.CancelledError:  # pylint: disable=broad-except
            report_attr("conn_idle_check_cancel_err", 1)
            logger.debug("[dispatcher] conn_idle_check CancelledError ")
        finally:
            logger.debug("[dispatcher] conn_idle_check exit")

    def connection_made(self, transport: asyncio.Transport) -> None:
        """Register a newly accepted client TCP connection on the I/O process.

        asyncio calls this once per connection after ``accept()``, before any
        ``data_received``. ``transport`` wraps the accepted client socket, not
        the listen fd.

        Example: client ``192.168.1.8:54321`` connects to ``10.0.0.5:8000``
        and the kernel assigns ``fd=42``. After this method:

        - ``self.fd == 42``, ``self.session_id == 7`` (next global id)
        - ``self.addr_info == b"10.0.0.5:8000|192.168.1.8:54321"``
        - ``app.client_connections[42] == (session_id=7, transport)``

        Args:
            transport: asyncio Transport for the accepted client socket.
        """
        try:
            self.aio_connection: asyncio.Transport = transport
            sock: Any = self.aio_connection.get_extra_info("socket")
            self.fd: int = sock.fileno()
            self.addr_info: bytes = make_addr_info_to_bytes(self.aio_connection)
            # Unix clients that do not bind a path report "" here. Indexing
            # that string closes the connection before any frame is served.
            peername: Any = self.aio_connection.get_extra_info("peername")
            self.remote_ip: Optional[str] = _peer_host(peername)
            self.app.session_id = (self.app.session_id + 1) & 0xffffffff
            self.session_id: int = self.app.session_id
            self.app.client_connections[self.fd] = ClientConnectionInfo(self.session_id, self.aio_connection)
            if self.check_idle_timeout > 0:
                self.last_recv_time: float = time.time()
                self.check_task: Optional[asyncio.Task] = self.app.loop.create_task(self._watch_idle_timeout())
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_dispatcher_client_conn_made_err", 1)
            logger.error(f"[dispatcher] failed to make connection with client, err: {err} ", exc_info=True)
            self.close_client_conn(self.fd, self.session_id)

    def _forward_frames_to_workers(self, packages) -> None:
        """Wrap each complete frame and write it to one live rpc task's pipe.

        A frame with nowhere to go is dropped and counted; the client times
        out. A stream frame whose rpc task died also closes this client, since
        every later frame of that stream would be dropped the same way.

        Args:
            packages: Complete tRPC frames split out by the parser.
        """
        worker_connections = self.app.worker_connections
        for package in packages:
            frame_head: TrpcFrameHead = parse_frame_head(package[:16])
            scheduler_params = SchedulerParams(worker_conn_num=len(worker_connections),
                                               remote_ip=self.remote_ip,
                                               session_id=self.session_id,
                                               stream_id=frame_head.stream_id,
                                               package=package,
                                               frame_type=frame_head.frame_type,
                                               target=self.schedule_target)
            index = select_rpc_task(scheduler_params, self.app.rpc_task_alive)
            if index is None:
                report_attr("trpc_dispatcher_no_worker_survive", 1)
                logger.warning("[dispatcher] no live rpc task for a frame from %s; dropped", self.remote_ip)
                if frame_head.frame_type == trpc_pb2.TRPC_STREAM_FRAME:
                    self.close_client_conn(self.fd, self.session_id)
                    return
                continue

            worker_conn: Optional[asyncio.Transport] = worker_connections[index]
            if worker_conn is None or worker_conn.is_closing():
                report_attr("trpc_dispatcher_worker_conn_closed", 1)
                logger.warning("[dispatcher] pipe to rpc task %s is closed; frame dropped", index)
                continue
            worker_conn.write(inner_encode_req(self.fd, self.index, self.session_id, now_ms(), self.addr_info,
                                               package))

    def data_received(self, data: bytes) -> None:
        """Feed upstream bytes into the parser and forward complete frames.

        Args:
            data: Bytes just read from the client TCP.
        """
        if self.check_idle_timeout > 0:
            self.last_recv_time = time.time()

        self.parser.feed_data(data)

        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_dispatcher_client_read_err", 1)
                logger.error("remote ip is %s", self.remote_ip)
                logger.error(f"[dispatcher] failed to decode in client conn, err: {err}", exc_info=True)
                self.close_client_conn(self.fd, self.session_id)
                return
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_dispatcher_checker_fatal_err", 1)
                logger.error("remote ip is %s", self.remote_ip)
                logger.error(f"[dispatcher] fatal err while decoding in client conn, err: {err}", exc_info=True)
                self.close_client_conn(self.fd, self.session_id)
                return

            try:
                if not packages:
                    return

                self._forward_frames_to_workers(packages)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_dispatcher_worker_write_err", 1)
                logger.error("remote ip is %s", self.remote_ip)
                logger.error(f"[dispatcher] failed to send data to worker, err: {err}", exc_info=True)
                self.close_client_conn(self.fd, self.session_id)

    def connection_lost(self, exc) -> None:
        """Cancel the idle watchdog and drop this client from the map.

        Args:
            exc: Close reason, or None on a clean close.
        """
        if exc:
            logger.warning(f"connection_lost err: {exc}")
        if self.check_task:
            self.check_task.cancel()
        self.close_client_conn(self.fd, self.session_id)

    def close_client_conn(self, fd, session_id) -> None:
        """Close the client TCP if ``session_id`` still matches the map entry.

        The session check is what stops a late close of connection A from
        tearing down connection B that reused the same fd.

        Args:
            fd: Kernel fd used as the map key.
            session_id: Connection-lifetime id that must match the map entry.
        """
        client_conn_info: ClientConnectionInfo = self.app.client_connections.get(fd, None)
        self.app.rpcz_span_map.pop((fd, session_id), None)
        if client_conn_info and session_id == client_conn_info.session_id:
            if client_conn_info.conn and not client_conn_info.conn.is_closing():
                client_conn_info.conn.close()
            del self.app.client_connections[fd]


class SeparateRpcTaskConnection(asyncio.Protocol):
    """I/O-process protocol for one rpc task req/rsp IPC pipe.

    ``connection_made`` stores the transport at ``app.worker_connections[index]``
    so :class:`SeparateClientConnection` can pick a worker. Incoming
    bytes are worker responses: decode ``fd`` / ``session_id``, look up the
    client, and write the body.
    """

    def __init__(self, app, index: int):
        """Bind this pipe to one rpc task.

        Args:
            app: The I/O process's worker.
            index: The rpc task this pipe leads to; the slot it fills in
                ``app.worker_connections``.
        """
        self.app = app
        self.parser = InnerStreamParser()
        self.aio_connection = None
        self.index = index

    def connection_made(self, transport) -> None:
        """Record this worker pipe so client frames can be forwarded to it.

        Stored by index rather than appended, so the slot matches the rpc
        task whatever order the pipes are attached in.

        Args:
            transport: asyncio Transport wrapping the I/O process end of the
                rpc task socketpair.
        """
        self.aio_connection = transport
        self.app.worker_connections[self.index] = transport

    def _write_rsps_to_clients(self, packages) -> None:
        """Decode worker responses and write matching ones to client TCPs.

        A response is dropped (not written) when the client is already
        closing, the fd is gone, or the session id no longer matches.

        Args:
            packages: Complete inner-IPC frames from the worker pipe.
        """
        for package in packages:
            fd, session_id, _, rsp_body_buf = inner_decode_rsp(package)
            if fd in self.app.client_connections:
                client_conn: ClientConnectionInfo = self.app.client_connections[fd]
                if client_conn.conn.is_closing():
                    report_attr("trpc_dispatcher_client_conn_closed", 1)
                    logger.debug("[dispatcher] client connection is already closing before sending data to client")
                    continue

                if client_conn.session_id == session_id:
                    post_sched_send_us = now_us()
                    pre_io_send_us = now_us()
                    client_conn.conn.write(rsp_body_buf)
                    post_io_send_us = now_us()
                    span_key = (fd, session_id)
                    span = self.app.rpcz_span_map.pop(span_key, None)
                    if isinstance(span, Span):
                        span.response_size = len(rsp_body_buf)
                        logger.debug("[dispatcher] finalize rpcz span on post-io, fd=%s, session_id=%s, span_id=%s", fd,
                                     session_id, span.span_id)
                        submit_server_span_on_dispatcher_post_io(span, post_sched_send_us, pre_io_send_us,
                                                                 post_io_send_us)
            else:
                report_attr("trpc_dispatcher_client_conn_lost", 1)
                logger.debug("[dispatcher] client connection lost while receiving rsp from worker")

    def data_received(self, data: bytes) -> None:
        """Feed worker-pipe bytes into the inner parser and write responses.

        Args:
            data: Bytes just read from the worker IPC pipe.
        """
        self.parser.feeds_data(data)

        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_dispatcher_worker_read_err", 1)
                logger.error(f"[dispatcher] failed to decode in worker conn, err: {err} ", exc_info=True)
                return

            try:
                if len(packages) == 0:
                    return

                self._write_rsps_to_clients(packages)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_dispatcher_client_write_err", 1)
                logger.error(f"[dispatcher] failed to send rsp to client, err: {err} ", exc_info=True)

    def connection_lost(self, exc) -> None:
        """Mark the rpc task dead and close this pipe when its process drops it.

        Usually arrives before SIGCHLD, so marking here is what stops frames
        from being written into a pipe nobody reads.

        Args:
            exc: Close reason, or None on a clean close.
        """
        self.app.mark_rpc_task_dead(self.index)
        if not self.app.exiting:
            report_attr("trpc_dispatcher_worker_conn_lost", 1)
            logger.error("[dispatcher] connection lost with rpc task %s: %s", self.index, exc)
        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class SeparateRpczConnection(asyncio.Protocol):
    """I/O-process protocol for the rpc task RPCZ side pipe.

    Each finished span is stored in ``app.rpcz_span_map[(fd, session_id)]``
    so :class:`SeparateRpcTaskConnection` can finalize it after the
    response write.
    """

    def __init__(self, app):
        self.app = app
        self.parser = InnerStreamParser()
        self.aio_connection = None

    def data_received(self, data: bytes) -> None:
        """Decode side-pipe spans and stash them until the response is written.

        Args:
            data: Bytes just read from the RPCZ side pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_dispatcher_worker_rpcz_read_err", 1)
                logger.error(f"[dispatcher] failed to decode rpcz side-pipe data, err: {err}", exc_info=True)
                return

            if len(packages) == 0:
                return

            for package in packages:
                try:
                    _, payload = decode_worker_msg_req(package)
                    item = json.loads(payload.decode("utf-8"))
                    fd = item.get("fd")
                    session_id = item.get("session_id")
                    span_data = item.get("span")

                    if fd is None or session_id is None or not span_data:
                        logger.warning("[dispatcher] invalid rpcz side-pipe payload, fd=%s, session_id=%s", fd,
                                       session_id)
                        continue

                    span = Span.from_dict(span_data)
                    self.app.rpcz_span_map[(fd, session_id)] = span
                    logger.debug("[dispatcher] received rpcz side-pipe span, fd=%s, session_id=%s, span_id=%s", fd,
                                 session_id, span.span_id)
                except Exception as err:  # pylint: disable=broad-except
                    report_attr("trpc_dispatcher_worker_rpcz_payload_err", 1)
                    logger.error(f"[dispatcher] failed to parse rpcz span payload, err: {err}", exc_info=True)


class RpcTaskConnection(asyncio.Protocol):
    """Rpc-task-side protocol for the req/rsp pipe to the I/O process.

    Each complete inner-IPC frame is decoded into a :class:`TrpcContext`, then
    a handler coroutine is spawned. The response is wrapped with
    ``inner_encode_rsp`` and written back on the same pipe.

    The frame head's port field carries the listen *address index* the
    I/O process received the frame on, not a port: it indexes ``handlers``,
    ``timeouts`` and ``tran_opts`` directly, so two addresses sharing a port
    still reach their own handler.
    """

    def __init__(self, app, handlers, timeouts, tran_opts: Optional[List[BaseServerTransportOption]] = None):
        self.app = app
        self.parser = InnerStreamParser()
        self.aio_connection = None
        self.handlers = handlers
        self.timeouts = timeouts
        self.loop: asyncio.BaseEventLoop = app.loop
        self.tran_opts = tran_opts

    def connection_made(self, transport) -> None:
        """Keep the I/O process pipe so responses can be written back.

        Args:
            transport: Rpc task end of the I/O process socketpair.
        """
        self.aio_connection = transport

    def _spawn_request_tasks(self, packages: List) -> None:
        """Decode each frame and spawn a handler coroutine.

        Args:
            packages: Complete inner-IPC frames from the I/O process.
        """
        for package in packages:
            fd, index, session_id, recv_time, addr_info, data = inner_decode_req(package)
            local_addr, peer_addr = parse_addr_info_from_bytes(addr_info)
            ctx = TrpcContext()

            if self.tran_opts[index].socket_type == "unix":
                remote_ip = self.tran_opts[index].address[0]
            else:
                remote_ip = peer_addr[0]

            ctx.timeout = self.timeouts[index]
            ctx.recv_time = recv_time
            ctx.rpcz_data.server_post_sched_recv_msg_us = now_us()
            ctx.with_meta_data("fd", fd)
            ctx.with_meta_data("session_id", session_id)
            # Only stream frames read it; telling them apart needs the frame head.
            ctx.with_meta_data(STREAM_WRITER_CTX_KEY, IpcStreamWriter(self.aio_connection, fd, session_id))
            ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(data))
            ctx.remote_ip = remote_ip
            ctx.remote_port = peer_addr[1]
            ctx.local_ip = local_addr[0]
            ctx.local_port = local_addr[1]

            msg = get_message(ctx)
            msg.set_local_ip(ctx.local_ip)
            msg.set_remote_addr(ctx.remote_ip)
            msg.set_session_id(session_id)

            self.loop.create_task(self._handle_request(ctx, index, data))

    def data_received(self, data: bytes) -> None:
        """Feed I/O process pipe bytes into the inner parser.

        Args:
            data: Bytes just read from the I/O process IPC pipe.
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
        """Close the I/O process pipe when the I/O process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_conn_lost", 1)
        if exc:
            logger.error(f"[worker] connection lost with dispatcher {exc}")
        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()

    async def _handle_request(self, ctx, index, req_data) -> None:
        """Run the service handler and write the response back to the I/O process.

        When RPCZ produced a span, it is sent on the write-only side pipe
        before the response so the I/O process can finalize it after I/O.

        Args:
            ctx: Request context already filled by ``_spawn_request_tasks``.
            index: Service index selecting ``handlers[index]``.
            req_data: Decoded request body.
        """
        try:
            handle = self.handlers[index]
            ret = await handle(ctx, req_data)

            if not self.aio_connection.is_closing() and ret is not None and len(ret) > 0:
                ctx.rpcz_data.server_pre_sched_send_msg_us = now_us()
                session_id = ctx.get_meta_data("session_id")
                conn_fd = ctx.get_meta_data("fd")
                rpcz_span = prepare_server_span_for_dispatch(ctx)

                if rpcz_span is not None and self.app.rpcz_pipe_conn is not None:
                    rpcz_payload = {
                        "fd": conn_fd,
                        "session_id": session_id,
                        "span": rpcz_span.to_dict(),
                    }
                    side_buf = encode_worker_msg_req(self.app.worker_id, json.dumps(rpcz_payload).encode("utf-8"))
                    self.app.rpcz_pipe_conn.send(side_buf)
                    logger.debug("[worker] rpcz side-pipe sent, worker_id=%s, fd=%s, session_id=%s, span_id=%s",
                                 self.app.worker_id, conn_fd, session_id, rpcz_span.span_id)

                session_id = ctx.pop_meta_data("session_id")
                conn_fd = ctx.pop_meta_data("fd")

                rsp_body_buf = inner_encode_rsp(conn_fd, session_id, 0, ret)

                ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(rsp_body_buf))
                self.aio_connection.write(rsp_body_buf)
                # dispatcher mode has no shared context across processes, record local post point.
                ctx.rpcz_data.server_post_sched_send_msg_us = now_us()
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_worker_write_err", 1)
            logger.error("[worker] failed to send rsp to dispatcher (normal rpc), err: %s, trace: %s", repr(err),
                         traceback.format_exc())
