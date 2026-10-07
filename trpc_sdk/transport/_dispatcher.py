# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols that run inside the dispatcher process.

Three connections share the dispatcher's event loop:

- :class:`DispatcherClientConnection` accepts an upstream TCP client, splits
  complete frames, and writes them to a worker IPC pipe.
- :class:`DispatcherWorkerConnection` reads worker responses and writes them
  back to the matching client TCP.
- :class:`DispatcherRpczConnection` receives finished RPCZ spans over the
  side pipe so they can be finalized after the response write.

Workers never hold a client socket. They only send ``fd`` and ``session_id``
back; this process looks those up in ``app.client_connections``.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.rpcz import Span
from trpc_sdk.rpcz import submit_server_span_on_dispatcher_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import make_addr_info_to_bytes
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from ._cython import InnerStreamParser
from ._cython import decode_worker_msg_req
from ._cython import inner_decode_rsp
from ._cython import inner_encode_req
from ._connection import BaseConnection
from ._scheduler import get_next_worker
from ._types import ClientConnectionInfo
from ._types import SchedulerParams


class DispatcherClientConnection(BaseConnection):
    """Dispatcher-side protocol for one accepted client TCP.

    asyncio calls ``connection_made`` after ``accept()``. The TCP object
    stays on this process; workers later see ``fd`` / ``session_id`` /
    ``addr_info`` via IPC. ``session_id`` exists because TCP fds are reused:
    a late worker response tagged ``(fd, old_session)`` must not be written
    to a new client that inherited the same fd.
    """

    def __init__(self, app, address, checker):
        super().__init__(app, checker.create())
        self.address = address
        self.port = address[1]
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
        """Register a newly accepted client TCP connection on the dispatcher.

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
            peername: Optional[Tuple] = self.aio_connection.get_extra_info("peername")
            self.remote_ip: Optional[str] = peername[0] if peername is not None else None
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
        """Wrap each complete frame and write it to one worker IPC pipe.

        Args:
            packages: Complete tRPC frames split out by the parser.
        """
        for package in packages:
            if len(self.app.worker_connections) > 0:
                req_body_buf = inner_encode_req(self.fd, self.port, self.session_id,
                                                now_ms(), self.addr_info, package)

                frame_head: TrpcFrameHead = parse_frame_head(package[:16])
                scheduler_params = SchedulerParams(worker_conn_num=len(self.app.worker_connections),
                                                   remote_ip=self.remote_ip,
                                                   session_id=self.session_id,
                                                   stream_id=frame_head.stream_id,
                                                   package=package,
                                                   frame_type=frame_head.frame_type)

                send_worker_index = get_next_worker(scheduler_params)
                worker_conn: asyncio.Transport = self.app.worker_connections[send_worker_index]
                if not worker_conn.is_closing():
                    worker_conn.write(req_body_buf)
            else:
                report_attr("trpc_dispatcher_no_worker_survive", 1)
                logger.warning("[dispatcher] no worker survives while sending data to worker")

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


class DispatcherWorkerConnection(BaseConnection):
    """Dispatcher-side protocol for one worker req/rsp IPC pipe.

    ``connection_made`` appends the transport to ``app.worker_connections``
    so :class:`DispatcherClientConnection` can pick a worker. Incoming
    bytes are worker responses: decode ``fd`` / ``session_id``, look up the
    client, and write the body.
    """

    def __init__(self, app):
        super().__init__(app, InnerStreamParser())

    def connection_made(self, transport) -> None:
        """Record this worker pipe so client frames can be forwarded to it.

        Args:
            transport: asyncio Transport wrapping the dispatcher end of the
                worker socketpair.
        """
        self.aio_connection = transport
        self.app.worker_connections.append(transport)

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
        """Close this worker pipe when the worker process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_dispatcher_worker_conn_lost", 1)
        logger.error(f"[dispatcher] connection lost with worker {exc}")
        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class DispatcherRpczConnection(BaseConnection):
    """Dispatcher-side protocol for the worker RPCZ side pipe.

    Each finished span is stored in ``app.rpcz_span_map[(fd, session_id)]``
    so :class:`DispatcherWorkerConnection` can finalize it after the
    response write.
    """

    def __init__(self, app):
        super().__init__(app, InnerStreamParser())

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
