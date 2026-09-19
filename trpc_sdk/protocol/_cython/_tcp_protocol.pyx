# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""asyncio Protocol that splits a client TCP stream and runs unary handlers.

``TCPConnection`` is bound to one accepted socket on the merge-worker /
multi-listener executor path. A per-connection checker splits bytes into
complete frames; each frame is scheduled as an asyncio task that runs
:func:`handle_request`. UDP on the same listen path uses
``ExecutorUDPConnection`` in :mod:`trpc_sdk.transport`, not this module.
"""

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import submit_server_span_on_executor_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import get_or_create_event_loop
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import set_addr_to_ctx_from_transport


async def handle_request(handle, req_buf, conn, ctx, skip_empty_write=False):
    """Run one unary handler and write the response on the same TCP transport.

    This is the body of the per-frame asyncio task created in
    :meth:`TCPConnection.data_received`. ``conn`` is the asyncio
    ``Transport`` stored as ``aio_connection``. Write-side rpcz stamps
    (``server_pre/post_sched_send_msg_us``, ``server_pre_io_send_msg_us``)
    share one ``now_us()`` because merge / multi-listener mode runs
    sched_send and io_send in the same process. Never raises: failures
    are counted with ``report_attr`` and logged.

    Args:
        handle: Unary service entry, typically ``Service.handle``.
        req_buf: One complete request frame.
        conn: asyncio ``Transport`` for this client socket.
        ctx: Per-request ``TrpcContext``.
        skip_empty_write: When True (executor stream mode), return
            without ``conn.write`` if ``handle`` yields a false value.
            Stream frames are written later by
            ``TrpcServerStreamTransport.send``.
    """
    try:
        # business
        ret = await handle(ctx, req_buf)

        if not conn.is_closing():
            # In multi-listener mode, sched_send and io_send are in the same process.
            send_us = now_us()
            ctx.rpcz_data.server_pre_sched_send_msg_us = send_us
            ctx.rpcz_data.server_post_sched_send_msg_us = send_us
            ctx.rpcz_data.server_pre_io_send_msg_us = send_us
            # Executor stream mode: stream-frame responses are written later
            # by TrpcServerStreamTransport.send. handle() returns None;
            # skip conn.write(None) which would raise TypeError.
            if skip_empty_write and not ret:
                return
            ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(ret))
            conn.write(ret)
            ctx.rpcz_data.server_post_io_send_msg_us = now_us()
            submit_server_span_on_executor_post_io(ctx)
            logger.debug("[executor] rpcz submit on executor post-io completed")
            # TODO use await conn.drain() to flush write buffer.
    except Exception as err:  # pylint: disable=broad-except
        report_attr("trpc_executor_tcp_write_err", 1)
        logger.error(f"[executor] failed to send rsp to tcp client, err: {err}", exc_info=True)


cdef class TCPConnection:
    """asyncio Protocol for one accepted client TCP connection.

    Constructed as the ``protocol_factory`` on a listen socket. The
    ``checker`` argument is a factory; :meth:`__init__` calls
    ``checker.create()`` so each connection has its own splitter.
    ``handle`` is the unary service entry. Stream-executor mode is
    cached from server config at construct time so
    :meth:`data_received` does not query global config on the hot path.
    """

    def __init__(self, app, checker, handle, timeout):
        """Bind app, checker factory, unary handle, and idle timeout (ms).

        Args:
            app: Service / app object stored for the connection lifetime.
            checker: Protocol-checker factory; ``create()`` is called here.
            handle: Unary handler invoked per complete frame.
            timeout: Idle timeout in milliseconds, copied to ``ctx.timeout``.
        """
        self.app = app
        self.checker = checker.create()
        self.handle = handle
        self.idle_timeout = timeout

        self.aio_connection = None
        self.closed = False
        self.paused = False
        # Cache once: data_received is the hot path and must not re-read
        # process-wide server config on every TCP chunk.
        self.enable_stream_executor_mode = bool(
            getattr(TrpcConfig.server_config(), "enable_stream_executor_mode", False)
        )

    cpdef void connection_made(self, transport):
        """asyncio callback: store the transport as ``aio_connection``.

        Args:
            transport: asyncio ``Transport`` for the accepted socket.
        """
        self.aio_connection = transport

    cpdef void data_received(self, bytes data):
        """asyncio callback: feed bytes, split frames, schedule handle tasks.

        Decode errors close the connection. In stream-executor mode the
        transport is hung on ``ctx`` as ``aio_transport`` so stream
        frames can write back without going through
        :func:`handle_request`.

        Args:
            data: Newly received TCP bytes, not necessarily one frame.
        """
        self.checker.feeds_data(data)

        packages = []
        while True:
            try:
                packages = self.checker.read()
            except ServerDecodeError as err:
                report_attr("trpc_executor_tcp_read_err", 1)
                logger.error(f"[executor] failed to decode in client tcp conn, err: {err}", exc_info=True)
                self.close()
                return
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_executor_checker_fatal_err", 1)
                logger.error(f"[executor] fatal err while decoding in client tcp conn, err: {err}", exc_info=True)
                self.close()
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    ctx = TrpcContext()
                    get_message(ctx)
                    set_addr_to_ctx_from_transport(ctx, self.aio_connection)
                    ctx.timeout = self.idle_timeout
                    ctx.recv_time = now_ms()
                    ctx.rpcz_data.server_post_sched_recv_msg_us = now_us()
                    ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(package))
                    if self.enable_stream_executor_mode:
                        ctx.with_meta_data("aio_transport", self.aio_connection)

                    loop = get_or_create_event_loop()
                    loop.create_task(handle_request(
                        self.handle, package, self.aio_connection, ctx,
                        self.enable_stream_executor_mode,
                    ))
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_executor_tcp_process_err", 1)
                logger.error(f"[executor] failed to process tcp package, err: {err}", exc_info=True)
                self.close()


    cpdef void connection_lost(self, ex):
        """asyncio callback: close the Protocol. ``ex`` is unused.

        Args:
            ex: Exception that caused the loss, or ``None`` on clean EOF.
        """
        self.close()

    cpdef void close(self):
        """Idempotent close: shut the transport and reset the checker.

        Safe to call from decode errors, ``connection_lost``, and
        process-error paths. ``checker.reset()`` drops leftover bytes
        so a reused factory cannot leak a half-frame.
        """
        if not self.closed:
            if self.aio_connection:
                self.aio_connection.close()
            self.checker.reset()
            self.closed = True

    def eof_received(self, *args):
        """asyncio callback: no-op; FIN is handled by ``connection_lost``.

        Args:
            *args: Unused; asyncio may pass extra positional values.
        """
        pass

    cpdef void pause_writing(self):
        """asyncio callback: mark the write buffer as past the high-water mark."""
        assert not self.paused
        self.paused = True

    cpdef void resume_writing(self):
        """asyncio callback: mark the write buffer as drained.

        Write flow-control is not wired yet; this only flips ``paused``.
        """
        # TODO Use asyncio.feature to control conn write buffer
        assert self.paused
        self.paused = False
