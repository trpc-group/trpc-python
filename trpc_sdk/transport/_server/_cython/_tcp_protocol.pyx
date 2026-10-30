# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""asyncio Protocol that splits a client TCP stream and runs its frames.

``TCPConnection`` is bound to one accepted socket on the merge-worker /
multi-listener executor path. A per-connection parser splits bytes into
complete frames; each frame is scheduled as an asyncio task that runs
:func:`handle_request`. UDP on the same listen path uses
``DefaultDatagramConnection`` in :mod:`trpc_sdk.transport`, not this module.

Every connection gets a ``session_id`` that is unique in the process, taken
from one process-wide counter, and stamped on every frame's message. Stream
state is keyed by ``(session_id, stream_id)``; ``stream_id`` alone is only
unique within one connection.

Stream frames always answer through the connection's ``ConnWriter``, hung on
the frame's context as ``stream_writer``: a stream handler may still write
after the client left, and the writer drops those writes once the connection
is closed. When the connection closes with streams on it,
``StreamDispatcher.abort_session`` is posted to every loop those streams run
on.

Under ``io_handle_type: separate`` the connection is given a
``SharedHandlerPool``: its loop is an I/O loop that only frames requests,
each unary frame goes to a handle thread as
:func:`handle_request_via_writer`, and the response comes back through the
connection's ``ConnWriter``. Stream frames are pinned to the handle thread
``(session_id + stream_id) % handle_thread_num``, so every frame of one
stream is handled, in order, on one loop.
"""

import itertools
from functools import partial

from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from .._conn_writer import ConnWriter
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import submit_server_span_on_executor_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import get_or_create_event_loop
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import set_addr_to_ctx_from_transport

# Process-wide: TrpcMessage.session_id is a 32-bit signed int, and 0 means
# "not assigned" (UDP). itertools.count.__next__ is a single C call, so I/O
# threads taking ids concurrently never get the same one under the GIL.
_session_ids = itertools.count(1)


cpdef int next_session_id():
    """Return the next connection id: 31 bits, never 0.

    Wraps after 2**31 connections; two live connections can only share an id
    if one outlives 2**31 newer ones.
    """
    cdef long long value = next(_session_ids) & 0x7FFFFFFF
    if value == 0:
        value = next(_session_ids) & 0x7FFFFFFF
    return <int>value


async def handle_request(handle, req_buf, conn, ctx):
    """Run one handler and write its response on the same TCP transport.

    This is the body of the per-frame asyncio task created in
    :meth:`TCPConnection.data_received`. ``conn`` is the asyncio
    ``Transport`` stored as ``aio_connection``. Write-side rpcz stamps
    (``server_pre/post_sched_send_msg_us``, ``server_pre_io_send_msg_us``)
    share one ``now_us()`` because merge / multi-listener mode runs
    sched_send and io_send in the same process. Never raises: failures
    are counted with ``report_attr`` and logged.

    Args:
        handle: Service entry, typically ``ServiceAdapter.handle``.
        req_buf: One complete request frame.
        conn: asyncio ``Transport`` for this client socket.
        ctx: Per-request ``TrpcContext``.
    """
    try:
        # business
        ret = await handle(ctx, req_buf)
        # Stream frames answer through their stream_writer; handle() returns None.
        if not ret:
            return

        if not conn.is_closing():
            # In multi-listener mode, sched_send and io_send are in the same process.
            send_us = now_us()
            ctx.rpcz_data.server_pre_sched_send_msg_us = send_us
            ctx.rpcz_data.server_post_sched_send_msg_us = send_us
            ctx.rpcz_data.server_pre_io_send_msg_us = send_us
            ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(ret))
            conn.write(ret)
            ctx.rpcz_data.server_post_io_send_msg_us = now_us()
            submit_server_span_on_executor_post_io(ctx)
            logger.debug("[executor] rpcz submit on executor post-io completed")
    except Exception as err:  # pylint: disable=broad-except
        report_attr("trpc_executor_tcp_write_err", 1)
        logger.error(f"[executor] failed to send rsp to tcp client, err: {err}", exc_info=True)


async def handle_request_via_writer(handle, req_buf, writer, ctx):
    """Run one handler on a handle thread and answer through ``writer``.

    The ``io_handle_type: separate`` counterpart of :func:`handle_request`.
    This coroutine runs on a handle thread, which must not touch the
    transport, so the response goes to the connection's ``ConnWriter``. The
    sched_send stamps are taken here; the io_send ones are taken by the
    writer once it writes on the I/O loop. Never raises.

    Args:
        handle: Service entry, typically ``ServiceAdapter.handle``.
        req_buf: One complete request frame.
        writer: ``ConnWriter`` of the connection the frame came from.
        ctx: Per-request ``TrpcContext``; handed to the writer with the
            response and not touched afterwards.
    """
    try:
        ret = await handle(ctx, req_buf)
        if not ret:
            return
        send_us = now_us()
        ctx.rpcz_data.server_pre_sched_send_msg_us = send_us
        ctx.rpcz_data.server_post_sched_send_msg_us = send_us
        writer.write(ret, ctx)
    except Exception as err:  # pylint: disable=broad-except
        report_attr("trpc_executor_tcp_write_err", 1)
        logger.error(f"[executor] failed to answer tcp request on handle thread, err: {err}", exc_info=True)


cdef inline bint _is_trpc_stream_frame(bytes package):
    """Whether ``package`` is a tRPC stream frame (magic 0x0930, data_frame_type 1)."""
    return len(package) > 2 and package[0] == 0x09 and package[1] == 0x30 and package[2] != 0


cdef inline unsigned int _stream_id_of(bytes package):
    """Big-endian ``stream_id`` at bytes 10-13 of the frame head; 0 if the head is short."""
    if len(package) < 14:
        return 0
    return ((<unsigned int>package[10] << 24) | (<unsigned int>package[11] << 16)
            | (<unsigned int>package[12] << 8) | <unsigned int>package[13])


cdef object _stream_dispatcher_of(object handle):
    """``StreamDispatcher`` of the service behind ``handle``, or ``None``.

    ``handle`` is normally the bound ``ServiceAdapter.handle``; anything else (a
    plain function in tests) has no dispatcher, and its streams are then not
    aborted on close.
    """
    opt = getattr(getattr(handle, "__self__", None), "opt", None)
    return getattr(opt, "stream_handle", None)


cdef class TCPConnection:
    """asyncio Protocol for one accepted client TCP connection.

    Constructed as the ``protocol_factory`` on a listen socket, as
    ``partial(TCPConnection, option)``. ``option.parser`` is a factory;
    :meth:`__init__` calls ``parser.create()`` so each connection has its own
    splitter. ``option.rpc_handler`` is the service entry, for unary and
    stream frames alike.
    """

    def __init__(self, option):
        """Copy the listener's options into this connection's C-level slots.

        ``option`` is read by attribute name only, so this module does not
        import :mod:`trpc_sdk.transport`. That package's ``__init__`` loads
        this module; a back-import would run while it is still loading.
        The option is not kept: the hot path reads the copied slots.

        Args:
            option: A :class:`~trpc_sdk.transport.ConnectionOption`.
                ``parser.create()`` is called here; ``rpc_handler`` runs per
                complete frame; ``timeout`` (ms) is copied to ``ctx.timeout``;
                ``shared_pool``, when set, makes this loop only frame
                requests and write responses while handlers run on the
                handle threads.
        """
        self.parser = option.parser.create()
        self.handle = option.rpc_handler
        self.idle_timeout = option.timeout
        self.shared_pool = option.shared_pool
        self.writer = None

        self.aio_connection = None
        self.closed = False
        self.paused = False
        self.session_id = 0
        self.has_streams = False
        self.stream_dispatcher = None
        self.stream_loops = None

    cpdef void connection_made(self, transport):
        """asyncio callback: store the transport and take a ``session_id``.

        Args:
            transport: asyncio ``Transport`` for the accepted socket.
        """
        self.aio_connection = transport
        self.session_id = next_session_id()
        if self.shared_pool is not None:
            self.writer = ConnWriter(get_or_create_event_loop(), transport)

    cpdef void data_received(self, bytes data):
        """asyncio callback: feed bytes, split frames, schedule handle tasks.

        Decode errors close the connection. Stream frames carry the
        connection's ``ConnWriter`` on ``ctx`` as ``stream_writer``.

        Args:
            data: Newly received TCP bytes, not necessarily one frame.
        """
        self.parser.feed_data(data)

        packages = []
        while True:
            try:
                packages = self.parser.read()
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
                    get_message(ctx).set_session_id(self.session_id)
                    set_addr_to_ctx_from_transport(ctx, self.aio_connection)
                    ctx.timeout = self.idle_timeout
                    ctx.recv_time = now_ms()
                    ctx.rpcz_data.server_post_sched_recv_msg_us = now_us()
                    ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(package))
                    if _is_trpc_stream_frame(package):
                        self._dispatch_stream(package, ctx)
                        continue
                    if self.shared_pool is not None:
                        self.shared_pool.submit(partial(handle_request_via_writer, self.handle, package,
                                                        self.writer, ctx))
                        continue

                    loop = get_or_create_event_loop()
                    loop.create_task(handle_request(self.handle, package, self.aio_connection, ctx))
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_executor_tcp_process_err", 1)
                logger.error(f"[executor] failed to process tcp package, err: {err}", exc_info=True)
                self.close()

    cdef object _stream_writer(self):
        """Return the connection's ``ConnWriter``, creating it on the first stream frame.

        Under ``shared_pool`` it already exists since ``connection_made``.
        Otherwise unary responses are written on the transport directly, and
        only stream frames need the writer, so connections that never stream
        do not pay for it.
        """
        if self.writer is None:
            self.writer = ConnWriter(get_or_create_event_loop(), self.aio_connection)
        return self.writer

    cdef void _dispatch_stream(self, bytes package, object ctx) except *:
        """Schedule one stream frame, on this loop or on its pinned handle thread.

        Stream frames bypass the shared handler pool's limit: dropping or delaying a
        FEEDBACK or CLOSE frame would stall or leak the stream.

        Args:
            package: One complete stream frame.
            ctx: Frame context, already stamped with this ``session_id``.
        """
        cdef int target
        writer = self._stream_writer()
        ctx.with_meta_data(STREAM_WRITER_CTX_KEY, writer)
        if not self.has_streams:
            self.has_streams = True
            self.stream_dispatcher = _stream_dispatcher_of(self.handle)
        if self.shared_pool is not None:
            target = self.shared_pool.stream_target(self.session_id, _stream_id_of(package))
            if self.stream_loops is None:
                self.stream_loops = set()
            self.stream_loops.add(target)
            self.shared_pool.submit_stream(target, partial(handle_request_via_writer, self.handle, package,
                                                           writer, ctx))
            return
        get_or_create_event_loop().create_task(handle_request(self.handle, package, self.aio_connection, ctx))

    cdef void _abort_streams(self):
        """Post ``abort_session`` to every loop this connection's streams run on.

        Called from :meth:`close` after the writer is marked closed, which is
        what lets ``StreamDispatcher.handle_init`` refuse an INIT that runs
        after this abort. Even on this loop it is posted rather than called,
        so frame tasks created earlier run first.
        """
        dispatcher = self.stream_dispatcher
        if dispatcher is None:
            return
        if self.shared_pool is None:
            try:
                self.writer.loop.call_soon(dispatcher.abort_session, self.session_id)
            except RuntimeError:
                report_attr("trpc_stream_abort_drop", 1)
            return
        for target in self.stream_loops or ():
            self.shared_pool.post(target, dispatcher.abort_session, self.session_id)

    cpdef void connection_lost(self, ex):
        """asyncio callback: close the Protocol. ``ex`` is unused.

        Args:
            ex: Exception that caused the loss, or ``None`` on clean EOF.
        """
        self.close()

    cpdef void close(self):
        """Idempotent close: end the streams, shut the transport, reset the parser.

        Safe to call from decode errors, ``connection_lost``, and
        process-error paths. The writer is marked closed *before* the stream
        abort is posted. ``parser.reset()`` drops leftover bytes so a reused
        factory cannot leak a half-frame.
        """
        if not self.closed:
            if self.writer is not None:
                self.writer.close()
            if self.has_streams:
                self._abort_streams()
            if self.aio_connection:
                self.aio_connection.close()
            self.parser.reset()
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
        assert self.paused
        self.paused = False
