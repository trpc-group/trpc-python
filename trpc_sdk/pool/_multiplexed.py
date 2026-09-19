# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Default TCP multiplexed pool for unary client calls.

One real TCP connection carries many in-flight RPCs. Each request is a
:class:`VirtualConnection` keyed by ``request_id``. A single
:meth:`MultiplexedConnection.recv_msg` loop reads frames and completes
the matching future. Callers never share a socket; they only hold the
virtual connection.

:data:`default_multiplexed_pool` is the process-wide instance that
:mod:`trpc_sdk.client._client` attaches when multiplexed mode is on.
"""

from __future__ import annotations

import asyncio
import socket
import threading
from asyncio import Future
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import TrpcMessage
from trpc_sdk.codec import ClientCodec
from trpc_sdk.codec import TrpcClientParser
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.common import wait_for_by_loop
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.log import logger
from trpc_sdk.utils import is_ipv6
from ._constants import ConnectionState
from ._tcp_connection import open_connection


class MultiplexedOptions:
    """Tuning knobs for one :class:`Multiplexed` pool.

    Attributes:
        connect_number: Real TCP connections kept per address.
        queue_size: Max in-flight virtual connections on one real
            connection. :meth:`MultiplexedConnection.new` returns
            ``None`` when this cap is hit.
        drop_full: Reserved. Current code never reads this flag; a
            full queue always fails the request.
        dial_timeout: Seconds to wait for TCP connect.
    """

    def __init__(self, connect_number: int, queue_size: int, drop_full: bool, dial_timeout: float = 3):
        self.connect_number = connect_number
        self.queue_size = queue_size
        self.drop_full = drop_full
        self.dial_timeout = dial_timeout


class VirtualConnection:
    """One in-flight request riding on a real multiplexed connection.

    ``future`` is created in :meth:`write` and completed by
    :meth:`MultiplexedConnection.send_rsp` on a matching response, or by
    :meth:`MultiplexedConnection.close` when the real connection dies.
    :meth:`read` then copies response heads onto the caller's
    :class:`~trpc_sdk.abc.TrpcMessage` via ``codec.update_msg_by_msg``.
    """

    def __init__(self, request_id: int, conn: MultiplexedConnection, msg: TrpcMessage):
        self.request_id = request_id
        self.conn = conn
        self.msg = msg
        self.future: Optional[Future] = None

    async def read(self):
        """Wait for the matching response and apply it to ``self.msg``.

        Returns:
            The decoded response body.

        Raises:
            Exception: The error stored on the future when the real
                connection failed or the peer returned an error.
        """
        origin_msg, rsp_body, err = await self.future
        if err is not None:
            raise (err)
        else:
            self.conn.codec.update_msg_by_msg(origin_msg, self.msg)
            return rsp_body

    def write(self, b: bytes) -> int:
        """Arm the response future and write the request bytes.

        Args:
            b: Encoded request frame.

        Returns:
            Bytes accepted by the underlying writer.
        """
        self.future = Future()

        try:
            return self.conn.write(b)
        except Exception as err:  # pylint: disable=broad-except
            # Whether the real connection should be closed is left to
            # the caller; this path only re-raises the write error.
            raise (err)

    def close(self):
        """Drop this virtual connection from the real connection's map."""
        self.conn.pop_vconn(self.request_id)


class MultiplexedConnection:
    """One real TCP connection that multiplexes many virtual connections.

    :meth:`close` broadcasts ``err`` to every in-flight future so callers
    waiting on :meth:`VirtualConnection.read` fail together.
    :meth:`check_remote_error` probes the wrapped
    :class:`~trpc_sdk.pool.Connection` via ``at_eof`` / ``is_closing``
    / ``exception``. A failed :meth:`recv_msg` read fails the whole
    connection, not just one request.
    """

    def __init__(self, address: Tuple[str, int], connections: ConnectionGroup, checker: TrpcClientParser,
                 codec: ClientCodec, opts: MultiplexedOptions):
        self.address = address
        self.state = ConnectionState.INITIAL
        self.connections = connections
        self.conn = None
        self.future_dict: Dict[int, VirtualConnection] = {}
        self.checker = checker
        self.codec = codec
        self.opts = opts
        self.lock = asyncio.Lock()

    async def dial(self, dial_timeout: float, loop):
        """Open the TCP socket and store it on ``self.conn``.

        Args:
            dial_timeout: Seconds to wait for connect.
            loop: Event loop; ``None`` falls back to the thread-local
                loop from :func:`get_or_create_event_loop`.
        """
        host, port = self.address
        loop = loop or get_or_create_event_loop()

        if is_ipv6(host):
            co_conn = open_connection(host=host, port=port, loop=loop, family=socket.AF_INET6)
        else:
            co_conn = open_connection(host=host, port=port, loop=loop)
        self.conn = await wait_for_by_loop(co_conn, dial_timeout, loop=loop)

    async def start(self) -> bool:
        """Dial, spawn :meth:`recv_msg`, and mark the connection connected.

        Returns:
            Always ``True`` on success. Dial failures propagate.
        """
        async with self.lock:
            await self.dial(self.opts.dial_timeout, None)

        get_or_create_event_loop().create_task(self.recv_msg(self.checker, self.codec))
        self.state = ConnectionState.CONNECTED
        return True

    def close(self, err: Exception = None):
        """Fail every in-flight virtual connection, then mark closed.

        ``clear_invalid_conn`` runs twice: once while ``CLOSING`` so the
        group can drop this entry before futures are completed, and
        again after ``CLOSED``.
        """
        self.state = ConnectionState.CLOSING
        self.connections.clear_invalid_conn()
        for vconn in self.future_dict.values():
            vconn.future.done()
            vconn.future.set_result((None, None, err))

        self.state = ConnectionState.CLOSED
        self.connections.clear_invalid_conn()

    def check_remote_error(self) -> bool:
        """Return True if the peer has closed or the transport has an error."""
        conn = self.conn
        return conn.at_eof() or conn.is_closing() or conn.exception()

    async def send_rsp(self, msg, request_id, rsp_body, err: Exception = None):
        """Complete the virtual connection matching ``request_id``.

        Missing ids are ignored: a late frame after the caller already
        popped the virtual connection is a no-op.
        """
        if request_id not in self.future_dict:
            return

        vconn = self.future_dict[request_id]
        vconn.future.done()
        vconn.future.set_result((msg, rsp_body, err))
        self.future_dict.pop(request_id)

    async def read(self, n: int) -> bytes:
        """Read exactly ``n`` bytes from the real connection.

        Args:
            n: Byte count expected by the frame checker.
        """
        data = await self.conn.readexactly(n)
        return data

    def write(self, b: bytes) -> int:
        """Write ``b`` on the real connection.

        Args:
            b: Encoded frame bytes.
        """
        return self.conn.write(b)

    async def recv_msg(self, checker: TrpcClientParser, codec: ClientCodec):
        """Read frames until the connection dies and dispatch by request id.

        A checker / codec failure closes the whole connection so every
        in-flight request fails together. A later ``send_rsp`` error
        only drops that one id.
        """
        while True:
            if self.check_remote_error():
                self.close(ClientPoolConnClosedError("ErrConnClosed"))
                return

            message = TrpcMessage()
            request_id = 0
            rsp_body = b''
            try:
                rsp_buffer = await checker.read_frame(self)
                rsp_body = codec.decode(message, rsp_buffer)
                request_id = message.client_rsp_head.request_id
            except Exception as err:  # pylint: disable=broad-except
                # Frame read failed: fail every request on this socket.
                self.close(err)

            try:
                await self.send_rsp(message, request_id, rsp_body)
            except Exception as err:  # pylint: disable=broad-except
                self.future_dict.pop(request_id)
                logger.error(f"recv_msg err {err}")

    def new(self, request_id: int, msg: TrpcMessage) -> Optional[VirtualConnection]:
        """Register a virtual connection, or return ``None`` if the queue is full.

        Args:
            request_id: Client request id used to match the response.
            msg: Caller's message; response heads are copied onto it
                in :meth:`VirtualConnection.read`.
        """
        if len(self.future_dict) >= self.opts.queue_size:
            return None

        vconn = VirtualConnection(request_id=request_id, conn=self, msg=msg)
        self.future_dict[request_id] = vconn
        return vconn

    def pop_vconn(self, request_id: int):
        """Remove the virtual connection for ``request_id`` if present."""
        if request_id in self.future_dict:
            self.future_dict.pop(request_id)


class ConnectionGroup:
    """All real multiplexed connections for one ``(host, port)``.

    ``cs`` is the live list of :class:`MultiplexedConnection` objects.
    :meth:`get_connection` round-robins for a ``CONNECTED`` socket;
    otherwise it waits on an ``INITIAL`` connection's dial lock.
    """

    def __init__(self, address: Tuple[str, int], multiplexed: Multiplexed, checker: TrpcClientParser,
                 codec: ClientCodec):
        self.cs: List[MultiplexedConnection] = []
        self.number = 0
        self.multiplexed = multiplexed
        self.address = address
        self.checker = checker
        self.codec = codec

        for _ in range(0, self.multiplexed.opts.connect_number):
            conn = MultiplexedConnection(address=address,
                                         connections=self,
                                         opts=multiplexed.opts,
                                         checker=checker,
                                         codec=codec)
            self.cs.append(conn)

    async def start_connections(self):
        """Dial every pre-allocated connection in ``cs``.

        A failure marks that connection ``CLOSED``, drops invalid
        entries, and re-raises so the caller can abort the group.
        """
        for conn in self.cs:
            try:
                await conn.start()
            except Exception as e:
                conn.state = ConnectionState.CLOSED
                self.clear_invalid_conn()
                raise e
        return

    def clear_invalid_conn(self):
        """Drop ``CLOSING`` / ``CLOSED`` entries from ``cs``."""
        for conn in self.cs:
            if conn.state == ConnectionState.CLOSED or conn.state == ConnectionState.CLOSING:
                self.cs.remove(conn)

    async def get_connection(self) -> Optional[MultiplexedConnection]:
        """Return a usable real connection, creating one if under quota.

        Prefers a ``CONNECTED`` socket via round-robin on ``self.number``.
        If none is ready, waits on the first ``INITIAL`` connection's
        lock until dial finishes (or that socket is already ``CLOSED``).
        """
        if len(self.cs) < self.multiplexed.opts.connect_number:
            conn = MultiplexedConnection(address=self.address,
                                         connections=self,
                                         opts=self.multiplexed.opts,
                                         checker=self.checker,
                                         codec=self.codec)
            self.cs.append(conn)
            try:
                await conn.start()
            except Exception as e:
                conn.state = ConnectionState.CLOSED
                self.clear_invalid_conn()
                raise e
            return conn

        number_connection = len(self.cs)
        initial_conn = None
        for _ in range(0, number_connection):
            self.number = self.number + 1
            current = self.number % number_connection
            connection = self.cs[current]
            if connection.state == ConnectionState.CONNECTED:
                return connection
            if initial_conn is None and connection.state == ConnectionState.INITIAL:
                initial_conn = connection

        # Block on the INITIAL connection's dial lock until start()
        # finishes, then reuse that socket (or give up if it closed).
        async with initial_conn.lock:
            if initial_conn.state == ConnectionState.CLOSED:
                return None
            return initial_conn
        return None


class Multiplexed(threading.local):
    """Per-thread multiplexed pool keyed by address.

    ``threading.local`` keeps ``connections`` and ``locks`` isolated per
    thread so event loops do not share sockets. ``locks`` is a
    per-address asyncio lock that serializes first-time group creation
    and prevents two concurrent callers from dialing the same address
    twice.
    """

    def __init__(self, opts: MultiplexedOptions):
        self.connections = {}
        self.opts = opts
        self.locks = {}

    async def get_conn(self, network: str, address: Tuple[str, int], request_id: int, msg: TrpcMessage,
                       checker: TrpcClientParser, codec: ClientCodec, timeout: float) -> VirtualConnection:
        """Return a virtual connection for one unary request.

        Creates the per-address :class:`ConnectionGroup` under that
        address's lock on first use.

        Args:
            network: Unused; kept for the historical transport signature.
            address: ``(host, port)`` of the peer.
            request_id: Client request id used to match the response.
            msg: Caller's request message.
            checker: Frame parser used by :meth:`MultiplexedConnection.recv_msg`.
            codec: Client codec used to decode responses.
            timeout: Seconds to wait for the initial group dial.

        Returns:
            A :class:`VirtualConnection` ready for :meth:`VirtualConnection.write`.

        Raises:
            ClientInvokeError: No real connection could be obtained.
            Exception: The per-connection in-flight queue is full.
        """
        conns = self.connections.get(address)
        if not conns:
            lock = self.locks.get(address)
            if not lock:
                lock = asyncio.Lock()
                self.locks[address] = lock

            async with lock:
                conns = self.connections.get(address)
                if not conns:
                    conns = ConnectionGroup(address=address, multiplexed=self, checker=checker, codec=codec)
                    await asyncio.wait_for(conns.start_connections(), timeout)
                    self.connections[address] = conns
        conn = await conns.get_connection()
        if conn is None:
            raise ClientInvokeError("get conn failed {}".format(address))
        vconn = conn.new(request_id=request_id, msg=msg)
        if vconn is None:
            raise Exception("connection queue_size full")
        return vconn


def create_multiplexed() -> Multiplexed:
    """Build a pool with the historical defaults.

    Defaults: 2 real connections per address, 100000 in-flight requests
    per connection, 3s dial timeout. ``drop_full`` is ``False`` and is
    not consumed.

    Returns:
        A new :class:`Multiplexed` instance (one per calling thread
        after first attribute access, because it is ``threading.local``).
    """
    opts = MultiplexedOptions(connect_number=2, queue_size=100000, drop_full=False, dial_timeout=3.0)
    return Multiplexed(opts=opts)


# Process-wide default attached by trpc_sdk.client._client when multiplexed
# mode is enabled and the caller did not inject a custom pool.
default_multiplexed_pool = create_multiplexed()
