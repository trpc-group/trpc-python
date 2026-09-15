# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Default client transport plugin and its task-stream subclass.

Importing this module registers :class:`ClientTransport` as
``PluginType.CLIENT_TRANSPORT`` / ``"trpc"``. The package ``__init__``
imports it eagerly so the slot is filled as soon as
``trpc_sdk.transport`` loads.

Unary calls go through :meth:`ClientTransport.round_trip`, which picks
TCP (one-shot or multiplexed) or UDP from :class:`RoundTripOptions`.
Streaming calls never use ``round_trip``: :meth:`ClientTransport.start`
takes a pooled connection, stores a :class:`ClientConn` under the
message ``stream_id``, and later :meth:`send` / :meth:`recv` /
:meth:`close` look that conn up.

The TCP / UDP helpers live in ``_client_transport_tcp`` and
``_client_transport_udp``. :class:`ClientConn` lives in
``_client_transport_stream``.
"""

from __future__ import annotations

from typing import Dict

from trpc_sdk.abc import BaseClientStreamTransport
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin
from trpc_sdk.pool import get_pool_connection
from ._client_transport_stream import ClientConn as ClientStreamConn
from ._client_transport_tcp import get_remain_time
from ._client_transport_tcp import tcp_multiplexed
from ._client_transport_tcp import tcp_round_trip
from ._client_transport_udp import udp_round_trip
from ._options import RoundTripOptions


@register_plugin(PluginType.CLIENT_TRANSPORT, "trpc", init_pos=PluginInitPos.MASTER)
class ClientTransport(BaseClientStreamTransport):
    """Default tRPC client transport (plugin name ``"trpc"``).

    Unary and stream paths share this class but not the same state.
    ``round_trip`` never reads :attr:`stream_id_to_conn`. The stream
    methods only talk to the :class:`ClientConn` stored there.

    Attributes:
        stream_id_to_conn: Live stream connections keyed by the tRPC
            stream id on the client message. Filled by :meth:`start`,
            removed by :meth:`close`.
    """

    def __init__(self):
        """Create an empty stream-id map.

        Unary ``round_trip`` does not use this map.
        """
        self.stream_id_to_conn: Dict[int, ClientStreamConn] = {}

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options: RoundTripOptions):
        """Send ``req_data`` and optionally wait for one response.

        Branch is ``options.network`` then ``options.enable_multiplexed``:

        - ``tcp`` + multiplexed: :func:`tcp_multiplexed` (virtual conn;
          the real TCP stays open for other requests)
        - ``tcp`` otherwise: :func:`tcp_round_trip` (pooled one-shot;
          ``close()`` in ``finally`` returns the conn to the pool)
        - ``udp``: :func:`udp_round_trip`

        Args:
            ctx: Client context; timestamps and RPCz sizes are written
                inside the TCP / UDP helpers.
            req_data: Encoded request bytes.
            options: Address, network, pool, and multiplexed knobs.

        Returns:
            Response frame bytes, or empty bytes for ``SendOnly``.

        Raises:
            ValueError: ``options.network`` is neither ``tcp`` nor ``udp``.
            ClientTimeoutError: Deadline expired on connect or read.
            ClientInvokeError: Any other I/O failure.
        """
        if options.network == 'tcp':
            if options.enable_multiplexed:
                # Multiplexed path: one TCP, many in-flight request ids.
                return await tcp_multiplexed(ctx, req_data, options)
            return await tcp_round_trip(ctx, req_data, options)
        if options.network == 'udp':
            return await udp_round_trip(ctx, req_data, options)
        raise ValueError(f"error network type {options.network}")

    def send(self, ctx: TrpcContext, req_data: bytes, _):
        """Write one stream frame on the conn stored by :meth:`start`.

        Lookup is by ``ctx``'s client-message ``stream_id``, not by
        ``options``. The ABC still passes options; this method ignores
        them.

        Args:
            ctx: Stream context whose message carries ``stream_id``.
            req_data: Encoded stream frame.
            _: Unused ``RoundTripOptions`` required by the ABC.

        Raises:
            ServerSystemError: No conn for this ``stream_id`` (already
                closed, or :meth:`start` was never called).
        """
        msg = ctx.get_client_message()
        stream_id = msg.stream_id
        if stream_id not in self.stream_id_to_conn:
            raise ServerSystemError("Connection is Closed")

        client_conn: ClientStreamConn = self.stream_id_to_conn[stream_id]
        client_conn.write(req_data)

    async def recv(self, ctx: TrpcContext, _: RoundTripOptions) -> bytes:
        """Read one stream frame from the conn stored by :meth:`start`.

        Same ``stream_id`` lookup as :meth:`send`. Deadline comes from
        ``ctx``, not from ``options``.

        Args:
            ctx: Stream context whose message carries ``stream_id``.
            _: Unused ``RoundTripOptions`` required by the ABC.

        Returns:
            One complete response frame.

        Raises:
            ServerSystemError: No conn for this ``stream_id``.
            ClientTimeoutError: Deadline expired while waiting.
            ClientInvokeError: Any other read failure.
        """
        msg = ctx.get_client_message()
        stream_id = msg.stream_id
        if stream_id not in self.stream_id_to_conn:
            raise ServerSystemError("Connection is Closed")

        client_conn: ClientStreamConn = self.stream_id_to_conn[stream_id]
        rsp_body = await client_conn.recv_frame(ctx)
        return rsp_body

    def close(self, ctx: TrpcContext):
        """Close the stream conn for ``ctx`` and drop it from the map.

        ``ClientConn.close`` is called with :attr:`ClientConn.need_close`.
        That flag is False when ``options.is_task_stream``: the task
        process still owns the pooled connection, so ending the stream
        must not return it to the pool. Missing ``stream_id`` is a no-op.

        Args:
            ctx: Stream context whose message carries ``stream_id``.
        """
        msg = ctx.get_client_message()
        stream_id = msg.stream_id
        if stream_id in self.stream_id_to_conn:
            client_conn: ClientStreamConn = self.stream_id_to_conn[stream_id]
            client_conn.close(client_conn.need_close)
            self.stream_id_to_conn.pop(stream_id)

    async def start(self, ctx: TrpcContext, options: RoundTripOptions):
        """Take a pooled TCP connection and bind it to this stream id.

        Forces ``options.short_conn = False`` so the pool keeps the
        socket for later :meth:`send` / :meth:`recv`. The conn is stored
        under ``ctx``'s client-message ``stream_id``.

        Args:
            ctx: Stream context whose message already has ``stream_id``.
            options: Dial / pool knobs; ``short_conn`` is overwritten.
        """
        # Stream send/recv need the same TCP; a short conn would be
        # dropped after the first write.
        options.short_conn = False
        conn = await get_pool_connection(options, get_remain_time(ctx.deadline))

        msg = ctx.get_client_message()
        client_conn = ClientStreamConn(conn=conn, options=options)
        self.stream_id_to_conn[msg.stream_id] = client_conn


class ClientTaskStreamTransport(ClientTransport):
    """Client transport for the inner worker-to-task stream.

    Framework-only: :mod:`trpc_sdk.client` installs this when the caller
    did not supply a transport for a task stream. Send / recv / start /
    close are inherited; unary :meth:`round_trip` is not a legal
    operation on this path.
    """

    async def round_trip(self, ctx, req_data: bytes, options: RoundTripOptions):
        """Reject unary round trips; task streams use start/send/recv.

        Args:
            ctx: Unused; present to match the ABC.
            req_data: Unused.
            options: Unused.

        Raises:
            RuntimeError: Always. Call :meth:`start` instead.
        """
        raise RuntimeError("task stream client transport not support 'round_trip'")


def get_client_transport(name: str):
    """Return the client transport registered under ``name``.

    Args:
        name: Plugin name, usually the YAML protocol such as ``"trpc"``
            or ``"http"``.

    Returns:
        The registered :class:`~trpc_sdk.abc.BaseClientStreamTransport`,
        or ``None`` if nothing is registered under ``name``.
    """
    return get_plugin(PluginType.CLIENT_TRANSPORT, name)


def get_default_client_transport():
    """Return the default tRPC client transport (plugin name ``"trpc"``).

    Returns:
        The :class:`ClientTransport` instance registered on import of
        this module.
    """
    return get_plugin(PluginType.CLIENT_TRANSPORT, 'trpc')
