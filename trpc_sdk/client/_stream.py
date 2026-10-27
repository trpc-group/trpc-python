# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client-side stream implementations.

``Stream`` is the public ``trpc_sdk.stream`` protocol transport.

Build instances through :func:`trpc_sdk.client.create_stream`. ``opts`` stays
``None`` until :meth:`Stream.start` injects the per-call transport,
codec and deadline.
"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import BaseClientStreamConnection
from trpc_sdk.abc import TrpcContext
from trpc_sdk.compressor import compress
from trpc_sdk.serialization import serialize
from trpc_sdk.utils import now_ms
from ._client import select_node
from ._client import set_timeout
from ._options import Options


class Stream(BaseClientStreamConnection):
    """Client stream for the public ``trpc_sdk.stream`` protocol.

    ``opts`` is filled by :meth:`start` and reused by send / recv / close
    for the rest of that call. Do not share one instance across calls.
    """

    def __init__(self):
        self.opts: Optional[Options] = None

    async def recv(self, ctx: TrpcContext) -> bytes:
        """Receive one decoded response frame.

        Args:
            ctx: Client context whose message holds the decode state.

        Returns:
            The decompressed-and-unframed body bytes from the codec.
        """
        msg = ctx.get_message()
        begin = now_ms()
        # Reset so a reused context cannot keep a previous call's deadline.
        ctx.deadline = 0
        set_timeout(ctx, msg, self.opts, begin)

        rsp = await self.opts.transport.recv(ctx, self.opts.call_options)
        rsp_body_buf = self.opts.codec.decode_data(ctx, rsp)
        return rsp_body_buf

    async def send(self, ctx: TrpcContext, req: object):
        """Serialize, compress, and send one request frame.

        ``current_serialization_type`` / ``current_compress_type`` on
        ``opts`` override the types stored on the message when they are
        ``>= 0``.

        Args:
            ctx: Client context whose message holds encode state.
            req: Business request object, or ``None`` for a control frame.
        """
        msg = ctx.get_message()

        serialization_type = msg.serialization_type
        compress_type = msg.compress_type
        if self.opts.current_serialization_type >= 0:
            serialization_type = self.opts.current_serialization_type
        if self.opts.current_compress_type >= 0:
            compress_type = self.opts.current_compress_type

        req_body_buf = serialize(serialization_type, req)
        req_body = compress(compress_type, req_body_buf)

        if req is not None and self.opts.s_control is not None:
            await self.opts.s_control.acquire_window(len(req_body))

        req_buff = self.opts.codec.encode_data(ctx, req_body)

        self.opts.transport.send(ctx, req_buff, self.opts.call_options)

    def close(self, ctx: TrpcContext):
        """Close the underlying transport for this stream.

        Args:
            ctx: Client context passed through to the transport.
        """
        self.opts.transport.close(ctx)

    async def start(self, ctx: TrpcContext, client_options: Options) -> Options:
        """Select a node, apply the deadline, and open the transport.

        Stores ``client_options`` on ``self.opts`` so later send / recv
        reuse the same transport, codec and call options. ``socket_type``
        is copied onto the transport options here because this method
        dials; unary ``invoke`` does that copy itself.

        Args:
            ctx: Client context whose message is updated with the node.
            client_options: Fully assembled options for this call.

        Returns:
            The same ``client_options``, with ``node`` cost filled in when
            the caller attached a selector node recorder.
        """
        msg = ctx.get_message()

        begin = now_ms()
        set_timeout(ctx, msg, client_options, begin)
        node = await select_node(msg, client_options)
        # Unary invoke copies this immediately before dial. This path dials
        # inside start_stream, so a unix socket_type never reaches the
        # transport unless it is copied here too.
        client_options.call_options.set_socket_type(client_options.socket_type)
        client_options.call_options.set_codec(client_options.codec)

        self.opts = client_options
        await self.opts.transport.start_stream(ctx, client_options.call_options)

        cost = now_ms() - begin
        if client_options.node:
            client_options.node.address = node.address
            client_options.node.service_name = node.service_name
            client_options.node.cost_time = cost

        return client_options
