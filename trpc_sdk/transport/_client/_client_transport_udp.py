# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""UDP round-trip helper used by :class:`ClientTransport`.

Opens a connected datagram endpoint via
:func:`open_datagram_endpoint`, writes the request, and (unless
``req_type`` is :attr:`RequestType.SEND_ONLY`) waits for one reply.
The connection is always closed in ``finally``.
"""

from __future__ import annotations

import asyncio

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.rpcz import RPCZ_CLIENT_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_CLIENT_RSP_SIZE_CTX_KEY
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from .._constants import RequestType
from ._udp_connection import open_datagram_endpoint
from ._options import RoundTripOptions


async def udp_round_trip(ctx: TrpcContext, req_data: bytes, options: RoundTripOptions):
    """Send ``req_data`` over UDP and optionally wait for one datagram.

    The endpoint is opened connected (``remote_addr`` set), so
    ``write`` needs no address. Timeout is taken from ``ctx.timeout``
    in milliseconds, converted to seconds for ``wait_for``. A deadline
    already in the past fails before the wait.

    Args:
        ctx: Client context; timestamps and RPCz sizes are written here.
        req_data: Encoded request bytes.
        options: Must carry ``address`` as ``(host, port)`` and
            ``req_type``.

    Returns:
        Response bytes, or ``None`` for ``SEND_ONLY``.

    Raises:
        ClientTimeoutError: Deadline already passed, or the wait expired.
    """
    ip, port = options.address
    conn = await open_datagram_endpoint(ip, port)
    send_us = now_us()
    ctx.rpcz_data.client_post_sched_send_msg_us = send_us
    ctx.rpcz_data.client_pre_io_send_msg_us = send_us
    ctx.with_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, len(req_data))
    conn.write(req_data)
    ctx.rpcz_data.client_post_io_send_msg_us = now_us()

    now = now_ms()

    if (ctx.deadline > 0) and (now > ctx.deadline):
        raise ClientTimeoutError("client udp_round_trip reached deadline")

    timeout = None
    if not ctx.timeout or ctx.timeout <= 0:
        timeout = None
    elif ctx.timeout > 0:
        timeout = ctx.timeout / 1000

    get_message(ctx)
    set_addr_to_ctx_from_transport(ctx, conn.transport)

    try:
        if options.req_type == RequestType.SEND_ONLY:
            return

        rsp_data, _ = await asyncio.wait_for(conn.receive(), timeout)
        ctx.rpcz_data.client_post_sched_recv_msg_us = now_us()
        ctx.with_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, len(rsp_data))

        return rsp_data
    except asyncio.TimeoutError as e:
        raise ClientTimeoutError("client timeout to %s io error: %s" % (options.address, str(e)))
    finally:
        conn.close()
