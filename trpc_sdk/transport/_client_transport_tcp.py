# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""TCP round-trip helpers used by :class:`ClientTransport`.

Two paths, picked by ``options.enable_multiplexed``:

- :func:`tcp_round_trip` takes a pooled connection, writes, optionally
  reads one frame, then ``close()``s the pool conn in ``finally`` so it
  is returned to the pool (or dropped on error, where ``close(True)``
  already ran in the except branch).
- :func:`tcp_multiplexed` takes a :class:`VirtualConnection`. Its
  ``close()`` only unregisters this request from the real connection's
  ``future_dict``; the underlying TCP stays open for other requests.
"""

from __future__ import annotations

import asyncio

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.log import logger
from trpc_sdk.pool import get_pool_connection
from trpc_sdk.rpcz import RPCZ_CLIENT_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_CLIENT_RSP_SIZE_CTX_KEY
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from ._constants import RequestType
from ._options import RoundTripOptions


def get_remain_time(deadline: int):
    """Seconds left until ``deadline``, or None when there is no deadline.

    Args:
        deadline: Absolute deadline in milliseconds since epoch. ``0``
            means "no deadline".

    Returns:
        Remaining seconds as a float, or ``None`` so ``wait_for`` waits
        forever.
    """
    if deadline == 0:
        return None
    return (deadline - now_ms()) / 1000.0


async def tcp_round_trip(ctx: TrpcContext, req_data: bytes, options: RoundTripOptions) -> bytes:
    """Send ``req_data`` on a pooled TCP connection and optionally read a reply.

    ``finally`` always calls ``conn.close()`` (no force). That returns a
    healthy connection to the pool. Timeout / invoke errors force-close
    first so the pool drops the broken socket; the subsequent
    ``close()`` is then a no-op on an already-closed pool conn.

    Args:
        ctx: Client context; timestamps and RPCz sizes are written here.
        req_data: Encoded request bytes.
        options: Address, pool, frame parser, and ``req_type``.

    Returns:
        Response frame bytes, or empty bytes for ``SendOnly``.

    Raises:
        ClientTimeoutError: Deadline expired on connect or read.
        ClientInvokeError: Any other I/O failure.
    """
    conn, rsp = None, b''
    try:
        logger.debug("%s|try get conn for", options.endpoint)
        conn = await get_pool_connection(options, get_remain_time(ctx.deadline))
        logger.debug("%s|get conn=%s", options.endpoint, conn)

        get_message(ctx)
        set_addr_to_ctx_from_transport(ctx, conn.stream_conn.transport)
        send_us = now_us()
        ctx.rpcz_data.client_post_sched_send_msg_us = send_us
        ctx.rpcz_data.client_pre_io_send_msg_us = send_us
        ctx.with_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, len(req_data))
        conn.write(req_data)
        ctx.rpcz_data.client_post_io_send_msg_us = now_us()
        logger.debug("%s|write data len=%d", options.endpoint, len(req_data))

        if options.req_type != RequestType.SendOnly:
            rsp = await asyncio.wait_for(options.checker.read_frame(conn), get_remain_time(ctx.deadline))
            ctx.rpcz_data.client_post_sched_recv_msg_us = now_us()
            ctx.with_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, len(rsp))
    except asyncio.TimeoutError as err:
        if conn:
            conn.close(True)
            logger.debug("client tcp transport round trip close connection")

        raise ClientTimeoutError(
            f"client tcp_round_trip reached deadline to {options.address}, {err} io error ") from err

    except Exception as err:  # pylint: disable=broad-except
        if conn:
            conn.close(True)
        raise ClientInvokeError(f"client tcp_round_trip invoke {options.address}, error {err}") from err
    finally:
        if conn:
            conn.close()

    return rsp


async def tcp_multiplexed(ctx: TrpcContext, req_data: bytes, options: RoundTripOptions) -> bytes:
    """Send ``req_data`` on a multiplexed virtual connection.

    ``conn`` here is a :class:`VirtualConnection`. ``close()`` only
    unregisters this request from the real connection's ``future_dict``;
    the underlying TCP stays pooled. ``SendOnly`` never gets a
    response, so ``send_rsp`` never pops the vconn — ``finally`` is what
    unregisters it. On the success path ``send_rsp`` already popped, and
    ``pop_vconn`` is idempotent.

    Args:
        ctx: Client context; timestamps and RPCz sizes are written here.
        req_data: Encoded request bytes.
        options: Must carry ``multiplexed``, ``msg``, ``checker``, and
            ``codec``.

    Returns:
        Response body bytes, or the placeholder ``b'hello: 0'`` for
        ``SendOnly``.

    Raises:
        ClientTimeoutError: Deadline expired on get-conn or read.
        ClientInvokeError: Any other I/O failure.
    """
    conn, rsp = None, b'hello: 0'
    try:
        conn = await options.multiplexed.get_conn(options.network, options.address, options.msg.request_id, options.msg,
                                                  options.checker, options.codec, get_remain_time(ctx.deadline))
        send_us = now_us()
        ctx.rpcz_data.client_post_sched_send_msg_us = send_us
        ctx.rpcz_data.client_pre_io_send_msg_us = send_us
        ctx.with_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, len(req_data))
        conn.write(req_data)
        ctx.rpcz_data.client_post_io_send_msg_us = now_us()

        if options.req_type != RequestType.SendOnly:
            rsp = await asyncio.wait_for(conn.read(), get_remain_time(ctx.deadline))
            ctx.rpcz_data.client_post_sched_recv_msg_us = now_us()
            ctx.with_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, len(rsp))
    except asyncio.TimeoutError as err:
        raise ClientTimeoutError(
            f"client tcp_multiplexed reached deadline to {options.address}, {err} io error ") from err
    except Exception as err:  # pylint: disable=broad-except
        raise ClientInvokeError(f"client tcp_multiplexed invoke {options.address}, error {err}") from err
    finally:
        if conn:
            conn.close()

    return rsp
