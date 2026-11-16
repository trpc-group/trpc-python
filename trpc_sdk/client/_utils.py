# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Client utility module."""

from __future__ import annotations

from trpc_sdk.abc import Node
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.config import SocketType
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.log import logger
from trpc_sdk.naming import get_selector
from trpc_sdk.transport import ClientTimeoutReason

from ._options import Options


def update_message(msg: TrpcMessage, opts: Options) -> None:
    """Copy caller-facing option fields onto the outbound message.

    Covers metadata, caller service name, serialization / compress types,
    and optional custom request / response heads. ``req_head`` replaces
    the codec request head; ``rsp_head`` is a caller-owned buffer that
    the codec fills with the backend response head.

    Args:
        msg: Outbound client message to mutate.
        opts: Per-call options already assembled by :func:`get_options`.
    """
    if opts.metadata:
        meta_data = msg.client_meta_data or {}
        meta_data.update(opts.metadata)
        msg.set_client_meta_data(meta_data)

    if opts.caller_service_name:
        msg.set_caller_service_name(opts.caller_service_name)

    if opts.serialization_type >= 0:
        msg.set_serialization_type(opts.serialization_type)

    if opts.compress_type >= 0:
        msg.set_compress_type(opts.compress_type)

    if opts.req_head:
        msg.set_client_req_head(opts.req_head)
    if opts.rsp_head:
        msg.set_client_rsp_head(opts.rsp_head)


async def select_node(msg: TrpcMessage, opts: Options) -> Node:
    """Resolve the downstream node and copy its address onto ``msg``.

    Unix-socket calls rewrite ``opts.target`` first. An explicit
    ``target`` of the form ``scheme://endpoint`` overrides the default
    Polar selector.

    Args:
        msg: Outbound message; remote address and env transfer are set.
        opts: Per-call options; selector and endpoint may be replaced.

    Returns:
        The selected :class:`~trpc_sdk.abc.Node`.

    Raises:
        ClientRouteError: ``target`` has no ``scheme://endpoint``, or
            the selector returned no addressable node.
        UnknownError: ``opts.selector`` is missing.
    """
    if opts.socket_type == SocketType.UNIX:
        opts.target = "ip://" + opts.unix_path + ":0"

    if opts.target:
        targets = opts.target.split("://")
        if len(targets) != 2:
            raise ClientRouteError("Client select node target schema invalid")
        opts.selector = get_selector(targets[0])
        opts.endpoint = targets[1]

    if not opts.selector:
        raise UnknownError("Client select node selector not exist")

    logger.debug("%s|select node", opts.endpoint)
    node = await opts.selector.select(opts.endpoint, opts.selector_options)
    logger.debug("%s|select end node=%s", opts.endpoint, node)

    if node is None or not node.address:
        raise ClientRouteError("cannot find downstream node")

    opts.load_node_config(node)
    opts.call_options.set_endpoint(opts.endpoint)

    msg.set_remote_addr(node.address)
    msg.set_callee_container_name(node.container_name)
    if len(msg.env_transfer) == 0:
        msg.set_env_transfer(node.env_key)

    return node


def set_timeout(ctx: TrpcContext, msg: TrpcMessage, opts: Options, begin: int) -> ClientTimeoutReason:
    """Apply the tighter of the client timeout and the server deadline.

    When the client deadline is sooner (or the server set none), a later
    timeout is blamed on the downstream and reported to the selector as
    :attr:`~trpc_sdk.transport.ClientTimeoutReason.DOWNSTREAM_TIMEOUT`. When
    the server deadline is sooner, the timeout is
    :attr:`~trpc_sdk.transport.ClientTimeoutReason.CURRENT_SERVICE_TIMEOUT`
    and is not treated as downstream failure.

    Args:
        ctx: Request context; ``deadline`` and ``timeout`` are updated.
        msg: Outbound message; remaining timeout is written for the peer.
        opts: Per-call options; ``timeout`` is in milliseconds.
        begin: ``now_ms()`` at the start of this attempt.

    Returns:
        Which side should be blamed if this call times out.

    Raises:
        ClientTimeoutError: Remaining time is already negative.
    """
    timeout_reason = ClientTimeoutReason.DOWNSTREAM_TIMEOUT
    if opts.timeout > 0:
        client_deadline = begin + opts.timeout
        if client_deadline < ctx.deadline or ctx.deadline == 0:
            ctx.deadline = client_deadline
            ctx.timeout = opts.timeout
        else:
            ctx.timeout = ctx.deadline - begin
            timeout_reason = ClientTimeoutReason.CURRENT_SERVICE_TIMEOUT
        if ctx.timeout < 0:
            raise ClientTimeoutError("client reach deadline")
        msg.set_timeout(ctx.timeout)
    else:
        ctx.timeout = 0

    return timeout_reason
