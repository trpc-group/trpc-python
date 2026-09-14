# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Unary tRPC client invoke path.

Consumes the :class:`Options` built by :mod:`trpc_sdk.client._options`,
copies them onto the outbound :class:`~trpc_sdk.codec.Message`, selects a
downstream node, applies the client/server deadline, and runs the
filter chain around encode / round-trip / decode.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from functools import partial
from typing import Any
from typing import Callable
from typing import Optional
from typing import Sequence
from typing import Union

from trpc_sdk.abc import BaseClient
from trpc_sdk.abc import Node
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import Message
from trpc_sdk.codec import compress
from trpc_sdk.codec import decompress
from trpc_sdk.codec import deserialize
from trpc_sdk.codec import serialize
from trpc_sdk.common import async_lib
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.config import SocketType
from trpc_sdk.exceptions import ClientConnectionError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.log import logger
from trpc_sdk.naming import get_selector
from trpc_sdk.transport import ClientTimeoutReason
from trpc_sdk.transport import RequestType
from trpc_sdk.transport import default_multiplexed_pool
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import singleton
from ._options import Options
from ._options import get_options
from ._options import normalize_options


def get_client():
    """Return the process-wide :class:`Client` singleton.

    ``Client`` is decorated with :func:`~trpc_sdk.utils.singleton`,
    so concurrent callers share one instance.
    """
    return Client()


async def _invoke_backend(ctx: TrpcContext, req: Any, rsp: Any, opts: Options) -> Any:
    """Encode ``req``, round-trip, and decode into ``rsp``.

    This is the innermost filter-chain handler. Multiplexed transports
    already return a decoded body, so that path skips ``codec.decode``.
    Send-only calls return before any response handling.

    Args:
        ctx: Client context whose message holds codec state.
        req: Business request object to serialize.
        rsp: Pre-constructed response object filled in place.
        opts: Per-call options, including codec and transport.

    Returns:
        ``rsp`` after deserialize, or ``None`` for send-only requests.
    """
    msg = ctx.get_client_message()
    serialization_type = msg.serialization_type
    compress_type = msg.compress_type
    # current_* >= 0 is a per-call override of the types stored on the message.
    if opts.current_serialization_type >= 0:
        serialization_type = opts.current_serialization_type
    if opts.current_compress_type >= 0:
        compress_type = opts.current_compress_type

    req_body = serialize(serialization_type, req)
    req_body = compress(compress_type, req_body)

    client_codec = opts.codec
    req_buff = client_codec.encode(msg, req_body)

    if opts.enable_multiplexed:
        opts.call_options.set_enable_multiplexed(True)
        opts.call_options.set_msg(msg)
        opts.call_options.set_codec(opts.codec)
        opts.call_options.set_multiplexed(default_multiplexed_pool)

    opts.call_options.set_socket_type(opts.socket_type)

    # Plugin-selected transport for the protocol (TCP/UDP, HTTP, etc.)
    client_ts = opts.transport
    rsp_buff = b""
    try:
        logger.debug("Client invoke backend begin request %s", opts.endpoint)
        rsp_buff = await client_ts.round_trip(ctx, req_buff, opts.call_options)
        logger.debug("Client invoke backend request %s end", opts.endpoint)
    except ConnectionError as err:
        raise ClientConnectionError(f"Client invoke backend bad connection to {opts.call_options.address} {err}") from err

    if opts.call_options.req_type == RequestType.SendOnly:
        return
    # Multiplexed round-trip already returns the decoded body.
    if opts.enable_multiplexed:
        rsp_body = rsp_buff
    else:
        rsp_body = client_codec.decode(msg, rsp_buff)

    if len(rsp_body) > 0:
        # Prefer types from the backend response, then the same per-call overrides.
        serialization_type = msg.serialization_type
        compress_type = msg.compress_type
        if opts.current_serialization_type >= 0:
            serialization_type = opts.current_serialization_type
        if opts.current_compress_type >= 0:
            compress_type = opts.current_compress_type

        rsp_body = decompress(compress_type, rsp_body)
        deserialize(serialization_type, rsp_body, rsp)

    return rsp


def update_message(msg: Message, opts: Options) -> None:
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


async def select_node(msg: Message, opts: Options) -> Node:
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


def set_timeout(ctx: TrpcContext, msg: Message, opts: Options, begin: int) -> ClientTimeoutReason:
    """Apply the tighter of the client timeout and the server deadline.

    When the client deadline is sooner (or the server set none), a later
    timeout is blamed on the downstream and reported to the selector as
    :attr:`~trpc_sdk.transport.ClientTimeoutReason.DownstreamTimeout`. When
    the server deadline is sooner, the timeout is
    :attr:`~trpc_sdk.transport.ClientTimeoutReason.CurrentServiceTimeout`
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
    timeout_reason = ClientTimeoutReason.DownstreamTimeout
    if opts.timeout > 0:
        client_deadline = begin + opts.timeout
        if client_deadline < ctx.deadline or ctx.deadline == 0:
            ctx.deadline = client_deadline
            ctx.timeout = opts.timeout
        else:
            ctx.timeout = ctx.deadline - begin
            timeout_reason = ClientTimeoutReason.CurrentServiceTimeout
        if ctx.timeout < 0:
            raise ClientTimeoutError("client reach deadline")
        msg.set_timeout(ctx.timeout)
    else:
        ctx.timeout = 0

    return timeout_reason


@singleton
class Client(BaseClient):
    """Default tRPC unary client.

    ``invoke`` builds a fresh :class:`Options` per request so concurrent
    calls cannot share mutable call state. The filter chain wraps
    :func:`_invoke_backend`.
    """

    def __init__(self):
        pass

    @staticmethod
    def check_handle_error(err, timeout_reason: ClientTimeoutReason):
        """Return whether ``err`` should be reported to the selector.

        Only a connect failure, or an invoke timeout blamed on the
        downstream, feeds Polar circuit breaking. Timeouts caused by the
        current service's own deadline do not.

        Args:
            err: Exception caught during invoke, or ``None``.
            timeout_reason: Value returned by :func:`set_timeout`.
        """
        return err is not None and isinstance(err, TrpcException) and \
               (err.code == TrpcErrorCode.TRPC_CLIENT_CONNECT_ERR or
                (err.code == TrpcErrorCode.TRPC_CLIENT_INVOKE_TIMEOUT_ERR and
                 timeout_reason == ClientTimeoutReason.DownstreamTimeout))

    @staticmethod
    def check_client_rsp_err(err, msg):
        """Return whether the codec stored a framework error on ``msg``.

        Used after a successful filter-chain return to surface connect
        or timeout errors that arrived in the response head rather than
        as a Python exception.

        Args:
            err: Exception already captured, or ``None``.
            msg: Client message that may hold ``client_rsp_err``.
        """
        return err is None and msg.client_rsp_err is not None and \
               msg.client_rsp_err.code != TrpcErrorCode.TRPC_INVOKE_SUCCESS

    def invoke_sync(self, ctx: TrpcContext, req: Any, rsp_cls: Any,
                    options: Optional[Union[Options, Callable, Sequence]] = None):
        """Run :meth:`invoke` to completion on the current event loop.

        If this thread has no loop, one is created and installed. That
        covers worker threads, where ``get_event_loop`` will not create
        a loop the way it does on the main thread.

        Args:
            ctx: Client context for this call.
            req: Business request object.
            rsp_cls: Response class; an instance is constructed and filled.
            options: See :func:`get_options`.
        """
        try:
            loop = get_or_create_event_loop()
        except RuntimeError:
            loop = async_lib.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.invoke(ctx, req, rsp_cls, options))

    async def invoke(self, ctx: TrpcContext, req: Any, rsp_cls: Any,
                     options: Optional[Union[Options, Callable, Sequence]] = None) -> Any:
        """Invoke a unary backend RPC.

        Builds a new :class:`Options` for this request, updates the
        message, selects a node, then runs the filter chain around
        encode / transport / decode. Selector reporting happens in
        ``finally`` so both success and failure are recorded.

        Args:
            ctx: Client context for this call.
            req: Business request object.
            rsp_cls: Response class; an instance is constructed and filled.
            options: See :func:`get_options`.

        Returns:
            The filled response instance.
        """
        msg = ctx.get_client_message()
        # Fresh Options per request so concurrent calls do not share state.
        opts = get_options(msg, options)

        opts.selector_options.set_context(ctx)

        update_message(msg, opts)

        node = await select_node(msg, opts)

        begin = now_ms()
        err = None
        timeout_reason = None

        try:
            if not opts.codec:
                raise ClientEncodeError("client codec empty")

            timeout_reason = set_timeout(ctx, msg, opts, begin)

            rsp = rsp_cls()
            _, err = await opts.filters.handle(ctx, req, partial(_invoke_backend, rsp=rsp, opts=opts))

            return rsp
        except TrpcException as trpc_err:
            err = trpc_err
        except asyncio.TimeoutError as time_err:
            err = ClientTimeoutError(f"client request timeout, {time_err}")
        except Exception as unknown_err:  # pylint: disable=broad-except
            logger.error(f"client catch unknown exception {traceback.format_exc()}")
            err = unknown_err.with_traceback(sys.exc_info()[2])
        finally:
            cost = now_ms() - begin

            if self.check_handle_error(err, timeout_reason):
                await opts.selector.report(node, cost, err.code)
            else:
                await opts.selector.report(node, cost, 0)

            if self.check_client_rsp_err(err, msg):
                err = msg.client_rsp_err

            if opts.node:
                opts.node.address = node.address
                opts.node.service_name = node.service_name
                opts.node.cost_time = cost

            if err is not None:
                if isinstance(err, ClientTimeoutError):
                    err.set_timeout_type(timeout_reason)
                raise err
