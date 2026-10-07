# -*- coding: utf-8 -*-
"""Rpcz server filter."""

from __future__ import annotations

from typing import Any

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterAsyncGenHandleType
from trpc_sdk.abc import FilterAsyncGenReturnType
from trpc_sdk.abc import FilterHandleType
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin
from trpc_sdk.utils import now_us

from .._span import Span
from .._span import SpanType
from .._storage import RPCZ_SERVER_SPAN_CTX_KEY
from ._sampler import HighLowWaterLevelSampler
from ._sampler import SpanIdGenerator


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "rpcz")
class RpczServerFilter(BaseFilter):
    """Rpcz server filter.

    Behavior for this stage:
    - Before business callback:
      1) decide sampling and create span if needed
      2) record SERVER_PRE_RPC_INVOKE
    - After business callback:
      record SERVER_POST_RPC_INVOKE for sampled span

    The span is left on the context rather than submitted here, because
    the later stages of the request are not visible from the filter
    chain: the transport is what finally submits it.
    """

    def __init__(self):
        super().__init__()
        self._id_generator = SpanIdGenerator()
        self._sampler = HighLowWaterLevelSampler()

    @staticmethod
    def _is_service_allowed(callee: str) -> bool:
        """Check ``callee`` against the configured service whitelist.

        Args:
            callee: Callee service name off the request message.

        Returns:
            True when the service may be sampled. An empty whitelist
            allows everything; an empty callee is always rejected.
        """
        rpcz_conf = TrpcConfig.global_config().rpcz
        whitelist = rpcz_conf.service_whitelist or []
        if not callee:
            return False
        if not whitelist:
            return True
        return callee in whitelist

    def _should_sample(self, ctx: TrpcContext, span_id: int) -> bool:
        """Decide whether this request produces a span.

        Args:
            ctx: Server-side RPC context.
            span_id: Id already allocated for this request.

        Returns:
            True when the request passes the whitelist and the sampler.
        """
        msg = get_message(ctx)
        if not self._is_service_allowed(msg.callee_service_name):
            return False
        rpcz_conf = TrpcConfig.global_config().rpcz
        return self._sampler.sample(
            span_id=span_id,
            sample_rate=rpcz_conf.sample_rate,
            lower_water_level=rpcz_conf.lower_water_level,
            high_water_level=rpcz_conf.high_water_level,
        )

    @staticmethod
    def _build_server_span(ctx: TrpcContext, span_id: int) -> Span:
        """Build a server span seeded from the request message.

        Args:
            ctx: Server-side RPC context.
            span_id: Id already allocated for this request.

        Returns:
            A span with the peer identity filled in and, when the
            transport recorded one, the receive timestamp.
        """
        msg = get_message(ctx)
        span = Span(
            span_id=span_id,
            span_type=SpanType.SERVER,
            full_method_name=msg.server_rpc_name or "",
            remote_name=msg.caller_service_name or "",
            remote_endpoint=msg.remote_addr or "",
        )
        if ctx.recv_time:
            # recv_time is in milliseconds in dispatcher path.
            span.server_pre_sched_recv_msg_us = int(ctx.recv_time * 1000)
        return span

    async def run(self, ctx: TrpcContext, req: Any, next_handle: FilterHandleType) -> FilterReturnType:
        """Wrap one unary handler call with server-side span bookkeeping.

        Args:
            ctx: Server-side RPC context.
            req: Application request; unused, spans read the message.
            next_handle: Next handler or filter in the chain.

        Returns:
            The ``(response, error)`` pair produced downstream, unchanged.
        """
        span_id = self._id_generator.generate_id()
        sampled = self._should_sample(ctx, span_id)
        logger.debug("rpcz_server_filter: sampling decided, span_id=%s, sampled=%s", span_id, sampled)

        span = None
        if sampled:
            span = self._build_server_span(ctx, span_id)
            ts = now_us()
            if span.server_pre_sched_recv_msg_us == 0:
                span.server_pre_sched_recv_msg_us = ts
            span.server_post_sched_recv_msg_us = ts
            span.server_pre_rpc_invoke_us = ts
            ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, span)
            logger.debug("rpcz_server_filter: server span initialized, span_id=%s, method=%s", span.span_id,
                         span.full_method_name)

        rsp, err = await next_handle()

        if sampled:
            post_ts = now_us()
            span.server_post_rpc_invoke_us = post_ts
            # save back to context for later submit points.
            ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, span)
            logger.debug("rpcz_server_filter: server span finalized in filter, span_id=%s, error=%s", span.span_id, err
                         is not None)
        return rsp, err

    async def run_stream(self, ctx: TrpcContext, req: Any,
                         next_handle: FilterAsyncGenHandleType) -> FilterAsyncGenReturnType:
        """Reject streaming calls; rpcz spans only cover unary RPCs.

        Args:
            ctx: Server-side RPC context.
            req: Application request.
            next_handle: Next streaming handler or filter in the chain.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("rpcz server filter does not support streaming calls")
