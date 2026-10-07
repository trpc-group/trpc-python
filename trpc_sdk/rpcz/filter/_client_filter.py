# -*- coding: utf-8 -*-
"""Rpcz client filter."""

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
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin
from trpc_sdk.utils import now_us

from .._span import Span
from .._span import SpanType
from .._storage import RPCZ_CLIENT_REQ_SIZE_CTX_KEY
from .._storage import RPCZ_CLIENT_RSP_SIZE_CTX_KEY
from .._storage import RPCZ_SERVER_SPAN_CTX_KEY
from .._storage import submit_span
from .._storage import submit_sub_span
from ._sampler import HighLowWaterLevelSampler
from ._sampler import SpanIdGenerator

RPCZ_CLIENT_SPAN_CTX_KEY = "rpcz_client_span"

# Client stamps the transport writes onto ``ctx.rpcz_data``. Names are
# shared by ``RpczData`` and ``Span``, so they copy across by name.
_CLIENT_TIMESTAMPS = [
    "client_post_sched_send_msg_us",
    "client_pre_io_send_msg_us",
    "client_post_io_send_msg_us",
    "client_post_sched_recv_msg_us",
]

# Module-level so every client filter instance in the process draws from
# one sequence; the sampler below is per-instance on purpose.
_id_generator = SpanIdGenerator()


@register_plugin(PluginType.CLIENT_FILTER_COMPONENT, "rpcz")
class RpczClientFilter(BaseFilter):
    """Rpcz client filter.

    A client call made while handling a server request is treated as a
    hop of that request: the span becomes a sub-span of the server span
    and follows it, instead of being submitted on its own.
    """

    def __init__(self):
        super().__init__()
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
        """Decide whether this outbound call produces a span.

        Args:
            ctx: Client-side RPC context.
            span_id: Id already allocated for this call.

        Returns:
            True when the call passes the whitelist and the sampler.
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
    def _build_client_span(ctx: TrpcContext, span_id: int) -> Span:
        """Build a client span seeded from the outbound message.

        Args:
            ctx: Client-side RPC context.
            span_id: Id already allocated for this call.

        Returns:
            A span with the callee identity and the call deadline.
        """
        msg = get_message(ctx)
        return Span(
            span_id=span_id,
            span_type=SpanType.CLIENT,
            full_method_name=msg.client_rpc_name or "",
            remote_name=msg.callee_service_name or "",
            remote_endpoint=msg.remote_addr or "",
            timeout_ms=ctx.timeout or 0,
        )

    @staticmethod
    def _finalize_client_span(ctx: TrpcContext, span: Span, rsp: Any, err: Any) -> None:
        """Close out a client span once the call returned.

        Args:
            ctx: Client-side RPC context; the transport left its I/O
                timestamps and byte counts on it.
            span: Span to fill in place.
            rsp: Response object, used only as a size fallback when the
                transport did not record the encoded size.
            err: Exception raised downstream, or None.
        """
        span.client_post_rpc_invoke_us = now_us()
        for name in _CLIENT_TIMESTAMPS:
            value = getattr(ctx.rpcz_data, name, 0)
            if value > 0:
                setattr(span, name, value)
        req_size = ctx.get_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, 0)
        rsp_size = ctx.get_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, 0)
        if isinstance(req_size, int) and req_size > 0:
            span.request_size = req_size
        if isinstance(rsp_size, int) and rsp_size > 0:
            span.response_size = rsp_size
        if isinstance(err, TrpcException):
            span.error_code = getattr(err, "code", -1)
        elif err is not None:
            span.error_code = -1
        else:
            span.error_code = 0
        if span.response_size <= 0:
            try:
                if rsp is not None:
                    span.response_size = len(str(rsp))
            except Exception:  # pylint: disable=broad-except
                pass

    def _submit_span(self, ctx: TrpcContext, client_span: Span) -> None:
        """Store the span, as a sub-span when this call was a hop.

        Args:
            ctx: Client-side RPC context.
            client_span: Finalized span for this call.
        """
        rpcz_conf = TrpcConfig.global_config().rpcz
        minimum_cost_us = int(rpcz_conf.sample_minimum_timecost) * 1000
        total_cost_us = max(0, client_span.client_post_rpc_invoke_us - client_span.client_pre_rpc_invoke_us)
        if total_cost_us < minimum_cost_us:
            logger.debug(
                "rpcz_client_filter: drop span by minimum_timecost, span_id=%s, total_cost_us=%s, threshold_us=%s",
                client_span.span_id,
                total_cost_us,
                minimum_cost_us,
            )
            return

        server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
        if isinstance(server_span, Span):
            submit_sub_span(server_span.span_id, client_span)
            logger.debug("rpcz_client_filter: submit as sub-span, parent_span_id=%s, span_id=%s", server_span.span_id,
                         client_span.span_id)
        else:
            submit_span(client_span)
            logger.debug("rpcz_client_filter: submit standalone client span, span_id=%s", client_span.span_id)

    async def run(self, ctx: TrpcContext, req: Any, next_handle: FilterHandleType) -> FilterReturnType:
        """Wrap one outbound call with client-side span bookkeeping.

        Args:
            ctx: Client-side RPC context.
            req: Application request; unused, spans read the message.
            next_handle: Next handler or filter in the chain.

        Returns:
            The ``(response, error)`` pair produced downstream, unchanged.
        """
        server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
        # A server span on the context means this call is a hop of a
        # request already being traced, so it is sampled unconditionally
        # rather than risking a parent span with missing children.
        is_route = isinstance(server_span, Span)

        sampled = False
        client_span = None
        span_id = _id_generator.generate_id()

        if is_route:
            sampled = True
        else:
            sampled = self._should_sample(ctx, span_id)
        logger.debug("rpcz_client_filter: sampling decided, span_id=%s, is_route=%s, sampled=%s", span_id, is_route,
                     sampled)

        if sampled:
            client_span = self._build_client_span(ctx, span_id)
            pre_ts = now_us()
            client_span.client_pre_rpc_invoke_us = pre_ts
            ctx.with_meta_data(RPCZ_CLIENT_SPAN_CTX_KEY, client_span)
            logger.debug("rpcz_client_filter: client span initialized, span_id=%s, method=%s", client_span.span_id,
                         client_span.full_method_name)

        rsp, err = await next_handle()

        if client_span is not None:
            self._finalize_client_span(ctx, client_span, rsp, err)
            ctx.with_meta_data(RPCZ_CLIENT_SPAN_CTX_KEY, client_span)
            logger.debug("rpcz_client_filter: client span finalized, span_id=%s, error_code=%s", client_span.span_id,
                         client_span.error_code)
            self._submit_span(ctx, client_span)

        return rsp, err

    async def run_stream(self, ctx: TrpcContext, req: Any,
                         next_handle: FilterAsyncGenHandleType) -> FilterAsyncGenReturnType:
        """Reject streaming calls; rpcz spans only cover unary RPCs.

        Args:
            ctx: Client-side RPC context.
            req: Application request.
            next_handle: Next streaming handler or filter in the chain.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("rpcz client filter does not support streaming calls")
