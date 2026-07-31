# -*- coding: utf-8 -*-
"""Rpcz server filter."""
from typing import Any

from trpc.codec.message import get_message
from trpc.config import config
from trpc.plugin import PluginInitPos
from trpc.plugin import PluginType
from trpc.plugin import register_plugin
from trpc.trpc_abc.context import Context
from trpc.trpc_abc.filter import Filter
from trpc.trpc_abc.filter import FilterReturnType
from trpc.log import logger

from trpc.rpcz.filter.common import HighLowWaterLevelSampler
from trpc.rpcz.filter.common import SpanIdGenerator
from trpc.rpcz.storage import RPCZ_SERVER_SPAN_CTX_KEY
from trpc.rpcz.span import Span
from trpc.rpcz.span import SpanType
from trpc.utils import now_us


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "rpcz", init_pos=PluginInitPos.WORKER, stat_flag=True)
class RpczServerFilter(Filter):
    """Rpcz server filter.

    Behavior for this stage:
    - Before business callback:
      1) decide sampling and create span if needed
      2) record SERVER_PRE_RPC_INVOKE
    - After business callback:
      record SERVER_POST_RPC_INVOKE for sampled span
    """

    def __init__(self):
        self._id_generator = SpanIdGenerator()
        self._sampler = HighLowWaterLevelSampler()

    @staticmethod
    def _is_service_allowed(callee: str) -> bool:
        rpcz_conf = config.global_config_obj.global_config.rpcz
        whitelist = rpcz_conf.service_whitelist or []
        if not callee:
            return False
        if not whitelist:
            return True
        return callee in whitelist

    def _should_sample(self, ctx: Context, span_id: int) -> bool:
        msg = get_message(ctx)
        if not self._is_service_allowed(msg.callee_service_name):
            return False
        rpcz_conf = config.global_config_obj.global_config.rpcz
        return self._sampler.sample(
            span_id=span_id,
            sample_rate=rpcz_conf.sample_rate,
            lower_water_level=rpcz_conf.lower_water_level,
            high_water_level=rpcz_conf.high_water_level,
        )

    @staticmethod
    def _build_server_span(ctx: Context, span_id: int) -> Span:
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

    async def run(self, ctx: Context, req: Any, handle) -> FilterReturnType:
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

        rsp, err = await handle()

        if sampled:
            post_ts = now_us()
            span.server_post_rpc_invoke_us = post_ts
            # save back to context for later submit points.
            ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, span)
            logger.debug("rpcz_server_filter: server span finalized in filter, span_id=%s, error=%s", span.span_id, err
                         is not None)
        return rsp, err
