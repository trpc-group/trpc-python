# -*- coding: utf-8 -*-
"""Rpcz client filter."""

from typing import Any

from trpc.codec.message import get_message
from trpc.config import config
from trpc.exceptions import TRPCBaseError
from trpc.plugin import PluginInitPos
from trpc.plugin import PluginType
from trpc.plugin import register_plugin
from trpc.trpc_abc.context import Context
from trpc.trpc_abc.filter import Filter
from trpc.trpc_abc.filter import FilterReturnType
from trpc.log import logger

from trpc.rpcz.filter.common import HighLowWaterLevelSampler
from trpc.rpcz.filter.common import SpanIdGenerator
from trpc.rpcz.storage import RPCZ_CLIENT_REQ_SIZE_CTX_KEY
from trpc.rpcz.storage import RPCZ_CLIENT_RSP_SIZE_CTX_KEY
from trpc.rpcz.storage import RPCZ_SERVER_SPAN_CTX_KEY
from trpc.rpcz.storage import submit_span
from trpc.rpcz.storage import submit_sub_span
from trpc.rpcz.span import Span
from trpc.rpcz.span import SpanType
from trpc.utils import now_us

RPCZ_CLIENT_SPAN_CTX_KEY = "rpcz_client_span"

_id_generator = SpanIdGenerator()


@register_plugin(PluginType.CLIENT_FILTER_COMPONENT, "rpcz", init_pos=PluginInitPos.WORKER, stat_flag=True)
class RpczClientFilter(Filter):
    """Rpcz client filter."""

    def __init__(self):
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
    def _build_client_span(ctx: Context, span_id: int) -> Span:
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
    def _finalize_client_span(ctx: Context, span: Span, rsp: Any, err: Any) -> None:
        span.client_post_rpc_invoke_us = now_us()
        if getattr(ctx, "client_post_sched_send_msg_us", 0) > 0:
            span.client_post_sched_send_msg_us = ctx.client_post_sched_send_msg_us
        if getattr(ctx, "client_pre_io_send_msg_us", 0) > 0:
            span.client_pre_io_send_msg_us = ctx.client_pre_io_send_msg_us
        if getattr(ctx, "client_post_io_send_msg_us", 0) > 0:
            span.client_post_io_send_msg_us = ctx.client_post_io_send_msg_us
        if getattr(ctx, "client_post_sched_recv_msg_us", 0) > 0:
            span.client_post_sched_recv_msg_us = ctx.client_post_sched_recv_msg_us
        req_size = ctx.get_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, 0)
        rsp_size = ctx.get_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, 0)
        if isinstance(req_size, int) and req_size > 0:
            span.request_size = req_size
        if isinstance(rsp_size, int) and rsp_size > 0:
            span.response_size = rsp_size
        if isinstance(err, TRPCBaseError):
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

    def _submit_span(self, ctx: Context, client_span: Span) -> None:
        rpcz_conf = config.global_config_obj.global_config.rpcz
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

    async def run(self, ctx: Context, req: Any, handle) -> FilterReturnType:
        server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
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

        rsp, err = await handle()

        if client_span is not None:
            self._finalize_client_span(ctx, client_span, rsp, err)
            ctx.with_meta_data(RPCZ_CLIENT_SPAN_CTX_KEY, client_span)
            logger.debug("rpcz_client_filter: client span finalized, span_id=%s, error_code=%s", client_span.span_id,
                         client_span.error_code)
            self._submit_span(ctx, client_span)

        return rsp, err
