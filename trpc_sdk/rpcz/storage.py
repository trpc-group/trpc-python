# -*- coding: utf-8 -*-
"""Global in-process rpcz span storage."""

from collections import defaultdict
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Optional

from trpc import config
from trpc.log import logger
from trpc.rpcz.span import Span

_spans: Dict[int, Span] = {}
_sub_spans: DefaultDict[int, List[Span]] = defaultdict(list)
RPCZ_SERVER_SPAN_CTX_KEY = "rpcz_server_span"
RPCZ_SERVER_REQ_SIZE_CTX_KEY = "rpcz_server_req_size"
RPCZ_SERVER_RSP_SIZE_CTX_KEY = "rpcz_server_rsp_size"
RPCZ_CLIENT_REQ_SIZE_CTX_KEY = "rpcz_client_req_size"
RPCZ_CLIENT_RSP_SIZE_CTX_KEY = "rpcz_client_rsp_size"


def submit_span(span: Span) -> None:
    """Submit standalone span into global span container."""
    _spans[span.span_id] = span
    logger.debug("rpcz_storage: submit span, span_id=%s, span_type=%s", span.span_id, span.span_type.value)


def submit_sub_span(parent_span_id: int, sub_span: Span) -> None:
    """Submit route sub span into global sub-span container."""
    _sub_spans[parent_span_id].append(sub_span)
    logger.debug("rpcz_storage: submit sub-span, parent_span_id=%s, sub_span_id=%s", parent_span_id, sub_span.span_id)


def pop_sub_spans(parent_span_id: int) -> List[Span]:
    """Pop all sub spans by parent span id."""
    spans = _sub_spans.pop(parent_span_id, [])
    return list(spans)


def get_all_spans() -> List[Span]:
    """Get snapshot of all standalone spans."""
    return list(_spans.values())


def pop_all_spans() -> List[Span]:
    """Pop and clear all standalone spans."""
    spans = list(_spans.values())
    _spans.clear()
    logger.debug("rpcz_storage: pop_all_spans, count=%d", len(spans))
    return spans


def submit_server_span_on_executor_post_io(ctx) -> Optional[Span]:
    """Submit sampled server span at SERVER_POST_IO_SEND_MSG.

    Steps:
    1) take server span from context
    2) transfer context-side timestamps into span
    3) merge associated client sub-spans
    4) submit span and clear sub-span container
    """
    server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
    if not isinstance(server_span, Span):
        logger.debug("rpcz_storage: skip executor submit, no server span in context")
        return None

    rpcz_conf = config.global_config_obj.global_config.rpcz
    minimum_cost_us = int(rpcz_conf.sample_minimum_timecost) * 1000
    total_cost_us = max(0, server_span.server_post_io_send_msg_us - server_span.server_pre_sched_recv_msg_us)
    if total_cost_us < minimum_cost_us:
        logger.debug(
            "rpcz_storage: drop exec span by min_cost, span_id=%s, cost_us=%s, threshold_us=%s",
            server_span.span_id,
            total_cost_us,
            minimum_cost_us,
        )
        return None

    # transfer transport timestamps from context
    if getattr(ctx, "server_post_sched_recv_msg_us", 0) > 0:
        server_span.server_post_sched_recv_msg_us = ctx.server_post_sched_recv_msg_us
    if getattr(ctx, "server_pre_sched_send_msg_us", 0) > 0:
        server_span.server_pre_sched_send_msg_us = ctx.server_pre_sched_send_msg_us
    if getattr(ctx, "server_post_sched_send_msg_us", 0) > 0:
        server_span.server_post_sched_send_msg_us = ctx.server_post_sched_send_msg_us
    if getattr(ctx, "server_pre_io_send_msg_us", 0) > 0:
        server_span.server_pre_io_send_msg_us = ctx.server_pre_io_send_msg_us
    if getattr(ctx, "server_post_io_send_msg_us", 0) > 0:
        server_span.server_post_io_send_msg_us = ctx.server_post_io_send_msg_us
    req_size = ctx.get_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, 0)
    rsp_size = ctx.get_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, 0)
    if isinstance(req_size, int) and req_size > 0:
        server_span.request_size = req_size
    if isinstance(rsp_size, int) and rsp_size > 0:
        server_span.response_size = rsp_size

    # merge sub-spans and clear sub-span container
    sub_spans = pop_sub_spans(server_span.span_id)
    if sub_spans:
        server_span.sub_spans.extend(sub_spans)

    submit_span(server_span)
    logger.debug("rpcz_storage: executor server span submitted, span_id=%s, sub_span_count=%s", server_span.span_id,
                 len(server_span.sub_spans))
    return server_span


def prepare_server_span_for_dispatch(ctx) -> Optional[Span]:
    """Prepare server span in worker process before sending response to dispatcher."""
    server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
    if not isinstance(server_span, Span):
        logger.debug("rpcz_storage: skip prepare_for_dispatch, no server span in context")
        return None

    if getattr(ctx, "server_post_sched_recv_msg_us", 0) > 0:
        server_span.server_post_sched_recv_msg_us = ctx.server_post_sched_recv_msg_us
    if getattr(ctx, "server_pre_sched_send_msg_us", 0) > 0:
        server_span.server_pre_sched_send_msg_us = ctx.server_pre_sched_send_msg_us
    if getattr(ctx, "server_post_sched_send_msg_us", 0) > 0:
        server_span.server_post_sched_send_msg_us = ctx.server_post_sched_send_msg_us
    req_size = ctx.get_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, 0)
    rsp_size = ctx.get_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, 0)
    if isinstance(req_size, int) and req_size > 0:
        server_span.request_size = req_size
    if isinstance(rsp_size, int) and rsp_size > 0:
        server_span.response_size = rsp_size

    # merge sub-spans and clear relation container in worker process
    sub_spans = pop_sub_spans(server_span.span_id)
    if sub_spans:
        server_span.sub_spans.extend(sub_spans)
    logger.debug("rpcz_storage: prepared server span for dispatch, span_id=%s, sub_span_count=%s", server_span.span_id,
                 len(server_span.sub_spans))
    return server_span


def submit_server_span_on_dispatcher_post_io(span: Span, post_sched_send_us: int, pre_io_send_us: int,
                                             post_io_send_us: int) -> Optional[Span]:
    """Finalize and submit server span in dispatcher process on SERVER_POST_IO_SEND_MSG."""
    span.server_post_sched_send_msg_us = post_sched_send_us
    span.server_pre_io_send_msg_us = pre_io_send_us
    span.server_post_io_send_msg_us = post_io_send_us

    rpcz_conf = config.global_config_obj.global_config.rpcz
    minimum_cost_us = int(rpcz_conf.sample_minimum_timecost) * 1000
    total_cost_us = max(0, span.server_post_io_send_msg_us - span.server_pre_sched_recv_msg_us)
    if total_cost_us < minimum_cost_us:
        logger.debug(
            "rpcz_storage: drop dispatcher span by min_cost, span_id=%s, cost_us=%s, threshold_us=%s",
            span.span_id,
            total_cost_us,
            minimum_cost_us,
        )
        return None

    submit_span(span)
    logger.debug("rpcz_storage: dispatcher server span submitted, span_id=%s", span.span_id)
    return span
