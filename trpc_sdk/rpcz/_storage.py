# -*- coding: utf-8 -*-
"""Global in-process rpcz span storage.

Both containers are plain module globals, so they are per-process: a
worker and the dispatcher each keep their own. A server span that starts
in a worker is handed to the dispatcher over the rpcz side pipe rather
than shared through this module.
"""

from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import TrpcContext
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger

from ._span import Span

_spans: Dict[int, Span] = {}
_sub_spans: DefaultDict[int, List[Span]] = defaultdict(list)
RPCZ_SERVER_SPAN_CTX_KEY = "rpcz_server_span"
RPCZ_SERVER_REQ_SIZE_CTX_KEY = "rpcz_server_req_size"
RPCZ_SERVER_RSP_SIZE_CTX_KEY = "rpcz_server_rsp_size"
RPCZ_CLIENT_REQ_SIZE_CTX_KEY = "rpcz_client_req_size"
RPCZ_CLIENT_RSP_SIZE_CTX_KEY = "rpcz_client_rsp_size"

# Server stamps a worker can observe. The rest of the send path happens
# in the dispatcher, which owns the client socket.
_SERVER_WORKER_TIMESTAMPS = [
    "server_post_sched_recv_msg_us",
    "server_pre_sched_send_msg_us",
    "server_post_sched_send_msg_us",
]
# Merged-worker path: one process owns the whole send path, so the I/O
# stamps are on the same context.
_SERVER_EXECUTOR_TIMESTAMPS = _SERVER_WORKER_TIMESTAMPS + [
    "server_pre_io_send_msg_us",
    "server_post_io_send_msg_us",
]


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


def _copy_sizes_from_context(ctx: TrpcContext, span: Span) -> None:
    """Copy request and response byte counts off the context.

    The transport records both as context metadata because it sees the
    encoded frame, which the filters never do.

    Args:
        ctx: Server-side RPC context.
        span: Span to fill in place. Sizes of zero are left alone so a
            value already set by a filter is not overwritten.
    """
    req_size = ctx.get_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, 0)
    rsp_size = ctx.get_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, 0)
    if isinstance(req_size, int) and req_size > 0:
        span.request_size = req_size
    if isinstance(rsp_size, int) and rsp_size > 0:
        span.response_size = rsp_size


def _copy_timestamps_from_context(ctx: TrpcContext, span: Span, names: List[str]) -> None:
    """Copy scheduling and I/O timestamps off ``ctx.rpcz_data``.

    Args:
        ctx: Server-side RPC context.
        span: Span to fill in place.
        names: Attribute names shared by ``RpczData`` and :class:`Span`.
            An unset stamp is zero and is skipped, so a filter-side value
            survives a transport that never reached that point.
    """
    for name in names:
        value = getattr(ctx.rpcz_data, name, 0)
        if value > 0:
            setattr(span, name, value)


def _is_above_minimum_cost(span: Span, total_cost_us: int, where: str) -> bool:
    """Check one span against the configured ``sample_minimum_timecost``.

    Args:
        span: Span under consideration; only used for the log line.
        total_cost_us: Measured end-to-end cost in microseconds.
        where: Submit point name, for the drop log line.

    Returns:
        True when the span is slow enough to keep.
    """
    rpcz_conf = TrpcConfig.global_config().rpcz
    minimum_cost_us = int(rpcz_conf.sample_minimum_timecost) * 1000
    if total_cost_us < minimum_cost_us:
        logger.debug(
            "rpcz_storage: drop %s span by min_cost, span_id=%s, cost_us=%s, threshold_us=%s",
            where,
            span.span_id,
            total_cost_us,
            minimum_cost_us,
        )
        return False
    return True


def submit_server_span_on_executor_post_io(ctx: TrpcContext) -> Optional[Span]:
    """Submit sampled server span at SERVER_POST_IO_SEND_MSG.

    This is the merged-worker path, where one process both runs the
    handler and writes the response, so the span can be finished and
    stored in place.

    Steps:
    1) take server span from context
    2) transfer context-side timestamps into span
    3) merge associated client sub-spans
    4) submit span and clear sub-span container

    Args:
        ctx: Server-side RPC context holding the sampled span.

    Returns:
        The submitted span, or None when nothing was sampled or the
        span was dropped by ``sample_minimum_timecost``.
    """
    server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
    if not isinstance(server_span, Span):
        logger.debug("rpcz_storage: skip executor submit, no server span in context")
        return None

    total_cost_us = max(0, server_span.server_post_io_send_msg_us - server_span.server_pre_sched_recv_msg_us)
    if not _is_above_minimum_cost(server_span, total_cost_us, "exec"):
        return None

    _copy_timestamps_from_context(ctx, server_span, _SERVER_EXECUTOR_TIMESTAMPS)
    _copy_sizes_from_context(ctx, server_span)

    # merge sub-spans and clear sub-span container
    sub_spans = pop_sub_spans(server_span.span_id)
    if sub_spans:
        server_span.sub_spans.extend(sub_spans)

    submit_span(server_span)
    logger.debug("rpcz_storage: executor server span submitted, span_id=%s, sub_span_count=%s", server_span.span_id,
                 len(server_span.sub_spans))
    return server_span


def prepare_server_span_for_dispatch(ctx: TrpcContext) -> Optional[Span]:
    """Prepare server span in worker process before sending response to dispatcher.

    Only the stages the worker can observe are filled here. The I/O
    stamps belong to the dispatcher, which finishes the span in
    :func:`submit_server_span_on_dispatcher_post_io` after it has
    written the frame.

    Args:
        ctx: Server-side RPC context holding the sampled span.

    Returns:
        The span to ship over the rpcz side pipe, or None when nothing
        was sampled.
    """
    server_span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
    if not isinstance(server_span, Span):
        logger.debug("rpcz_storage: skip prepare_for_dispatch, no server span in context")
        return None

    _copy_timestamps_from_context(ctx, server_span, _SERVER_WORKER_TIMESTAMPS)
    _copy_sizes_from_context(ctx, server_span)

    # merge sub-spans and clear relation container in worker process
    sub_spans = pop_sub_spans(server_span.span_id)
    if sub_spans:
        server_span.sub_spans.extend(sub_spans)
    logger.debug("rpcz_storage: prepared server span for dispatch, span_id=%s, sub_span_count=%s", server_span.span_id,
                 len(server_span.sub_spans))
    return server_span


def submit_server_span_on_dispatcher_post_io(span: Span, post_sched_send_us: int, pre_io_send_us: int,
                                             post_io_send_us: int) -> Optional[Span]:
    """Finalize and submit server span in dispatcher process on SERVER_POST_IO_SEND_MSG.

    Args:
        span: Span received from a worker over the rpcz side pipe.
        post_sched_send_us: Stamp taken when the dispatcher picked the
            response off the worker pipe.
        pre_io_send_us: Stamp taken just before the client socket write.
        post_io_send_us: Stamp taken once the write completed.

    Returns:
        The submitted span, or None when it was dropped by
        ``sample_minimum_timecost``.
    """
    span.server_post_sched_send_msg_us = post_sched_send_us
    span.server_pre_io_send_msg_us = pre_io_send_us
    span.server_post_io_send_msg_us = post_io_send_us

    total_cost_us = max(0, span.server_post_io_send_msg_us - span.server_pre_sched_recv_msg_us)
    if not _is_above_minimum_cost(span, total_cost_us, "dispatcher"):
        return None

    submit_span(span)
    logger.debug("rpcz_storage: dispatcher server span submitted, span_id=%s", span.span_id)
    return span
