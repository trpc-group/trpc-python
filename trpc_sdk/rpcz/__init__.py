# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python rpcz module: per-request timing and size spans.

The package is split by responsibility:

- :mod:`~trpc_sdk.rpcz._span` — the :class:`Span` data model and its
  JSON / bytes codec
- :mod:`~trpc_sdk.rpcz._storage` — the per-process span containers and
  the submit points each process role calls
- :mod:`~trpc_sdk.rpcz.filter` — the filters that decide sampling and
  open the spans, plus the samplers they use

A span is opened in the filter chain but finished by whoever writes the
response, which differs by deployment: a merged worker does both and
calls :func:`submit_server_span_on_executor_post_io`, while a split
deployment has the worker call :func:`prepare_server_span_for_dispatch`
and ship the span to the dispatcher, which finishes it with
:func:`submit_server_span_on_dispatcher_post_io`.

Importing this package also registers the rpcz filter plugins.
"""

from ._span import Span
from ._span import SpanType
from ._storage import RPCZ_CLIENT_REQ_SIZE_CTX_KEY
from ._storage import RPCZ_CLIENT_RSP_SIZE_CTX_KEY
from ._storage import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from ._storage import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from ._storage import RPCZ_SERVER_SPAN_CTX_KEY
from ._storage import get_all_spans
from ._storage import pop_all_spans
from ._storage import pop_sub_spans
from ._storage import prepare_server_span_for_dispatch
from ._storage import submit_server_span_on_dispatcher_post_io
from ._storage import submit_server_span_on_executor_post_io
from ._storage import submit_span
from ._storage import submit_sub_span
# Imported last: the filter package reads back from ``_span`` and
# ``_storage``, so those must already be bound here.
from .filter import RPCZ_CLIENT_SPAN_CTX_KEY
from .filter import HighLowWaterLevelSampler
from .filter import ProbabilitySampler
from .filter import RpczClientFilter
from .filter import RpczServerFilter
from .filter import SampleRateLimiter
from .filter import SpanIdGenerator

__all__ = [
    # Span model
    "Span",
    "SpanType",
    # Context keys
    "RPCZ_CLIENT_REQ_SIZE_CTX_KEY",
    "RPCZ_CLIENT_RSP_SIZE_CTX_KEY",
    "RPCZ_CLIENT_SPAN_CTX_KEY",
    "RPCZ_SERVER_REQ_SIZE_CTX_KEY",
    "RPCZ_SERVER_RSP_SIZE_CTX_KEY",
    "RPCZ_SERVER_SPAN_CTX_KEY",
    # Span containers
    "get_all_spans",
    "pop_all_spans",
    "pop_sub_spans",
    "submit_span",
    "submit_sub_span",
    # Submit points
    "prepare_server_span_for_dispatch",
    "submit_server_span_on_dispatcher_post_io",
    "submit_server_span_on_executor_post_io",
    # Filters
    "RpczClientFilter",
    "RpczServerFilter",
    # Sampling
    "HighLowWaterLevelSampler",
    "ProbabilitySampler",
    "SampleRateLimiter",
    "SpanIdGenerator",
]
