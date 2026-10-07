# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Filter-chain half of rpcz: sampling and span creation.

Importing this package registers ``RpczServerFilter`` and
``RpczClientFilter`` under the plugin name ``"rpcz"``, which is what a
service YAML refers to in its ``filter`` list.

The submit helpers from :mod:`trpc_sdk.rpcz._storage` are re-exported
here because a filter is the usual place to call them from.
"""

from .._storage import RPCZ_SERVER_SPAN_CTX_KEY
from .._storage import pop_sub_spans
from .._storage import submit_span
from .._storage import submit_sub_span
from ._client_filter import RPCZ_CLIENT_SPAN_CTX_KEY
from ._client_filter import RpczClientFilter
from ._sampler import HighLowWaterLevelSampler
from ._sampler import ProbabilitySampler
from ._sampler import SampleRateLimiter
from ._sampler import SpanIdGenerator
from ._server_filter import RpczServerFilter

__all__ = [
    # Filters
    "RpczClientFilter",
    "RpczServerFilter",
    # Context keys
    "RPCZ_CLIENT_SPAN_CTX_KEY",
    "RPCZ_SERVER_SPAN_CTX_KEY",
    # Sampling
    "HighLowWaterLevelSampler",
    "ProbabilitySampler",
    "SampleRateLimiter",
    "SpanIdGenerator",
    # Storage helpers used from the filter chain
    "pop_sub_spans",
    "submit_span",
    "submit_sub_span",
]
