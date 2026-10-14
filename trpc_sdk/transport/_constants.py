# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants used by both halves of the transport package.

The listen backlog lives in :data:`trpc_sdk.utils.DEFAULT_BACKLOG` next to
the listen-socket constructors. The IPC frame constants are the single
source of truth for the Cython inner-IPC codec in
:mod:`trpc_sdk.transport._cython._ipc_codec`, which copies them into typed
``cdef`` locals at module import.

The client-side request enums live here too:
:class:`RequestType` (unary vs one-way) and :class:`ClientTimeoutReason`
(who caused a client timeout).
:class:`~trpc_sdk.transport.RoundTripOptions` reads them; they are not
options themselves.
"""

from __future__ import annotations

import struct
from enum import IntEnum
from enum import unique

# Inner IPC frame: big-endian ushort magic + uint32 total length.
INNER_MAGIC_VALUE = 1314
FRAME_HEAD_LEN = struct.calcsize("!HI")

# Initial growable buffer size for the Cython stream IPC parser.
DEFAULT_MEM_SIZE = 1024


@unique
class RequestType(IntEnum):
    """Whether a client call waits for a response.

    ``SEND_AND_RECV`` is the unary path. ``SEND_ONLY`` writes the request and
    returns without waiting, used by one-way RPCs. ``IntEnum`` keeps
    integer comparisons valid without a cast.
    """

    SEND_AND_RECV = 0
    SEND_ONLY = 1


@unique
class ClientTimeoutReason(IntEnum):
    """Who ran out of time when a client call times out.

    ``DOWNSTREAM_TIMEOUT`` means the callee did not answer in time.
    ``CURRENT_SERVICE_TIMEOUT`` means this service's own remaining budget
    expired first (the parent RPC already had a tighter deadline).
    """

    DOWNSTREAM_TIMEOUT = 1
    CURRENT_SERVICE_TIMEOUT = 2
