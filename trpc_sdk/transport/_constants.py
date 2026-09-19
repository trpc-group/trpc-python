# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants used by both halves of the transport package.

Listen-socket defaults live here so :mod:`trpc_sdk.transport._socket` and any
future listener code share one backlog. The IPC frame constants are the
single source of truth for the Cython inner-IPC codec in
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

# backlog passed to listen() for TCP and UNIX stream sockets created by
# create_server_socket(). 1500 matches the historical net_util default.
DEFAULT_BACKLOG = 1500

# Inner IPC frame: big-endian ushort magic + uint32 total length.
INNER_MAGIC_VALUE = 1314
FRAME_HEAD_LEN = struct.calcsize("!HI")

# Initial growable buffer size for the Cython stream IPC parser.
DEFAULT_MEM_SIZE = 1024


@unique
class RequestType(IntEnum):
    """Whether a client call waits for a response.

    ``SendAndRecv`` is the unary path. ``SendOnly`` writes the request and
    returns without waiting, used by one-way RPCs. ``IntEnum`` keeps
    integer comparisons valid without a cast.
    """

    SendAndRecv = 0
    SendOnly = 1


@unique
class ClientTimeoutReason(IntEnum):
    """Who ran out of time when a client call times out.

    ``DownstreamTimeout`` means the callee did not answer in time.
    ``CurrentServiceTimeout`` means this service's own remaining budget
    expired first (the parent RPC already had a tighter deadline).
    """

    DownstreamTimeout = 1
    CurrentServiceTimeout = 2
