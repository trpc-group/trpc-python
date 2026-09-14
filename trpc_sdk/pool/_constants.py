# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants used by the connection-pool package.

:class:`ConnectionState` is the lifecycle of one real multiplexed TCP
connection in :mod:`trpc_sdk.pool._multiplexed`. Values are process-local
only; they never reach the wire.

:data:`DEFAULT_STREAM_LIMIT` is the default :class:`StreamReader`
buffer half-limit used by :mod:`trpc_sdk.pool._stream_io` and by
:func:`~trpc_sdk.pool.open_connection` /
:func:`~trpc_sdk.pool._tcp_connection.open_unix_connection`.
"""

from __future__ import annotations

from enum import IntEnum
from enum import unique


@unique
class ConnectionState(IntEnum):
    """Lifecycle state of one real multiplexed connection.

    ``IntEnum`` keeps the historical integer comparisons valid without a
    cast. :attr:`RECONNECTING` is reserved: current code never writes it.
    """

    # Socket has been allocated but :meth:`MultiplexedConnection.start`
    # has not finished dialing yet.
    INITIAL = 0
    # TCP is up and :meth:`MultiplexedConnection.recv_msg` is running.
    CONNECTED = 1
    # Reserved for a future reconnect path; never assigned today.
    RECONNECTING = 2
    # :meth:`MultiplexedConnection.close` is broadcasting the error to
    # in-flight virtual connections.
    CLOSING = 3
    # Socket is gone; :meth:`ConnectionGroup.clear_invalid_conn` drops
    # this entry from the per-address list.
    CLOSED = 4


# Half the StreamReader buffer limit; feed_data pauses the transport at
# 2 * this value. Also the default read buffer for open_connection /
# open_unix_connection.
DEFAULT_STREAM_LIMIT = 2**16  # 64 KiB
