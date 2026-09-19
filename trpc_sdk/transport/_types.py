# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the server-side transport package."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import IntEnum
from enum import unique
from typing import Optional

from trpc_sdk._inner import trpc_pb2


@dataclass
class ClientConnectionInfo:
    """One live client TCP connection as seen by the dispatcher.

    Stored in ``app.client_connections`` keyed by the kernel fd. Workers never
    hold this object: they only send ``fd`` and ``session_id`` back over IPC.
    The dispatcher looks up the entry by fd, then checks ``session_id`` before
    writing or closing, because the kernel can reuse the same fd for a later
    client.

    Example: client A is ``client_connections[42] == (session_id=7, transport)``.
    After A closes, client B may get fd 42 with session_id 8. A late worker
    response tagged ``(42, 7)`` must not be written to B.
    """

    # Connection-lifetime id; must match the worker's IPC tag before write/close.
    session_id: int
    # asyncio Transport wrapping the accepted client socket; used to write/close.
    conn: asyncio.Transport


@dataclass
class SchedulerParams:
    """Inputs used to pick which worker should handle one complete frame.

    Built on the dispatcher after a frame is split out, before the frame is
    written to a worker IPC pipe. Custom schedulers registered with
    ``register_dispatcher_worker_func(..., is_scheduler=True)`` receive this
    object; the default unary path only needs ``worker_conn_num``.
    """

    # Number of live worker IPC connections the dispatcher can write to.
    worker_conn_num: int
    # Client IP, for IP-hash custom schedulers; empty when the peer is unknown.
    remote_ip: str
    # Connection-lifetime id of the client TCP that produced this frame.
    session_id: str
    # Frame-head StreamID: stream id for streaming RPC, request id for unary.
    stream_id: str
    # Complete frame bytes; unused by the default scheduler.
    package: Optional[bytes] = None
    # tRPC frame head FrameType. Per tRPC wire protocol the StreamID field is
    # reused: for streaming RPC it carries the stream id, for unary RPC it
    # carries the request id (used by multiplexed-connection clients for
    # response matching). Therefore ``stream_id != 0`` alone cannot tell
    # whether a frame belongs to a streaming RPC — the authoritative flag is
    # FrameType.
    frame_type: int = trpc_pb2.TRPC_UNARY_FRAME  # 0 = UNARY, 1 = STREAM


@unique
class UDPPackageType(IntEnum):
    """Flag values written into ``DataHeader.flags`` for C-dispatcher UDP IPC.

    ``feeds_data`` and ``inner_encode_data_header_rsp`` exchange these as C ``uint``.
    ``IntEnum`` keeps ``package_flag == UDPPackageType.COMPLETE`` true when
    the Cython side returns a plain ``int``.
    """

    # Payload fits in one datagram.
    COMPLETE = 0
    # Intermediate slice of a payload larger than one UDP body.
    CHUNK = 2
    # Last slice; the peer concatenates by session id and then handles it.
    CHUNK_END = 6
