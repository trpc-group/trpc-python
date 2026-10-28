# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Hand-rolled unary tRPC frames for the connection-affinity checks.

The public client pools sockets, so it cannot show that one TCP connection
stays on a single merge thread, or that one connection's requests spread
across separate handle threads. These helpers open the socket themselves.
The header layout matches ``tests/server/_e2e_support.py``.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk._inner.pb import trpc_pb2

from trpc_trpc_python_group_helloworld import pb

_HEADER = struct.Struct(">HBBIHIH")


def encode(request_id: int, msg: str, service: str, callee: Optional[str] = None) -> bytes:
    """Build one unary request frame.

    Args:
        request_id: Frame request id. Replies are matched on this.
        msg: ``HelloRequest.msg`` payload.
        service: Service name in the method path. ``fix_rpc_name`` makes the
            server expect ``/<service>/SayHello``.
        callee: Callee written into the request head. Defaults to ``service``.
            A separate-mode server answers with the listen address's own
            service name, so a checker can put a different callee here and
            still see that address's name come back.

    Returns:
        One complete tRPC frame.
    """
    head = trpc_pb2.RequestProtocol(request_id=request_id,
                                    callee=(service if callee is None else callee).encode(),
                                    func=f"/{service}/SayHello".encode(),
                                    timeout=10000).SerializeToString()
    body = pb.HelloRequest(msg=msg).SerializeToString()
    total = 16 + len(head) + len(body)
    return _HEADER.pack(0x930, 0, 0, total, len(head), 0, 0) + head + body


def decode(frame: bytes) -> Tuple[int, int, str]:
    """Decode one unary response frame.

    Args:
        frame: A complete frame, header included.

    Returns:
        ``(request_id, ret, reply.msg)``.
    """
    _, _, _, total, head_len, _, _ = _HEADER.unpack(frame[:16])
    head = trpc_pb2.ResponseProtocol()
    head.ParseFromString(frame[16:16 + head_len])
    reply = pb.HelloReply()
    reply.ParseFromString(frame[16 + head_len:total])
    return head.request_id, head.ret, reply.msg


async def read_frame(reader: asyncio.StreamReader, timeout: float = 15) -> Tuple[int, int, str]:
    """Read one response frame from ``reader``.

    Args:
        reader: Connected stream.
        timeout: Seconds for the whole frame.

    Returns:
        :func:`decode` of the frame.
    """
    head = await asyncio.wait_for(reader.readexactly(16), timeout)
    total = struct.unpack(">I", head[4:8])[0]
    rest = await asyncio.wait_for(reader.readexactly(total - 16), timeout)
    return decode(head + rest)


async def tcp_call(port: int, service: str, request_id: int, msg: str) -> Tuple[int, int, str]:
    """Send one request on its own TCP connection.

    Args:
        port: Server port on ``127.0.0.1``.
        service: Callee service name.
        request_id: Frame request id.
        msg: ``HelloRequest.msg``.

    Returns:
        ``(request_id, ret, reply.msg)``.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(encode(request_id, msg, service))
        await writer.drain()
        return await read_frame(reader)
    finally:
        writer.close()


async def tcp_calls(port: int, service: str, msgs: List[str]) -> List[Tuple[int, int, str]]:
    """Open one connection per message and wait for every reply.

    Args:
        port: Server port.
        service: Callee service name.
        msgs: One ``HelloRequest.msg`` per connection. Request ids start at 1.

    Returns:
        Replies in the same order as ``msgs``.
    """
    tasks = [asyncio.ensure_future(tcp_call(port, service, i + 1, msg)) for i, msg in enumerate(msgs)]
    return list(await asyncio.gather(*tasks))


async def tcp_pipelined(port: int, service: str, msgs: List[str]) -> Dict[int, Tuple[int, str]]:
    """Write every request on one connection before reading replies.

    Args:
        port: Server port.
        service: Callee service name.
        msgs: Payloads. Request ids start at 1.

    Returns:
        ``request_id -> (ret, reply.msg)``.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"".join(encode(i + 1, msg, service) for i, msg in enumerate(msgs)))
        await writer.drain()
        replies: Dict[int, Tuple[int, str]] = {}
        for _ in msgs:
            request_id, ret, reply = await read_frame(reader)
            replies[request_id] = (ret, reply)
        return replies
    finally:
        writer.close()
