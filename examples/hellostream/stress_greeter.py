# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Stream handlers for the hellostream stress client.

Selected by ``HELLOSTREAM_STRESS=1`` in ``trpc_main``. Replies carry no
per-frame log line. Each reply is exactly ``pid=<pid> tid=<tid> n=<n>`` so
the client can check order and which process and thread ran the handler.

``n`` is the request index echoed from ``SayHelloAll`` and ``SayHelloRsp``,
and the number of data frames received for ``SayHelloReq``.
"""

from __future__ import annotations

import os
import re
import sys
import threading

from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger

_STUB = os.path.join(os.path.dirname(__file__), "stub")
if _STUB not in sys.path:
    sys.path.insert(0, _STUB)

from trpc_py_stream_helloworld import pb  # noqa: E402  # pyright: ignore[reportMissingImports]
from trpc_py_stream_helloworld import rpc  # noqa: E402  # pyright: ignore[reportMissingImports]
from trpc_py_stream_helloworld import rpc_stream  # noqa: E402  # pyright: ignore[reportMissingImports]

# Leading ``n=<int>`` on a bidi request. The rest of the name is padding.
_N_RE = re.compile(r"^n=(\d+)")
# Leading ``count=<int>`` on a server-stream request.
_COUNT_RE = re.compile(r"^count=(\d+)")


def _stamp(n: int) -> str:
    """Return the reply body for one frame.

    Args:
        n: Sequence index, or a frame count for the client-stream close.

    Returns:
        ``pid=<pid> tid=<tid> n=<n>`` for the calling thread.
    """
    return f"pid={os.getpid()} tid={threading.get_ident()} n={n}"


def _request_n(name: str) -> int:
    """Read the ``n=<int>`` prefix of a request name.

    Args:
        name: ``HelloRequest.name`` sent by the stress client.

    Returns:
        The parsed index, or ``-1`` when the prefix is missing.
    """
    match = _N_RE.match(name)
    if match is None:
        return -1
    return int(match.group(1))


def _reply_count(name: str) -> int:
    """Read how many server-stream replies to send.

    Args:
        name: ``HelloRequest.name``, expected to start with ``count=<int>``.

    Returns:
        The parsed count, or ``0`` when the prefix is missing.
    """
    match = _COUNT_RE.match(name)
    if match is None:
        return 0
    return int(match.group(1))


class StressGreeter(rpc.GreeterServicer):
    """Echo stream handlers with no sleep and no per-frame logging."""

    # pylint: disable=invalid-name
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Echo ``request.name`` for a unary call on the stress process.

        A stress process still registers this method, so a stray unary call
        does not fall through to ``NotImplementedError``.

        Args:
            ctx: Server context. The reply does not read it.
            request: Unary request.

        Returns:
            A reply whose ``message`` is ``request.name``.
        """
        del ctx
        reply = pb.HelloReply()
        reply.message = request.name
        return reply

    # pylint: disable=invalid-name
    async def SayHelloReq(self, stream: rpc_stream.GreeterServerStream.SayHelloReq) -> None:
        """Count data frames and close with that count.

        Args:
            stream: Client-streaming call. ``recv`` raises ``EOFError`` when
                the client has closed its sends.
        """
        count = 0
        while True:
            try:
                await stream.recv()
            except EOFError:
                reply = pb.HelloReply()
                reply.message = _stamp(count)
                await stream.send_and_close(reply)
                return
            except Exception as err:  # pylint: disable=broad-except
                logger.error("stress SayHelloReq failed: %s", err)
                return
            count += 1

    # pylint: disable=invalid-name
    async def SayHelloRsp(self, request: pb.HelloRequest, stream: rpc_stream.GreeterServerStream.SayHelloRsp) -> None:
        """Send ``count=<n>`` replies immediately.

        Args:
            request: Opening request. ``name`` starts with ``count=<n>``.
            stream: Server-streaming call.
        """
        for index in range(_reply_count(request.name)):
            reply = pb.HelloReply()
            reply.message = _stamp(index)
            await stream.send(reply)

    # pylint: disable=invalid-name
    async def SayHelloAll(self, stream: rpc_stream.GreeterServerStream.SayHelloAll) -> None:
        """Echo each data frame with this process and thread stamped on it.

        Args:
            stream: Bidi call. ``recv`` raises ``EOFError`` when the client
                has closed its sends.
        """
        while True:
            try:
                request = await stream.recv()
            except EOFError:
                return
            except Exception as err:  # pylint: disable=broad-except
                logger.error("stress SayHelloAll failed: %s", err)
                return
            reply = pb.HelloReply()
            reply.message = _stamp(_request_n(request.name))
            await stream.send(reply)
