# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client for the three hellostream shapes.

``python test_service_rpc.py [--port N]`` talks to a server that is already
listening. It does not start one. The layout sweep is ``check_modes.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_py_stream_helloworld import pb  # noqa: E402
from trpc_py_stream_helloworld import rpc  # noqa: E402

_SERVICE = "trpc.py_stream.helloworld.Greeter"


def _options(port: int) -> Options:
    """Build call options that do not load YAML and do not enable stream filters.

    Args:
        port: TCP port of the already-running server.

    Returns:
        Options whose target, protocol, and timeout are set for one call.
    """
    return (Options().set_target(f"ip://127.0.0.1:{port}").set_protocol("trpc").set_serialization_type(
        SerializationType.PB).set_network("tcp").set_timeout(15000))


def _proxy() -> rpc.GreeterClientProxyImpl:
    """Return a proxy bound to the generated service name."""
    return rpc.GreeterClientProxyImpl(_SERVICE)


async def say_hello_req(port: int) -> None:
    """Send two requests on a client stream and expect one closing reply.

    Args:
        port: TCP port of the already-running server.

    Raises:
        AssertionError: The closing reply is not ``SayHelloReq done``.
    """
    stream = await _proxy().asyncSayHelloReq(TrpcContext(), _options(port))
    if stream is None:
        raise AssertionError("SayHelloReq returned no stream")
    for index in range(1, 3):
        request = pb.HelloRequest()
        request.name = f"trpc-python {index}"
        await stream.send(request)
    reply = await stream.close_and_recv()
    if reply.message != "SayHelloReq done":
        raise AssertionError(f"SayHelloReq reply {reply.message!r}")


async def say_hello_rsp(port: int) -> None:
    """Open a server stream and expect ``rsp: 0``, ``rsp: 1``, ``rsp: 2``.

    The server sleeps one second between sends, so the call timeout is 15
    seconds.

    Args:
        port: TCP port of the already-running server.

    Raises:
        AssertionError: The reply sequence is not those three messages
            followed by end-of-stream.
    """
    request = pb.HelloRequest()
    request.name = "trpc-python"
    stream = await _proxy().asyncSayHelloRsp(TrpcContext(), request, _options(port))
    if stream is None:
        raise AssertionError("SayHelloRsp returned no stream")
    received = []
    while True:
        try:
            reply = await stream.recv()
        except EOFError:
            break
        received.append(reply.message)
    if received != ["rsp: 0", "rsp: 1", "rsp: 2"]:
        raise AssertionError(f"SayHelloRsp replies {received}")


async def say_hello_all(port: int) -> None:
    """Send two requests on a bidi stream and expect two ``rsp data`` replies.

    Args:
        port: TCP port of the already-running server.

    Raises:
        AssertionError: The reply count or text does not match the two sends.
    """
    stream = await _proxy().asyncSayHelloAll(TrpcContext(), _options(port))
    if stream is None:
        raise AssertionError("SayHelloAll returned no stream")
    for index in range(1, 3):
        request = pb.HelloRequest()
        request.name = f"trpc-python {index}"
        await stream.send(request)
    await stream.close_send()
    received = []
    while True:
        try:
            reply = await stream.recv()
        except EOFError:
            break
        received.append(reply.message)
    if received != ["rsp data", "rsp data"]:
        raise AssertionError(f"SayHelloAll replies {received}")


async def run_shapes(port: int) -> None:
    """Run client-stream, server-stream, and bidi against one port.

    Args:
        port: TCP port of the already-running server.
    """
    await say_hello_req(port)
    await say_hello_rsp(port)
    await say_hello_all(port)


def main(argv: list[str] | None = None) -> None:
    """Talk to an already-running server. Default port is 8000.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Call a running hellostream server")
    parser.add_argument("--port", type=int, default=8000, help="TCP port of the running server")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    asyncio.run(run_shapes(args.port))


if __name__ == "__main__":
    main()
