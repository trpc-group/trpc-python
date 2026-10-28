# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client for the user-defined plugin example.

SayHello must come back as ``hello: ping token=secret``. SayHi does not
read the plugin, so its reply stays ``hi: ping``.
"""

from __future__ import annotations

import asyncio
import os
import sys

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.config import TrpcConfig
from trpc_sdk.serialization import SerializationType

_HERE = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(_HERE, "../helloworld/stub")))

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

_TARGET = "ip://127.0.0.1:8200"
_HELLO = "hello: ping token=secret"
_HI = "hi: ping"


def _options() -> Options:
    """Return client options aimed at this example's Greeter."""
    return (Options().set_target(_TARGET).set_protocol("trpc").set_serialization_type(
        SerializationType.PB).set_network("tcp").set_namespace("Development").set_timeout(3000))


async def test_say_hello() -> None:
    """Call SayHello and require the plugin token in the reply."""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = "ping"
    ret = await proxy.asyncSayHello(TrpcContext(), req, _options())
    print(f"\n  SayHello recv result is {ret.msg}\n")
    assert ret.msg == _HELLO


async def test_say_hi() -> None:
    """Call SayHi and require a reply that does not include the token."""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = "ping"
    ret = await proxy.asyncSayHi(TrpcContext(), req, _options())
    print(f"\n  SayHi recv result is {ret.msg}\n")
    assert ret.msg == _HI


async def run_all() -> None:
    """Run SayHello and SayHi together."""
    await asyncio.gather(test_say_hello(), test_say_hi())


def main() -> None:
    """Load this example's YAML, then run both calls."""
    TrpcConfig.load(os.path.join(_HERE, "trpc_python.yaml"), "utf-8")
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
