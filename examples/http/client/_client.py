# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""test client module"""

import os
import sys
import asyncio
from trpc_sdk.abc import TrpcContext
from trpc_sdk.serialization import SerializationType
from trpc_sdk.http import Client as HttpClient
from trpc_sdk.http import get_http_header_in_client
from trpc_sdk.http import get_http_status_in_client

# add the stub to the path
current_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
parent_path = os.path.abspath(os.path.join(current_path, "../"))
rpc_path = os.path.abspath(os.path.join(parent_path, "rpc/stub"))
sys.path.append(rpc_path)

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc
from trpc_sdk.client import Options


def get_addr(ctx: TrpcContext):
    """Print the addresses stored on ``ctx``.

    HTTP ``post`` copies them back onto the caller's context. An RPC stub
    invokes a clone, so these fields may stay empty after ``SayHello``.
    """
    print(f"ctx remote_addr: {ctx.remote_ip}:{ctx.remote_port}")
    print(f"ctx local_ip: {ctx.local_ip}:{ctx.local_port}")


async def test_async_say_hello(port: int = 8001):
    """test_async_say_hello
    :return:
    """
    proxy = rpc.GreeterClientProxyImpl("trpc.trpc_python_group.helloworld.Greeter")
    req = pb.HelloRequest()
    req.msg = 'hello'
    options = [
        Options().set_target(f'ip://127.0.0.1:{port}'),
        # Options().set_short_conn(True),
        Options().set_serialization_type(SerializationType.PB),
        Options().set_network('tcp'),
        Options().set_protocol("http"),
        Options().set_namespace('Development'),
        Options().set_timeout(3000)
    ]
    ctx = TrpcContext()
    ret = await proxy.asyncSayHello(ctx, req, options)
    get_addr(ctx)
    print(f"\n  test_async_say_hello recv result is {ret}\n")
    if ret.msg != "hello: hello":
        raise AssertionError(f"SayHello msg {ret.msg!r}, expected 'hello: hello'")


async def test_async_say_hi(port: int = 8001):
    """test_async_say_hi
    :return:
    """
    proxy = rpc.GreeterClientProxyImpl("trpc.trpc_python_group.helloworld.Greeter")
    req = pb.HelloRequest()
    req.msg = 'hello'
    options = Options().set_target(f'ip://127.0.0.1:{port}').set_serialization_type(SerializationType.PB).set_network('tcp').set_protocol('http').set_namespace('Development').set_timeout(3000)
    ctx = TrpcContext()
    ret = await proxy.asyncSayHi(ctx, req, options)

    get_addr(ctx)
    print(f"\n  test_async_say_hi recv result is {ret}\n")
    if ret.msg != "hi: hello":
        raise AssertionError(f"SayHi msg {ret.msg!r}, expected 'hi: hello'")


async def test_http_client_post(url: str = "127.0.0.1:8001/test"):
    """Test http client post"""
    cli = HttpClient()
    ctx = TrpcContext()
    stream = False
    rsp = await cli.post(ctx, f"http://{url}", data="text", stream=stream)
    if stream:
        body = await rsp.text()
    else:
        body = rsp.body
    header = get_http_header_in_client(rsp)
    status = get_http_status_in_client(rsp)
    get_addr(ctx)
    print(f"\n  test_http_client_post stream : {stream} header: {header}, status: {status} recv result is {body}\n")
    if status != 200:
        raise AssertionError(f"{url} status {status}, body {body!r}")
    path = url[url.find("/"):] if "/" in url else "/"
    if path == "/route_with_rsp_header":
        if body != "user-defined test":
            raise AssertionError(f"{url} body {body!r}, expected 'user-defined test'")
        test_header = header.get("test") if header is not None else None
        if test_header != "data":
            raise AssertionError(f"{url} header test={test_header!r}, expected 'data'")


HTTP_TEST = [
    test_async_say_hello(port=9002),
    test_async_say_hi(port=9002),
    test_http_client_post("127.0.0.1:9002/normal_http"),
    test_http_client_post("127.0.0.1:9002/route_with_rsp_header"),
    test_http_client_post("127.0.0.1:9003/bind_service_name"),
    test_http_client_post("127.0.0.1:9003/route_with_rsp_header"),
    test_http_client_post("127.0.0.1:9003/normal_http"),
    test_http_client_post("127.0.0.1:9003/route_with_rsp_header"),
]


async def test_http_client():
    """Test http client"""
    print(f"\n=== test_http_client start ===\n")
    await asyncio.gather(*HTTP_TEST)
    print(f"\n=== test_http_client end ===\n")
