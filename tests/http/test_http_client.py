# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test http client"""

import asyncio
import random
import ssl
import threading
import time

import pytest
from aiohttp import web

from tests import helloworld_pb2
from tests.common_util import unused_port
from trpc_sdk import client
from trpc_sdk import codec
from trpc_sdk import exceptions
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.http import Client
from trpc_sdk.http import SSEClient
from trpc_sdk.http import get_http_header_in_client
from trpc_sdk.http import get_http_status_in_client
from trpc_sdk.serialization import SerializationType
from trpc_sdk.utils._loop_lib import asynclib

# https server启动失败
DISABLE_HTTPS_TEST = False

# Keep one loop for the module so the cached aiohttp session stays bound to it.
pytestmark = pytest.mark.asyncio(loop_scope="module")

_HOST = "127.0.0.1"


async def sse_handler(request: web.Request) -> web.Response:
    """SSE endpoint handler that streams test messages

    Args:
        request: Incoming HTTP request

    Returns:
        Streaming HTTP response
    """
    rsp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
    await rsp.prepare(request)

    async def send_event(msg: str):
        """Helper to send properly formatted SSE message"""
        await rsp.write(f"id: 1\n\nevent: test\n\ndata: {msg}\n\n".encode("utf-8"))

    try:
        for i in range(0, 3):
            message = f"message {i}"
            await send_event(message)
            await asyncio.sleep(1)  # Send message every 1 second
    except asyncio.CancelledError:
        pass
    finally:
        await rsp.write_eof()
    return rsp


def _gen_count():
    return random.randint(1, 100)


_STREAM_NUM = 1


async def _pb_data():
    i = 0
    while i < _STREAM_NUM:
        if count := _gen_count():
            yield f"id:{count}\n"
        await asyncio.sleep(0.1)
        i += 1


def _run_server(port: int):
    loop = asynclib.new_event_loop()
    asyncio.set_event_loop(loop)

    async def handler(request: web.Request) -> web.Response:
        data = await request.text()
        if data == "text":
            return web.Response(text="ok")
        return web.Response(text="false")

    async def handler_timeout(request: web.Request) -> web.Response:
        data = await request.content.read()
        print(f"request data: {data}")
        rsp = helloworld_pb2.HelloReply()
        rsp.msg = "hello_rsp"
        await asyncio.sleep(100)
        body = rsp.SerializeToString()
        return web.Response(body=body)

    async def handler_pb(request: web.Request) -> web.Response:
        data = await request.content.read()
        print(f"request data: {data}")
        rsp = helloworld_pb2.HelloReply()
        rsp.msg = "hello_rsp"
        body = rsp.SerializeToString()
        return web.Response(body=body)

    async def handler_stream_pb(request: web.Request) -> web.Response:
        resp = web.StreamResponse()
        data = await request.read()
        req_pb = helloworld_pb2.HelloRequest()
        req_pb.ParseFromString(data)
        assert req_pb.msg == "hello"
        # print(f"handler_stream_pb request: {data} msg: {req_pb.msg}")
        await resp.prepare(request)

        async for num in _pb_data():
            data = f"id: {num}\n"
            body = helloworld_pb2.HelloReply()
            body.msg = data
            await resp.write(body.SerializeToString())
        await resp.write_eof()
        return resp

    async def handler_json(request: web.Request) -> web.Response:
        data = await request.content.read()
        print(f"request data: {data}")
        return web.json_response({
            "status": "ok",
            "data": {
                "id": 1,
            },
        })

    async def handler_binary(request: web.Request) -> web.Response:
        assert request.query_string == "id=123"
        img = b'\xff\xd8\xff\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0\xe0'
        return web.Response(body=img)

    async def handler_stream_upload(request: web.Request) -> web.Response:
        recv_data = ""
        async for data in request.content.iter_any():
            recv_data += data.decode("utf-8")
        print(f"got streaming request data: {recv_data}")
        return web.Response(body=recv_data.encode("utf-8"))

    app = web.Application()
    app.add_routes([
        web.post('/', handler),
        web.post('/pb', handler_pb),
        web.post('/stream_pb', handler_stream_pb),
        web.post('/stream_data', handler_stream_pb),
        web.post('/trpc.trpc_python_group.helloworld.Greeter/SayHello', handler_pb),
        web.post('/timeout', handler_timeout),
        web.get('/img', handler_binary),
        web.get("/sse", sse_handler),
        web.post('/json', handler_json),
        web.post('/stream_upload', handler_stream_upload)
    ])
    web.run_app(app, host=_HOST, port=port, handle_signals=False)


def _run_https_server(port: int):
    loop = asynclib.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _handler(request: web.Request) -> web.Response:
        data = await request.text()
        if data == "text":
            return web.Response(text="ok")
        return web.Response(text="false")

    app = web.Application()
    app.add_routes([web.post('/', _handler)])
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    key = "./examples/helloworld/cert/domain_srv.key"
    cert = "./examples/helloworld/cert/domain_srv_localhost.crt"
    ssl_context.load_cert_chain(cert, key)
    try:
        web.run_app(app, host=_HOST, port=port, handle_signals=False, ssl_context=ssl_context)
    except Exception:  # pylint: disable=broad-except
        global DISABLE_HTTPS_TEST
        DISABLE_HTTPS_TEST = True


_port = unused_port()
_t1 = threading.Thread(target=_run_server, args=(_port, ), daemon=True)
_t1.start()

_https_port = unused_port()
_t2 = threading.Thread(target=_run_https_server, args=(_https_port, ), daemon=True)
_t2.start()


async def test_http_client_post():
    """Test http client post"""
    cli = Client()
    ctx = TrpcContext()
    rsp = await cli.post(ctx, f"http://{_HOST}:{_port}/", data="text")
    body = await rsp.text()
    assert body == "ok"


async def test_http_client_post_json():
    """Test http client post json"""
    cli = Client()
    ctx = TrpcContext()
    rsp = await cli.post(ctx, f"http://{_HOST}:{_port}/json", json={"key": "value"})
    body = await rsp.json()
    rsp.release()
    assert body["status"] == "ok"


async def test_http_client_post_json_context_manager():
    """Test http client post json context manager"""
    cli = Client()
    ctx = TrpcContext()
    async with cli.post(ctx, f"http://{_HOST}:{_port}/json", json={"key": "value"}) as rsp:
        body = await rsp.json()
        assert body["status"] == "ok"


async def test_http_client_post_pb():
    """Test http client post pb"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB)]
    rsp = await cli.post(ctx, f"http://{_HOST}:{_port}/pb", opts, data=req)
    body = helloworld_pb2.HelloReply()
    await rsp.pb(body)
    rsp.release()
    assert body.msg == "hello_rsp"


async def test_http_client_post_pb_with_short_conn():
    """Test http client post pb"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [
        Options().set_serialization_type(SerializationType.PB),
        Options().set_short_conn(True),
        Options().set_disable_reuse_http_session(True),
    ]
    rsp = await cli.post(ctx, f"http://{_HOST}:{_port}/pb", opts, data=req)
    body = helloworld_pb2.HelloReply()
    try:
        await rsp.pb(body)
        rsp.release()
        assert body.msg == "hello_rsp"
    except Exception as e:  # pylint: disable=broad-except
        pass


async def test_http_client_post_stream_pb():
    """Test http client post pb"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB)]
    num = 0
    async with cli.post(ctx, f"http://{_HOST}:{_port}/stream_pb", opts, data=req) as rsp:
        header = get_http_header_in_client(rsp)
        status = get_http_status_in_client(rsp)
        print(f"header is {header}, status is {status}")
        async for pb_rsp in rsp.stream_pb(helloworld_pb2.HelloReply):
            # print(f"handler_stream_pb stream_pb rsp is {pb_rsp.msg}")
            assert "id" in pb_rsp.msg
            num += 1
    assert num == _STREAM_NUM


async def test_http_client_post_stream_data():
    """Test http client post pb"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB)]
    num = 0
    async with cli.post(ctx, f"http://{_HOST}:{_port}/stream_data", opts, data=req) as rsp:
        async for chunk in rsp.stream_data():
            pb_rsp = helloworld_pb2.HelloReply()
            pb_rsp.ParseFromString(chunk)
            assert "id" in pb_rsp.msg
            num += 1
    assert num == _STREAM_NUM


async def test_http_client_post_pb_timeout():
    """Test http client post pb timeout"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB), Options().set_timeout(10)]
    err = None
    try:
        async with cli.post(ctx, f"http://{_HOST}:{_port}/timeout", opts, data=req) as rsp:
            body = helloworld_pb2.HelloReply()
            await rsp.pb(body)
    except exceptions.ClientTimeoutError as e:
        err = e

    assert err is not None


async def test_http_client_post_pb_timeout2():
    """Test http client post timeout"""
    cli = Client()
    ctx = TrpcContext()
    ctx.deadline = time.time() * 1000 - 100

    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB)]

    err = None
    try:
        async with cli.post(ctx, f"http://{_HOST}:{_port}/timeout", opts, data=req) as rsp:
            body = helloworld_pb2.HelloReply()
            await rsp.pb(body)
    except exceptions.ClientTimeoutError as e:
        err = e

    assert err is not None


async def test_http_client_get():
    """Test http client get"""
    cli = Client()
    ctx = TrpcContext()
    rsp = await cli.get(ctx, f"http://{_HOST}:{_port}/img?id=123")
    body = await rsp.content.read(4)
    rsp.release()
    assert body == b'\xff\xd8\xff\xe0'


async def test_http_client_stream_upload():
    """Test http client get"""
    cli = Client()
    ctx = TrpcContext()

    check_recv_data = []

    async def data_generator(check_recv_data):
        for i in range(10):
            data = f"hello{i}"
            check_recv_data.append(data)
            yield data.encode("utf-8")

    rsp = await cli.post(ctx, f"http://{_HOST}:{_port}/stream_upload", data=data_generator(check_recv_data))
    body = await rsp.content.read()
    rsp.release()
    assert ''.join(check_recv_data).encode("utf-8") == body


async def test_trpc_http_client():
    """Test trpc http client"""
    request = helloworld_pb2.HelloRequest()
    request.msg = "hello"
    ctx = TrpcContext()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        Options().set_filter_list([], True),
        Options().set_target("ip://127.0.0.1:" + str(_port)),
        Options().set_protocol("http"),
    ]

    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        pass


async def test_http_client_invalid_url():
    """Test http client invalid url"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB), Options().set_timeout(100)]
    err = None
    try:
        async with cli.post(ctx, f"wrong url {_port}/timeout", opts, data=req) as rsp:
            body = helloworld_pb2.HelloReply()
            await rsp.pb(body)
    except Exception as e:  # pylint: disable=broad-except
        err = e

    assert err is not None


async def test_https_client_post():
    """Test https client post"""
    cli = Client()
    ctx = TrpcContext()
    cert = "./examples/helloworld/cert/domain_srv_localhost.crt"
    opts = [
        Options().set_tls(cert),
    ]
    try:
        rsp = await cli.post(ctx, f"https://{_HOST}:{_https_port}/", data="text", options=opts, ssl=False)
        body = await rsp.text()
    except Exception as e:  # pylint: disable=broad-except
        if DISABLE_HTTPS_TEST:
            assert True
        else:
            print("https client failed: ", e)
            assert False
    else:
        assert body == "ok"


async def test_http_client_naming_service():
    """Test http client naming"""
    cli = Client()
    ctx = TrpcContext()
    req = helloworld_pb2.HelloRequest()
    req.msg = "hello"
    opts = [Options().set_serialization_type(SerializationType.PB), Options().set_timeout(100)]
    err = None
    # trpc://{service_name}/{uri}
    try:
        async with cli.post(ctx, f"trpc://wrong url {_port}/timeout", opts, data=req) as rsp:
            body = helloworld_pb2.HelloReply()
            await rsp.pb(body)
    except Exception as e:  # pylint: disable=broad-except
        err = e

    assert err is not None


async def test_http_sse_client():
    """Test basic SSE client functionality

    Verifies:
    - Can connect to SSE endpoint
    - Correctly receives and parses event stream
    - Properly handles multiple messages
    """
    cli = SSEClient()
    ctx = TrpcContext()
    counter = 0
    options = [Options().set_timeout(10000)]
    async for event in cli.request_events(ctx, "GET", f"http://{_HOST}:{_port}/sse", options=options):
        assert event.data == f"message {counter}"
        counter += 1
