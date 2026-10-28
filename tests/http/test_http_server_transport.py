# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test cases for HTTP server transport module"""

import asyncio
import base64
import json
import socket

from typing import Callable
from unittest import mock

import pytest

from aiohttp import test_utils as aiohttp_test_utils
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from trpc_sdk import http as http_defines
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.runtime import ResourceKind
from trpc_sdk.runtime import ThreadModel
from trpc_sdk.runtime import use_thread_model
from trpc_sdk.abc import TrpcContext as Context
from trpc_sdk.abc import get_message
from trpc_sdk.http import HttpRoute as HttpHandle
from trpc_sdk.http import HttpServerTransport
from trpc_sdk.http import HttpServerTransportFactory
from trpc_sdk.http import get_http_request
from trpc_sdk.http import get_http_response
from trpc_sdk.http import register_user_http_decode_func
from trpc_sdk.http import _routes


def test_http_server_transport():
    """bind keeps the configured network; HTTP is never forwarded over a pipe."""
    transport = HttpServerTransportFactory().create()
    opts = BaseServerTransportOption()

    transport.bind(opts)

    assert transport.opts is opts
    assert opts.network == "tcp"
    assert transport.ipc_forwardable is False


def test_http_listen_serves_the_runner_on_one_loop(monkeypatch):
    """On a single-reactor model the runner is served on that loop and kept on the model to clean up."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", port))
    transport = HttpServerTransportFactory().create()
    transport.bind(opts)

    def _app():
        app = web.Application()
        app.router.add_get("/", lambda request: web.Response(text="ok"))
        return app

    monkeypatch.setattr(transport, "_build_app", _app)

    async def _get():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        data = await reader.read()
        writer.close()
        return data

    loop = asyncio.new_event_loop()
    model = ThreadModel.single(loop)
    reactor = model.acceptor
    try:
        with use_thread_model(model):
            transport.listen()
        assert loop.run_until_complete(_get()).endswith(b"ok")

        transport.stop()
        assert model.pop_resources(reactor, ResourceKind.ENDPOINT) == []
        runners = model.pop_resources(reactor, ResourceKind.RUNNER)
        assert len(runners) == 1
        model.keep_resource(reactor, runners[0], ResourceKind.RUNNER)
        loop.run_until_complete(model.release_resources(reactor, ResourceKind.RUNNER))
        assert model.pop_resources(reactor) == []
        loop.run_until_complete(transport.destroy())
    finally:
        loop.close()


class _UserHttpHandler:
    """Mock HTTP handler for testing user-defined handlers"""

    async def handle_http(self, ctx, usr_handle: Callable):
        """Test function for handling HTTP requests
        
        Args:
            ctx: Context object
            usr_handle: User-defined handler function
            
        Returns:
            Result from user handler
        """
        req = get_http_request(ctx)
        return await usr_handle(ctx, req)


async def _rpc_handle(ctx, data: bytes):
    """Mock tRPC handler of the catch-all route.

    Args:
        ctx: Context object
        data: Request data

    Returns:
        Test response bytes
    """
    assert not data
    assert ctx.remote_ip == "127.0.0.1"
    assert ctx.timeout == 1000
    req = get_http_request(ctx)
    assert req
    rsp = get_http_response(ctx)
    assert rsp is not None
    is_base64 = ctx.get_meta_data(http_defines.TRANS_INFO_BASE64_KEY, False)
    assert is_base64 is False
    local_addr = ctx.get_meta_data(http_defines.LOCAL_ADDR, "")
    assert local_addr == "127.0.0.1:8008"
    return b'test'


def _transport(is_base64: bool = False) -> HttpServerTransport:
    """An HTTP transport bound to mock options, as :meth:`HttpServerTransport.listen` leaves it."""
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", 8008))
    opts.set_idle_timeout(1000)
    opts.rpc_handler = _rpc_handle
    opts.set_service_adapter(_UserHttpHandler())
    transport = HttpServerTransportFactory().create()
    transport.bind(opts)
    transport._is_base64 = is_base64
    return transport


@pytest.fixture(autouse=True)
def _default_route_decode(monkeypatch):
    monkeypatch.setattr(_routes, "_user_http_decode_cb", None)


@pytest.mark.asyncio
async def test_handle_http_request():
    """The catch-all route runs the tRPC handler and writes its result as the body."""
    transp = mock.Mock()
    transp.get_extra_info.return_value = ("127.0.0.1", 1234)
    req = aiohttp_test_utils.make_mocked_request("GET", "/handle_http_request", transport=transp, headers={})
    rsp = await _transport()._handle_rpc(req)
    assert rsp.body == b'test'


async def _user_route_with_rsp(ctx: Context, req: Request) -> Response:
    """Test route handler that returns Response object
    
    Args:
        ctx: Context object
        req: Request object (unused)
        
    Returns:
        Response object
    """
    rsp = get_http_response(ctx)
    is_base64 = ctx.get_meta_data(http_defines.TRANS_INFO_BASE64_KEY, False)
    msg = get_message(ctx)
    if is_base64:
        assert msg.server_meta_data["a"] == "aa"
        assert msg.server_meta_data["b"] == "cc"
    else:
        assert msg.server_meta_data["a"] == "b"
        assert msg.server_meta_data["c"] == "d"
    rsp.body = "user_route_with_rsp"
    return rsp


async def _user_route_with_none(ctx: Context, req: Request) -> Response:
    """Test route handler that returns None
    
    Args:
        ctx: Context object
        req: Request object (unused)
        
    Returns:
        None
    """
    rsp = get_http_response(ctx)
    rsp.body = "user_route_with_none"
    return None


async def _user_route_bytes(ctx: Context, req: Request) -> bytes:
    """Test route handler that returns bytes
    
    Args:
        ctx: Context object (unused)
        req: Request object (unused)
        
    Returns:
        Response bytes
    """
    return b"user_route_bytes"


async def _user_route_dict(ctx: Context, req: Request) -> dict:
    """Test route handler that returns dict
    
    Args:
        ctx: Context object
        req: Request object (unused)
        
    Returns:
        Response dictionary
    """
    rsp = get_http_response(ctx)
    rsp.body = "user_route_dict"
    return {"a": 1}


@pytest.mark.asyncio
async def test_handle_user_http_request():
    """Test handling user-defined HTTP requests with various handlers"""
    transp = mock.Mock()
    transp.get_extra_info.return_value = ("127.0.0.1", 1234)
    headers = {}
    headers[http_defines.HTTP_TRPC_HEADER.trpc_request_id] = "1"
    headers[http_defines.HTTP_TRPC_HEADER.trpc_timeout] = "100"
    headers[http_defines.HTTP_TRPC_HEADER.trpc_caller_method] = "SayHello"
    headers[http_defines.HTTP_TRPC_HEADER.trpc_caller] = "trpc_sdk.test.server.http_service"
    headers[http_defines.HTTP_TRPC_HEADER.trpc_callee] = "trpc_sdk.test.server.service"

    service_name = "trpc_sdk.test.server.service"

    # Test with base64 encoded trans info
    trans_info = {"a": "aa", "b": "cc"}
    for k, v in trans_info.items():
        trans_info[k] = base64.b64encode(v.encode()).decode()
    headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)

    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_rsp", transport=transp, headers=headers)
    user_route_func_str_handle = HttpHandle(_user_route_with_rsp, "GET", "/user_route_with_rsp", service_name)
    ret = await _transport(True)._handle_route(user_route_func_str_handle, req)
    assert ret.text == "user_route_with_rsp"

    # Test with regular trans info
    trans_info = {"a": "b", "c": "d"}
    headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_rsp", transport=transp, headers=headers)
    user_route_func_str_handle = HttpHandle(_user_route_with_rsp, "GET", "/user_route_with_rsp", service_name)
    ret = await _transport()._handle_route(user_route_func_str_handle, req)
    assert ret.text == "user_route_with_rsp"

    # Test with registered decode function
    def _decode(_):
        """Mock decode function"""
        pass

    register_user_http_decode_func(_decode)
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_none", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_with_none, "GET", "/user_route_with_none", service_name)
    ret = await _transport()._handle_route(user_route_func_handle, req)
    assert ret.text == "user_route_with_none"

    # Test with bytes return type
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_bytes", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_bytes, "GET", "/user_route_bytes", service_name)
    ret = await _transport()._handle_route(user_route_func_handle, req)
    assert ret.body == b"user_route_bytes"

    # Test with dict return type
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_dict", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_dict, "GET", "/user_route_dict", service_name)
    ret = await _transport()._handle_route(user_route_func_handle, req)
    assert ret.text == "user_route_dict"
