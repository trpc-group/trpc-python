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
from trpc_sdk.transport import ListenOption
from trpc_sdk.transport import release_loop_resources
from trpc_sdk.abc import TrpcContext as Context
from trpc_sdk.abc import get_message
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.http import HttpRoute as HttpHandle
from trpc_sdk.http import HttpServerTransportFactory
from trpc_sdk.http import get_http_request
from trpc_sdk.http import get_http_response
from trpc_sdk.http import register_user_http_decode_func


def test_http_server_transport():
    """bind marks the endpoint as HTTP; listen needs the runtime's runner factory."""
    transport = HttpServerTransportFactory().create()
    opts = BaseServerTransportOption()
    assert opts.network == "tcp"

    transport.bind(opts)

    assert transport.opts is opts
    assert opts.network == http_defines.HTTP
    with pytest.raises(ValueError, match="http_runner_factory"):
        transport.listen(ListenOption(loop=None))
    with pytest.raises(ValueError, match="http_runner_factory"):
        transport.listen()


def test_http_listen_serves_the_runner_on_one_loop():
    """With no serving loops the runner is served on opt.loop and kept to clean up."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", port))
    transport = HttpServerTransportFactory().create()
    transport.bind(opts)

    async def _runner():
        app = web.Application()
        app.router.add_get("/", lambda request: web.Response(text="ok"))
        runner = web.AppRunner(app)
        await runner.setup()
        return runner

    async def _get():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        data = await reader.read()
        writer.close()
        return data

    loop = asyncio.new_event_loop()
    try:
        transport.listen(ListenOption(loop=loop, http_runner_factory=_runner))
        assert loop.run_until_complete(_get()).endswith(b"ok")

        transport.stop()
        runners = transport.pop_loop_resources(None)
        assert len(runners) == 1
        loop.run_until_complete(release_loop_resources(runners))
        assert transport.pop_loop_resources(None) == []
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


class _MockWorker:
    """Mock worker for testing HTTP message handling"""

    def __init__(self) -> None:
        """Initialize mock worker with test handlers"""
        self.handlers = [self._http_handle]
        self.timeouts = [1000]
        self.services = [_UserHttpHandler()]
        self.addresses = [("127.0.0.1", 8008)]
        self._http_msg_handles = {}

    async def _http_handle(self, ctx, data: bytes):
        """Mock HTTP handler function
        
        Args:
            ctx: Context object
            data: Request data
            
        Returns:
            Test response bytes
        """
        assert not data
        assert ctx.remote_ip == "127.0.0.1"
        req = get_http_request(ctx)
        assert req
        rsp = get_http_response(ctx)
        assert rsp is not None
        is_base64 = ctx.get_meta_data(http_defines.TRANS_INFO_BASE64_KEY, False)
        assert is_base64 is False
        local_addr = ctx.get_meta_data(http_defines.LOCAL_ADDR, "")
        assert local_addr == "127.0.0.1:8008"
        return b'test'


@pytest.mark.asyncio
async def test_handle_http_request():
    """Test handling HTTP request through HttpMessageHandle"""
    transp = mock.Mock()
    transp.get_extra_info.return_value = ("127.0.0.1", 1234)
    req = aiohttp_test_utils.make_mocked_request("GET", "/handle_http_request", transport=transp, headers={})
    mock_worker = _MockWorker()
    msg_handle = HttpMessageHandle(0, mock_worker, False)
    rsp = await msg_handle.handle_http_request(req)
    assert rsp.body == b'test'


async def _user_route_with_rsp(ctx: Context, req: Request) -> Response:
    """Test route handler that returns Response object
    
    Args:
        ctx: Context object
        req: Request object (unused)
        
    Returns:
        Response object
    """
    rsp = ctx.get_meta_data(http_defines.HTTP_RSP_KEY, None)
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
    rsp = ctx.get_meta_data(http_defines.HTTP_RSP_KEY, None)
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
    rsp = ctx.get_meta_data(http_defines.HTTP_RSP_KEY, None)
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

    mock_worker = _MockWorker()
    service_name = "trpc_sdk.test.server.service"

    # Test with base64 encoded trans info
    trans_info = {"a": "aa", "b": "cc"}
    for k, v in trans_info.items():
        trans_info[k] = base64.b64encode(v.encode()).decode()
    headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)

    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_rsp", transport=transp, headers=headers)
    user_route_func_str_handle = HttpHandle(_user_route_with_rsp, "GET", "/user_route_with_rsp", service_name)
    msg_handle = HttpMessageHandle(0, mock_worker, True)
    ret = await msg_handle.handle_user_http_request(user_route_func_str_handle, req)
    assert ret.text == "user_route_with_rsp"

    # Test with regular trans info
    trans_info = {"a": "b", "c": "d"}
    headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_rsp", transport=transp, headers=headers)
    user_route_func_str_handle = HttpHandle(_user_route_with_rsp, "GET", "/user_route_with_rsp", service_name)
    msg_handle = HttpMessageHandle(0, mock_worker, False)
    ret = await msg_handle.handle_user_http_request(user_route_func_str_handle, req)
    assert ret.text == "user_route_with_rsp"

    # Test with registered decode function
    def _decode(_):
        """Mock decode function"""
        pass

    register_user_http_decode_func(_decode)
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_with_none", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_with_none, "GET", "/user_route_with_none", service_name)
    msg_handle = HttpMessageHandle(0, mock_worker)
    ret = await msg_handle.handle_user_http_request(user_route_func_handle, req)
    assert ret.text == "user_route_with_none"

    # Test with bytes return type
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_bytes", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_bytes, "GET", "/user_route_bytes", service_name)
    msg_handle = HttpMessageHandle(0, mock_worker)
    ret = await msg_handle.handle_user_http_request(user_route_func_handle, req)
    assert ret.body == b"user_route_bytes"

    # Test with dict return type
    req = aiohttp_test_utils.make_mocked_request("GET", "/user_route_dict", transport=transp, headers=headers)
    user_route_func_handle = HttpHandle(_user_route_dict, "GET", "/user_route_dict", service_name)
    msg_handle = HttpMessageHandle(0, mock_worker)
    ret = await msg_handle.handle_user_http_request(user_route_func_handle, req)
    assert ret.text == "user_route_dict"
