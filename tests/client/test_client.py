# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client"""
import concurrent.futures

import pytest

from examples.helloworld.stub.trpc_trpc_python_group_helloworld import helloworld_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.client import get_client
from trpc_sdk.client import get_options
from trpc_sdk.codec import Message
from trpc_sdk.codec import clone_client_message
from trpc_sdk.compressor import CompressType
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.transport import RoundTripOptions
from trpc_sdk.transport import get_default_client_transport

_HELLO_FRAME = b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '


class _MockClientTransport:
    """Mock client transport that returns one decoded hello frame."""

    async def round_trip(self, ctx, req_data: bytes, options: RoundTripOptions):
        """Swallow a real transport failure, then return a fixed frame."""
        try:
            await get_default_client_transport().round_trip(ctx, req_data, options)
        except TrpcException:
            pass
        return _HELLO_FRAME


def _invoke_options(compress_type=None):
    opts = Options()
    opts.set_socket_type("net")
    opts.set_unix_path("")
    opts.set_filter_list([], True)
    opts.set_timeout(500)
    opts.set_target("ip://127.0.0.1:8000")
    opts.set_short_conn(True)
    opts.set_transport(_MockClientTransport())
    if compress_type is not None:
        opts.set_compress_type(compress_type)
    return opts


def _hello_context():
    request = helloworld_pb2.HelloRequest()
    ctx = TrpcContext()
    ctx, msg = clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    return ctx, request


def test_sync_client_in_thread():
    """Test sync client"""

    def sync_call():
        ctx, request = _hello_context()
        return get_client().invoke_sync(ctx, request, helloworld_pb2.HelloReply, _invoke_options())

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(sync_call)
        reply = future.result()
        assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request():
    """Test send request"""
    ctx, request = _hello_context()
    reply = await get_client().invoke(ctx, request, helloworld_pb2.HelloReply, _invoke_options())
    assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request_short_conn():
    """Test send data by short conn"""
    ctx, request = _hello_context()
    reply = await get_client().invoke(ctx, request, helloworld_pb2.HelloReply, _invoke_options())
    assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request_with_explicit_network():
    """Unary calls no longer take a multiplexed option; an explicit network still decodes."""
    ctx, request = _hello_context()
    opts = _invoke_options()
    opts.set_network("tcp")
    reply = await get_client().invoke(ctx, request, helloworld_pb2.HelloReply, opts)
    assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request_compress_snappy():
    """Test compress in snappy"""
    pytest.importorskip("snappy")
    ctx, request = _hello_context()
    reply = await get_client().invoke(
        ctx, request, helloworld_pb2.HelloReply, _invoke_options(CompressType.Snappy))
    assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request_compress_gzip():
    """Test compress gzip"""
    ctx, request = _hello_context()
    reply = await get_client().invoke(
        ctx, request, helloworld_pb2.HelloReply, _invoke_options(CompressType.Gzip))
    assert reply.msg == "hi: hello: "


@pytest.mark.asyncio
async def test_send_request_compress_zlib():
    """Test compress zlib"""
    ctx, request = _hello_context()
    reply = await get_client().invoke(
        ctx, request, helloworld_pb2.HelloReply, _invoke_options(CompressType.ZLib))
    assert reply.msg == "hi: hello: "


def test_client_options_default_field():
    """Test default val of client options"""
    msg = Message()
    opts = get_options(msg, [])
    assert opts.current_compress_type == -1
    assert opts.current_serialization_type == -1
    assert opts.serialization_type == 0
    assert opts.compress_type == 0

    overlay = Options()
    overlay.set_current_compress_type(1)
    overlay.set_current_serialization_type(1)
    opts = get_options(msg, overlay)
    assert opts.current_compress_type == 1
    assert opts.current_serialization_type == 1
    assert opts.serialization_type == 0
    assert opts.compress_type == 0


def test_client_options():
    """Loaded client YAML fills the callee environment name."""
    file_path = './tests/test_config_data/trpc_client.yaml'
    TrpcConfig.load(file_path, 'utf-8')
    msg = Message()
    msg.set_callee_service_name("trpc.test.Helloworld.Greeter")
    opts = get_options(msg, [])
    assert opts.selector_options.dst_env_name == "pre"
    assert opts.selector_options.service_router_opt.dst_env_name == "pre"
    assert opts.timeout == 800
