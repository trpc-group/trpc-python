# -*- coding: utf-8 -*-
"""Test client"""
import concurrent.futures

from examples.helloworld.stub.trpc_trpc_python_group_helloworld import helloworld_pb2
from trpc_sdk import client
from trpc_sdk import codec
from trpc_sdk import context
from trpc_sdk import exceptions
from trpc_sdk import utils
from trpc_sdk.client.options import with_compress_type
from trpc_sdk.client.options import with_current_compress_type
from trpc_sdk.client.options import with_current_serialization_type
from trpc_sdk.client.options import with_enable_multiplexed
from trpc_sdk.client.options import with_filter_list
from trpc_sdk.client.options import with_short_conn
from trpc_sdk.client.options import with_socket_type
from trpc_sdk.client.options import with_target
from trpc_sdk.client.options import with_timeout
from trpc_sdk.client.options import with_transport
from trpc_sdk.client.options import with_unix_path
from trpc_sdk.codec.compress import CompressType
from trpc_sdk.config import client_config as cli_config
from trpc_sdk.config import config
from trpc_sdk.transport import get_default_client_transport
from trpc_sdk.transport.options import RoundTripOptions


class _MockClientTransport:
    """Mock client transport"""

    async def round_trip(self, ctx, req_data: bytes, options: RoundTripOptions):
        """invoke"""

        try:
            await get_default_client_transport().round_trip(ctx, req_data, options)
        except exceptions.TRPCBaseError:
            pass
        if options.enable_multiplexed:
            return await self.mock_send_multiplexed()
        return await self.mock_send()

    async def mock_send(self):
        """Mock send"""
        return b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '

    async def mock_send_multiplexed(self):
        """mock send by multiplexed"""
        return b'\n\x0bhi: hello: '


def test_sync_client_in_thread():
    """Test sync client"""

    def sync_call():
        request = helloworld_pb2.HelloRequest()
        ctx = context.Context()
        rsp_cls = helloworld_pb2.HelloReply
        ctx, msg = codec.clone_client_message(ctx)
        msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
        msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
        msg.set_callee_method('SayHello')
        opts = [
            with_socket_type("net"),
            with_unix_path(""),
            with_filter_list([], True),
            with_timeout(500),
            with_target("ip://127.0.0.1:8000"),
            with_transport(_MockClientTransport())
        ]
        err = None
        try:
            client.get_client().invoke_sync(ctx, request, rsp_cls, opts)
        except Exception as exc:  # pylint: disable=broad-except
            err = exc
        return err

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(sync_call)
        return_value = future.result()
        assert return_value is None


@utils.trpc_async_pytest
async def test_send_request():
    """Test send request"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_transport(_MockClientTransport())
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except Exception:  # pylint: disable=broad-except
        assert False


@utils.trpc_async_pytest
async def test_send_request_short_conn():
    """Test send data by short conn"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_short_conn(True),
        with_transport(_MockClientTransport())
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        assert False


@utils.trpc_async_pytest
async def test_send_request_multiplexed():
    """Test send request by multiplexed"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_short_conn(True),
        with_transport(_MockClientTransport()),
        with_enable_multiplexed(True),
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        assert False


@utils.trpc_async_pytest
async def test_send_request_compress_snappy():
    """Test compress in snappy"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_short_conn(True),
        with_transport(_MockClientTransport()),
        with_compress_type(CompressType.Snappy)
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        assert False


@utils.trpc_async_pytest
async def test_send_request_compress_gzip():
    """Test compress gzip"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_short_conn(True),
        with_transport(_MockClientTransport()),
        with_compress_type(CompressType.Gzip)
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        assert False


@utils.trpc_async_pytest
async def test_send_request_compress_zlib():
    """Test compress zlib"""
    request = helloworld_pb2.HelloRequest()
    ctx = context.Context()
    rsp_cls = helloworld_pb2.HelloReply
    ctx, msg = codec.clone_client_message(ctx)
    msg.set_client_rpc_name('/trpc.trpc_python_group.helloworld.Greeter/SayHello')
    msg.set_callee_service_name('trpc.trpc_python_group.helloworld.Greeter')
    msg.set_callee_method('SayHello')
    opts = [
        with_filter_list([], True),
        with_timeout(500),
        with_target("ip://127.0.0.1:8000"),
        with_short_conn(True),
        with_transport(_MockClientTransport()),
        with_compress_type(CompressType.ZLib)
    ]
    try:
        await client.get_client().invoke(ctx, request, rsp_cls, opts)
    except exceptions.ClientDecodeError:
        assert False


def test_client_options_default_field():
    """Test default val of client options"""
    msg = codec.Message()
    opts = client.client.get_options(msg, [])
    assert opts.current_compress_type == -1
    assert opts.current_serialization_type == -1
    assert opts.serialization_type == 0
    assert opts.compress_type == 0

    opts = client.client.get_options(msg, [with_current_compress_type(1), with_current_serialization_type(1)])
    assert opts.current_compress_type == 1
    assert opts.current_serialization_type == 1
    assert opts.serialization_type == 0
    assert opts.compress_type == 0


def test_client_options():
    """Test client options"""
    opts = client.Options()
    assert opts.current_compress_type == -1
    assert opts.current_serialization_type == -1
    assert opts.serialization_type == -1
    assert opts.compress_type == -1
    file_path = './tests/test_config_data/trpc_client.yaml'
    config.load_global_config(file_path, 'utf-8')
    msg = codec.Message()
    msg.callee_service_name = "trpc_sdk.test.Helloworld.Greeter"
    opts = client.client.get_options(msg, [])
    assert opts.selector_options.destination_envname == "pre"
    assert opts.selector_options.servicerouter_opt.destination_envname == "pre"
