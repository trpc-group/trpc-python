# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test transport abstract module"""
import asyncio

import pytest

from trpc_sdk.abc import BaseClientStreamTransport
from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseService
from trpc_sdk.abc import TrpcContext


class MockServerTransport(BaseServerTransport):
    """Server transport that only records what the lifecycle calls did."""

    def __init__(self) -> None:
        super().__init__()
        self.opts = None
        self.listened = False

    def bind(self, opts: BaseServerTransportOption) -> None:
        self.opts = opts

    def listen(self) -> None:
        self.listened = True


def test_server_transport_option_defaults():
    """Defaults of the server transport option."""
    options = BaseServerTransportOption()
    assert options.service is None
    assert options.address == ("", 0)
    assert options.port == 8000
    assert options.parser is None
    assert options.rpc_handler is None
    assert options.network == "tcp"
    assert options.worker_num == 2
    assert options.idle_timeout == 0
    assert options.backlog == 1024
    assert options.reuse_port is False
    assert options.is_stream is False
    assert not options.socket_type
    assert not options.service_name
    assert not options.mode
    assert not options.tls_key
    assert not options.tls_cert
    assert not options.target


def test_server_transport_option_setters():
    """Setters of the server transport option."""
    options = BaseServerTransportOption()

    options.set_address(("127.0.0.1", "8001"))
    assert options.address == ("127.0.0.1", 8001)
    assert options.port == 8001

    parser = BaseServerProtocolParser()
    options.set_parser(parser)
    assert options.parser is parser

    options.set_service(BaseService())
    assert options.service is not None
    options.set_service_name("service_name")
    assert options.service_name == "service_name"
    options.set_network("udp")
    assert options.network == "udp"
    options.set_socket_type("net")
    assert options.socket_type == "net"
    options.set_idle_timeout(2.0)
    assert options.idle_timeout == 2.0
    options.set_worker_num(4)
    assert options.worker_num == 4
    options.set_mode("merge")
    assert options.mode == "merge"
    options.set_tls_key("/tls/key")
    assert options.tls_key == "/tls/key"
    options.set_tls_cert("/tls/cert")
    assert options.tls_cert == "/tls/cert"
    options.set_target("target")
    assert options.target == "target"


def test_server_transport_lifecycle():
    """bind records the options, listen serves, the other members have defaults."""
    options = BaseServerTransportOption()
    trans = MockServerTransport()

    trans.bind(options)
    assert trans.opts is options
    assert not trans.listened
    assert trans.check_endpoint() is True

    loop = asyncio.new_event_loop()
    try:
        assert trans.listen() is None
        assert trans.listened is True
        assert trans.stop() is None
        assert loop.run_until_complete(trans.destroy()) is None
    finally:
        loop.close()


def test_server_transport_requires_bind_and_listen():
    """bind and listen are abstract."""

    class _NoListen(BaseServerTransport):

        def bind(self, opts):
            pass

    with pytest.raises(TypeError):
        _NoListen()  # pylint: disable=abstract-class-instantiated


class MockClientTransport(BaseClientTransport):
    """Mock client transport """

    def __init__(self) -> None:
        super().__init__()
        self.opts = None

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options: BaseClientTransportOption):
        self.opts = options


@pytest.mark.asyncio
async def test_client_transport():
    """Test client transport"""
    options = BaseClientTransportOption()
    assert options.address == ("", 0)
    assert options.endpoint == ""
    assert options.socket_type == 'net'
    assert options.network == 'tcp'
    assert options.short_conn is False
    assert options.parser is None
    assert options.codec is None

    client_trans = MockClientTransport()
    await client_trans.round_trip(TrpcContext(), b'', options)
    assert client_trans.opts == options


class MockClientStreamTransport(BaseClientStreamTransport):
    """Mock client transport """

    def __init__(self) -> None:
        super().__init__()
        self.opts = None
        self.data = None
        self.is_close = False
        self.is_start = False

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options: BaseClientTransportOption):
        self.opts = options

    def send(self, ctx: TrpcContext, req_data: bytes, options: BaseClientTransportOption):
        self.data = req_data

    async def recv(self, ctx: TrpcContext, options: BaseClientTransportOption) -> bytes:
        return self.data

    def close(self, ctx: TrpcContext):
        self.is_close = True

    async def start_stream(self, ctx: TrpcContext, options: BaseClientTransportOption):
        self.is_start = True


@pytest.mark.asyncio
async def test_client_stream_transport():
    """Test client stream transport"""
    options = BaseClientTransportOption()
    client_trans = MockClientStreamTransport()
    await client_trans.start_stream(TrpcContext(), options)
    assert client_trans.is_start is True
    await client_trans.round_trip(TrpcContext(), b'', options)
    assert client_trans.opts == options
    client_trans.send(TrpcContext(), b'123', options)
    assert client_trans.data == b'123'
    data = await client_trans.recv(TrpcContext(), options)
    assert data == b'123'
    client_trans.close(TrpcContext())
    assert client_trans.is_close is True
