# -*- coding: utf-8 -*-
"""Test transport abstract module"""
import pytest

# pylint: disable=import-error,no-name-in-module
from trpc_sdk.abc.codec import Parser
from trpc_sdk.abc.context import Context
from trpc_sdk.abc.server import BaseService
from trpc_sdk.abc.server import BaseTrpcServerEngine
from trpc_sdk.abc.transport import BaseClientStreamTransport
from trpc_sdk.abc.transport import BaseClientTransport
from trpc_sdk.abc.transport import BaseClientTransportOptions
from trpc_sdk.abc.transport import BaseServerTransport
from trpc_sdk.abc.transport import BaseServerTransportOption


class TestChecker(Parser):
    """Test checker"""

    def create(self):
        """Create server parser"""
        return None


class MockServerTransport(BaseServerTransport):
    """Mock server transport class"""

    def __init__(self) -> None:
        super().__init__()
        self._opts = None

    @property
    def options(self):
        """Get options"""
        return self._opts

    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine):
        """listen anf serve"""
        self._opts = opts


def test_server_transport():
    """Test server transport"""
    options = BaseServerTransportOption()
    assert options.service is None
    assert not options.address
    assert not options.socket_type
    assert not options.service_name
    assert not options.mode
    assert not options.tls_key
    assert not options.tls_cert
    assert not options.target
    assert options.checker is None
    assert options.handle is None
    assert options.network == "tcp"
    assert options.worker_num == 2
    assert options.idle_timeout == 0
    assert options.port == 8000

    options.set_target("target")
    assert options.target == "target"

    options.set_service(BaseService())
    assert options.service is not None

    options.set_service_name("service_name")
    assert options.service_name == "service_name"

    options.set_network("udp")
    assert options.network == "udp"

    options.set_address(("127.0.0.1", 8000))
    assert options.address == "127.0.0.1"
    assert options.port == 8000

    # 不能直接实例化 Parser
    checker = TestChecker()
    options.set_checker(checker)
    assert options.checker == checker

    options.set_socket_type("net")
    assert options.socket_type == "net"

    options.set_idle_timeout()
    assert options.idle_timeout == 1.0
    options.set_idle_timeout(2.0)
    assert options.idle_timeout == 2.0

    options.set_worker_num()
    assert options.worker_num == 2
    options.set_worker_num(4)
    assert options.worker_num == 4

    options.set_mode()
    assert not options.mode
    options.set_mode("Muti-listener")
    assert options.mode == "Muti-listener"

    options.set_tls_key("/tls/key")
    assert options.tls_key == "/tls/key"

    options.set_tls_cert("/tls/cert")
    assert options.tls_cert == "/tls/cert"

    trans = MockServerTransport()
    trans.listen_and_serve(options, BaseTrpcServerEngine())
    assert trans.options == options


class MockClientTransport(BaseClientTransport):
    """Mock client transport """

    def __init__(self) -> None:
        super().__init__()
        self.opts = None

    async def round_trip(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        self.opts = options


@pytest.mark.asyncio
async def test_client_transport():
    """Test client transport"""
    options = BaseClientTransportOptions()
    assert options.address == ('127.0.0.1', 8080)
    assert options.endpoint == '127.0.0.1:8080'
    assert options.socket_type == 'net'
    assert options.network == 'tcp'
    assert options.short_conn is False
    assert options.checker is None
    assert options.codec is None

    client_trans = MockClientTransport()
    await client_trans.round_trip(Context(), b'', options)
    assert client_trans.opts == options


class MockClientStreamTransport(BaseClientStreamTransport):
    """Mock client transport """

    def __init__(self) -> None:
        super().__init__()
        self.opts = None
        self.data = None
        self.is_close = False
        self.is_start = False

    async def round_trip(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        self.opts = options

    def send(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        self.data = req_data

    async def recv(self, ctx: Context, options: BaseClientTransportOptions) -> bytes:
        return self.data

    def close(self, ctx: Context):
        self.is_close = True

    async def start(self, ctx: Context, options: BaseClientTransportOptions):
        self.is_start = True


@pytest.mark.asyncio
async def test_client_stream_transport():
    """Test client stream transport"""
    options = BaseClientTransportOptions()
    client_trans = MockClientStreamTransport()
    await client_trans.start(Context(), options)
    assert client_trans.is_start is True
    await client_trans.round_trip(Context(), b'', options)
    assert client_trans.opts == options
    client_trans.send(Context(), b'123', options)
    assert client_trans.data == b'123'
    data = await client_trans.recv(Context(), options)
    assert data == b'123'
    client_trans.close(Context())
    assert client_trans.is_close is True
