# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test the tRPC server transport bind / listen / stop / destroy lifecycle."""
import asyncio
import socket
from types import SimpleNamespace

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.codec import get_server_parser
from trpc_sdk.server import _engine
from trpc_sdk.transport import ListenOption
from trpc_sdk.transport import TrpcServerStreamTransport
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import release_loop_resources
from trpc_sdk.transport._server import _trpc_transport as server_transport


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _is_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


async def _handler(ctx, data):  # pylint: disable=unused-argument
    return b""


def _make_opts(port: int) -> BaseServerTransportOption:
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", port))
    opts.set_socket_type("net")
    opts.set_network("tcp")
    opts.set_parser(get_server_parser("trpc"))
    opts.rpc_handler = _handler
    return opts


@pytest.fixture
def loop():
    """A fresh event loop installed as the current loop, as a worker has after fork."""
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    yield new_loop
    asyncio.set_event_loop(None)
    new_loop.close()


def test_bind_only_records_options():
    """bind keeps the options and opens no socket."""
    port = _free_port()
    opts = _make_opts(port)
    transport = TrpcServerTransport()

    transport.bind(opts)

    assert transport.opts is opts
    assert not _is_listening(port)


def test_listen_option_defaults():
    """An option with only a loop serves on that loop, without SO_REUSEPORT."""
    opt = ListenOption(loop=None)
    assert not opt.serving_loops
    assert opt.reuse_port is False
    assert opt.shared_pool is None
    assert opt.protocol_factory is None
    assert opt.http_runner_factory is None
    assert opt.schedule_target == ""
    assert opt.ssl_context is None
    assert opt.worker is None


def test_check_endpoint_probes_by_network(monkeypatch):
    """tcp is probed with a connect, udp with a bind."""
    probed = []
    monkeypatch.setattr(server_transport, "is_tcp_port_available", lambda host, port: probed.append("tcp") or True)
    monkeypatch.setattr(server_transport, "is_udp_port_available", lambda host, port: probed.append("udp") or True)
    transport = TrpcServerTransport()
    opts = _make_opts(_free_port())
    transport.bind(opts)

    assert transport.check_endpoint() is True
    opts.set_network("udp")
    assert transport.check_endpoint() is True
    assert probed == ["tcp", "udp"]


def test_listen_serves_until_stop_and_destroy(loop):  # pylint: disable=redefined-outer-name
    """listen serves on opt.loop; stop + destroy release the port."""
    port = _free_port()
    transport = TrpcServerTransport()
    transport.bind(_make_opts(port))

    assert transport.listen(ListenOption(loop=loop)) is None
    assert _is_listening(port)

    async def _connect_and_close():
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        await writer.wait_closed()
        # Let the server side see the EOF before closing the listener.
        await asyncio.sleep(0.05)

    loop.run_until_complete(_connect_and_close())

    transport.stop()
    transport.stop()
    loop.run_until_complete(transport.destroy())

    assert not _is_listening(port)


def test_listen_without_opt_serves_on_the_current_loop(loop):  # pylint: disable=redefined-outer-name
    """listen() with no option serves on the calling thread's loop."""
    port = _free_port()
    transport = TrpcServerTransport()
    transport.bind(_make_opts(port))

    transport.listen()
    assert _is_listening(port)

    transport.stop()
    loop.run_until_complete(transport.destroy())
    assert not _is_listening(port)


def test_listen_uses_the_protocol_factory_override(loop):  # pylint: disable=redefined-outer-name
    """opt.protocol_factory replaces the transport's own connection class."""
    port = _free_port()
    transport = TrpcServerTransport()
    transport.bind(_make_opts(port))
    made = []

    def _factory():
        made.append(True)
        return asyncio.Protocol()

    transport.listen(ListenOption(loop=loop, protocol_factory=_factory))

    async def _connect():
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.05)
        writer.close()

    loop.run_until_complete(_connect())
    transport.stop()

    assert made == [True]


def test_datagram_listen_keeps_the_endpoint_to_close(loop):  # pylint: disable=redefined-outer-name
    """A udp endpoint is bound on opt.loop and kept under the main-loop key."""
    opts = _make_opts(_free_port())
    opts.set_network("udp")
    transport = TrpcServerTransport()
    transport.bind(opts)

    transport.listen(ListenOption(loop=loop, worker=SimpleNamespace()))

    assert transport.pop_loop_resources(1) == []
    (endpoint,) = transport.pop_loop_resources(None)
    assert transport.pop_loop_resources(None) == []
    assert isinstance(endpoint, asyncio.DatagramTransport)
    loop.run_until_complete(release_loop_resources([endpoint]))
    assert endpoint.is_closing()


def test_stop_and_destroy_without_listen_are_no_ops(loop):  # pylint: disable=redefined-outer-name
    """A transport that never listened has nothing to stop."""
    transport = TrpcServerTransport()
    transport.bind(_make_opts(_free_port()))

    transport.stop()
    loop.run_until_complete(transport.destroy())


def test_stream_transport_bind_marks_is_stream():
    """The stream transport flags its options as streaming."""
    opts = _make_opts(_free_port())
    transport = TrpcServerStreamTransport()

    transport.bind(opts)

    assert transport.opts is opts
    assert opts.is_stream is True


def test_engine_registers_bound_transport(monkeypatch):
    """add_listen_endpoint reads every table entry off transport.opts."""
    monkeypatch.setattr(server_transport, "is_tcp_port_available", lambda host, port: True)
    engine = _engine.TrpcServerEngine(SimpleNamespace(service_num=1))
    opts = _make_opts(20000)
    transport = TrpcServerTransport()
    transport.bind(opts)

    engine.add_listen_endpoint(transport)

    info = engine.process_info
    assert info.addresses == [("127.0.0.1", 20000)]
    assert info.networks == ["tcp"]
    assert info.parsers == [opts.parser]
    assert info.handlers == [_handler]
    assert info.transport_opt == [opts]
    assert info.transports == [transport]
