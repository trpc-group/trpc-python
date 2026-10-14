# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test option of client"""
import socket
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trpc_sdk.client import options as options_mod
from trpc_sdk.client.options import *
from trpc_sdk.client.options import Options
from trpc_sdk.codec import *
from trpc_sdk.pool.connpool import ConnPoolOptions
from trpc_sdk.transport import ClientTransport
from trpc_sdk.abc.node import Node
from trpc_sdk.trpcpb import ResponseProtocol


def test_options():
    """Test options"""
    opts = Options()

    o = with_namespace('development')
    o(opts)
    assert opts.namespace == 'development'

    o = with_service_name('trpc_sdk.test.helloworld.Greeter')
    o(opts)
    assert opts.service_name == 'trpc_sdk.test.helloworld.Greeter'

    o = with_caller_service_name('trpc_sdk.test.helloworld.Greeter1')
    o(opts)
    assert opts.caller_service_name == 'trpc_sdk.test.helloworld.Greeter1'

    o = with_balancer_name('RandomLB')
    o(opts)
    assert opts.selector_options.loadbalance == 'RandomLB'

    o = with_discovery_name('polaris')
    o(opts)
    assert opts.selector_options.discovery == 'polaris'

    o = with_circuit_breaker_name('NoopCB')
    o(opts)
    assert opts.selector_options.circuitbreaker == 'NoopCB'

    o = with_target('ip://1.1.1.1')
    o(opts)
    assert opts.target == 'ip://1.1.1.1'

    o = with_network('udp')
    o(opts)
    assert opts.call_options.network == 'udp'

    o = with_short_conn(True)
    o(opts)
    assert opts.call_options.short_conn is True

    o = with_timeout(1000)
    o(opts)
    assert opts.timeout == 1000

    o = with_current_serialization_type(SerializationType.JCE)
    o(opts)
    assert opts.current_serialization_type == SerializationType.JCE

    o = with_serialization_type(SerializationType.JSON)
    o(opts)
    assert opts.serialization_type == SerializationType.JSON

    o = with_current_compress_type(CompressType.Snappy)
    o(opts)
    assert opts.current_compress_type == CompressType.Snappy

    o = with_compress_type(CompressType.ZLib)
    o(opts)
    assert opts.compress_type == CompressType.ZLib

    o = with_transport(ClientTransport())
    o(opts)
    assert isinstance(opts.transport, ClientTransport)

    o = with_protocol('trpc')
    o(opts)
    assert isinstance(opts.codec, ClientCodec)
    assert isinstance(opts.transport, ClientTransport)
    assert isinstance(opts.call_options.parser, TrpcClientParser)

    o = with_req_head(None)
    o(opts)
    assert opts.req_head is None

    o = with_rsp_head(ResponseProtocol())
    o(opts)
    assert isinstance(opts.rsp_head, ResponseProtocol)

    o = with_metadata('test_trpc', 'test'.encode('utf8'))
    o(opts)
    assert len(opts.metadata) > 0
    assert opts.metadata['test_trpc'] == 'test'.encode('utf8')

    node = Node(service_name='trpc_sdk.test.helloworld.Greeter', address='1.1.1.1:8080', network='tcp', protocol='trpc')
    o = with_selector_node(node)
    o(opts)
    assert isinstance(opts.node, Node)

    opts.load_node_config(node)
    assert opts.call_options.network == 'tcp'
    assert isinstance(opts.codec, ClientCodec)
    assert isinstance(opts.transport, ClientTransport)
    assert isinstance(opts.call_options.parser, TrpcClientParser)

    o = with_caller_setname('test_caller_setname')
    o(opts)
    assert opts.selector_options.source_setname == 'test_caller_setname'

    o = with_callee_setname('test_callee_setname')
    o(opts)
    assert opts.selector_options.destination_setname == 'test_callee_setname'

    o = with_caller_envname('test_caller_envname')
    o(opts)
    assert opts.selector_options.source_envname == 'test_caller_envname'

    o = with_callee_envname('test_callee_envname')
    o(opts)
    assert opts.selector_options.destination_envname == 'test_callee_envname'

    o = with_max_window_size(65536)
    o(opts)
    assert opts.max_window_size == 65536

    conn_poo_opts = ConnPoolOptions(dial_timeout=100)
    o = with_pool_opts(conn_poo_opts)
    o(opts)
    assert opts.call_options.pool_opts == conn_poo_opts
    assert opts.call_options.pool_opts.dial_timeout == 100

    o = with_enable_stream_filter(True)
    o(opts)
    assert opts.enable_stream_filter is True

    # 测试 with_selector_metadata
    metadata = {
        'version': 'v2.0',
        'env': 'Production',
        'region': 'shanghai',
        'business': 'payment',
        'priority': 'high',
    }
    o = with_selector_metadata(metadata)
    o(opts)
    assert opts.selector_options.servicerouter_opt.metadata == metadata


def test_with_protocol_options():
    """Test with_protocol_options"""
    opts = Options()
    protocol = "non"
    o = with_protocol(protocol)
    o(opts)
    assert opts.codec is None


def test_with_target_socket():
    """Test with_target_socket"""
    opts = Options()

    # Test with TCP socket
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    o = with_target_socket(tcp_socket)
    o(opts)
    assert opts.call_options.sock == tcp_socket
    assert opts.call_options.network == 'net'
    assert opts.call_options.socket_type == 'tcp'

    # Test with UDP socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    o = with_target_socket(udp_socket)
    o(opts)
    assert opts.call_options.sock == udp_socket
    assert opts.call_options.network == 'net'
    assert opts.call_options.socket_type == 'udp'

    # Test with Unix domain socket
    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    o = with_target_socket(unix_socket)
    o(opts)
    assert opts.call_options.sock == unix_socket
    assert opts.call_options.network == 'unix'
    assert opts.call_options.socket_type == 'tcp'

    # Test with invalid socket type (should raise ValueError)
    with pytest.raises(ValueError):
        with_target_socket("not_a_socket")(opts)
    with pytest.raises(ValueError):
        with_target_socket(123)(opts)
    with pytest.raises(ValueError):
        with_target_socket(None)(opts)


def test_with_local_address():
    """Test with_local_address"""
    opts = Options()
    o = with_local_address(('127.0.0.1', 8080))
    o(opts)
    assert opts.call_options.local_addr == ('127.0.0.1', 8080)

    with pytest.raises(ValueError):
        with_local_address({})(opts)
    with pytest.raises(ValueError):
        with_local_address(("127.0.0.1", 1, 4))(opts)
    with pytest.raises(ValueError):
        with_local_address((1, 3))(opts)
    with pytest.raises(ValueError):
        with_local_address(("127.0.0.1", "234"))(opts)

    with_local_address(("127.0.0.1"))(opts)
    assert opts.call_options.local_addr == ('127.0.0.1', 0)


def _service(**overrides):
    values = {
        "target": "",
        "discovery": "",
        "loadbalance": "",
        "circuitbreaker": "",
        "timeout": 0,
        "service_name": "",
        "namespace": "",
        "serialization": 0,
        "compression": 0,
        "protocol": "",
        "filter": None,
        "network": "",
        "password": "",
        "disable_reuse_http_session": None,
        "short_conn": None,
        "dial_timeout": 0,
        "idle_time": 0,
        "max_conn_lifetime": 0,
        "max_idle_conn": 0,
        "max_active_conn": 0,
        "is_wait_active_conn": False,
        "set_name": "",
        "env_name": "",
        "enable_stream_filter": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_load_client_config_discovery_and_connection_flags(monkeypatch):
    service = _service(
        discovery="polaris",
        loadbalance="ring",
        circuitbreaker="cb",
        password="secret",
        disable_reuse_http_session=False,
        short_conn=True,
    )
    monkeypatch.setattr(options_mod, "client_config", lambda _: service)

    opts = Options().load_client_config("svc")

    assert opts.target == ""
    assert opts.selector_options.discovery == "polaris"
    assert opts.selector_options.loadbalance == "ring"
    assert opts.selector_options.circuitbreaker == "cb"
    assert opts.call_options.password == "secret"
    assert opts.call_options.disable_reuse_http_session is False
    assert opts.call_options.short_conn is True


@pytest.mark.parametrize(
    ("factory", "value", "assertion"),
    [
        ("with_caller_namespace", "source-ns", lambda o: o.selector_options.source_namespace == "source-ns"),
        ("with_disable_service_router", True, lambda o: o.selector_options.disable_service_router is True),
        ("with_env_key", "env-key", lambda o: o.selector_options.env_key == "env-key"),
        ("with_servicerouter_name", "router", lambda o: o.selector_options.servicerouter == "router"),
        ("with_key", "hash-key", lambda o: o.selector_options.key == "hash-key"),
        ("with_socket_type", "unix", lambda o: o.socket_type == "unix"),
        ("with_unix_path", "/tmp/service.sock", lambda o: o.unix_path == "/tmp/service.sock"),
        ("with_password", "pw", lambda o: o.call_options.password == "pw"),
        ("with_disable_reuse_http_session", True,
         lambda o: o.call_options.disable_reuse_http_session is True),
        ("with_enable_multiplexed", True, lambda o: o.enable_multiplexed is True),
        ("with_req_head", object(), lambda o: o.req_head is not None),
        ("with_rsp_head", object(), lambda o: o.rsp_head is not None),
        ("with_enable_stream_filter", True, lambda o: o.enable_stream_filter is True),
    ],
)
def test_additional_option_closures(factory, value, assertion):
    opts = Options()
    getattr(options_mod, factory)(value)(opts)
    assert assertion(opts)


def test_filter_add_and_override():
    first = object()
    second = object()
    opts = Options()
    opts.filters = MagicMock()

    options_mod.with_filter(first)(opts)
    options_mod.with_filter(second, override=True)(opts)

    opts.filters.add_filter.assert_called_once_with(first)
    opts.filters.set_filter.assert_called_once_with([second])


def test_with_tls_loads_cert_chain_from_tmp_pem_files(tmp_path, monkeypatch):
    ca_file = tmp_path / "ca.pem"
    cert_file = tmp_path / "client.pem"
    key_file = tmp_path / "client-key.pem"
    for path in (ca_file, cert_file, key_file):
        path.write_text("-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n", encoding="utf-8")

    ssl_context = MagicMock()
    create_context = MagicMock(return_value=ssl_context)
    monkeypatch.setattr(options_mod.ssl, "create_default_context", create_context)
    opts = Options()

    options_mod.with_tls(str(ca_file), str(cert_file), str(key_file))(opts)

    create_context.assert_called_once_with(cafile=str(ca_file))
    ssl_context.load_cert_chain.assert_called_once_with(certfile=str(cert_file), keyfile=str(key_file))
    assert opts.call_options.ssl_context is ssl_context
