# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test option of client"""
import socket
from unittest.mock import MagicMock

import pytest

from trpc_sdk._inner.pb import trpc_pb2
from trpc_sdk.abc import Node
from trpc_sdk.client import Options
from trpc_sdk.client import _options as options_mod
from trpc_sdk.codec import TrpcClientCodec
from trpc_sdk.codec import TrpcClientParser
from trpc_sdk.compressor import CompressType
from trpc_sdk.config import ClientServiceConfig
from trpc_sdk.serialization import SerializationType
from trpc_sdk.transport import ClientTransport
from trpc_sdk.transport import ConnPoolOptions


def test_options():
    """Test options"""
    opts = Options()

    opts.set_namespace('development')
    assert opts.namespace == 'development'

    opts.set_service_name('trpc_sdk.test.helloworld.Greeter')
    assert opts.service_name == 'trpc_sdk.test.helloworld.Greeter'

    opts.set_caller_service_name('trpc_sdk.test.helloworld.Greeter1')
    assert opts.caller_service_name == 'trpc_sdk.test.helloworld.Greeter1'

    opts.set_balancer_name('RandomLB')
    assert opts.selector_options.loadbalance == 'RandomLB'

    opts.set_discovery_name('polaris')
    assert opts.selector_options.discovery == 'polaris'

    opts.set_circuit_breaker_name('NoopCB')
    assert opts.selector_options.circuit_breaker == 'NoopCB'

    opts.set_target('ip://1.1.1.1')
    assert opts.target == 'ip://1.1.1.1'

    opts.set_network('udp')
    assert opts.call_options.network == 'udp'

    opts.set_short_conn(True)
    assert opts.call_options.short_conn is True

    opts.set_timeout(1000)
    assert opts.timeout == 1000

    opts.set_current_serialization_type(SerializationType.JCE)
    assert opts.current_serialization_type == SerializationType.JCE

    opts.set_serialization_type(SerializationType.JSON)
    assert opts.serialization_type == SerializationType.JSON

    opts.set_current_compress_type(CompressType.Snappy)
    assert opts.current_compress_type == CompressType.Snappy

    opts.set_compress_type(CompressType.ZLib)
    assert opts.compress_type == CompressType.ZLib

    opts.set_transport(ClientTransport())
    assert isinstance(opts.transport, ClientTransport)

    opts.set_protocol('trpc')
    assert isinstance(opts.codec, TrpcClientCodec)
    assert isinstance(opts.transport, ClientTransport)
    assert isinstance(opts.call_options.parser, TrpcClientParser)

    opts.set_req_head(None)
    assert opts.req_head is None

    opts.set_rsp_head(trpc_pb2.ResponseProtocol())
    assert isinstance(opts.rsp_head, trpc_pb2.ResponseProtocol)

    opts.set_metadata('test_trpc', 'test'.encode('utf8'))
    assert len(opts.metadata) > 0
    assert opts.metadata['test_trpc'] == 'test'.encode('utf8')

    node = Node(service_name='trpc_sdk.test.helloworld.Greeter',
                ip='1.1.1.1',
                port=8080,
                network='tcp',
                protocol='trpc')
    opts.set_selector_node(node)
    assert isinstance(opts.node, Node)

    opts.load_node_config(node)
    assert opts.call_options.network == 'tcp'
    assert opts.call_options.address == ('1.1.1.1', 8080)
    assert isinstance(opts.codec, TrpcClientCodec)
    assert isinstance(opts.transport, ClientTransport)
    assert isinstance(opts.call_options.parser, TrpcClientParser)

    opts.set_caller_setname('test_caller_setname')
    assert opts.selector_options.src_setname == 'test_caller_setname'

    opts.set_callee_setname('test_callee_setname')
    assert opts.selector_options.dst_setname == 'test_callee_setname'

    opts.set_caller_envname('test_caller_envname')
    assert opts.selector_options.src_env_name == 'test_caller_envname'

    opts.set_callee_envname('test_callee_envname')
    assert opts.selector_options.dst_env_name == 'test_callee_envname'

    opts.set_max_window_size(65536)
    assert opts.max_window_size == 65536

    conn_pool_opts = ConnPoolOptions(dial_timeout=100)
    opts.set_pool_opts(conn_pool_opts)
    assert opts.call_options.pool_opts == conn_pool_opts
    assert opts.call_options.pool_opts.dial_timeout == 100

    opts.set_enable_stream_filter(True)
    assert opts.enable_stream_filter is True

    metadata = {
        'version': 'v2.0',
        'env': 'Production',
        'region': 'shanghai',
        'business': 'payment',
        'priority': 'high',
    }
    opts.set_selector_metadata(metadata)
    assert opts.selector_options.service_router_opt.metadata == metadata


def test_set_protocol_unknown_leaves_codec_empty():
    """An unknown protocol name does not install a codec."""
    opts = Options()
    opts.set_protocol("non")
    assert opts.codec is None


def test_set_target_socket():
    """Test set_target_socket"""
    opts = Options()

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    opts.set_target_socket(tcp_socket)
    assert opts.call_options.sock == tcp_socket
    assert opts.call_options.network == 'net'
    assert opts.call_options.socket_type == 'tcp'

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    opts.set_target_socket(udp_socket)
    assert opts.call_options.sock == udp_socket
    assert opts.call_options.network == 'net'
    assert opts.call_options.socket_type == 'udp'

    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    opts.set_target_socket(unix_socket)
    assert opts.call_options.sock == unix_socket
    assert opts.call_options.network == 'unix'
    assert opts.call_options.socket_type == 'tcp'

    with pytest.raises(ValueError):
        opts.set_target_socket("not_a_socket")
    with pytest.raises(ValueError):
        opts.set_target_socket(123)
    with pytest.raises(ValueError):
        opts.set_target_socket(None)


def test_set_local_address():
    """Test set_local_address"""
    opts = Options()
    opts.set_local_address(('127.0.0.1', 8080))
    assert opts.call_options.local_addr == ('127.0.0.1', 8080)

    with pytest.raises(ValueError):
        opts.set_local_address({})
    with pytest.raises(ValueError):
        opts.set_local_address(("127.0.0.1", 1, 4))
    with pytest.raises(ValueError):
        opts.set_local_address((1, 3))
    with pytest.raises(ValueError):
        opts.set_local_address(("127.0.0.1", "234"))

    opts.set_local_address("127.0.0.1")
    assert opts.call_options.local_addr == ('127.0.0.1', 0)


def test_load_client_config_discovery_and_connection_flags(monkeypatch):
    service = ClientServiceConfig(
        target="",
        discovery="polaris",
        load_balance="ring",
        circuit_breaker="cb",
        password="secret",
        disable_reuse_http_session=False,
        short_conn=True,
        protocol="",
    )
    monkeypatch.setattr(options_mod, "get_client_service_config", lambda _: service)

    opts = Options().load_client_config("svc")

    assert opts.target == ""
    assert opts.selector_options.discovery == "polaris"
    assert opts.selector_options.loadbalance == "ring"
    assert opts.selector_options.circuit_breaker == "cb"
    assert opts.call_options.password == "secret"
    assert opts.call_options.disable_reuse_http_session is False
    assert opts.call_options.short_conn is True


@pytest.mark.parametrize(
    ("method", "value", "assertion"),
    [
        ("set_caller_namespace", "source-ns", lambda o: o.selector_options.src_namespace == "source-ns"),
        ("set_disable_service_router", True, lambda o: o.selector_options.disable_service_router is True),
        ("set_env_key", "env-key", lambda o: o.selector_options.env_key == "env-key"),
        ("set_servicerouter_name", "router", lambda o: o.selector_options.service_router == "router"),
        ("set_key", "hash-key", lambda o: o.selector_options.key == "hash-key"),
        ("set_socket_type", "unix", lambda o: o.socket_type == "unix"),
        ("set_unix_path", "/tmp/service.sock", lambda o: o.unix_path == "/tmp/service.sock"),
        ("set_password", "pw", lambda o: o.call_options.password == "pw"),
        ("set_disable_reuse_http_session", True,
         lambda o: o.call_options.disable_reuse_http_session is True),
        ("set_req_head", object(), lambda o: o.req_head is not None),
        ("set_rsp_head", object(), lambda o: o.rsp_head is not None),
        ("set_enable_stream_filter", True, lambda o: o.enable_stream_filter is True),
    ],
)
def test_additional_option_setters(method, value, assertion):
    opts = Options()
    getattr(opts, method)(value)
    assert assertion(opts)


def test_filter_add_and_override():
    first = object()
    second = object()
    opts = Options()
    opts.filters = MagicMock()

    opts.set_filter(first)
    opts.set_filter(second, override=True)

    opts.filters.add_filter.assert_called_once_with(first)
    opts.filters.set_filter.assert_called_once_with([second])


def test_set_tls_loads_cert_chain_from_tmp_pem_files(tmp_path, monkeypatch):
    ca_file = tmp_path / "ca.pem"
    cert_file = tmp_path / "client.pem"
    key_file = tmp_path / "client-key.pem"
    for path in (ca_file, cert_file, key_file):
        path.write_text("-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n", encoding="utf-8")

    ssl_context = MagicMock()
    create_context = MagicMock(return_value=ssl_context)
    monkeypatch.setattr(options_mod.ssl, "create_default_context", create_context)
    opts = Options()

    opts.set_tls(str(ca_file), str(cert_file), str(key_file))

    create_context.assert_called_once_with(cafile=str(ca_file))
    ssl_context.load_cert_chain.assert_called_once_with(certfile=str(cert_file), keyfile=str(key_file))
    assert opts.call_options.ssl_context is ssl_context
