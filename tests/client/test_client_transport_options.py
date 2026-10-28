# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client transport options module"""

from trpc_sdk.codec import TrpcClientCodec
from trpc_sdk.transport import RequestType
from trpc_sdk.transport import RoundTripOptions


def test_client_transport_options():
    """Test client transport options"""
    client_opts = RoundTripOptions()
    assert client_opts.address == ('', 0)
    assert client_opts.endpoint == ''
    assert client_opts.socket_type == 'net'
    assert client_opts.network == 'tcp'
    assert client_opts.req_type == RequestType.SEND_AND_RECV
    assert client_opts.short_conn is False
    assert client_opts.password is None
    assert client_opts.parser is not None
    assert client_opts.codec is None
    assert client_opts.pool is None
    assert client_opts.ssl_context is None

    client_opts.set_address(('127.0.0.1', 8000))
    assert client_opts.address == ('127.0.0.1', 8000)

    client_opts.set_endpoint('127.0.0.1:8000')
    assert client_opts.endpoint == '127.0.0.1:8000'

    client_opts.set_socket_type('unix')
    assert client_opts.socket_type == 'unix'

    client_opts.set_network('udp')
    assert client_opts.network == 'udp'

    client_opts.set_req_type(RequestType.SEND_ONLY)
    assert client_opts.req_type == RequestType.SEND_ONLY

    client_opts.set_short_conn(True)
    assert client_opts.short_conn is True

    client_opts.set_password('123')
    assert client_opts.password == '123'

    client_opts.set_parser(None)
    assert client_opts.parser is None

    client_opts.set_pool(None)
    assert client_opts.pool is None

    codec = TrpcClientCodec()
    client_opts.set_codec(codec)
    assert client_opts.codec == codec
