# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client transport options module"""

from trpc_sdk.codec import ClientCodec
from trpc_sdk.codec import Message
from trpc_sdk.transport import RequestType
from trpc_sdk.transport import RoundTripOptions


def test_client_transport_options():
    """Test client transport options"""
    client_opts = RoundTripOptions()
    assert client_opts.address == ('127.0.0.1', 8080)
    assert client_opts.endpoint == '127.0.0.1:8080'
    assert client_opts.socket_type == 'net'
    assert client_opts.network == 'tcp'
    assert client_opts.req_type == RequestType.SendAndRecv
    assert client_opts.short_conn is False
    assert client_opts.password is None
    assert client_opts.parser is not None
    assert client_opts.codec is None
    assert client_opts.pool is None
    assert client_opts.enable_multiplexed is False
    assert client_opts.multiplexed is None
    assert client_opts.msg is None
    assert client_opts.ssl_context is None

    client_opts.set_address(('127.0.0.1', 8000))
    assert client_opts.address == ('127.0.0.1', 8000)

    client_opts.set_endpoint('127.0.0.1:8000')
    assert client_opts.endpoint == '127.0.0.1:8000'

    client_opts.set_socket_type('unix')
    assert client_opts.socket_type == 'unix'

    client_opts.set_network('udp')
    assert client_opts.network == 'udp'

    client_opts.set_req_type(RequestType.SendOnly)
    assert client_opts.req_type == RequestType.SendOnly

    client_opts.set_short_conn(True)
    assert client_opts.short_conn is True

    client_opts.set_password('123')
    assert client_opts.password == '123'

    client_opts.set_parser(None)
    assert client_opts.parser is None

    client_opts.set_pool(None)
    assert client_opts.pool is None

    client_opts.set_enable_multiplexed(True)
    assert client_opts.enable_multiplexed is True

    client_opts.set_multiplexed(None)
    assert client_opts.multiplexed is None

    msg = Message()
    client_opts.set_msg(msg)
    assert client_opts.msg == msg

    codec = ClientCodec()
    client_opts.set_codec(codec)
    assert client_opts.codec == codec
