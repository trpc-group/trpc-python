# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Client transport options and the inner IPC frame helpers tests used to import from ``abc.transport``."""

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.transport import ConnPoolOptions
from trpc_sdk.transport import RoundTripOptions
from trpc_sdk.transport._cython import inner_decode_rsp
from trpc_sdk.transport._cython import inner_encode_rsp


def test_client_transport_option_defaults():
    opt = BaseClientTransportOption()
    assert opt.address == ("", 0)
    assert opt.network == "tcp"
    assert opt.socket_type == "net"
    assert opt.short_conn is False
    assert opt.parser is None


def test_round_trip_options_carry_pool_settings():
    pool = ConnPoolOptions(dial_timeout=100, idle_timeout=1, max_idle=2, max_active=4)
    options = RoundTripOptions()
    options.set_network("udp")
    options.set_pool_options(pool)
    options.address = ("127.0.0.1", 8000)

    assert options.network == "udp"
    assert options.pool_opts is pool
    assert options.pool_opts.max_active == 4
    assert options.address == ("127.0.0.1", 8000)
    assert isinstance(options, BaseClientTransportOption)


def test_server_transport_option_address_keeps_port():
    opt = BaseServerTransportOption()
    opt.set_address(("127.0.0.1", 9000))
    opt.set_network("tcp")
    assert opt.port == 9000
    assert opt.address == ("127.0.0.1", 9000)


def test_inner_response_frame_round_trip():
    encoded = inner_encode_rsp(3, 8, 0, b"rsp")
    assert inner_decode_rsp(encoded) == (3, 8, 0, b"rsp")
