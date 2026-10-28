# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test http client"""

import json

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import create_message
from trpc_sdk.codec import Message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.http import HTTP_TRPC_HEADER
from trpc_sdk.http import HttpClientReqHeader
from trpc_sdk.http import HttpClientTransport
from trpc_sdk.serialization import SerializationType
from trpc_sdk.transport import RoundTripOptions


def test_base64_encode():
    """Test base64 """
    msg = Message()
    transport = HttpClientTransport()
    req_header = HttpClientReqHeader()
    msg.client_meta_data = {"aa": (1, 2), "bb": "test", "cc": b"cc"}
    header = transport._get_headers(msg, req_header)
    assert header


def test_http_transport_with_polaris_url():
    """Test http transport"""
    transport = HttpClientTransport()
    opts = RoundTripOptions()
    opts.address = ("127.0.0.1", "80")
    req_header = HttpClientReqHeader(scheme="polaris")
    msg = Message()
    msg.client_rpc_name = "/hello"
    url = transport._get_url(req_header, opts, msg)
    assert url == "http://127.0.0.1:80/hello"


class _TestReqHead:

    def __init__(self):
        self.method = "GET"
        self.headers = {"t": "t"}


@pytest.mark.asyncio
async def test_http_client_transport():
    """Test client transport"""
    ctx = TrpcContext()
    ctx, msg = create_message(ctx)

    trans = HttpClientTransport()
    try:
        await trans.round_trip(ctx, b'', None)
        assert False
    except ClientInvokeError:
        assert True

    msg.client_req_head = _TestReqHead()
    msg.serialization_type = SerializationType.JSON
    msg.compress_type = 1
    msg.client_meta_data = {'t': 't'}
    try:
        await trans.round_trip(ctx, b'', None)
        assert False
    except Exception:  # pylint: disable=broad-exception-caught
        assert True


def test_str_trans_info():
    """Test trans_info """
    msg = Message()
    transport = HttpClientTransport()
    test_data = {"aa": "bb"}
    req_header = HttpClientReqHeader(headers={HTTP_TRPC_HEADER.trpc_trans_info: json.dumps(test_data)})

    msg.client_meta_data = {"aa": (1, 2), "bb": "test", "cc": b"cc"}
    header = transport._get_headers(msg, req_header)
    assert header
