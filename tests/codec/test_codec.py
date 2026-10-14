# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the context-first ``"trpc"`` codec plugins."""

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import create_frame_head
from trpc_sdk.abc import get_message
from trpc_sdk.codec import FRAME_HEAD_LEN
from trpc_sdk.codec import TrpcClientCodec
from trpc_sdk.codec import TrpcServerCodec
from trpc_sdk.codec import parse_frame_head
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import ServerSystemError


def _ctx(frame_type: int = trpc_pb2.TRPC_UNARY_FRAME, frame_state: int = 0, stream_id: int = 0) -> TrpcContext:
    ctx = TrpcContext()
    get_message(ctx).set_frame_head(create_frame_head(frame_type, frame_state, stream_id))
    return ctx


def _stream_ctx(frame_state: int, session_id: int = 1, stream_id: int = 100) -> TrpcContext:
    ctx = _ctx(trpc_pb2.TRPC_STREAM_FRAME, frame_state, stream_id)
    msg = get_message(ctx)
    msg.session_id = session_id
    msg.stream_id = stream_id
    return ctx


@pytest.mark.asyncio
async def test_unary_request_round_trip():
    """Client encode_data -> server decode_data carries body and routing fields."""
    client_ctx = _ctx()
    client_msg = get_message(client_ctx)
    client_msg.set_callee_service_name("trpc.app.server.Greeter")
    client_msg.set_client_rpc_name("/trpc.app.server.Greeter/SayHello")
    req_buff = TrpcClientCodec().encode_data(client_ctx, b"hello world")

    assert parse_frame_head(req_buff[:FRAME_HEAD_LEN]).frame_type == trpc_pb2.TRPC_UNARY_FRAME

    server_ctx = TrpcContext()
    req_body = await TrpcServerCodec().decode_data(server_ctx, req_buff)
    server_msg = get_message(server_ctx)
    assert req_body == b"hello world"
    assert server_msg.callee_service_name == "trpc.app.server.Greeter"
    assert server_msg.server_rpc_name == "/trpc.app.server.Greeter/SayHello"
    assert server_msg.server_rsp_head.request_id == client_msg.request_id


def test_unary_response_round_trip():
    """Server encode_data -> client decode_data carries body and meta data."""
    server_ctx = _ctx()
    get_message(server_ctx).set_server_meta_data({"trpc": b"meta"})
    rsp_buff = TrpcServerCodec().encode_data(server_ctx, b"sayHello")

    client_ctx = TrpcContext()
    rsp_body = TrpcClientCodec().decode_data(client_ctx, rsp_buff)
    assert rsp_body == b"sayHello"
    assert get_message(client_ctx).client_meta_data["trpc"] == b"meta"


@pytest.mark.asyncio
async def test_request_attachment_round_trip():
    client_ctx = _ctx()
    get_message(client_ctx).set_request_attachment(b"test attachment")
    req_buff = TrpcClientCodec().encode_data(client_ctx, b"hello world")

    server_ctx = TrpcContext()
    req_body = await TrpcServerCodec().decode_data(server_ctx, req_buff)
    server_msg = get_message(server_ctx)
    assert req_body == b"hello world"
    assert server_msg.request_attachment == b"test attachment"
    assert server_msg.server_req_head.attachment_size == len(b"test attachment")


def test_response_attachment_round_trip():
    server_ctx = _ctx()
    get_message(server_ctx).set_response_attachment(b"test attachment")
    rsp_buff = TrpcServerCodec().encode_data(server_ctx, b"sayHello")

    client_ctx = TrpcContext()
    rsp_body = TrpcClientCodec().decode_data(client_ctx, rsp_buff)
    assert rsp_body == b"sayHello"
    assert get_message(client_ctx).response_attachment == b"test attachment"


@pytest.mark.asyncio
async def test_server_decode_rejects_bad_frames():
    server_codec = TrpcServerCodec()
    with pytest.raises(ServerDecodeError):
        await server_codec.decode_data(TrpcContext(), b"")

    client_ctx = _ctx()
    get_message(client_ctx).set_request_attachment(b"test attachment")
    truncated = TrpcClientCodec().encode_data(client_ctx, b"hello world")[:-12]
    with pytest.raises(ServerDecodeError):
        await server_codec.decode_data(TrpcContext(), truncated)


def test_client_decode_rejects_bad_frames():
    client_codec = TrpcClientCodec()
    with pytest.raises(ClientDecodeError):
        client_codec.decode_data(TrpcContext(), b"")

    server_ctx = _ctx()
    get_message(server_ctx).set_response_attachment(b"test attachment")
    truncated = TrpcServerCodec().encode_data(server_ctx, b"sayHello")[:-9]
    with pytest.raises(ClientDecodeError):
        client_codec.decode_data(TrpcContext(), truncated)


def test_cython_codecs_require_a_context():
    """The cpdef hooks are typed on TrpcContext; a bare message is rejected."""
    client_codec = TrpcClientCodec()
    msg = get_message(_ctx())
    with pytest.raises(TypeError):
        client_codec.codec.encode_data(msg, b"")
    with pytest.raises(TypeError):
        client_codec.stream_codec.encode_stream(msg, b"")


@pytest.mark.asyncio
async def test_stream_round_trip_restores_rpc_name():
    """INIT is cached by the server codec; DATA recovers server_rpc_name from it."""
    client_codec = TrpcClientCodec()
    server_codec = TrpcServerCodec()

    init_ctx = _stream_ctx(trpc_pb2.TRPC_STREAM_FRAME_INIT)
    get_message(init_ctx).set_client_rpc_name("/trpc.app.server.Greeter/SayHelloAll")
    init_buff = client_codec.encode_data(init_ctx, None)

    server_init_ctx = TrpcContext()
    get_message(server_init_ctx).session_id = 1
    assert await server_codec.decode_data(server_init_ctx, init_buff) == b""
    assert get_message(server_init_ctx).stream_id == 100

    data_buff = client_codec.encode_data(_stream_ctx(trpc_pb2.TRPC_STREAM_FRAME_DATA), b"payload")
    server_data_ctx = TrpcContext()
    get_message(server_data_ctx).session_id = 1
    assert await server_codec.decode_data(server_data_ctx, data_buff) == b"payload"
    assert get_message(server_data_ctx).server_rpc_name == "/trpc.app.server.Greeter/SayHelloAll"

    orphan_ctx = TrpcContext()
    get_message(orphan_ctx).session_id = 2
    with pytest.raises(ServerSystemError):
        await server_codec.decode_data(orphan_ctx, data_buff)


def test_server_stream_frames_decode_on_client():
    server_codec = TrpcServerCodec()
    client_codec = TrpcClientCodec()

    data_buff = server_codec.encode_data(_stream_ctx(trpc_pb2.TRPC_STREAM_FRAME_DATA), b"\n\x05test0")
    assert client_codec.decode_data(TrpcContext(), data_buff) == b"\n\x05test0"

    close_ctx = _stream_ctx(trpc_pb2.TRPC_STREAM_FRAME_CLOSE)
    get_message(close_ctx).set_stream_frame(trpc_pb2.TrpcStreamCloseMeta())
    close_buff = server_codec.encode_data(close_ctx, None)
    client_ctx = TrpcContext()
    assert client_codec.decode_data(client_ctx, close_buff) == b""
    assert isinstance(get_message(client_ctx).stream_frame, trpc_pb2.TrpcStreamCloseMeta)


def test_unknown_stream_frame_state_raises():
    with pytest.raises(ValueError):
        TrpcClientCodec().encode_data(_stream_ctx(9), b"")


def test_init_meta_cache_bookkeeping():
    server_codec = TrpcServerCodec()
    msg = get_message(TrpcContext())
    for i in range(5):
        msg.session_id = i
        msg.stream_id = i
        server_codec.store_init_meta(msg, trpc_pb2.TrpcStreamInitMeta())
        msg.stream_id = i + 1
        server_codec.store_init_meta(msg, trpc_pb2.TrpcStreamInitMeta())
    assert len(server_codec.init_metas) == 5

    msg.session_id = 1
    msg.stream_id = 1
    server_codec.delete_init_meta(msg)
    assert len(server_codec.init_metas[1]) == 1
    with pytest.raises(ServerSystemError):
        server_codec.set_init_meta(msg)

    msg.stream_id = 2
    server_codec.delete_init_meta(msg)
    assert 1 not in server_codec.init_metas

    server_codec.drop_session(3)
    assert 3 not in server_codec.init_metas
