# -*- coding: utf-8 -*-
"""Error / boundary coverage for task_stream_codec."""

import socket
import struct

import pytest

from trpc_sdk.codec.defines import FRAME_HEAD_LEN
from trpc_sdk.codec.message import get_message, new_frame_head
from trpc_sdk.codec.task_stream_codec import (
    TaskStreamClientCodec,
    TaskStreamServerCodec,
    TaskStreamServerParser,
    _common_pack,
)
from trpc_sdk.context import Context
from trpc_sdk.trpcpb import (
    TRPC_MAGIC_VALUE,
    TRPC_STREAM_CLOSE,
    TRPC_STREAM_FRAME_CLOSE,
    TRPC_STREAM_FRAME_DATA,
    TRPC_STREAM_FRAME_FEEDBACK,
    TRPC_STREAM_FRAME_INIT,
    TRPC_STREAM_RESET,
    TrpcStreamCloseMeta,
    TrpcStreamFeedBackMeta,
    TrpcStreamInitMeta,
    TrpcStreamInitResponseMeta,
)


def _ctx_msg(frame_state=TRPC_STREAM_FRAME_DATA, stream_id=1, reserved=0):
    ctx = Context()
    msg = get_message(ctx)
    fh = new_frame_head(0, frame_state, stream_id)
    if reserved:
        fh.frame_reserved = reserved
    msg.set_frame_head(fh)
    return msg


def test_common_pack_rejects_non_bytes_str(monkeypatch):
    msg = _ctx_msg()
    # Must be sized but neither bytes nor str to reach the explicit TypeError.
    with pytest.raises(TypeError, match="encode stream body"):
        _common_pack(msg.frame_head, [1, 2, 3])

    # Cover frame_reserved fill path without OverflowError from htonl+to_bytes(2).
    monkeypatch.setattr(socket, "htonl", lambda x: x & 0xFFFF)
    msg_r = _ctx_msg(reserved=3)
    packed = _common_pack(msg_r.frame_head, b"ab")
    assert isinstance(packed, bytes)
    assert len(packed) >= FRAME_HEAD_LEN

    packed2 = _common_pack(msg.frame_head, "hi")
    assert isinstance(packed2, bytes)


def test_encode_invalid_frame_state():
    codec = TaskStreamClientCodec()
    msg = _ctx_msg(frame_state=255)
    with pytest.raises(Exception, match="invalid"):
        codec.encode(msg, b"")


def test_encode_close_and_feedback_type_errors():
    codec = TaskStreamClientCodec()
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_CLOSE)
    msg.set_stream_frame(object())
    with pytest.raises(Exception, match="invalid"):
        codec.encode(msg, b"")

    msg2 = _ctx_msg(frame_state=TRPC_STREAM_FRAME_FEEDBACK)
    msg2.set_stream_frame(object())
    with pytest.raises(Exception, match="invalid"):
        codec.encode(msg2, b"")


def test_encode_feedback_ok():
    codec = TaskStreamClientCodec()
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_FEEDBACK)
    fb = TrpcStreamFeedBackMeta()
    fb.window_size_increment = 9
    msg.set_stream_frame(fb)
    assert isinstance(codec.encode(msg, b""), bytes)


def test_decode_invalid_frame_state():
    codec = TaskStreamClientCodec()
    msg = _ctx_msg()
    good = _common_pack(msg.frame_head, b"")
    bad = bytearray(good)
    bad[3] = 255
    with pytest.raises(Exception, match="invalid"):
        codec.decode(msg, bytes(bad))


def test_decode_close_reset_and_feedback():
    codec = TaskStreamClientCodec()
    close = TrpcStreamCloseMeta()
    close.close_type = TRPC_STREAM_RESET
    close.ret = 0
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_CLOSE)
    buf = _common_pack(msg.frame_head, close.SerializeToString())
    with pytest.raises(Exception, match="decode close"):
        codec.decode(msg, buf)

    fb = TrpcStreamFeedBackMeta()
    fb.window_size_increment = 1
    msg2 = _ctx_msg(frame_state=TRPC_STREAM_FRAME_FEEDBACK)
    buf2 = _common_pack(msg2.frame_head, fb.SerializeToString())
    codec.decode(msg2, buf2)
    assert msg2.stream_frame.window_size_increment == 1


def test_client_init_encode_none_body_and_bad_type():
    codec = TaskStreamClientCodec()
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_INIT)
    msg.client_rpc_name = "/a/b"
    data = codec.encode(msg, None)
    assert isinstance(data, bytes)

    msg2 = _ctx_msg(frame_state=TRPC_STREAM_FRAME_INIT)
    msg2.client_rpc_name = "/a/b"
    msg2.set_stream_frame(object())
    with pytest.raises(Exception, match="invalid"):
        codec.encode(msg2, b"")


def test_client_decode_init_nonzero_ret():
    codec = TaskStreamClientCodec()
    init_meta = TrpcStreamInitMeta()
    rsp = TrpcStreamInitResponseMeta()
    rsp.ret = 13
    init_meta.response_meta.CopyFrom(rsp)
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_INIT)
    buf = _common_pack(msg.frame_head, init_meta.SerializeToString())
    with pytest.raises(Exception, match="error 13"):
        codec.decode(msg, buf)


def test_server_encode_init_default_and_bad_type():
    codec = TaskStreamServerCodec()
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_INIT)
    data = codec.encode(msg, b"")
    assert isinstance(data, bytes)

    msg2 = _ctx_msg(frame_state=TRPC_STREAM_FRAME_INIT)
    msg2.set_stream_frame(object())
    with pytest.raises(Exception, match="invalid"):
        codec.encode(msg2, b"")


def test_server_delete_init_metas_missing():
    codec = TaskStreamServerCodec()
    msg = _ctx_msg(frame_state=TRPC_STREAM_FRAME_CLOSE)
    close = TrpcStreamCloseMeta()
    close.close_type = TRPC_STREAM_CLOSE
    close.ret = 0
    msg.set_stream_frame(close)
    assert isinstance(codec.encode(msg, b""), bytes)


def _pack_head(package_len, magic=TRPC_MAGIC_VALUE, frame_state=TRPC_STREAM_FRAME_DATA):
    buf = bytearray(FRAME_HEAD_LEN)
    struct.pack_into("=H", buf, 0, socket.htons(magic))
    buf[2] = 0
    buf[3] = frame_state
    struct.pack_into("=I", buf, 4, socket.htonl(package_len))
    return bytes(buf)


def test_server_parser_short_package_and_incomplete():
    parser = TaskStreamServerParser()
    parser.feeds_data(_pack_head(FRAME_HEAD_LEN - 1))
    with pytest.raises(Exception, match="package_len"):
        parser.read()

    parser2 = TaskStreamServerParser()
    parser2.feeds_data(_pack_head(FRAME_HEAD_LEN + 8))
    assert parser2.read() == []

    parser2.reset()
    assert len(parser2.buf) == 0
