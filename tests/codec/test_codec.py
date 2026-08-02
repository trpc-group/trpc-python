# -*- coding: utf-8 -*-
"""Test codec module"""
import time
import asyncio

import pytest

from trpc_sdk.codec import (
    codec,
    set_request_attachment,
    get_request_attachment,
    set_response_attachment,
    get_response_attachment,
)
from trpc_sdk.codec.exceptions import (
    TaskStreamClientReadFrameError,
    TaskStreamClientDecodeError,
    TaskStreamClientEncodeError,
    TaskStreamServerEncodeError,
)
from trpc_sdk.trpcpb import TrpcStreamInitMeta, TrpcStreamCloseMeta, TrpcStreamFeedBackMeta
from trpc_sdk.exceptions import ServerDecodeError, ClientDecodeError, ServerSystemError
from trpc_sdk.context import Context
from trpc_sdk.config.config import SocketType
from trpc_sdk.pool.connpool import get_pool_connection
from trpc_sdk.abc.transport import BaseClientTransportOptions
from trpc_sdk.codec.task_stream_codec import TaskStreamServerCodec
from trpc_sdk.codec.compress import CompressType
from trpc_sdk.codec.compress_abc import AbstractCompress
from trpc_sdk.codec.serialization_abc import AbstractSerializer

from ..common_util import ThreadUnixServer


def test_checker():
    """Test checker"""
    checker = codec.TrpcServerParser()
    assert not checker.read()

    # buf len < package len
    data = b'\t0\x00\x00\x00\x00\x00\x1d\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01hello'
    checker.feeds_data(data)
    assert not checker.read()

    checker = codec.TrpcServerParser()
    # invalid package len
    checker.feeds_data(b' ')
    results = checker.read()
    assert results == []

    checker = codec.TrpcServerParser()
    # invalid magic number
    data = b'\t1\x00\x00\x00\x00\x00\x1d\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01hello world'
    checker.feeds_data(data)
    try:
        checker.read()
        assert False
    except ServerDecodeError as e:
        assert e.msg == 'framer head magic not match'

    checker = codec.TrpcServerParser()
    server_codec = codec.ServerCodec()
    msg = codec.Message()
    req_body = b'hello world' * (1024**2)
    req_buff = server_codec.encode(msg, req_body)
    checker.feeds_data(req_buff)
    try:
        checker.read()
        assert False
    except ServerDecodeError as e:
        assert e.msg == 'frame header total len too large'


def test_client_codec_encode():
    """Test client codec encode"""
    client_codec = codec.ClientCodec()
    msg = codec.Message()
    req_body = b'hello world'
    req_buff = client_codec.encode(msg, req_body)
    assert req_buff == b'\t0\x00\x00\x00\x00\x006\x00\x1b\x00\x00\x00\x00\x00\x00\x18\x01*\x17trpc_sdk.' \
                       b'app.server.servicehello world'


def test_client_codec_encode_with_attachment():
    """Test client codec encode with attachment"""
    client_codec = codec.ClientCodec()
    server_codec = codec.ServerCodec()
    msg = codec.Message()
    msg.set_request_attachment(b'test attachment')

    req_buff = client_codec.encode(msg, b'hello world')

    server_msg = codec.Message()
    req_body = server_codec.codec.decode(server_msg, req_buff)
    assert req_body == b'hello world'
    assert server_msg.request_attachment == b'test attachment'
    assert server_msg.server_req_head.attachment_size == len(b'test attachment')


def test_server_codec_decode_invalid_attachment_size():
    """Test server codec rejects invalid request attachment size"""
    client_codec = codec.ClientCodec()
    server_codec = codec.ServerCodec()
    msg = codec.Message()
    msg.set_request_attachment(b'test attachment')
    invalid_req_buff = client_codec.encode(msg, b'hello world')[:-12]

    with pytest.raises(ServerDecodeError):
        server_codec.codec.decode(codec.Message(), invalid_req_buff)


@pytest.mark.asyncio
async def test_server_codec_decode():
    """Test server codec decode"""
    server_codec = codec.ServerCodec()
    msg = codec.Message()
    req_buff = b'\t0\x00\x00\x00\x00\x00\x1d\x00\x02\x00\x00\x00\x00\x00\x00 \x01hello world'
    req_body = await server_codec.decode(msg, req_buff)
    assert req_body == b'hello world'
    for i in range(0, 5):
        msg.stream_id = i
        msg.session_id = i
        init_meta = TrpcStreamInitMeta()
        server_codec.store_init_meta(msg, init_meta)
        msg.stream_id = i + 1
        server_codec.store_init_meta(msg, init_meta)
    assert len(server_codec.init_metas) == 5
    msg.session_id = 10
    msg.stream_id = 10
    server_codec.delete_init_meta(msg)
    assert len(server_codec.init_metas) == 5
    msg.session_id = 1
    msg.stream_id = 1
    server_codec.delete_init_meta(msg)
    assert len(server_codec.init_metas) == 5
    assert len(server_codec.init_metas[1]) == 1
    with pytest.raises(ServerSystemError):
        server_codec.set_init_meta(msg)
    msg.session_id = 1
    msg.stream_id = 2
    server_codec.delete_init_meta(msg)
    assert len(server_codec.init_metas) == 4
    assert 1 not in server_codec.init_metas
    with pytest.raises(ServerSystemError):
        server_codec.set_init_meta(msg)


@pytest.mark.asyncio
async def test_server_codec_decode_with_attachment():
    """Test server codec decode with attachment"""
    client_codec = codec.ClientCodec()
    server_codec = codec.ServerCodec()
    client_msg = codec.Message()
    client_msg.set_request_attachment(b'test attachment')

    req_buff = client_codec.encode(client_msg, b'hello world')
    server_msg = codec.Message()
    req_body = await server_codec.decode(server_msg, req_buff)

    assert req_body == b'hello world'
    assert server_msg.request_attachment == b'test attachment'


def test_server_codec_encode():
    """Test server codec encode"""
    server_codec = codec.ServerCodec()
    msg = codec.Message()
    msg.set_server_meta_data({"trpc": b""})
    rsp_body = b'sayHello'
    rsp_buff = server_codec.encode(msg, rsp_body)
    assert rsp_buff == b'\t0\x00\x00\x00\x00\x00"\x00\n\x00\x00\x00\x00\x00\x00B\x08\n\x04trpc\x12\x00sayHello'


def test_server_codec_encode_with_attachment():
    """Test server codec encode with attachment"""
    server_codec = codec.ServerCodec()
    client_codec = codec.ClientCodec()
    msg = codec.Message()
    msg.set_response_attachment(b'test attachment')

    rsp_buff = server_codec.encode(msg, b'sayHello')

    client_msg = codec.Message()
    rsp_body = client_codec.decode(client_msg, rsp_buff)
    assert rsp_body == b'sayHello'
    assert client_msg.response_attachment == b'test attachment'
    assert client_msg.client_rsp_head.attachment_size == len(b'test attachment')


def test_client_codec_decode_invalid_attachment_size():
    """Test client codec rejects invalid response attachment size"""
    server_codec = codec.ServerCodec()
    client_codec = codec.ClientCodec()
    msg = codec.Message()
    msg.set_response_attachment(b'test attachment')
    invalid_rsp_buff = server_codec.encode(msg, b'sayHello')[:-9]

    with pytest.raises(ClientDecodeError):
        client_codec.decode(codec.Message(), invalid_rsp_buff)


def test_client_codec_decode():
    """Test client codec decode"""
    client_codec = codec.ClientCodec()
    msg = codec.Message()
    rsp_buff = b'\t0\x00\x00\x00\x00\x00"\x00\n\x00\x00\x00\x00\x00\x00B\x08\n\x04trpc\x12\x00sayHello'
    rsp_body = client_codec.decode(msg, rsp_buff)
    assert rsp_body == b'sayHello'


def test_client_codec_decode_with_attachment():
    """Test client codec decode with attachment"""
    server_codec = codec.ServerCodec()
    client_codec = codec.ClientCodec()
    server_msg = codec.Message()
    server_msg.set_response_attachment(b'test attachment')

    rsp_buff = server_codec.encode(server_msg, b'sayHello')
    client_msg = codec.Message()
    rsp_body = client_codec.decode(client_msg, rsp_buff)

    assert rsp_body == b'sayHello'
    assert client_msg.response_attachment == b'test attachment'


def test_context_attachment_api():
    """Test context attachment API"""
    ctx = Context()

    set_request_attachment(ctx, b'request attachment')
    assert get_request_attachment(ctx) == b'request attachment'
    set_request_attachment(ctx, 'request attachment')
    assert get_request_attachment(ctx, is_str=True) == 'request attachment'

    set_response_attachment(ctx, b'response attachment')
    assert get_response_attachment(ctx) == b'response attachment'
    set_response_attachment(ctx, 'response attachment')
    assert get_response_attachment(ctx, is_str=True) == 'response attachment'


def test_context_attachment_methods():
    """Test context attachment methods"""
    ctx = Context()
    with pytest.raises(ValueError):
        ctx.set_request_attachment(b'request attachment')
    with pytest.raises(ValueError):
        ctx.get_request_attachment()
    with pytest.raises(ValueError):
        ctx.set_response_attachment(b'response attachment')
    with pytest.raises(ValueError):
        ctx.get_response_attachment()

    codec.get_message(ctx)
    ctx.set_request_attachment(b'request attachment')
    assert ctx.get_request_attachment() == b'request attachment'
    ctx.set_response_attachment(b'response attachment')
    assert ctx.get_response_attachment() == b'response attachment'


def get_msg(state):
    """Get msg"""
    msg = codec.Message()
    head = codec.CFrameHead()
    head.frame_type = 1
    head.frame_state = state
    msg.set_frame_head(head)
    return msg


def test_client_codec_stream_encode_init():
    """Test stream encode init for client"""
    client_codec = codec.ClientCodec()
    msg = get_msg(1)
    rsp_buff = b''
    rsp_body = client_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x01\x00\x00\x00+\x00\x00\x00\x00\x00\x00\x00\x00\n\x19\n\x17trpc_sdk.app.server.service'


def test_client_codec_stream_encode_data():
    """Test stream encode data in client-side"""
    client_codec = codec.ClientCodec()
    msg = get_msg(2)
    rsp_buff = b''
    rsp_body = client_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x02\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00'


def test_client_codec_stream_encode_feedback():
    """Test stream encode feedback in client-side"""
    client_codec = codec.ClientCodec()
    msg = get_msg(3)
    msg.set_stream_frame(TrpcStreamFeedBackMeta())
    rsp_buff = b''
    rsp_body = client_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x03\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00'


def test_client_codec_stream_encode_close():
    """Test stream encode close in client-side"""
    client_codec = codec.ClientCodec()
    msg = get_msg(4)
    msg.set_stream_frame(TrpcStreamCloseMeta())
    rsp_buff = b''
    rsp_body = client_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x04\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00'


def test_client_codec_stream_encode_invalid():
    """Test stream invalid encode in client-side"""
    client_codec = codec.ClientCodec()
    msg = get_msg(5)
    rsp_buff = b''
    try:
        client_codec.encode(msg, rsp_buff)
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None


def test_client_codec_stream_decode():
    """Test stream decode in client-side"""
    client_codec = codec.ClientCodec()
    msg = get_msg(1)
    rsp_buff = b'\t0\x01\x01\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'

    rsp_body = client_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    rsp_buff = b'\t0\x01\x02\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = client_codec.decode(msg, rsp_buff)
    assert rsp_body == b'\x12\x00'

    rsp_buff = b'\t0\x01\x03\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = client_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    rsp_buff = b'\t0\x01\x04\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = client_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    try:
        rsp_buff = b'\t0\x01\x05\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
        client_codec.decode(msg, rsp_buff)
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None

    try:
        msg = get_msg(1)
        client_codec.decode(msg, b'')
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None


@pytest.mark.asyncio
async def test_server_codec_stream_decode():
    """Test stream encode data in server-side"""
    server_codec = codec.ServerCodec()
    msg = get_msg(1)
    rsp_buff = b'\t0\x01\x01\x00\x00\x00f\x00\x00\x00\x00\x00d\x00\x00\nP\n\x17trpc_sdk.app.server.service'\
        b'\x12\x13trpc_sdk.stream.Greeter\x1a /trpc_sdk.stream.Greeter/SayHelloAll\x18\xff\xff\x03'

    rsp_body = await server_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    rsp_buff = b'\t0\x01\x02\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = await server_codec.decode(msg, rsp_buff)
    assert rsp_body == b'\x12\x00'

    rsp_buff = b'\t0\x01\x03\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = await server_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    rsp_buff = b'\t0\x01\x04\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
    rsp_body = await server_codec.decode(msg, rsp_buff)
    assert rsp_body == b''

    try:
        rsp_buff = b'\t0\x01\x05\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'
        await server_codec.decode(msg, rsp_buff)
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None

    try:
        msg = get_msg(1)
        await server_codec.decode(msg, b'')
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None


@pytest.mark.asyncio
async def test_server_codec_stream_encode():
    """Test stream encode in server-side"""
    server_codec = codec.ServerCodec()
    msg = get_msg(1)
    rsp_buff = b''

    rsp_body = server_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x01\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x00\x12\x00'

    rsp_buff = b'\n\x05test0'
    msg = get_msg(2)
    rsp_body = server_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x02\x00\x00\x00\x17\x00\x00\x00\x00\x00\x00\x00\x00\n\x05test0'

    rsp_buff = b''
    msg = get_msg(4)
    msg.stream_frame = TrpcStreamCloseMeta()
    rsp_body = server_codec.encode(msg, rsp_buff)
    assert rsp_body == b'\t0\x01\x04\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00'

    try:
        rsp_buff = b''
        server_codec.encode(msg, rsp_buff)
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None


class _TestProtocol(asyncio.Protocol):
    """Test protocol"""
    test_data = b''

    @classmethod
    def _get_data(cls):
        return cls.test_data

    @classmethod
    def set_data(cls, data):
        """Set test data"""
        cls.test_data = data

    def __init__(self, loop: asyncio.BaseEventLoop):
        self._transport = None
        self.__loop = loop

    def connection_made(self, transport: asyncio.Transport):
        self._transport = transport

    async def _handle_package(self, _: bytes):
        """Handle a complete package"""
        ctx = Context()
        ctx.recv_time = time.time() * 1000
        ctx.timeout = 1000
        self._transport.write(self._get_data())

    def data_received(self, data):
        self.__loop.create_task(self._handle_package(data))


class TestUnixConnectionPool:
    """Test unix connection pool"""
    unix_srv = None

    def setup_class(self):
        """Setup class"""
        self.unix_srv = ThreadUnixServer(("./trpc_local", 0), _TestProtocol)
        self.unix_srv.start()

    def teardown_class(self):
        """Teardown_class class"""
        # self.unix_srv.stop()

    @pytest.mark.asyncio
    async def test_client_parse(self):
        """Test client parse"""
        client_codec = codec.ClientCodec()
        msg = codec.Message()
        req_body = b'hello world'
        req_buff = client_codec.encode(msg, req_body)

        trans_opt = BaseClientTransportOptions()
        trans_opt.network = "tcp"
        trans_opt.socket_type = SocketType.UNIX
        trans_opt.address = ("./trpc_local", 0)
        trans_opt.short_conn = True
        while self.unix_srv.is_start is False:
            await asyncio.sleep(0.1)
        conn = await get_pool_connection(trans_opt, 10)
        conn.write(req_buff)
        client_checker = codec.get_client_checker("trpc")
        rsp_buff = b''
        _TestProtocol.set_data(b'\t0\x01\x04\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00')
        rsp_buff = await client_checker.read_frame(conn)
        conn.write(req_buff)
        _TestProtocol.set_data(b'aaaaaaaaaaaaaaaaaaaaa')
        with pytest.raises(ClientDecodeError):
            await client_checker.read_frame(conn)
        client_stream_checker = codec.get_client_checker('inner_task_stream')
        conn = await get_pool_connection(trans_opt, 10)
        conn.write(req_buff)
        _TestProtocol.set_data(b'\t0\x01\x04\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00')
        rsp_buff = await client_stream_checker.read_frame(conn)
        assert rsp_buff
        _TestProtocol.set_data(b'aaaaaaaaaaaaaaaaaaaaa')
        conn.write(req_buff)
        with pytest.raises(TaskStreamClientReadFrameError):
            await client_stream_checker.read_frame(conn)


class _MockTaskStreamServerCodec(TaskStreamServerCodec):

    def get_init_metas(self):
        """Get init meta"""
        return self._init_metas

    def delete_init_metas(self, message: codec.Message):
        """del meta"""
        self._delete_init_metas(message)

    def check_init_metas(self, message: codec.Message):
        """check meta"""
        self._check_init_metas(message)


def test_task_stream_codec():
    """Test stream codec"""
    mock_codec = _MockTaskStreamServerCodec()
    init_metas = mock_codec.get_init_metas()
    init_metas[1][1] = TrpcStreamInitMeta()
    init_metas[1][2] = TrpcStreamInitMeta()
    msg = codec.Message()
    msg.session_id = 10
    msg.stream_id = 10
    with pytest.raises(Exception):
        mock_codec.check_init_metas(msg)
    msg.session_id = 1
    with pytest.raises(Exception):
        mock_codec.check_init_metas(msg)

    msg.session_id = 1
    msg.stream_id = 1
    mock_codec.delete_init_metas(msg)
    msg.stream_id = 2
    mock_codec.delete_init_metas(msg)
    assert len(init_metas) == 0

class _Compress(AbstractCompress):
    def compress(self, data: bytes) -> bytes:
        return super().compress(data)

    def decompress(self, data: bytes) -> bytes:
        return super().decompress(data)


class _Serializer(AbstractSerializer):
    def serialize(self, body: object) -> bytes:
        return super().serialize(body)

    def deserialize(self, data: bytes, body: object):
        return super().deserialize(data, body)


def test_compress_abc_default_bodies():
    c = _Compress()
    assert c.compress(b"x") == b""
    assert c.decompress(b"x") == b""


def test_serializer_abc_default_serialize():
    assert _Serializer().serialize(object()) == b""


def test_task_stream_exception_ctors():
    assert TaskStreamClientEncodeError("e").msg == "e"
    assert TaskStreamClientDecodeError("d").msg == "d"
    assert TaskStreamServerEncodeError("s").msg == "s"


def test_compress_type_fallback_to_trpc_and_missing():
    # Class-level fallback into TrpcCompressType (compress.py:27).
    assert CompressType.TRPC_DEFAULT_COMPRESS == 0
    # Instance-level missing attr raises (compress.py:56).
    with pytest.raises(AttributeError):
        _ = CompressType().DefinitelyMissingCompressType
