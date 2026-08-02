# -*- coding: utf-8 -*-
"""Test task stream handler"""
import asyncio
import time
import traceback

import pytest

from tests.common_util import ThreadUnixServer
from trpc_sdk.client import with_current_serialization_type
from trpc_sdk.codec.message import get_message
from trpc_sdk.codec.serialization import SerializationType
from trpc_sdk.codec.serialization_noop import Body
from trpc_sdk.codec.task_stream_codec import TaskStreamServerParser
from trpc_sdk.context import Context
from trpc_sdk.stream.exceptions import TaskStreamClientDecodeError
from trpc_sdk.stream.exceptions import TaskStreamClientEncodeError
from trpc_sdk.stream.exceptions import TaskStreamClientReadFrameError
from trpc_sdk.stream.exceptions import TaskStreamServerDecodeError
from trpc_sdk.stream.exceptions import TaskStreamServerEncodeError
from trpc_sdk.stream.server import ServerStream
from trpc_sdk.stream.task_client import BaseTaskStreamClientHandler
from trpc_sdk.stream.task_service import BaseTaskStreamServiceHandler
from trpc_sdk.stream.task_service import TaskStreamServiceAdapter


class MockTaskStreamServiceHandler(BaseTaskStreamServiceHandler):
    """Task stream service"""

    def create_req_obj(self) -> object:
        return Body()

    def create_rsp_obj(self) -> object:
        return Body()

    async def rsp_method_handle(self, request: object, stream: ServerStream):
        stream_method = self._create_rsp_stream_method(stream)
        print(f"#### recv req is {request.data}")
        rsp = Body()
        rsp.data = b'rsp_method_handle'
        await stream_method.send(rsp)
        await asyncio.sleep(1)

    async def req_method_handle(self, stream: ServerStream):
        stream_method = self._create_req_stream_method(stream)
        while True:
            try:
                ret = await stream_method.recv()
                print(f"#### req_method_handle recv {ret.data}")
            except EOFError:
                rsp = b"req_method_handle"
                await stream_method.send_and_close(rsp)
                return
            except Exception as err:  # pylint: disable=broad-exception-caught
                print(f"#### req_method_handle {err}")

    async def bidi_method_handle(self, stream: ServerStream):
        stream_method = self._create_bidi_stream_method(stream)
        while True:
            try:
                req = await stream_method.recv()
                print(f"####  bidi recv request is : {req.data}")
            except EOFError as err:
                print(f"#### recv done error {err}")
                return
            except Exception as err:  # pylint: disable=broad-except
                print(f"recv err: {err}, {traceback.format_stack()}")
            # business
            rsp = Body()
            rsp.data = f"{req.data} rsp is task data"
            await stream_method.send(rsp)


class MockTaskStreamClientHandler(BaseTaskStreamClientHandler):
    """Task stream service"""

    def create_req_obj(self) -> object:
        return Body()

    def create_rsp_obj(self) -> object:
        return Body()


class _TestProtocol(asyncio.Protocol):
    """Test protocol"""
    __count: int = 0  # record object num

    def __init__(self, loop: asyncio.BaseEventLoop):
        self._transport = None
        self.__service_adapter = TaskStreamServiceAdapter(task_service=MockTaskStreamServiceHandler())
        self._checker = TaskStreamServerParser()
        self.__loop = loop
        self.__session_id = 0

    def connection_made(self, transport: asyncio.Transport):
        self._transport = transport
        self.__session_id = self.__add_count() & 0x7fffffff

    @classmethod
    def __add_count(cls):
        """Add object num"""
        last = cls.__count
        cls.__count += 1
        return last

    async def _handle_package(self, package: bytes):
        """Handle a complete package"""
        ctx = Context()
        ctx.recv_time = time.time() * 1000
        ctx.timeout = self.__service_adapter.service_opts.timeout or 1000
        ctx.with_meta_data("aio_transport", self._transport)
        ctx.with_meta_data("session_id", self.__session_id)
        req_body = self.__service_adapter.decode_header(ctx, package)
        await self.__service_adapter.handle(ctx, req_body)

    def data_received(self, data):
        self._checker.feeds_data(data)
        packages = self._checker.read()
        for package in packages:
            self.__loop.create_task(self._handle_package(package))


class TestUnixConnectionPool:
    """Test unix connection pool"""

    def setup_class(self):
        """Setup class"""
        self.unix_srv = ThreadUnixServer(("./trpc_local", 0), _TestProtocol)
        self.unix_srv.start()
        self._options = [with_current_serialization_type(SerializationType.Noop)]

    def teardown_class(self):
        """Teardown_class class"""
        pass

    @pytest.mark.asyncio
    async def test_client_stream_transport(self):
        """Test client transport"""
        while self.unix_srv.tid == 0:
            await asyncio.sleep(0.5)
        mock_handler = MockTaskStreamClientHandler()
        mock_handler.set_unix_path("./trpc_local")
        assert mock_handler.stream_client is not None
        assert mock_handler.client_unix_path == "./trpc_local"

        ctx = Context()
        get_message(ctx)
        print("xx start req_method_handle")
        req_stream = await mock_handler.req_method_handle(ctx, self._options)
        with pytest.raises(NotImplementedError):
            await req_stream.close_send()
        with pytest.raises(NotImplementedError):
            await req_stream.recv()

        print("xx start send")
        body = Body()
        body.data = b'req_stream'
        await req_stream.send(body)
        rsp = await req_stream.close_and_recv()
        print(f"#### req stream recv rps {rsp.data}")

    @pytest.mark.asyncio
    async def test_server_stream_transport(self):
        """Test client transport"""

        while self.unix_srv.tid == 0:
            await asyncio.sleep(0.5)
        mock_handler = MockTaskStreamClientHandler()
        mock_handler.set_unix_path("./trpc_local")
        assert mock_handler.stream_client is not None
        assert mock_handler.client_unix_path == "./trpc_local"

        ctx = Context()
        get_message(ctx)
        body = Body()
        body.data = b'rsp_stream'
        rsp_stream = await mock_handler.rsp_method_handle(ctx, body, self._options)
        with pytest.raises(NotImplementedError):
            await rsp_stream.send('xx')
        with pytest.raises(NotImplementedError):
            await rsp_stream.close_and_recv()
        with pytest.raises(NotImplementedError):
            await rsp_stream.close_send()

        rsp = await rsp_stream.recv()
        print(f"#### rsp stream recv rps {rsp.data}")

    @pytest.mark.asyncio
    async def test_bidi_stream_transport(self):
        """Test client transport"""

        while self.unix_srv.tid == 0:
            await asyncio.sleep(0.5)
        mock_handler = MockTaskStreamClientHandler()
        mock_handler.set_unix_path("./trpc_local")
        assert mock_handler.stream_client is not None
        assert mock_handler.client_unix_path == "./trpc_local"

        ctx = Context()
        get_message(ctx)
        body = Body()
        body.data = b'bidi_stream'
        bidi_stream = await mock_handler.bidi_method_handle(ctx, self._options)

        rsp = await bidi_stream.send(body)
        await bidi_stream.close_send()
        rsp = await bidi_stream.recv()
        print(f"#### bidi stream recv rps {rsp.data}")


def test_exception():
    """Test exception"""
    client_read_err = TaskStreamClientReadFrameError("test1")
    assert client_read_err.code == 171
    assert client_read_err.type == 1
    assert client_read_err.msg == "test1"

    client_read_err = TaskStreamClientEncodeError("test2")
    assert client_read_err.code == 321
    assert client_read_err.type == 1
    assert client_read_err.msg == "test2"

    client_read_err = TaskStreamClientDecodeError("test3")
    assert client_read_err.code == 322
    assert client_read_err.type == 1
    assert client_read_err.msg == "test3"

    server_read_err = TaskStreamServerEncodeError("test4")
    assert server_read_err.code == 221
    assert server_read_err.type == 1
    assert server_read_err.msg == "test4"

    server_read_err = TaskStreamServerDecodeError("test5")
    assert server_read_err.code == 222
    assert server_read_err.type == 1
    assert server_read_err.msg == "test5"
