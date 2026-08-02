# -*- coding: utf-8 -*-
"""Protocol / flow-control coverage for trpc.pool.tcpconn."""

import asyncio
import socket
from unittest.mock import MagicMock

import pytest

from trpc_sdk.pool import tcpconn
from trpc_sdk.pool.tcpconn import (
    FlowControlMixin,
    StreamReader,
    StreamReaderProtocol,
    StreamWriter,
)
from trpc_sdk.utils.loop_lib import get_or_create_event_loop


@pytest.mark.asyncio
async def test_flow_control_pause_resume_and_drain():
    loop = get_or_create_event_loop()
    loop.set_debug(True)
    mixin = FlowControlMixin(loop=loop)
    mixin.pause_writing()
    assert mixin._paused is True

    async def resume_later():
        await asyncio.sleep(0)
        mixin.resume_writing()

    drain_task = loop.create_task(mixin._drain_helper())
    loop.create_task(resume_later())
    await drain_task

    mixin2 = FlowControlMixin(loop=loop)
    mixin2.pause_writing()
    waiter = loop.create_future()
    mixin2._drain_waiter = waiter
    mixin2.connection_lost(None)
    assert waiter.result() is None

    mixin3 = FlowControlMixin(loop=loop)
    mixin3.pause_writing()
    waiter3 = loop.create_future()
    mixin3._drain_waiter = waiter3
    mixin3.connection_lost(RuntimeError("x"))
    with pytest.raises(RuntimeError):
        await waiter3

    mixin4 = FlowControlMixin(loop=loop)
    mixin4._connection_lost = True
    with pytest.raises(ConnectionResetError):
        await mixin4._drain_helper()
    loop.set_debug(False)


@pytest.mark.asyncio
async def test_stream_reader_protocol_connection_and_eof():
    loop = get_or_create_event_loop()
    reader = StreamReader(limit=64, loop=loop)
    called = {}

    async def on_connect(r, w):
        called["ok"] = (r, w)

    proto = StreamReaderProtocol(reader, client_connected_cb=on_connect, loop=loop)
    assert proto.stream_reader is reader
    assert proto.close_future is proto._closed

    transport = MagicMock()
    sock = MagicMock()
    sock.fileno.return_value = 3
    sock.proto = socket.IPPROTO_TCP
    transport.get_extra_info.side_effect = lambda key, default=None: {
        "sslcontext": object(),
        "socket": sock,
    }.get(key, default)

    proto.connection_made(transport)
    await asyncio.sleep(0)
    assert "ok" in called
    assert proto._over_ssl is True
    assert proto.eof_received() is False

    proto2 = StreamReaderProtocol(StreamReader(loop=loop), loop=loop)
    proto2._over_ssl = False
    proto2.connection_made(MagicMock(get_extra_info=lambda *a, **k: None))
    assert proto2.eof_received() is True

    proto3 = StreamReaderProtocol(StreamReader(loop=loop), loop=loop)
    proto3.connection_lost(ValueError("boom"))
    with pytest.raises(ValueError):
        await proto3.close_future


@pytest.mark.asyncio
async def test_stream_writer_repr_drain_and_helpers():
    loop = get_or_create_event_loop()
    transport = MagicMock()
    transport.is_closing.return_value = False
    transport.can_write_eof.return_value = True
    transport.get_extra_info.return_value = "extra"
    reader = StreamReader(loop=loop)
    proto = StreamReaderProtocol(reader, loop=loop)
    writer = StreamWriter(transport, proto, reader, loop)

    text = repr(writer)
    assert "StreamWriter" in text
    assert writer.can_write_eof() is True
    writer.write_eof()
    assert writer.get_extra_info("x") == "extra"

    reader.set_exception(OSError("r"))
    with pytest.raises(OSError):
        await writer.drain()

    reader2 = StreamReader(loop=loop)
    proto2 = StreamReaderProtocol(reader2, loop=loop)
    transport2 = MagicMock()
    transport2.is_closing.return_value = True
    writer2 = StreamWriter(transport2, proto2, reader2, loop)
    await writer2.drain()

    closed = loop.create_future()
    closed.set_result(None)
    proto2._closed = closed
    await writer2.wait_closed()


@pytest.mark.asyncio
async def test_stream_reader_limit_backpressure_and_readexactly():
    loop = get_or_create_event_loop()
    with pytest.raises(ValueError, match="Limit"):
        StreamReader(limit=0, loop=loop)

    reader = StreamReader(limit=4, loop=loop)
    transport = MagicMock()
    reader.set_transport(transport)
    reader.feed_data(b"abcdefghij")  # > 2 * limit → pause
    assert reader._paused is True
    transport.pause_reading.assert_called()
    text = repr(reader)
    assert "paused" in text or "bytes" in text

    # NotImplementedError on pause → drop transport
    reader2 = StreamReader(limit=2, loop=loop)
    bad_transport = MagicMock()
    bad_transport.pause_reading.side_effect = NotImplementedError
    reader2.set_transport(bad_transport)
    reader2.feed_data(b"xxxxxx")
    assert reader2._transport is None

    reader3 = StreamReader(loop=loop)
    reader3.set_exception(RuntimeError("e"))
    with pytest.raises(RuntimeError):
        await reader3.readexactly(1)

    with pytest.raises(ValueError):
        await reader3.readexactly(-1)

    reader4 = StreamReader(loop=loop)
    assert await reader4.readexactly(0) == b""

    reader5 = StreamReader(loop=loop)
    reader5.feed_data(b"ab")
    reader5.feed_eof()
    with pytest.raises(asyncio.IncompleteReadError):
        await reader5.readexactly(4)

    reader6 = StreamReader(loop=loop)
    reader6._waiter = loop.create_future()
    with pytest.raises(RuntimeError, match="already waiting"):
        await reader6._wait_for_data("read")

    reader7 = StreamReader(limit=2, loop=loop)
    t = MagicMock()
    reader7.set_transport(t)
    reader7._paused = True
    reader7._buffer.extend(b"x")
    reader7._maybe_resume_transport()
    t.resume_reading.assert_called()
