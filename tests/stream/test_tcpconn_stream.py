# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Flow control and datagram read/write that replaced ``trpc.pool.tcpconn``.

Client UDP uses ``PacketReader`` / ``PacketWriter``. Server TCP pause/resume
lives on ``DefaultIoConnection``.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from tests.protocol._fakes import FakeTransport
from tests.protocol._fakes import ParserFactory
from trpc_sdk.transport import ConnectionOption
from trpc_sdk.transport import DefaultIoConnection
from trpc_sdk.transport import PacketConnection
from trpc_sdk.transport import PacketReader
from trpc_sdk.transport import PacketReaderProtocol
from trpc_sdk.transport import PacketWriter


@pytest.mark.asyncio
async def test_default_io_connection_pause_and_resume():
    async def _noop(_conn, _frame):
        return None

    conn = DefaultIoConnection(ConnectionOption(parser=ParserFactory(), msg_handler=_noop))
    conn.connection_made(FakeTransport())
    conn.pause_writing()
    assert conn._paused is True
    conn.pause_writing()
    assert conn._paused is True
    conn.resume_writing()
    assert conn._paused is False
    conn.resume_writing()
    assert conn._paused is False


@pytest.mark.asyncio
async def test_packet_reader_waits_then_returns_one_datagram():
    loop = asyncio.get_running_loop()
    reader = PacketReader(loop=loop)
    waiting = asyncio.create_task(reader.receive())
    await asyncio.sleep(0)
    assert not waiting.done()

    reader.feed_data(b"", ("127.0.0.1", 1))
    reader.feed_data(b"hi", ("127.0.0.1", 9))
    data, addr = await waiting
    assert data == b"hi"
    assert addr == ("127.0.0.1", 9)
    assert "PacketReader" in repr(reader) or "packets" not in repr(reader)


@pytest.mark.asyncio
async def test_packet_reader_exception_and_double_wait():
    loop = asyncio.get_running_loop()
    reader = PacketReader(loop=loop)
    reader.set_exception(RuntimeError("e"))
    with pytest.raises(RuntimeError, match="e"):
        await reader.receive()

    busy = PacketReader(loop=loop)
    busy._waiter = loop.create_future()
    with pytest.raises(RuntimeError, match="already waiting"):
        await busy._wait_for_data("read")


@pytest.mark.asyncio
async def test_packet_protocol_connection_lost_completes_close_future():
    loop = asyncio.get_running_loop()
    reader = PacketReader(loop=loop)
    proto = PacketReaderProtocol(reader, loop=loop)
    proto.connection_lost(None)
    assert proto._closed.result() is None
    assert reader.exception() is None

    failed_reader = PacketReader(loop=loop)
    failed = PacketReaderProtocol(failed_reader, loop=loop)
    failed.connection_lost(RuntimeError("x"))
    with pytest.raises(RuntimeError, match="x"):
        await failed._closed
    with pytest.raises(RuntimeError, match="x"):
        await failed_reader.receive()


@pytest.mark.asyncio
async def test_packet_writer_write_close_and_wait():
    loop = asyncio.get_running_loop()
    transport = MagicMock()
    transport.is_closing.return_value = False
    transport.get_extra_info.return_value = "extra"
    reader = PacketReader(loop=loop)
    proto = PacketReaderProtocol(reader, loop=loop)
    writer = PacketWriter(transport, proto, reader, loop)

    assert "PacketWriter" in repr(writer)
    writer.write(b"out")
    transport.sendto.assert_called_once_with(b"out")
    assert writer.get_extra_info("x") == "extra"
    assert writer.is_closing() is False
    writer.close()
    transport.close.assert_called_once()

    proto.connection_lost(None)
    await writer.wait_closed()

    conn = PacketConnection(transport, proto, reader, loop)
    reader.feed_data(b"back", ("127.0.0.1", 1))
    # connection_lost above stored the close, not an exception on a fresh reader.
    # The same reader may already have an exception from connection_lost(None), which does not set one.
    assert conn.exception() is None
    data, addr = await conn.receive()
    assert (data, addr) == (b"back", ("127.0.0.1", 1))
