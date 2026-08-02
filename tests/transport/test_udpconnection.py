# -*- coding: utf-8 -*-
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import trpc_sdk.transport.udp_connection as udp_connection
from trpc_sdk.transport import udp_connection as udp
from trpc_sdk.utils.loop_lib import get_or_create_event_loop


def test_new():
    try:
        preader = udp_connection.PacketReader()
        preader.feed_data(
            b'\t0\x00\x00\x00\x00\x00)\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x15hello: trpc-go-client', '127.0.0.1')
    except Exception:  # pylint: disable=broad-except
        assert False


@pytest.mark.asyncio
async def test_packet_reader_protocol_error_and_lost():
    loop = get_or_create_event_loop()
    reader = udp.PacketReader(loop=loop)
    proto = udp.PacketReaderProtocol(reader, loop=loop)
    proto.datagram_received(b"hi", ("127.0.0.1", 1))
    assert reader._packet

    proto.error_received(RuntimeError("e"))
    assert reader.exception() is not None

    reader2 = udp.PacketReader(loop=loop)
    proto2 = udp.PacketReaderProtocol(reader2, loop=loop)
    proto2.connection_lost(None)
    await proto2._closed

    reader3 = udp.PacketReader(loop=loop)
    proto3 = udp.PacketReaderProtocol(reader3, loop=loop)
    proto3.connection_lost(ValueError("x"))
    with pytest.raises(ValueError):
        await proto3._closed


@pytest.mark.asyncio
async def test_packet_reader_repr_receive_and_wait_guard():
    loop = get_or_create_event_loop()
    reader = udp.PacketReader(loop=loop)
    reader.feed_data(b"", ("1", 1))  # no-op
    reader.feed_data(b"a", ("127.0.0.1", 9))
    reader.set_exception(None)  # clear
    reader._exception = None
    text = repr(reader)
    assert "PacketReader" in text

    data = await reader.receive()
    assert data[0] == b"a"

    reader2 = udp.PacketReader(loop=loop)
    reader2.set_exception(OSError("boom"))
    with pytest.raises(OSError):
        await reader2.receive()

    reader3 = udp.PacketReader(loop=loop)
    reader3._waiter = loop.create_future()
    with pytest.raises(RuntimeError, match="already waiting"):
        await reader3._wait_for_data("read")

    reader4 = udp.PacketReader(loop=loop)

    async def feed():
        await asyncio.sleep(0)
        reader4.feed_data(b"z", ("::1", 1))

    loop.create_task(feed())
    assert (await reader4.receive())[0] == b"z"


@pytest.mark.asyncio
async def test_packet_writer_and_connection():
    loop = get_or_create_event_loop()
    transport = MagicMock()
    transport.is_closing.return_value = False
    transport.get_extra_info.return_value = "info"
    reader = udp.PacketReader(loop=loop)
    proto = udp.PacketReaderProtocol(reader, loop=loop)
    writer = udp.PacketWriter(transport, proto, reader, loop)
    assert "PacketWriter" in repr(writer)
    writer.write(b"x")
    transport.sendto.assert_called_with(b"x")
    assert writer.is_closing() is False
    assert writer.get_extra_info("k") == "info"
    writer.close()

    closed = loop.create_future()
    closed.set_result(None)
    proto._closed = closed
    await writer.wait_closed()

    conn = udp.PacketConnection(transport, proto, reader, loop)
    assert conn.exception() is None
    reader.feed_data(b"p", ("127.0.0.1", 2))
    assert (await conn.receive())[0] == b"p"
