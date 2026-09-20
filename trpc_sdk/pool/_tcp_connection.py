# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Open a TCP or Unix stream and wrap it as :class:`Connection`.

:func:`open_connection` and :func:`open_unix_connection` build a
:class:`~trpc_sdk.pool.StreamReader` plus
:class:`~trpc_sdk.pool.StreamReaderProtocol`, then return a
:class:`Connection` that subclasses
:class:`~trpc_sdk.pool.StreamWriter` and delegates reads to
that reader. Callers in :mod:`trpc_sdk.pool._dial` and
:mod:`trpc_sdk.pool._multiplexed` use these helpers instead of
``loop.create_connection`` directly.
"""

from __future__ import annotations

from asyncio import BaseEventLoop

from trpc_sdk.common import get_or_create_event_loop

from ._constants import DEFAULT_STREAM_LIMIT
from ._stream_io import StreamReader
from ._stream_io import StreamReaderProtocol
from ._stream_io import StreamWriter


async def open_connection(host: str = None,
                          port: int = None,
                          *,
                          loop: BaseEventLoop = None,
                          limit: int = DEFAULT_STREAM_LIMIT,
                          **kwds):
    """A wrapper for create_connection() returning a Connection.

    The reader returned is a StreamReader instance; the writer is a
    StreamWriter instance.

    The arguments are all the usual arguments to create_connection()
    except protocol_factory; most common are positional host and port,
    with various optional keyword arguments following.

    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).

    (If you want to customize the StreamReader and/or
    StreamReaderProtocol classes, just copy the code -- there's
    really nothing special here except some convenience.)
    """
    loop = loop or get_or_create_event_loop()

    reader = StreamReader(limit=limit, loop=loop)
    protocol = StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port, **kwds)
    return Connection(transport, protocol, reader, loop)


async def open_unix_connection(host: str = None,
                               *,
                               loop: BaseEventLoop = None,
                               limit: int = DEFAULT_STREAM_LIMIT,
                               **kwds):
    """A wrapper for create_connection() returning a Connection.

    The reader returned is a StreamReader instance; the writer is a
    StreamWriter instance.

    The arguments are all the usual arguments to create_connection()
    except protocol_factory; most common are positional host and port,
    with various optional keyword arguments following.

    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).

    (If you want to customize the StreamReader and/or
    StreamReaderProtocol classes, just copy the code -- there's
    really nothing special here except some convenience.)
    """
    loop = loop or get_or_create_event_loop()

    reader = StreamReader(limit=limit, loop=loop)
    protocol = StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_unix_connection(lambda: protocol, host, **kwds)
    return Connection(transport, protocol, reader, loop)


class Connection(StreamWriter):
    """Wraps a StreamWriter and StreamReader.
    """

    # reader
    def exception(self):
        """Read exception"""
        self._reader.exception()

    def at_eof(self):
        """At end of buff"""
        return self._reader.at_eof()

    def buffer_size(self):
        """The size of buff"""
        return len(self._reader.buffer)

    async def readexactly(self, num):
        """read n bytes"""
        return await self._reader.readexactly(num)
