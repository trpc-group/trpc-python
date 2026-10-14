# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin factories, protocol parsers, and codec lookups for tRPC.

Importing this module registers the ``"trpc"`` entries for
``PluginType.SERVER_PARSER`` and ``PluginType.CLIENT_PARSER``. The
codec plugins themselves are registered by :mod:`trpc_sdk.codec._codec`;
``get_client_codec`` / ``get_server_codec`` only read the plugin table,
so they work regardless of which module is imported first.

A parser cuts complete frames out of a byte stream by magic plus
``package_len``; it never parses ``RequestProtocol``. The server side
buffers a TCP stream (:class:`~trpc_sdk.codec._cython._codec_parser.TrpcServerProtocolParser`),
while the client side reads exactly one frame off a pooled connection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Optional

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseClientProtocolParser
from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin
from trpc_sdk.telemetry import report_attr
from ._constants import FRAME_HEAD_LEN
from ._cython import TrpcServerProtocolParser

if TYPE_CHECKING:
    from trpc_sdk.transport import PooledConnection


class TrpcServerParserFactory(BasePluginFactory):
    """Build a per-connection tRPC server parser.

    The parser keeps buffered bytes across reads, so the dispatcher
    needs one instance per connection rather than a shared singleton.
    """

    def create(self) -> TrpcServerProtocolParser:
        """Return a new :class:`TrpcServerProtocolParser`."""
        return TrpcServerProtocolParser()


@register_plugin(PluginType.SERVER_PARSER, "trpc", init_pos=PluginInitPos.WORKER)
def setup_server_parser() -> TrpcServerParserFactory:
    """Build the plugin instance stored under ``SERVER_PARSER`` / ``"trpc"``.

    The plugin manager calls the registered object once and caches the
    result, so registering this function (rather than the class) is what
    puts a :class:`TrpcServerParserFactory` in the table.
    """
    return TrpcServerParserFactory()


def get_server_parser(name: str) -> Optional[TrpcServerParserFactory]:
    """Return the server parser factory registered as ``name``.

    Args:
        name: Plugin name, usually the YAML ``service.protocol`` value.
    """
    return get_plugin(PluginType.SERVER_PARSER, name)


@register_plugin(PluginType.CLIENT_PARSER, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcClientParser(BaseClientProtocolParser):
    """Read one complete tRPC frame off a pooled client connection."""

    async def read_frame(self, conn: PooledConnection) -> bytes:
        """Read the 16-byte head, then the rest of the frame.

        Args:
            conn: Pooled connection to read from.

        Returns:
            The complete frame, head included.

        Raises:
            ClientDecodeError: ``stx`` is not ``trpc_pb2.TRPC_MAGIC_VALUE``. The
                connection is closed first because the stream can no
                longer be resynchronized.
        """
        frame_head_buff = await conn.read(FRAME_HEAD_LEN)

        frame_head: TrpcFrameHead = parse_frame_head(frame_head_buff)

        if frame_head.stx != trpc_pb2.TRPC_MAGIC_VALUE:
            conn.close(True)
            report_attr("trpc_codec_invalid_magic_number", 1)
            raise ClientDecodeError("invalid magic number")

        left_pkg = await conn.read(frame_head.package_len - FRAME_HEAD_LEN)

        return frame_head_buff + left_pkg


def get_client_parser(name: str) -> Optional[BaseClientProtocolParser]:
    """Return the client parser registered as ``name``.

    Args:
        name: Plugin name, usually the client ``protocol`` value.
    """
    return get_plugin(PluginType.CLIENT_PARSER, name)


def get_client_codec(name: str):
    """Return the client codec registered as ``name``.

    Args:
        name: Plugin name, usually the client ``protocol`` value.
    """
    return get_plugin(PluginType.CLIENT_CODEC, name)


def get_server_codec(name: str):
    """Return the server codec registered as ``name``.

    Args:
        name: Plugin name, usually the YAML ``service.protocol`` value.
    """
    return get_plugin(PluginType.SERVER_CODEC, name)
