# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin factories, protocol checkers, and codec lookups for tRPC.

Importing this module registers the ``"trpc"`` entries for
``PluginType.SERVER_CHECKER`` and ``PluginType.CLIENT_CHECKER``. The
codec plugins themselves are registered by :mod:`trpc_sdk.codec._codec`;
``get_client_codec`` / ``get_server_codec`` only read the plugin table,
so they work regardless of which module is imported first.

A checker cuts complete frames out of a byte stream by magic plus
``package_len``; it never parses ``RequestProtocol``. The server side
buffers a TCP stream (:class:`~trpc_sdk.codec._cython._codec_parser.TrpcServerProtocolChecker`),
while the client side reads exactly one frame off a pooled connection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Optional

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseClientProtocolChecker
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
from ._cython import TrpcServerProtocolChecker

if TYPE_CHECKING:
    from trpc_sdk.transport import PooledConnection


class TrpcServerCheckerFactory(BasePluginFactory):
    """Build a per-connection tRPC server checker.

    The checker keeps buffered bytes across reads, so the dispatcher
    needs one instance per connection rather than a shared singleton.
    """

    def create(self) -> TrpcServerProtocolChecker:
        """Return a new :class:`TrpcServerProtocolChecker`."""
        return TrpcServerProtocolChecker()


@register_plugin(PluginType.SERVER_CHECKER, "trpc", init_pos=PluginInitPos.WORKER)
def setup_server_checker() -> TrpcServerCheckerFactory:
    """Build the plugin instance stored under ``SERVER_CHECKER`` / ``"trpc"``.

    The plugin manager calls the registered object once and caches the
    result, so registering this function (rather than the class) is what
    puts a :class:`TrpcServerCheckerFactory` in the table.
    """
    return TrpcServerCheckerFactory()


def get_server_checker(name: str) -> Optional[TrpcServerCheckerFactory]:
    """Return the server checker factory registered as ``name``.

    Args:
        name: Plugin name, usually the YAML ``service.protocol`` value.
    """
    return get_plugin(PluginType.SERVER_CHECKER, name)


@register_plugin(PluginType.CLIENT_CHECKER, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcClientParser(BaseClientProtocolChecker):
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


def get_client_checker(name: str) -> Optional[BaseClientProtocolChecker]:
    """Return the client checker registered as ``name``.

    Args:
        name: Plugin name, usually the client ``protocol`` value.
    """
    return get_plugin(PluginType.CLIENT_CHECKER, name)


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
