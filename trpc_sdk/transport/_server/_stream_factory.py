# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin factory and public aliases for the tRPC server transport.

Importing this module registers ``TrpcServerTransportFactory`` under
``PluginType.SERVER_TRANSPORT`` / ``"trpc"``. The server package
``__init__`` imports it eagerly, and :mod:`trpc_sdk.transport` imports
that package, so the slot is filled as soon as the transport package
loads. The plugin manager overwrites silently, so anything else that
registers the same name later wins — notably :mod:`trpc_sdk.old_transport`,
which tests may import.

``get_server_transport`` is the renamed ``get_svr_transport``.
"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin

from ._stream_transport import TrpcServerStreamTransport

DefaultServerTransportOption = BaseServerTransportOption
DefaultServerTransport = TrpcServerStreamTransport


@register_plugin(PluginType.SERVER_TRANSPORT, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcServerTransportFactory(BasePluginFactory):
    """Build the default tRPC streaming server transport."""

    def create(self) -> TrpcServerStreamTransport:
        """Return a new :class:`TrpcServerStreamTransport`."""
        return TrpcServerStreamTransport()


def get_server_transport(protocol: str) -> Optional[BaseServerTransport]:
    """Return a server transport for ``protocol``, or None if unregistered.

    Args:
        protocol: Plugin name, usually the YAML ``service.protocol`` value
            such as ``"trpc"`` or ``"http"``.
    """
    trans_factory = get_plugin(PluginType.SERVER_TRANSPORT, protocol)
    if trans_factory:
        return trans_factory.create()
    return None
