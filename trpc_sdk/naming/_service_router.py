# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in no-op service router.

:class:`NoopServiceRouter` is registered as
:data:`~trpc_sdk.naming._constants.DEFAULT_SERVICEROUTER_NAME`. It returns
the discovered node list unchanged. Polar plugins replace it through
:func:`set_default_servicerouter` or
:meth:`~trpc_sdk.abc.SelectorOption.set_service_router`.

:class:`~trpc_sdk.abc.BaseServiceRouter` is the abstract contract this
plugin implements.
"""

from __future__ import annotations

from typing import List

from trpc_sdk.abc import BaseServiceRouter
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import ServerRouterOption
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_SERVICEROUTER_NAME
from ._defaults import set_default_servicerouter


@register_plugin(PluginType.NAMING_SERVICE_ROUTER, DEFAULT_SERVICEROUTER_NAME)
class NoopServiceRouter(BaseServiceRouter):
    """Service router that keeps every discovered node.

    Polar-style env/set filtering is skipped. The load balancer sees
    the same list discovery produced.
    """

    async def filter(
            self,
            service_name: str,
            nodes: List[Node],
            opt: ServerRouterOption) -> List[Node]:
        """Return ``nodes`` without filtering.

        Args:
            service_name: Callee name; ignored.
            nodes: Nodes from discovery.
            opt: Routing option; ignored.

        Returns:
            The same list object that was passed in.
        """
        return nodes
