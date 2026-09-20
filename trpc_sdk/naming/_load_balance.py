# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in random load balancer.

:class:`RandomLoadBalance` is registered as
:data:`~trpc_sdk.naming._constants.DEFAULT_LOADBALANCE_NAME`. It picks one
node uniformly from the list :class:`~trpc_sdk.naming._trpc_selector.TrpcSelector`
hands it after discovery and service routing.
"""

from __future__ import annotations

from random import randint
from typing import List

from trpc_sdk.abc import BaseLoadBalance
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_LOADBALANCE_NAME
from ._defaults import set_default_loadbalance


@register_plugin(PluginType.NAMING_LOADBALANCE, DEFAULT_LOADBALANCE_NAME)
class RandomLoadBalance(BaseLoadBalance):
    """Pick a node at random from a non-empty list.

    An empty ``node_list`` is an error: either discovery returned
    nothing or the service router filtered every node out. There is no
    retry inside this plugin.
    """

    async def select(
            self,
            service_name: str,
            node_list: List[Node],
            namespace: str) -> Node:
        """Return one uniformly chosen node.

        Args:
            service_name: Callee name; used only in the empty-list error.
            node_list: Candidates remaining after routing.
            namespace: Discovery namespace; used only in the error.

        Returns:
            One element of ``node_list``.

        Raises:
            Exception: ``node_list`` is empty.
        """
        if len(node_list) == 0:
            raise Exception(
                f"RandomLoadBalance.select has no nodes for service "
                f"{service_name!r} in namespace {namespace!r}"
            )
        n = randint(0, len(node_list) - 1)
        return node_list[n]
