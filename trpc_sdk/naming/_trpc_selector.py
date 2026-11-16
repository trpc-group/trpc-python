# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Composite selector that chains discovery, routing, load balancing, and breaking.

:class:`TrpcSelector` is the default client selector constructed in
:func:`trpc_sdk.client._options._create_client_options`. On each
:meth:`TrpcSelector.select` it:

1. Resolves plugin names from :class:`~trpc_sdk.abc.SelectorOption`,
   falling back to :mod:`trpc_sdk.naming._defaults`.
2. Asks discovery for a single node via ``select_node``.
3. If that is ``None``, lists every node, runs the service router, then
   the load balancer.
4. Stashes the circuit-breaker instance on ``node.metadata['circuitbreaker']``
   so :meth:`TrpcSelector.report` can find it later.

It is not itself a ``@register_plugin`` selector; Polar and others
register under ``trpc``. :meth:`get_selector_name` still reports
:data:`~trpc_sdk.naming._constants.TRPC_SELECTOR_NAME` so URL schemes such
as ``trpc://`` can match it.

:func:`get_selector` is re-exported from
:mod:`trpc_sdk.naming._selector_lookup` for
``from trpc_sdk.naming._trpc_selector import get_selector``.
"""

from __future__ import annotations

from typing import List
from typing import NoReturn
from typing import Optional

from trpc_sdk.abc import BaseCircuitBreaker
from trpc_sdk.abc import BaseDiscovery
from trpc_sdk.abc import BaseLoadBalance
from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import BaseServiceRouter
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.plugin import get_plugin

from ._constants import TRPC_SELECTOR_NAME
from ._defaults import get_default_circuit_breaker
from ._defaults import get_default_discovery
from ._defaults import get_default_loadbalance
from ._defaults import get_default_service_router


class TrpcSelector(BaseSelector):
    """Chain discovery → service router → load balancer → circuit breaker.

    Plugin *names* on this instance are overwritten on every
    :meth:`select` from the current process defaults and ``opt``. They
    are not a sticky configuration set at construction.
    """

    def __init__(self):
        self.selector_plugin_name = TRPC_SELECTOR_NAME
        self.discovery = ""
        self.loadbalance = ""
        self.circuit_breaker = ""
        self.service_router = ""

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Resolve one node for ``service_name`` through the naming plugins.

        Args:
            service_name: Callee target. For the default client path
                this is ``opts.endpoint`` (a service name or host:port).
            opt: Per-call plugin names, namespace, and router option.

        Returns:
            A node with ``metadata['circuitbreaker']`` set so
            :meth:`report` can record the outcome.

        Raises:
            Exception: ``service_name`` is empty; a named plugin is not
                registered; or no node remains after discovery/routing.
        """
        if service_name == "":
            raise Exception("TrpcSelector.select received an empty service_name; "
                            "pass a non-empty target or callee service name")

        namespace = opt.namespace

        self.discovery = get_default_discovery()
        self.loadbalance = get_default_loadbalance()
        self.circuit_breaker = get_default_circuit_breaker()
        self.service_router = get_default_service_router()
        if opt.discovery != "":
            self.discovery = opt.discovery
        if opt.loadbalance != "":
            self.loadbalance = opt.loadbalance
        if opt.circuit_breaker != "":
            self.circuit_breaker = opt.circuit_breaker
        if opt.service_router != "":
            self.service_router = opt.service_router

        discovery_instance: Optional[BaseDiscovery] = get_plugin(PluginType.NAMING_DISCOVERY, self.discovery)
        loadbalance_instance: Optional[BaseLoadBalance] = get_plugin(PluginType.NAMING_LOADBALANCE, self.loadbalance)
        circuit_breaker_instance: Optional[BaseCircuitBreaker] = get_plugin(PluginType.NAMING_CIRCUIT_BREAKER,
                                                                            self.circuit_breaker)
        service_router_instance: Optional[BaseServiceRouter] = get_plugin(PluginType.NAMING_SERVICE_ROUTER,
                                                                          self.service_router)

        if discovery_instance is None:
            raise Exception(f"TrpcSelector.select cannot resolve service {service_name!r}: "
                            f"discovery plugin {self.discovery!r} is not registered under "
                            f"PluginType.NAMING_DISCOVERY; register the plugin before calling "
                            f"select()")
        if loadbalance_instance is None:
            raise Exception(f"TrpcSelector.select cannot resolve service {service_name!r}: "
                            f"load-balance plugin {self.loadbalance!r} is not registered under "
                            f"PluginType.NAMING_LOADBALANCE; register the plugin before calling "
                            f"select()")
        if circuit_breaker_instance is None:
            raise Exception(f"TrpcSelector.select cannot resolve service {service_name!r}: "
                            f"circuit-breaker plugin {self.circuit_breaker!r} is not registered "
                            f"under PluginType.NAMING_CIRCUIT_BREAKER; register the plugin "
                            f"before calling select()")
        if service_router_instance is None:
            raise Exception(f"TrpcSelector.select cannot resolve service {service_name!r}: "
                            f"service-router plugin {self.service_router!r} is not registered "
                            f"under PluginType.NAMING_SERVICE_ROUTER; register the plugin "
                            f"before calling select()")

        node = await discovery_instance.select_node(service_name, opt)
        discovered_count: Optional[int] = None
        filtered_count: Optional[int] = None
        # discovery.select_node is optional; the default IpDiscovery always
        # returns None, so the list → router → load-balancer path runs.
        if node is None:
            node_list: List[Node] = await discovery_instance.list(service_name, namespace)
            discovered_count = len(node_list)
            node_list = await service_router_instance.filter(service_name, node_list, opt.service_router_opt)
            filtered_count = len(node_list)
            node = await loadbalance_instance.select(service_name, node_list, namespace)

        if not node:
            if discovered_count is None:
                reason = "discovery.select_node returned no node"
            elif discovered_count == 0:
                reason = "discovery returned an empty node list"
            elif filtered_count == 0:
                reason = "service router filtered out every discovered node"
            else:
                reason = (f"load balancer returned no node "
                          f"(discovery returned {discovered_count} node(s), "
                          f"service router kept {filtered_count})")
            raise Exception(f"TrpcSelector.select found no usable node for service "
                            f"{service_name!r} in namespace {namespace!r}: {reason}")

        if node.metadata is None:
            node.metadata = {}

        node.metadata["circuit_breaker"] = circuit_breaker_instance
        return node

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """Forward the call outcome to the breaker stashed by :meth:`select`.

        Args:
            node: The node :meth:`select` returned. Must carry
                ``metadata['circuit_breaker']``.
            cost: Call duration passed to the breaker.
            success: Call result passed to the breaker.

        Raises:
            Exception: ``node`` is ``None``, or ``node.metadata`` is
                missing (the node did not come from :meth:`select`).
        """
        if node is None:
            raise Exception("TrpcSelector.report requires the Node returned by select(); "
                            "got None")
        if node.metadata is None:
            raise Exception("TrpcSelector.report requires node.metadata populated by "
                            "select() with a 'circuit_breaker' entry; got None")
        breaker: BaseCircuitBreaker = node.metadata["circuit_breaker"]
        await breaker.report(node, cost, success)

    def get_selector_name(self) -> str:
        """Return ``trpc``, the scheme :class:`TrpcSelector` compares against."""
        return self.selector_plugin_name
