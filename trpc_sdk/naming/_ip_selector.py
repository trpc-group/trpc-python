# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""IP and DNS selectors that parse the target as host:port.

:class:`IpSelector` is registered as ``ip``, ``http``, and ``https`` so
client targets such as ``ip://127.0.0.1:8000`` and ``http://host:80``
share this implementation. It does not resolve DNS.

:class:`DNSSelector` is registered as
:data:`~trpc_sdk.naming._constants.DNS_SELECTOR_NAME` and resolves the host
through :func:`~trpc_sdk.naming._discovery.resolve_host`.

Both accept a comma-separated address list and pick one entry with
:func:`get_random_addr`.
"""

from __future__ import annotations

from random import randint
from typing import NoReturn

from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.plugin import register_plugin

from ._constants import DNS_SELECTOR_NAME
from ._discovery import resolve_host
from ._selector_lookup import get_selector


@register_plugin(PluginType.NAMING_SELECTOR, DNS_SELECTOR_NAME)
class DNSSelector(BaseSelector):
    """Selector that treats the target as a DNS-resolvable host:port.

    Comma-separated targets pick one address at random, then resolve
    that host. Empty ``service_name`` is rejected before any lookup.
    """

    def __init__(self):
        pass

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Resolve one address from ``service_name`` through DNS.

        Args:
            service_name: ``host:port``, or several joined by commas.
            opt: Selector option; unused by this plugin.

        Returns:
            A node whose ``ip`` is the resolved A record.

        Raises:
            Exception: ``service_name`` is empty.
        """
        if service_name == "":
            raise Exception(
                "DNSSelector.select received an empty service_name; "
                "pass a host:port target"
            )

        addr = get_random_addr(service_name)
        ip, port = resolve_host(addr)
        return Node(service_name=service_name, ip=ip, port=port)

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """No-op; this selector has no circuit breaker of its own.

        Args:
            node: Node that was invoked; ignored.
            cost: Call duration; ignored.
            success: Call result; ignored.
        """
        pass


@register_plugin(PluginType.NAMING_SELECTOR, "ip")
@register_plugin(PluginType.NAMING_SELECTOR, "http")
@register_plugin(PluginType.NAMING_SELECTOR, "https")
class IpSelector(BaseSelector):
    """Selector that treats the target as a literal host:port.

    DNS is not consulted (``resolve_host(..., False)``), so a hostname
    is stored as-is. Registered under the ``ip``, ``http``, and
    ``https`` target schemes.
    """

    def __init__(self):
        pass

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Parse one address from ``service_name`` without DNS.

        Args:
            service_name: ``host:port``, or several joined by commas.
            opt: Selector option; unused by this plugin.

        Returns:
            A node whose ``ip``/``port`` come from the chosen address.

        Raises:
            Exception: ``service_name`` is empty.
        """
        if service_name == "":
            raise Exception(
                "IpSelector.select received an empty service_name; "
                "pass a host:port target"
            )

        addr = get_random_addr(service_name)
        ip, port = resolve_host(addr, False)
        return Node(service_name=service_name, ip=ip, port=port)

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """No-op; this selector has no circuit breaker of its own.

        Args:
            node: Node that was invoked; ignored.
            cost: Call duration; ignored.
            success: Call result; ignored.
        """
        pass


def get_random_addr(service_name: str) -> str:
    """Pick one address from a comma-separated target list.

    A single-address target is returned as-is. Several addresses are
    chosen uniformly at random so :class:`DNSSelector` and
    :class:`IpSelector` share the same multi-address behavior.

    Args:
        service_name: One address, or several joined by commas.

    Returns:
        The chosen address string, still unresolved.
    """
    addr_list = service_name.split(",")
    if len(addr_list) == 1:
        addr = addr_list[0]
    else:
        addr = addr_list[randint(0, len(addr_list) - 1)]
    return addr
