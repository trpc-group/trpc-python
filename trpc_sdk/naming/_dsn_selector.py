# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Data-source-name selectors.

:class:`DSNSelector` is registered as
:data:`~trpc_sdk.naming._constants.DEFAULT_SELECTOR_NAME` (``dsn``). It
copies the whole target into :attr:`~trpc_sdk.abc.Node.address`
without resolving a host, which is what SQL / Redis-style DSNs need.

:class:`ResolveSelector` is not auto-registered. Callers construct it
with another selector name plus a :class:`~trpc_sdk.abc.ServiceNameExtractor`
so a DSN such as ``mongodb+polaris://user:passwd@{polaris_name}`` can
have only the inner name resolved, then spliced back into the original
string.
"""

from __future__ import annotations

from typing import NoReturn

from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.abc import ServiceNameExtractor
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_SELECTOR_NAME
from ._selector_lookup import get_default_selector as default_selector
from ._selector_lookup import get_selector


@register_plugin(PluginType.NAMING_SELECTOR, DEFAULT_SELECTOR_NAME)
class DSNSelector(BaseSelector):
    """Treat the client target as a data-source name.

    The whole ``service_name`` is returned as ``Node.address``. Typical
    form: ``dsn://user:passwd@tcp(ip:port)/db``.
    """

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Return a node whose address is the raw DSN.

        Args:
            service_name: Full data-source name.
            opt: Selector option; unused by this plugin.

        Returns:
            A node with ``address=service_name``.

        Raises:
            ValueError: ``service_name`` is empty.
        """
        if service_name == "":
            raise ValueError(
                "DSNSelector.select received an empty service_name; "
                "pass a data-source name such as "
                "'dsn://user:passwd@tcp(ip:port)/db'"
            )

        return Node(service_name=service_name, address=service_name)

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """No-op; a DSN is not a load-balanced node.

        Args:
            node: Node that was invoked; ignored.
            cost: Call duration; ignored.
            success: Call result; ignored.
        """
        pass


class ResolveSelector(BaseSelector):
    """Resolve an inner service name inside a DSN, then splice it back.

    Typical form: ``mongodb+polaris://user:passwd@{polaris_name}``.
    ``extractor`` locates the ``{polaris_name}`` span;
    ``resolver_selector_name`` names the selector that turns that span
    into an address.

    The inner lookup uses :meth:`DEFAULT_PLUGIN_MANAGER.get_instance`,
    not :func:`get_plugin`, so the resolver must already be
    instantiated (``plugin.setup()`` or an explicit
    :meth:`DEFAULT_PLUGIN_MANAGER.set_instance`).
    """

    def __init__(self, resolver_selector_name: str, extractor: ServiceNameExtractor):
        self.resolver_selector_name = resolver_selector_name
        self.extractor = extractor

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Replace the extracted span with the resolved address.

        Args:
            service_name: Full DSN containing an extractable name.
            opt: Forwarded to the inner selector.

        Returns:
            A node whose ``address`` is ``service_name`` with the
            extracted span replaced by the inner node's address.

        Raises:
            ValueError: ``service_name`` is empty, ``extractor`` is
                ``None``, or the inner selector is not instantiated.
        """
        if service_name == "":
            raise ValueError(
                "ResolveSelector.select received an empty service_name; "
                "pass a DSN that contains an extractable service name"
            )
        if self.extractor is None:
            raise ValueError(
                "ResolveSelector.select cannot extract a service name "
                "because extractor is None"
            )
        pos, length = self.extractor.extract(service_name)
        extracted_service_name = service_name[pos:pos + length]
        resolved_node = await self.resolve_address(extracted_service_name, opt)
        address = service_name[:pos] + resolved_node.address + service_name[pos + length:]
        return Node(service_name=service_name, address=address)

    def resolve_address(self, service_name: str, opt: SelectorOption) -> str:
        """Return the inner selector's ``select`` coroutine for ``service_name``.

        This method is synchronous and returns an awaitable: callers
        ``await`` it. The inner plugin is loaded with
        :meth:`DEFAULT_PLUGIN_MANAGER.get_instance`, which does not
        instantiate on demand.

        Args:
            service_name: Extracted inner name, not the full DSN.
            opt: Forwarded to the inner selector.

        Returns:
            The coroutine produced by the inner :meth:`BaseSelector.select`.

        Raises:
            ValueError: No instance is stored for
                ``resolver_selector_name``.
        """
        resolver = DEFAULT_PLUGIN_MANAGER.get_instance(
            PluginType.NAMING_SELECTOR, self.resolver_selector_name)
        if resolver is None:
            raise ValueError(
                f"ResolveSelector.resolve_address cannot load naming selector "
                f"{self.resolver_selector_name!r} under PluginType.NAMING_SELECTOR; "
                f"the plugin is not instantiated. Register it and call "
                f"plugin.setup() first"
            )
        return resolver.select(service_name, opt)

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """No-op; reporting is left to the inner selector if callers need it.

        Args:
            node: Node that was invoked; ignored.
            cost: Call duration; ignored.
            success: Call result; ignored.
        """
        pass
