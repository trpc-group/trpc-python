# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in IP / DNS discovery.

:class:`IpDiscovery` treats ``service_name`` as a ``host:port`` (or
``[ipv6]:port``) address, optionally resolving the host through DNS.
It is registered as
:data:`~trpc_sdk.naming._constants.DEFAULT_DISCOVERY_NAME` and is what
:class:`~trpc_sdk.naming._trpc_selector.TrpcSelector` uses when the option
does not name another discovery plugin.

:func:`resolve_host` is also used by :mod:`trpc_sdk.naming._ip_selector` so
IP and DNS selectors parse addresses the same way. DNS failures in
:func:`_safe_gethostbyname` are swallowed and surface as ``ip is None``;
callers historically do not distinguish "parse failed" from "empty
address".
"""

from __future__ import annotations

import socket
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseDiscovery
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_DISCOVERY_NAME
from ._defaults import set_default_discovery


@register_plugin(PluginType.NAMING_DISCOVERY, DEFAULT_DISCOVERY_NAME)
class IpDiscovery(BaseDiscovery):
    """Discovery that parses ``service_name`` as a single host:port.

    The returned list always has one :class:`~trpc_sdk.abc.Node`.
    DNS resolution is on, so a hostname is replaced with an A-record
    IP when :func:`socket.gethostbyname` succeeds.
    """

    async def list(self, service_name: str, namespace: str) -> List[Node]:
        """Return one node whose address is ``service_name`` resolved.

        ``namespace`` is accepted for the :class:`BaseDiscovery` contract
        and is not consulted; this plugin has no registry to query.

        Args:
            service_name: ``host:port``, ``[ipv6]:port``, or a hostname
                with an optional ``?query`` suffix that is stripped.
            namespace: Ignored.

        Returns:
            A one-element list. ``node.ip`` is ``None`` when DNS lookup
            failed.
        """
        ip, port = resolve_host(service_name)
        node = Node(service_name=service_name, ip=ip, port=port)
        return [node]

    async def select_node(self, service_name: str, opt: Any) -> Optional[Node]:
        """Decline direct selection so the selector falls back to :meth:`list`.

        ``service_name`` already is the single address, so there is nothing
        to pick here. Returning ``None`` makes
        :class:`~trpc_sdk.naming._trpc_selector.TrpcSelector` run the
        list, service-router and load-balancer path instead.

        Args:
            service_name: Ignored.
            opt: Ignored.

        Returns:
            Always ``None``.
        """
        return None


def _safe_gethostbyname(host: str) -> Optional[str]:
    """Resolve ``host`` to an IPv4 address, or ``None`` on DNS failure.

    ``socket.gaierror`` is swallowed. Callers cannot tell a failed
    lookup from a missing host; that is historical behavior.

    Args:
        host: Hostname or dotted IP.

    Returns:
        An IPv4 string, or ``None`` if :func:`socket.gethostbyname`
        raised :class:`socket.gaierror`.
    """
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return None


def resolve_host(host: str, resolve_dns: bool = True) -> Tuple[Optional[str], int]:
    """Split ``host`` into ``(ip, port)``, optionally resolving DNS.

    Bracketed IPv6 (``[::1]:8080``) is parsed without DNS. Anything
    else is split on the first ``:``; a ``?query`` suffix is stripped
    first. When ``resolve_dns`` is True the host part is passed through
    :func:`_safe_gethostbyname`.

    Args:
        host: Address string. IPv6 must be ``[ip]:port``.
        resolve_dns: If True, replace the host with its A record.

    Returns:
        ``(ip, port)``. ``port`` is 0 when the string has no ``:port``.
        ``ip`` is ``None`` when DNS was requested and failed.

    Raises:
        Exception: IPv6 form has brackets but no ``:port`` after ``]``.
    """
    if host.find('[') != -1 and host.find(']') != -1:
        ip = host[host.index('[') + 1:host.index(']')]
        port = host[host.index(']'):]
        port_arr = port.split(':')
        if len(port_arr) != 2:
            raise Exception(
                f"IPv6 address {host!r} is missing ':port' after the closing "
                f"bracket (expected '[ip]:port')"
            )
        return ip, int(port_arr[1])
    else:
        if host.find("?") != -1:
            host = host.split("?")[0]
        addr_arr = host.split(':')
        port = int(addr_arr[1]) if len(addr_arr) > 1 else 0
        ip = addr_arr[0]
        if resolve_dns:
            ip = _safe_gethostbyname(addr_arr[0])
        return ip, port
