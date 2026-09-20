# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Immutable plugin-name literals for the naming package.

These strings are the names passed to ``@register_plugin``. Mutable
"current default" names live in :mod:`trpc_sdk.naming._defaults` and start
from the values here.

:data:`IP_SELECTOR_NAMES` documents the three aliases
:class:`~trpc_sdk.naming._ip_selector.IpSelector` registers as; the
decorators themselves still list each name so registration order stays
unchanged.
"""

from __future__ import annotations

# Built-in discovery plugin registered by :class:`IpDiscovery`.
DEFAULT_DISCOVERY_NAME = "IPDiscovery"

# Built-in load-balancer plugin registered by :class:`RandomLoadBalance`.
DEFAULT_LOADBALANCE_NAME = "RandomLB"

# Built-in circuit-breaker plugin registered by :class:`NoopCircuitBreaker`.
DEFAULT_CIRCUITBREAKER_NAME = "NoopCB"

# Built-in service-router plugin registered by :class:`NoopServiceRouter`.
DEFAULT_SERVICEROUTER_NAME = "NoopSR"

# Built-in DSN selector plugin; also the framework-wide default selector.
DEFAULT_SELECTOR_NAME = "dsn"

# Name :class:`~trpc_sdk.naming._trpc_selector.TrpcSelector` reports via
# :meth:`~trpc_sdk.naming._trpc_selector.TrpcSelector.get_selector_name`.
# Compared with a URL scheme (for example ``trpc://``) by the HTTP client.
TRPC_SELECTOR_NAME = "trpc"

# Built-in DNS selector plugin registered by :class:`DNSSelector`.
DNS_SELECTOR_NAME = "dns"

# Aliases :class:`IpSelector` registers under. Kept as a tuple for
# documentation; ``@register_plugin`` still names each alias explicitly.
IP_SELECTOR_NAMES = ("ip", "http", "https")
