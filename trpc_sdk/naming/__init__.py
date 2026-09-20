# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python naming package: discovery, routing, load balancing, selectors.

Public symbols are re-exported here. Callers should ``from trpc_sdk.naming
import ...``. Importing this package also registers the built-in naming
plugins, so ``@register_plugin`` runs before the first
:func:`trpc_sdk.plugin.get_plugin` lookup.

The package is split by responsibility:

- :mod:`~trpc_sdk.naming._constants` — immutable plugin-name literals
- :mod:`~trpc_sdk.naming._defaults` — process-wide default plugin names
- :mod:`~trpc_sdk.naming._selector_lookup` — :func:`get_selector`
- :mod:`~trpc_sdk.naming._discovery` — :class:`IpDiscovery` and
  :func:`resolve_host`
- :mod:`~trpc_sdk.naming._load_balance` — :class:`RandomLoadBalance`
- :mod:`~trpc_sdk.naming._registry` — :func:`get_registry` and
  heartbeat gating
- :mod:`~trpc_sdk.naming._circuit_breaker` — :class:`NoopCircuitBreaker`
- :mod:`~trpc_sdk.naming._service_router` — :class:`NoopServiceRouter`
- :mod:`~trpc_sdk.naming._ip_selector` — :class:`IpSelector` and
  :class:`DNSSelector`
- :mod:`~trpc_sdk.naming._dsn_selector` — :class:`DSNSelector` and
  :class:`ResolveSelector`
- :mod:`~trpc_sdk.naming._trpc_selector` — :class:`TrpcSelector`
"""

from __future__ import annotations

from ._circuit_breaker import NoopCircuitBreaker
from ._constants import DEFAULT_CIRCUITBREAKER_NAME
from ._constants import DEFAULT_DISCOVERY_NAME
from ._constants import DEFAULT_LOADBALANCE_NAME
from ._constants import DEFAULT_SELECTOR_NAME
from ._constants import DEFAULT_SERVICEROUTER_NAME
from ._constants import DNS_SELECTOR_NAME
from ._constants import IP_SELECTOR_NAMES
from ._constants import TRPC_SELECTOR_NAME
from ._defaults import get_default_circuitbreaker
from ._defaults import get_default_discovery
from ._defaults import get_default_loadbalance
from ._defaults import get_default_servicerouter
from ._defaults import set_default_circuitbreaker
from ._defaults import set_default_discovery
from ._defaults import set_default_loadbalance
from ._defaults import set_default_servicerouter
from ._discovery import IpDiscovery
from ._discovery import resolve_host
from ._dsn_selector import DSNSelector
from ._dsn_selector import ResolveSelector
from ._ip_selector import DNSSelector
from ._ip_selector import IpSelector
from ._ip_selector import get_random_addr
from ._load_balance import RandomLoadBalance
from ._registry import defaultRegistry
from ._registry import get_registry
from ._registry import pause_heartbeat
from ._registry import resume_heartbeat
from ._registry import should_heartbeat
from ._selector_lookup import get_default_selector
from ._selector_lookup import get_default_selector as default_selector
from ._selector_lookup import get_selector
from ._service_router import NoopServiceRouter
from ._trpc_selector import TrpcSelector


__all__ = [
    # Constants
    "DEFAULT_DISCOVERY_NAME",
    "DEFAULT_LOADBALANCE_NAME",
    "DEFAULT_CIRCUITBREAKER_NAME",
    "DEFAULT_SERVICEROUTER_NAME",
    "DEFAULT_SELECTOR_NAME",
    "TRPC_SELECTOR_NAME",
    "DNS_SELECTOR_NAME",
    "IP_SELECTOR_NAMES",
    # Process-wide defaults
    "get_default_discovery",
    "set_default_discovery",
    "get_default_loadbalance",
    "set_default_loadbalance",
    "get_default_circuitbreaker",
    "set_default_circuitbreaker",
    "get_default_servicerouter",
    "set_default_servicerouter",
    # Discovery
    "IpDiscovery",
    "resolve_host",
    # Load balance
    "RandomLoadBalance",
    # Registry
    "defaultRegistry",
    "get_registry",
    "pause_heartbeat",
    "resume_heartbeat",
    "should_heartbeat",
    # Circuit breaker
    "NoopCircuitBreaker",
    # Service router
    "NoopServiceRouter",
    # Selectors
    "TrpcSelector",
    "IpSelector",
    "DNSSelector",
    "DSNSelector",
    "ResolveSelector",
    "get_selector",
    "get_default_selector",
    "default_selector",
    "get_random_addr",
]
