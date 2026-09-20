# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-wide default plugin names used by :class:`TrpcSelector`.

Each value starts as the matching literal in
:mod:`trpc_sdk.naming._constants` and can be replaced at runtime through
the ``set_default_*`` helpers. :class:`~trpc_sdk.naming._trpc_selector.TrpcSelector`
reads the current names on every :meth:`~trpc_sdk.naming._trpc_selector.TrpcSelector.select`
unless the :class:`~trpc_sdk.abc.SelectorOption` already named a
plugin.

The original modules re-export the setters so existing imports such as
``from trpc_sdk.naming._discovery import set_default_discovery`` keep
working.
"""

from __future__ import annotations

from ._constants import DEFAULT_CIRCUITBREAKER_NAME
from ._constants import DEFAULT_DISCOVERY_NAME
from ._constants import DEFAULT_LOADBALANCE_NAME
from ._constants import DEFAULT_SERVICEROUTER_NAME

_default_discovery: str = DEFAULT_DISCOVERY_NAME
_default_loadbalance: str = DEFAULT_LOADBALANCE_NAME
_default_circuitbreaker: str = DEFAULT_CIRCUITBREAKER_NAME
_default_servicerouter: str = DEFAULT_SERVICEROUTER_NAME


def get_default_discovery() -> str:
    """Return the discovery plugin name :class:`TrpcSelector` uses by default."""
    return _default_discovery


def set_default_discovery(discovery: str) -> None:
    """Replace the process-wide default discovery plugin name.

    Args:
        discovery: Registered name under ``PluginType.NAMING_DISCOVERY``.
    """
    global _default_discovery
    _default_discovery = discovery


def get_default_loadbalance() -> str:
    """Return the load-balancer plugin name :class:`TrpcSelector` uses by default."""
    return _default_loadbalance


def set_default_loadbalance(lb: str) -> None:
    """Replace the process-wide default load-balancer plugin name.

    Args:
        lb: Registered name under ``PluginType.NAMING_LOADBALANCE``.
    """
    global _default_loadbalance
    _default_loadbalance = lb


def get_default_circuitbreaker() -> str:
    """Return the circuit-breaker plugin name :class:`TrpcSelector` uses by default."""
    return _default_circuitbreaker


def set_default_circuitbreaker(cb: str) -> None:
    """Replace the process-wide default circuit-breaker plugin name.

    Args:
        cb: Registered name under ``PluginType.NAMING_CIRCUIT_BREAKER``.
    """
    global _default_circuitbreaker
    _default_circuitbreaker = cb


def get_default_servicerouter() -> str:
    """Return the service-router plugin name :class:`TrpcSelector` uses by default."""
    return _default_servicerouter


def set_default_servicerouter(sr: str) -> None:
    """Replace the process-wide default service-router plugin name.

    Args:
        sr: Registered name under ``PluginType.NAMING_SERVICE_ROUTER``.
    """
    global _default_servicerouter
    _default_servicerouter = sr
