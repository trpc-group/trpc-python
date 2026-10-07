# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in no-op circuit breaker.

:class:`NoopCircuitBreaker` is registered as
:data:`~trpc_sdk.naming._constants.DEFAULT_CIRCUITBREAKER_NAME`. It reports
every node as available and ignores :meth:`report`. Real breakers
(Polar, and so on) replace it through
:func:`~trpc_sdk.naming.set_default_circuitbreaker` or
:meth:`~trpc_sdk.abc.SelectorOption.set_circuit_breaker`.
"""

from __future__ import annotations

from typing import NoReturn

from trpc_sdk.abc import BaseCircuitBreaker
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin
from ._constants import DEFAULT_CIRCUITBREAKER_NAME


@register_plugin(PluginType.NAMING_CIRCUIT_BREAKER, DEFAULT_CIRCUITBREAKER_NAME)
class NoopCircuitBreaker(BaseCircuitBreaker):
    """Circuit breaker that never trips.

    :meth:`available` is always True. :meth:`report` is a no-op, so
    consecutive failures do not isolate a node. Use this when the
    client has a single static address or when a real breaker plugin
    has not been loaded.
    """

    async def available(self, node: Node) -> bool:
        """Return True for every node.

        Args:
            node: Candidate node; ignored.

        Returns:
            Always ``True``.
        """
        return True

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """Ignore the call outcome.

        Args:
            node: Node that was invoked; ignored.
            cost: Call duration; ignored.
            success: Call result; ignored.
        """
        return None
