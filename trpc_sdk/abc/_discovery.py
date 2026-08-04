# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Service discovery extension interfaces.

This module defines the abstract contract used by pluggable discovery
implementations that list service endpoints and select one node for a call.
"""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import List
from typing import Optional

from ._node import Node


class BaseDiscovery(ABC):
    """Define service endpoint listing and node selection."""

    @abstractmethod
    async def list(self, service_name: str, namespace: str) -> List[Node]:
        """List the available endpoints for one service.

        Args:
            service_name: Logical service name to discover.
            namespace: Deployment namespace that scopes the lookup.

        Returns:
            Service nodes currently available for the request.
        """
        ...

    @abstractmethod
    async def select_node(self, service_name: str, opt: Any) -> Optional[Node]:
        """Select one endpoint for the given service and options.

        Args:
            service_name: Logical service name to select from.
            opt: Selection options used by the concrete discovery backend.

        Returns:
            Selected service node, or ``None`` when no endpoint is available.
        """
        ...
