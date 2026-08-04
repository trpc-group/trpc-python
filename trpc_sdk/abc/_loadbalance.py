# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Load balancing service selection."""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import List

from ._node import Node


class BaseLoadBalance(ABC):
    """Define load balancing service selection."""

    @abstractmethod
    async def select(self, service_name: str, node_list: List[Node], namespace: str) -> Node:
        """
        Select one endpoint for the given service and options.

        Args:
            service_name: Logical service name to select from.
            node_list: List of nodes to select from.
            namespace: Deployment namespace that scopes the lookup.

        Returns:
            Selected service node.
        """
        ...
