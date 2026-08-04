# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Circuit breaker extension interfaces.

This module defines the abstract contract used by pluggable circuit breaker
implementations that decide whether a service node may receive traffic and
that receive call-result feedback after each attempt.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import NoReturn

from ._node import Node


class BaseCircuitBreaker(ABC):
    """Define node availability checks and call-result reporting.

    Concrete implementations track node health, trip open when failures exceed
    a threshold, and decide whether subsequent requests should be allowed.
    """

    @abstractmethod
    def available(self, node: Node) -> bool:
        """Return whether the given node may receive a new request.

        Args:
            node: Candidate service node to evaluate.

        Returns:
            ``True`` if the node is available for traffic, otherwise ``False``.
        """
        ...

    @abstractmethod
    def report(self, node: Node, cost: int, code: int) -> NoReturn:
        """Report the outcome of one call against a service node.

        Args:
            node: Service node that handled the call.
            cost: Call duration in milliseconds.
            code: Result code produced by the call.

        Returns:
            This method does not return a value.
        """
        ...
