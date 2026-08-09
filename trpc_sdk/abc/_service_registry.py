# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service registry extension interfaces.

This module defines the abstract contract used by pluggable registry backends
that publish and withdraw service endpoint addresses.
"""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Tuple


class BaseServiceRegistry(ABC):
    """Define service endpoint registration and deregistration."""

    @abstractmethod
    def register(self, service: str, address: Tuple[str, int]) -> bool:
        """Publish one service endpoint to the registry.

        Args:
            service: Logical service name to register.
            address: Endpoint as ``(host, port)``.

        Returns:
            ``True`` if registration succeeded, otherwise ``False``.
        """
        ...

    @abstractmethod
    def deregister(self, service: str, address: Tuple[str, int]) -> bool:
        """Withdraw one service endpoint from the registry.

        Args:
            service: Logical service name to deregister.
            address: Endpoint as ``(host, port)``.

        Returns:
            ``True`` if deregistration succeeded, otherwise ``False``.
        """
        ...
