# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service node data model.

This module defines the ``Node`` dataclass used by naming, load balancing, and
circuit breaker components to describe one selectable service endpoint.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Node:
    """Describe one selectable service endpoint.

    Attributes:
        service_name: Logical service name that owns this endpoint.
        container_name: Optional container or instance identifier.
        address: Host and port in ``ip:port`` form. When empty, it is derived
            from ``ip`` and ``port`` during initialization.
        ip: Endpoint IP address.
        port: Endpoint port number.
        network: Network type such as ``tcp`` or ``udp``.
        protocol: Application protocol such as ``trpc`` or ``http``.
        weight: Relative selection weight used by load balancers.
        cost_time: Latest observed call cost in milliseconds.
        env_key: Environment routing key associated with the endpoint.
        metadata: Arbitrary key and value pairs attached to the endpoint.
    """

    service_name: str
    container_name: str = ""
    address: str = ""
    ip: str = ""
    port: int = 0
    network: str = ""
    protocol: str = ""
    weight: int = 0
    cost_time: int = 0
    env_key: str = ""
    metadata: dict = None

    def __post_init__(self):
        """Derive ``address`` from ``ip`` and ``port`` when it is unset."""
        if not self.address:
            self.address = str(self.ip) + ":" + str(self.port)
