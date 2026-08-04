# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Service selector extension interfaces and routing options.

This module defines routing option models and the abstract selector contract
used to choose a service node and report call outcomes.
"""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import Optional

from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]
from ._node import Node


@dataclass
class ServerRouterOption:
    """Describe options consumed by a service router.

    Attributes:
        ctx: Request-scoped RPC context.
        env_key: Environment routing key.
        namespace: Target namespace used for routing.
        src_namespace: Caller namespace.
        src_servicename: Caller service name.
        src_env_name: Caller environment name.
        src_setname: Caller set name.
        dst_env_name: Callee environment name.
        dst_setname: Callee set name.
        env_transfer: Environment-transfer routing value.
        metadata: Additional routing metadata.
        disable_service_router: Whether service routing should be skipped.
    """

    ctx: Optional[TrpcContext] = None
    env_key: str = ""
    namespace: str = ""
    src_namespace: str = ""
    src_servicename: str = ""
    src_env_name: str = ""
    src_setname: str = ""
    dst_env_name: str = ""
    dst_setname: str = ""
    env_transfer: str = ""
    metadata: Dict = field(default_factory=dict)
    disable_service_router: bool = False


@dataclass
class SelectorOption:
    """Describe options used when selecting one service endpoint.

    Attributes:
        ctx: Request-scoped RPC context.
        key: Load-balance key, such as a hash key.
        env_key: Environment routing key.
        namespace: Target namespace used for selection.
        src_namespace: Caller namespace.
        src_servicename: Caller service name.
        src_env_name: Caller environment name.
        src_setname: Caller set name.
        dst_env_name: Callee environment name.
        dst_setname: Callee set name.
        env_transfer: Environment-transfer routing value.
        discovery: Discovery plugin name.
        loadbalance: Load balancer plugin name.
        circuit_breaker: Circuit breaker plugin name.
        service_router: Service router plugin name.
        service_router_opt: Options forwarded to the service router.
        disable_service_router: Whether service routing should be skipped.
    """

    ctx: Optional[TrpcContext] = None
    key: str = ""
    env_key: str = ""
    namespace: str = ""
    src_namespace: str = ""
    src_servicename: str = ""
    src_env_name: str = ""
    src_setname: str = ""
    dst_env_name: str = ""
    dst_setname: str = ""
    env_transfer: str = ""
    discovery: str = ""
    loadbalance: str = ""
    circuit_breaker: str = ""
    service_router: str = ""
    service_router_opt: ServerRouterOption = field(default_factory=ServerRouterOption)
    disable_service_router: bool = False

    def set_context(self, ctx: TrpcContext):
        """Set the request context on this option and the router option.

        Args:
            ctx: Request-scoped RPC context.
        """
        self.ctx = ctx
        self.service_router_opt.ctx = ctx

    def set_key(self, key: str):
        """Set the load-balance key.

        Args:
            key: Load-balance key, such as a hash key.
        """
        self.key = key

    def set_env_key(self, env_key: str):
        """Set the environment routing key.

        Args:
            env_key: Environment routing key.
        """
        self.env_key = env_key
        self.service_router_opt.env_key = env_key

    def set_namespace(self, namespace: str):
        """Set the target namespace.

        Args:
            namespace: Target namespace used for selection.
        """
        self.namespace = namespace
        self.service_router_opt.namespace = namespace

    def set_src_namespace(self, src_namespace: str):
        """Set the caller namespace.

        Args:
            src_namespace: Caller namespace.
        """
        self.src_namespace = src_namespace
        self.service_router_opt.src_namespace = src_namespace

    def set_src_servicename(self, src_servicename: str):
        """Set the caller service name.

        Args:
            src_servicename: Caller service name.
        """
        self.src_servicename = src_servicename
        self.service_router_opt.src_servicename = src_servicename

    def set_src_env_name(self, src_env_name: str):
        """Set the caller environment name.

        Args:
            src_env_name: Caller environment name.
        """
        self.src_env_name = src_env_name
        self.service_router_opt.src_env_name = src_env_name

    def set_src_setname(self, src_setname: str):
        """Set the caller set name.

        Args:
            src_setname: Caller set name.
        """
        self.src_setname = src_setname
        self.service_router_opt.src_setname = src_setname

    def set_dst_env_name(self, dst_env_name: str):
        """Set the callee environment name.

        Args:
            dst_env_name: Callee environment name.
        """
        self.dst_env_name = dst_env_name
        self.service_router_opt.dst_env_name = dst_env_name

    def set_dst_setname(self, dst_setname: str):
        """Set the callee set name.

        Args:
            dst_setname: Callee set name.
        """
        self.dst_setname = dst_setname
        self.service_router_opt.dst_setname = dst_setname

    def set_env_transfer(self, env_transfer: str):
        """Set the environment-transfer routing value.

        Args:
            env_transfer: Environment-transfer routing value.
        """
        self.env_transfer = env_transfer
        self.service_router_opt.env_transfer = env_transfer

    def set_discovery(self, discovery: str):
        """Set the discovery plugin name.

        Args:
            discovery: Discovery plugin name.
        """
        self.discovery = discovery

    def set_loadbalance(self, loadbalance: str):
        """Set the load balancer plugin name.

        Args:
            loadbalance: Load balancer plugin name.
        """
        self.loadbalance = loadbalance

    def set_circuit_breaker(self, circuit_breaker: str):
        """Set the circuit breaker plugin name.

        Args:
            circuit_breaker: Circuit breaker plugin name.
        """
        self.circuit_breaker = circuit_breaker

    def set_service_router(self, service_router: str):
        """Set the service router plugin name.

        Args:
            service_router: Service router plugin name.
        """
        self.service_router = service_router

    def set_disable_service_router(self, disable_service_router: bool):
        """Enable or disable service routing for this selection.

        Args:
            disable_service_router: Whether service routing should be skipped.
        """
        self.disable_service_router = disable_service_router
        self.service_router_opt.disable_service_router = disable_service_router

    def set_metadata(self, metadata: Dict[str, str]):
        """Replace the router metadata map.

        Args:
            metadata: Additional routing metadata.
        """
        self.service_router_opt.metadata = metadata


class BaseSelector(ABC):
    """Define service endpoint selection and call-result reporting."""

    @abstractmethod
    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """Select one service endpoint for the given options.

        Args:
            service_name: Logical service name to select from.
            opt: Selection and routing options.

        Returns:
            Selected service node.
        """
        ...

    @abstractmethod
    async def report(self, node: Node, cost: int, code: int) -> bool:
        """Report the outcome of one call against a selected node.

        Args:
            node: Service node that handled the call.
            cost: Call duration in milliseconds.
            code: Result code produced by the call.

        Returns:
            ``True`` if the report was accepted, otherwise ``False``.
        """
        ...
