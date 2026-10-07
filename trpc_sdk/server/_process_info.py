# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Listen addresses, handlers, and IPC metadata shared by server processes."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from multiprocessing import Process
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseServerProtocolChecker
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.ipc import PipeConnection


@dataclass
class ProcessTaskInfo:
    """Per-process task IPC metadata held by the master engine."""

    # Task names paired with ``task_pipe_conn``.
    task_name: List[str] = field(default_factory=list)
    # IPC pipes between this process and its tasks. ``None`` for a task
    # this process has no pipe to.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)


@dataclass
class ProcessInfo:
    """Listen/handler metadata shared by dispatcher and worker processes.

    List fields are parallel: index ``i`` describes the same listen endpoint
    across ``addresses``, ``socket_types``, ``networks``, ``checkers``,
    ``handlers``, ``timeouts``, ``services``, ``transport_opt``, and ``targets``.
    """

    # Live process instance after the process has been started.
    process: Optional[Process] = None
    # Task IPC metadata associated with this process.
    process_task_info: Optional[ProcessTaskInfo] = None
    # IPC pipe used by this process.
    pipe_conn: Optional[PipeConnection] = None
    # IPC pipes from the dispatcher to worker processes.
    dispatcher_pipe_conn_list: List[PipeConnection] = field(default_factory=list)
    # Public listen addresses as ``(host, port)``.
    addresses: List[Tuple[str, int]] = field(default_factory=list)
    # Socket type (TCP/UDP/UNIX) for each listen address.
    socket_types: List[str] = field(default_factory=list)
    # Network protocol (tcp/udp/http) for each listen address.
    networks: List[str] = field(default_factory=list)
    # Protocol checkers used to inspect inbound client connections. ``None``
    # where no checker plugin is registered for the service protocol.
    checkers: List[Optional[BaseServerProtocolChecker]] = field(default_factory=list)
    # Request handlers for each listen address.
    handlers: List[Callable[..., Any]] = field(default_factory=list)
    # Idle/request timeout (ms) for each listen address.
    timeouts: List[int] = field(default_factory=list)
    # Service adapters for each listen address.
    services: List[Any] = field(default_factory=list)
    # Transport options for each listen address.
    transport_opt: List[BaseServerTransportOption] = field(default_factory=list)
    # Custom address targets for each listen address.
    targets: List[str] = field(default_factory=list)

    @classmethod
    def deep_copy(cls, process_info: ProcessInfo) -> ProcessInfo:
        """Return a copy whose list fields are independent containers.

        Element objects are still shared with ``process_info``.

        Args:
            process_info: Source process info to copy.

        Returns:
            A new ``ProcessInfo`` with copied list containers.
        """
        return replace(
            process_info,
            dispatcher_pipe_conn_list=list(process_info.dispatcher_pipe_conn_list),
            addresses=list(process_info.addresses),
            socket_types=list(process_info.socket_types),
            networks=list(process_info.networks),
            checkers=list(process_info.checkers),
            handlers=list(process_info.handlers),
            timeouts=list(process_info.timeouts),
            services=list(process_info.services),
            transport_opt=list(process_info.transport_opt),
            targets=list(process_info.targets),
        )

    def add_address(self, address: Tuple[str, int]) -> None:
        """Append a listen address.

        Args:
            address: ``(host, port)`` tuple.
        """
        self.addresses.append(address)

    def add_socket_type(self, socket_type: str) -> None:
        """Append a socket type for the latest listen address.

        Args:
            socket_type: Socket type such as TCP, UDP, or UNIX.
        """
        self.socket_types.append(socket_type)

    def add_network(self, network: str) -> None:
        """Append a network protocol for the latest listen address.

        Args:
            network: Network protocol such as ``tcp``, ``udp``, or ``http``.
        """
        self.networks.append(network)

    def add_checker(self, checker: Optional[BaseServerProtocolChecker]) -> None:
        """Append a protocol checker for the latest listen address.

        Args:
            checker: Parser used to inspect inbound client connections, or
                ``None`` when the service protocol registers no checker.
        """
        self.checkers.append(checker)

    def add_handler(self, handler: Callable[..., Any]) -> None:
        """Append a request handler for the latest listen address.

        Args:
            handler: Request handler callable.
        """
        self.handlers.append(handler)

    def add_timeout(self, timeout: int) -> None:
        """Append an idle/request timeout for the latest listen address.

        Args:
            timeout: Timeout in milliseconds.
        """
        self.timeouts.append(timeout)

    def add_service(self, service: Any) -> None:
        """Append a service adapter for the latest listen address.

        Args:
            service: Service adapter bound to the listen address.
        """
        self.services.append(service)

    def add_transport_opt(self, opt: BaseServerTransportOption) -> None:
        """Append transport options for the latest listen address.

        Args:
            opt: Server transport options.
        """
        self.transport_opt.append(opt)

    def add_target(self, target: str) -> None:
        """Append a custom address target for the latest listen address.

        Args:
            target: Custom target string, such as ``ip://127.0.0.1``.
        """
        self.targets.append(target)
