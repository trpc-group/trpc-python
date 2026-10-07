# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server-side descriptors and extension interfaces.

This module defines service method descriptors, streaming interfaces, process
roles, and the abstract contracts used by pluggable server engines.
"""

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from enum import unique
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]


@unique
class ProcessType(IntEnum):
    """Identify the role stamped on ``multiprocessing.current_process().type``.

    Attributes:
        UNKNOWN: Default before a role is stamped, or an unclassified process.
        WORKER: Worker process that serves RPC traffic.
        TASK: Dedicated task process for offloaded work.
        TIMER: Timer process that only runs timer executors.
    """

    UNKNOWN = 0
    WORKER = 1
    TASK = 2
    TIMER = 3


@dataclass
class Method:
    """Describe one unary RPC method.

    Attributes:
        name: Fully qualified RPC method name.
        req_cls: Request class used to decode inbound bodies.
        rsp_cls: Response class used to encode outbound bodies.
        impl_func: User-provided handler that serves the method.
    """

    name: str
    req_cls: Any
    rsp_cls: Any
    impl_func: Callable[..., Any]


@dataclass
class StreamDesc:
    """Describe one streaming RPC method.

    Attributes:
        name: Fully qualified streaming RPC method name.
        impl_func: User-provided handler that serves the stream.
        server_stream: Server stream object or factory used by the method.
    """

    name: str
    impl_func: Callable[..., Any]
    server_stream: Any


@dataclass
class ServiceDesc:
    """Describe the methods and streams exposed by one service.

    Attributes:
        service_name: Logical service name.
        methods: Unary methods registered on the service.
        streams: Optional streaming methods registered on the service.
        stream_handler: Optional shared streaming dispatch handler.
    """

    service_name: str
    methods: List[Method]
    streams: Optional[List[StreamDesc]] = None
    stream_handler: Any = None


class BaseServer(ABC):
    """Define the server interface: start serving and block."""

    @abstractmethod
    def serve(self) -> None:
        """Start the server and run until shutdown.

        Args:
            None.
        """


class BaseServerStream(ABC):
    """Define bidirectional server-stream operations."""

    @abstractmethod
    def context(self) -> TrpcContext:
        """Return the context associated with this stream.

        Returns:
            Stream-scoped RPC context.
        """
        ...

    @abstractmethod
    async def send_msg(self, message: Any) -> None:
        """Send one application message to the peer.

        Args:
            message: Application message to encode and send.
        """
        ...

    @abstractmethod
    async def recv_msg(self, message: Any) -> Any:
        """Receive and decode one application message from the peer.

        Args:
            message: Destination object or type used to decode the payload.

        Returns:
            Decoded application message.
        """
        ...


class BaseTrpcServerEngine(ABC):
    """Define the server engine lifecycle accepted by transports."""

    @abstractmethod
    def set_process_info(self, process_type: ProcessType) -> None:
        """Initialize process-role specific engine state.

        Args:
            process_type: Role of the current process.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Shut down the engine and release owned resources."""
        ...
