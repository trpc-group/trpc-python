# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Abstract contracts for the inner worker-to-task stream.

The public ``trpc_sdk.stream`` protocol is implemented by
:class:`~trpc_sdk.stream.ClientStream` and
:class:`~trpc_sdk.stream.ServerStream`. This module is the narrower
IPC variant used between a worker and a task process over a Unix socket:

- :class:`BaseTaskStreamClientMethod` / :class:`BaseTaskStreamServerMethod`
  are the per-call send/recv surface a generated stub talks to.
- :class:`BaseTaskStreamClientHandler` / :class:`BaseTaskStreamServiceHandler`
  are what application code subclasses to create request/response objects
  and to implement ``req`` / ``rsp`` / ``bidi`` handlers.

Default method wrappers live in :mod:`trpc_sdk.stream._task_client` and
:mod:`trpc_sdk.stream._task_service`; the factories below import them lazily
so those modules can subclass the ABCs defined here.

This module stays a leaf at import time: ``client`` / ``codec`` / ``server``
/ ``stream`` are imported inside methods, not at module level.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]


class TaskStreamMethodType:
    """RPC names that select a task-stream method kind.

    The client stamps one of these onto ``client_rpc_name``; the service
    adapter looks the same string up to dispatch ``req`` / ``rsp`` /
    ``bidi``. Values must stay in sync on both sides of the Unix socket.
    """

    CLIENT = "task_stream_client"
    SERVER = "task_stream_server"
    BIDI = "task_stream_bidi"


def _with_addr(path: str) -> Callable:
    """Return an option that points the call at a Unix socket.

    The inner worker-to-task stream is not resolved through naming; the
    worker already knows the task's listen path and stamps it onto
    ``call_options.address`` with socket type ``unix``. Port ``-1`` is
    the Unix-socket sentinel used elsewhere in the client.

    Args:
        path: Absolute path of the task process Unix socket.

    Returns:
        An option callable suitable for :class:`~trpc_sdk.client.Options`
        chaining.
    """

    def func(opts):
        # ``opts`` is a ``trpc_sdk.client.Options``; left untyped so this
        # module does not import ``trpc_sdk.client`` at load time.
        opts.call_options.set_address((path, -1))
        opts.call_options.set_socket_type("unix")

    return func


class BaseTaskStreamServerMethod:
    """Per-call send/recv surface on the task-process (server) side.

    Wraps one :class:`~trpc_sdk.stream.ServerStream`. Subclasses decide
    which of ``send``, ``recv`` and ``send_and_close`` are legal for the
    ``req`` / ``rsp`` / ``bidi`` kind they represent.
    """

    def __init__(self, stream) -> None:
        # ``stream`` is a ``trpc_sdk.stream.ServerStream``.
        self.server_stream = stream

    @abstractmethod
    async def send_and_close(self, rsp: Any):
        """Send ``rsp`` as the last message and close the stream.

        Args:
            rsp: Response object produced by :meth:`BaseTaskStreamServiceHandler.create_rsp_obj`.
        """

    @abstractmethod
    async def recv(self):
        """Receive the next message on this stream.

        Returns:
            A request object produced by
            :meth:`BaseTaskStreamServiceHandler.create_req_obj`.
        """

    @abstractmethod
    async def send(self, req: Any):
        """Send one message without closing the stream.

        Args:
            req: Object to encode onto the next DATA frame.
        """


class BaseTaskStreamClientMethod:
    """Per-call send/recv surface on the worker (client) side.

    Wraps one :class:`~trpc_sdk.stream.ClientStream`. Subclasses decide
    which of ``send``, ``recv``, ``close_send`` and ``close_and_recv`` are
    legal for the ``req`` / ``rsp`` / ``bidi`` kind they represent.
    """

    def __init__(self, stream) -> None:
        # ``stream`` is a ``trpc_sdk.stream.ClientStream``.
        self.client_stream = stream

    @abstractmethod
    async def send(self, req: Any):
        """Send ``req`` as a DATA frame.

        Args:
            req: Object to encode onto the next DATA frame.
        """

    @abstractmethod
    async def close_send(self):
        """Send a CLOSE frame; further ``send`` calls on this stream fail."""

    @abstractmethod
    async def close_and_recv(self) -> Any:
        """Close the send half and wait for a single response.

        Returns:
            A response object produced by
            :meth:`BaseTaskStreamClientHandler.create_rsp_obj`.
        """

    @abstractmethod
    async def recv(self) -> Any:
        """Receive the next message on this stream.

        Returns:
            A response object produced by
            :meth:`BaseTaskStreamClientHandler.create_rsp_obj`.
        """


class BaseTaskStreamClientHandler(ABC):
    """Worker-side factory for inner task-stream calls.

    Subclasses supply :meth:`create_req_obj` / :meth:`create_rsp_obj`. The
    three ``*_method_handle`` entry points open a stream whose RPC name is
    a :class:`TaskStreamMethodType` value, then wrap it in the matching
    default method implementation.
    """

    def __init__(self, stream_client=None) -> None:
        # ``stream_client`` is a ``trpc_sdk.stream.Client``.
        from trpc_sdk.stream import default_task_stream_client

        self._stream_client = stream_client or default_task_stream_client
        self._unix_path = None

    @property
    def stream_client(self):
        """Return the :class:`~trpc_sdk.stream.Client` used to open streams."""
        return self._stream_client

    @property
    def client_unix_path(self) -> Optional[str]:
        """Return the Unix-socket path of the peer task process."""
        return self._unix_path

    def set_unix_path(self, path: str) -> None:
        """Remember the task process Unix-socket path for later calls.

        Args:
            path: Absolute path the worker should dial.
        """
        self._unix_path = path

    @abstractmethod
    def create_req_obj(self) -> object:
        """Return an empty request object for this task's payload type."""

    @abstractmethod
    def create_rsp_obj(self) -> object:
        """Return an empty response object for this task's payload type."""

    def _create_req_stream_method(self, stream) -> BaseTaskStreamClientMethod:
        """Build the default client-side req-stream method wrapper.

        Imported here, not at module level: the default implementations
        subclass the ABCs in this module, so a module-level import would
        close the loop. Only ``_task_client`` -> this module is a real
        module-level edge.

        Args:
            stream: Already-initialized :class:`~trpc_sdk.stream.ClientStream`.

        Returns:
            A :class:`BaseTaskStreamClientMethod` that only allows
            ``send`` / ``close_and_recv``.
        """
        from trpc_sdk.stream._task_client import _DefaultClientReqMethod

        return _DefaultClientReqMethod(task_client=self, stream=stream)

    def _create_rsp_stream_method(self, stream) -> BaseTaskStreamClientMethod:
        """Build the default client-side rsp-stream method wrapper.

        Imported here, not at module level: see
        :meth:`_create_req_stream_method`.

        Args:
            stream: Already-initialized :class:`~trpc_sdk.stream.ClientStream`.

        Returns:
            A :class:`BaseTaskStreamClientMethod` that only allows ``recv``.
        """
        from trpc_sdk.stream._task_client import _DefaultClientRspMethod

        return _DefaultClientRspMethod(task_client=self, stream=stream)

    def _create_bidi_stream_method(self, stream) -> BaseTaskStreamClientMethod:
        """Build the default client-side bidi-stream method wrapper.

        Imported here, not at module level: see
        :meth:`_create_req_stream_method`.

        Args:
            stream: Already-initialized :class:`~trpc_sdk.stream.ClientStream`.

        Returns:
            A :class:`BaseTaskStreamClientMethod` that allows ``send``,
            ``close_send`` and ``recv``.
        """
        from trpc_sdk.stream._task_client import _DefaultClientBidiMethod

        return _DefaultClientBidiMethod(task_client=self, stream=stream)

    async def _create_stream(
            self,
            ctx: TrpcContext,
            method_name: str,
            options: Optional[List[Callable]] = None,
    ):
        """Open one inner stream against the peer task process.

        Serialization is ``Noop`` and the window is ``0`` (flow control
        off): the payload is already framed by the task codec, and the
        Unix socket is a single hop that does not need INIT-window
        negotiation.

        Args:
            ctx: Call context; a cloned client message is stamped with
                ``method_name``.
            method_name: A :class:`TaskStreamMethodType` value, used both
                as the RPC name and as the stream descriptor name.
            options: Extra client option callables appended after the
                Unix-socket defaults.

        Returns:
            An initialized :class:`~trpc_sdk.stream.ClientStream`.
        """
        from trpc_sdk.client import Options
        from trpc_sdk.codec import INNER_TASK_STREAM
        from trpc_sdk.serialization import SerializationType
        from trpc_sdk.stream import ClientStreamDesc
        from ._cython import clone_client_message

        stream_opts = [
            (
                Options()
                .set_protocol(INNER_TASK_STREAM)
                .set_serialization_type(SerializationType.Noop)
                .set_network("tcp")
                .set_unix_path(self._unix_path)
                .set_max_window_size(0)
                .set_socket_type("unix")
            ),
            _with_addr(self._unix_path),
        ]
        if options:
            stream_opts.extend(options)
        ctx, msg = clone_client_message(ctx)
        msg.set_client_rpc_name(method_name)
        desc = ClientStreamDesc(method_name, True)
        return await self._stream_client.create_client_stream(ctx, desc, desc.stream_name, stream_opts)

    async def req_method_handle(
            self,
            ctx: TrpcContext,
            options: Optional[List[Callable]] = None,
    ) -> BaseTaskStreamClientMethod:
        """Open a client-streaming (req) call.

        Args:
            ctx: Call context.
            options: Extra client option callables.

        Returns:
            A method wrapper the caller uses to ``send`` then
            ``close_and_recv``.
        """
        stream = await self._create_stream(ctx, TaskStreamMethodType.CLIENT, options)
        return self._create_req_stream_method(stream)

    async def rsp_method_handle(
            self,
            ctx: TrpcContext,
            request: object,
            options: Optional[List[Callable]] = None,
    ) -> BaseTaskStreamClientMethod:
        """Open a server-streaming (rsp) call and send the one request.

        The request is written and the send half closed before this
        returns, so the caller only iterates ``recv``.

        Args:
            ctx: Call context.
            request: Single request object to send.
            options: Extra client option callables.

        Returns:
            A method wrapper the caller uses to ``recv`` responses.
        """
        stream = await self._create_stream(ctx, TaskStreamMethodType.SERVER, options)
        stream_method = self._create_rsp_stream_method(stream)
        await stream_method.client_stream.send_msg(request)
        await stream_method.client_stream.close_send()
        return stream_method

    async def bidi_method_handle(
            self,
            ctx: TrpcContext,
            options: Optional[List[Callable]] = None,
    ) -> BaseTaskStreamClientMethod:
        """Open a bidirectional call.

        Args:
            ctx: Call context.
            options: Extra client option callables.

        Returns:
            A method wrapper the caller uses to ``send`` / ``recv`` /
            ``close_send``.
        """
        stream = await self._create_stream(ctx, TaskStreamMethodType.BIDI, options)
        return self._create_bidi_stream_method(stream)


class BaseTaskStreamServiceHandler(ABC):
    """Task-process side of an inner stream: object factories plus handlers.

    Subclasses implement :meth:`create_req_obj` / :meth:`create_rsp_obj`
    and override the ``*_method_handle`` they actually serve. The default
    bodies raise ``NotImplementedError`` so an unused kind fails loudly
    rather than hanging on an empty stream.
    """

    def __init__(self, options=None) -> None:
        """Bind optional per-service options.

        Args:
            options: Service options reused for the inner stream. ``None``
                constructs a fresh :class:`~trpc_sdk.server.ServiceOption` so a
                handler can be registered before the worker has filled
                codec / transport. Left untyped to avoid a runtime import
                cycle with ``trpc_sdk.server``.
        """
        if options is None:
            # Imported here, not at module level: ``trpc_sdk.server`` imports
            # ``trpc_sdk.abc``, so a module-level ServiceOption import would
            # close that loop.
            from trpc_sdk.server import ServiceOption as service_option_cls

            options = service_option_cls()
        self.options = options

    @abstractmethod
    def create_req_obj(self) -> object:
        """Return an empty request object for this task's payload type."""

    @abstractmethod
    def create_rsp_obj(self) -> object:
        """Return an empty response object for this task's payload type."""

    def _create_req_stream_method(self, stream) -> BaseTaskStreamServerMethod:
        """Build the default server-side req-stream method wrapper.

        Imported here, not at module level: the default implementations
        subclass the ABCs in this module, so a module-level import would
        close the loop. Only ``_task_service`` -> this module is a real
        module-level edge.

        Args:
            stream: :class:`~trpc_sdk.stream.ServerStream` for this call.

        Returns:
            A :class:`BaseTaskStreamServerMethod` that allows ``recv``
            and ``send_and_close``.
        """
        from trpc_sdk.stream._task_service import _DefaultServerReqMethod

        return _DefaultServerReqMethod(stream, self)

    def _create_rsp_stream_method(self, stream) -> BaseTaskStreamServerMethod:
        """Build the default server-side rsp-stream method wrapper.

        Imported here, not at module level: see
        :meth:`_create_req_stream_method`.

        Args:
            stream: :class:`~trpc_sdk.stream.ServerStream` for this call.

        Returns:
            A :class:`BaseTaskStreamServerMethod` that only allows ``send``.
        """
        from trpc_sdk.stream._task_service import _DefaultServerRspMethod

        return _DefaultServerRspMethod(stream, self)

    def _create_bidi_stream_method(self, stream) -> BaseTaskStreamServerMethod:
        """Build the default server-side bidi-stream method wrapper.

        Imported here, not at module level: see
        :meth:`_create_req_stream_method`.

        Args:
            stream: :class:`~trpc_sdk.stream.ServerStream` for this call.

        Returns:
            A :class:`BaseTaskStreamServerMethod` that allows ``send``
            and ``recv``.
        """
        from trpc_sdk.stream._task_service import _DefaultServerBidiMethod

        return _DefaultServerBidiMethod(stream, self)

    async def req_method_handle(self, stream):
        """Handle a client-streaming (req) call.

        Args:
            stream: :class:`~trpc_sdk.stream.ServerStream` for this call.

        Raises:
            NotImplementedError: The subclass does not serve this kind.
        """
        raise NotImplementedError("task stream service req_method_handle not impl")

    async def rsp_method_handle(self, request: object, stream):
        """Handle a server-streaming (rsp) call.

        Args:
            request: The single request already read by the adapter.
            stream: :class:`~trpc_sdk.stream.ServerStream` used to send responses.

        Raises:
            NotImplementedError: The subclass does not serve this kind.
        """
        raise NotImplementedError("task stream service stream rsp_method_handle not impl")

    async def bidi_method_handle(self, stream):
        """Handle a bidirectional call.

        Args:
            stream: :class:`~trpc_sdk.stream.ServerStream` for this call.

        Raises:
            NotImplementedError: The subclass does not serve this kind.
        """
        raise NotImplementedError("task stream service bidi_method_handle not impl")
