# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Default client-side method wrappers for the inner worker-to-task stream.

Application code subclasses :class:`BaseTaskStreamClientHandler` (defined
in :mod:`trpc_sdk.abc`) and calls
``req_method_handle`` / ``rsp_method_handle`` / ``bidi_method_handle``.
Those factories wrap the opened stream in one of the ``_DefaultClient*``
classes below, which enable only the send/recv operations legal for that
kind.

This module is framework-internal IPC between a worker and a task
process, not the public ``trpc_sdk.stream`` protocol.
"""

from __future__ import annotations

from typing import Any

from trpc_sdk.abc import BaseTaskStreamClientHandler
from trpc_sdk.abc import BaseTaskStreamClientMethod
from ._client import ClientStream


class _DefaultClientReqMethod(BaseTaskStreamClientMethod):
    """Client-streaming wrapper: ``send`` then ``close_and_recv``."""

    def __init__(self, task_client: BaseTaskStreamClientHandler, stream: ClientStream) -> None:
        super().__init__(stream)
        self._task_client = task_client

    async def send(self, req: Any):
        """Send ``req`` as a DATA frame.

        Args:
            req: Request object.
        """
        return await self.client_stream.send_msg(req)

    async def close_and_recv(self) -> Any:
        """CLOSE the send half and wait for the single response.

        Returns:
            A response object from :meth:`BaseTaskStreamClientHandler.create_rsp_obj`.
        """
        await self.client_stream.close_send()
        rsp = self._task_client.create_rsp_obj()
        await self.client_stream.recv_msg(rsp)
        return rsp

    async def close_send(self):
        """Not supported on a req stream; use :meth:`close_and_recv`.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'close_send' method in client req stream")

    async def recv(self) -> Any:
        """Not supported on a req stream; use :meth:`close_and_recv`.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'recv' method in client req stream")


class _DefaultClientRspMethod(BaseTaskStreamClientMethod):
    """Server-streaming wrapper: only ``recv`` (the request was already sent)."""

    def __init__(self, task_client: BaseTaskStreamClientHandler, stream: ClientStream) -> None:
        super().__init__(stream)
        self._task_client = task_client

    async def send(self, req: Any):
        """Not supported on a rsp stream.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'send' method in client rsp stream")

    async def close_and_recv(self) -> Any:
        """Not supported on a rsp stream.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'close_and_recv' method in client rsp stream")

    async def close_send(self):
        """Not supported on a rsp stream; the send half is already closed.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'close_send' method in client rsp stream")

    async def recv(self) -> Any:
        """Receive the next response.

        Returns:
            A response object from :meth:`BaseTaskStreamClientHandler.create_rsp_obj`.
        """
        rsp = self._task_client.create_rsp_obj()
        await self.client_stream.recv_msg(rsp)
        return rsp


class _DefaultClientBidiMethod(BaseTaskStreamClientMethod):
    """Bidirectional wrapper: ``send``, ``close_send``, ``recv``."""

    def __init__(self, task_client: BaseTaskStreamClientHandler, stream: ClientStream) -> None:
        super().__init__(stream)
        self._task_client = task_client

    async def send(self, req: Any):
        """Send ``req`` as a DATA frame.

        Args:
            req: Request object.
        """
        return await self.client_stream.send_msg(req)

    async def close_and_recv(self) -> Any:
        """Not supported on a bidi stream; use ``close_send`` plus ``recv``.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'close_and_recv' method in client bidi stream")

    async def close_send(self):
        """Send CLOSE; further ``send`` calls on this stream fail."""
        await self.client_stream.close_send()

    async def recv(self) -> Any:
        """Receive the next response.

        Returns:
            A response object from :meth:`BaseTaskStreamClientHandler.create_rsp_obj`.
        """
        rsp = self._task_client.create_rsp_obj()
        await self.client_stream.recv_msg(rsp)
        return rsp
