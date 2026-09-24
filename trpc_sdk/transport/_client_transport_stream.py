# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pooled connection plus recv queue used by a client streaming call.

:class:`ClientConn` is what :class:`ClientTransport.start` stores under
``stream_id``. Later ``send`` / ``recv`` / ``close`` look it up. The
queue is filled either by a background :meth:`ClientConn.recv` loop or
by :meth:`ClientConn.push_data_to_recv_queue`.

``need_close`` is False when ``options.is_task_stream`` is True: the
task process still owns the pooled connection, so ending the stream
must not return it to the pool.
"""

from __future__ import annotations

import asyncio
from typing import Any

from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.log import logger
from trpc_sdk.utils import Queue
from ._client_transport_tcp import get_remain_time
from ._options import RoundTripOptions
from ._pool import PooledConnection


class ClientConn:
    """Client-side stream connection: pooled TCP plus a recv queue.

    Args:
        conn: Pooled connection obtained by ``start``.
        options: Round-trip options, including the frame parser and the
            ``is_task_stream`` flag that decides whether ``close`` may
            return the connection to the pool.
    """

    def __init__(self, conn: PooledConnection, options: RoundTripOptions):
        self._recv_queue = Queue()
        self._options = options
        self._conn = conn
        self._is_closed = False

    @property
    def recv_queue(self):
        """Queue that holds frames pushed by :meth:`push_data_to_recv_queue`."""
        return self._recv_queue

    @property
    def need_close(self) -> bool:
        """Whether ending the stream should return the conn to the pool.

        Task streams keep the pooled connection alive for the task
        process, so this is False when ``options.is_task_stream``.
        """
        return self._options.is_task_stream is False

    def push_data_to_recv_queue(self, data: Any):
        """Push a frame onto the recv queue.

        Args:
            data: Frame bytes (or whatever the caller stores).
        """
        self._recv_queue.push(data)

    def close(self, force=False):
        """Close the pooled connection.

        Args:
            force: Passed through to the pool conn. True drops it instead
                of returning it to the pool.
        """
        self._is_closed = force
        if self._conn:
            self._conn.close(force)

    async def recv_frame(self, ctx: TrpcContext):
        """Read one complete frame, bounded by ``ctx.deadline``.

        On timeout or any other I/O error the pooled connection is
        force-closed and ``_conn`` is set to None so a later
        :meth:`close` does not touch it again.

        Args:
            ctx: Client context whose ``deadline`` caps the wait.

        Returns:
            One complete response frame.

        Raises:
            ClientTimeoutError: Deadline expired while waiting.
            ClientInvokeError: Any other read failure.
        """
        rsp = b''
        try:
            rsp = await asyncio.wait_for(
                self._options.checker.read_frame(self._conn),
                get_remain_time(ctx.deadline),
            )
        except asyncio.TimeoutError as err:
            if self._conn:
                self._conn.close(True)
                self._conn = None

            raise ClientTimeoutError(
                f"recv_frame deadline to {self._options.address}, {err} io error "
            )

        except Exception as err:  # pylint: disable=broad-except
            if self._conn:
                self._conn.close(True)
                self._conn = None
            raise ClientInvokeError(
                f"recv_frame failed {self._options.address}, error {err}"
            )

        return rsp

    async def recv(self):
        """Background loop that reads frames onto the recv queue.

        Runs until the remote side errors, a read fails, or
        :meth:`close` marks the connection closed. Any failure closes
        the pooled connection (without force) and stops the loop; it
        does not raise to the caller.
        """
        while not self._is_closed:
            if self._conn.check_remote_error():
                self._conn.close(False)
                self._is_closed = True
                logger.error(" check_remote_error ")
                return

            try:
                rsp_buffer = await self._options.checker.read_frame(self._conn)
                self._recv_queue.push(rsp_buffer)
            except Exception as err:  # pylint: disable=broad-except
                # Read failed: every request on this connection is done.
                self._conn.close(False)
                self._is_closed = True
                logger.error(f" recv error {err} ")
                return

    def write(self, data: bytes):
        """Write bytes on the pooled connection.

        Args:
            data: Encoded request (or stream frame) to send.
        """
        self._conn.write(data)
