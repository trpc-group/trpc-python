# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""asyncio protocols for both ends of the worker <-> task IPC pipe.

The worker forks each task child before its event loop starts; the socketpair
is created pre-fork and each side drops the end it does not use:

- :class:`WorkerTaskConnection` runs on the worker and receives replies from a
  task process, waking the user coroutine parked in :func:`send_to_task`.
- :class:`TaskWorkerConnection` runs in the task process and receives requests
  from one worker. It records its transport in :data:`TASK_CONN` so the native
  response pump can write the answer back through the same pipe.
"""

from __future__ import annotations

import asyncio
import traceback
from typing import Dict

from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import InnerStreamParser
from ._cython import inner_decode_task_rsp
from ._cython import receive_pkg_from_worker
from ._utils import receive_from_task

# Remaining socketpair end per worker index, in the task process.
# ``TaskWorkerConnection`` fills this after fork; the native response pump in
# ``_task_queue.pyx`` writes responses back through the same map.
TASK_CONN: Dict[int, object] = {}


class WorkerTaskConnection(asyncio.Protocol):
    """Worker-side protocol that receives replies from a task process.

    Each decoded ``(pkg_id, timeout, rsp_obj)`` wakes the user coroutine
    that :func:`receive_from_task` parked when the request was sent.
    """

    def __init__(self):
        self.aio_connection = None
        self.parser = InnerStreamParser()

    def connection_made(self, transport) -> None:
        """Keep the task pipe so connection_lost can close it.

        Args:
            transport: Worker end of the worker <-> task socketpair.
        """
        self.aio_connection = transport

    def data_received(self, data: bytes) -> None:
        """Decode task replies and resume the matching user coroutine.

        Args:
            data: Bytes just read from the task IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_worker_task_read_err", 1)
                logger.error(f"[worker] failed to decode in task conn, err: {err}", exc_info=1)
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    pkg_id, timeout, rsp_obj = inner_decode_task_rsp(package)
                    receive_from_task(self.aio_connection, pkg_id, timeout, rsp_obj)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_worker_task_perform_err", 1)
                logger.error("[worker] failed to perform task, err: %s, trace: %s", repr(err), traceback.format_stack())

    def connection_lost(self, exc) -> None:
        """Close the task pipe when the task process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        report_attr("trpc_worker_task_conn_lost", 1)
        logger.error(f"[worker] connection lost with task server {exc}")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()


class TaskWorkerConnection(asyncio.Protocol):
    """Task-side protocol for one worker req/rsp socketpair.

    ``connection_made`` stores the transport in :data:`TASK_CONN` keyed by
    worker index. Each complete inner-IPC frame is handed to
    :func:`receive_pkg_from_worker`.
    """

    def __init__(self, app, index):
        self.app = app
        self.index = index
        self.parser = InnerStreamParser()
        self.aio_connection = None

    def connection_made(self, transport) -> None:
        """Record this worker pipe so the native response pump can write to it.

        Args:
            transport: Task end of the worker <-> task socketpair.
        """
        self.aio_connection = transport
        TASK_CONN[self.index] = transport

    def data_received(self, data: bytes) -> None:
        """Decode worker frames and dispatch them to the native task queue.

        Args:
            data: Bytes just read from the worker IPC pipe.
        """
        self.parser.feeds_data(data)
        while True:
            try:
                packages = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_task_read_err", 1)
                logger.error(f"[task] failed to decode in conn, err: {err}\n, trace: {traceback.format_stack()}",
                             exc_info=True)
                return

            try:
                if len(packages) == 0:
                    return

                for package in packages:
                    receive_pkg_from_worker(self.index, package)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_task_perform_err", 1)
                logger.error(f"[task] failed to perform task err: {err}\n, trace: {traceback.format_stack()}",
                             exc_info=True)

    def connection_lost(self, exc) -> None:
        """Close this worker pipe when the worker process drops it.

        Args:
            exc: Close reason, or None on a clean close.
        """
        if exc:
            logger.error(f"[task] connection lost, reason {exc}")
        report_attr("trpc_task_conn_lost", 1)
        logger.info("[task] connection lost")

        if self.aio_connection and not self.aio_connection.is_closing():
            self.aio_connection.close()
