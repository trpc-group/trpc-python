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
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.utils import PipeConnection


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

    ``transport_opt`` and ``transports`` are parallel: index ``i`` describes
    the same listen endpoint, and in the parent ``transports[i].opts is
    transport_opt[i]``. Every other per-endpoint field (address, handler,
    parser, timeout, ...) is read off ``transport_opt[i]``.
    """

    # Live process instance after the process has been started.
    process: Optional[Process] = None
    # Task IPC metadata associated with this process.
    process_task_info: Optional[ProcessTaskInfo] = None
    # IPC pipe used by this process.
    pipe_conn: Optional[PipeConnection] = None
    # IPC pipes from the dispatcher to worker processes.
    dispatcher_pipe_conn_list: List[PipeConnection] = field(default_factory=list)
    # Transport options for each listen address.
    transport_opt: List[BaseServerTransportOption] = field(default_factory=list)
    # Server transports already bound in ``ServiceAdapter.init_transport``. Inherited across
    # fork; a worker calls ``listen()`` on the entries it serves itself.
    transports: List[BaseServerTransport] = field(default_factory=list)

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
            transport_opt=list(process_info.transport_opt),
            transports=list(process_info.transports),
        )

    def add_transport_opt(self, opt: BaseServerTransportOption) -> None:
        """Append transport options for the latest listen address.

        Args:
            opt: Server transport options.
        """
        self.transport_opt.append(opt)

    def add_transport(self, transport: BaseServerTransport) -> None:
        """Append the bound server transport for the latest listen address.

        Args:
            transport: Server transport whose ``bind`` already ran.
        """
        self.transports.append(transport)
