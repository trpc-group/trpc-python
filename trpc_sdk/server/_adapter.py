# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Options and listen-endpoint tables the worker reads at startup.

:class:`TrpcServerAdapter` lives in the same process as the worker. It does
not listen for clients, does not run service handlers, and does not fork
anyone. ``ServiceAdapter.init_transport`` appends listen endpoints onto ``process_info``;
:meth:`~trpc_sdk.server.Server.serve` then builds the worker for ``server.mode``
and that worker binds the addresses itself.

The master / dispatcher / executor topology that used to live here is gone.
What remains is the data the worker still needs:

* ``opts`` and ``process_info`` — process-model options and the per-endpoint
  tables filled during ``ServiceAdapter.init_transport``.
* ``registry_semaphore`` — one permit per service, so shutdown can wait for
  naming-service logout.
* ``worker_process_list`` and :meth:`send_msg_to_worker` — the inter-worker
  message pipe. ``merge`` and ``separate`` leave the list empty; only a
  custom mode that fills it can send.
"""

from __future__ import annotations

import sys
import threading
from typing import TYPE_CHECKING
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseTrpcServerAdapter
from trpc_sdk.abc import ProcessType
from trpc_sdk.log import logger
from trpc_sdk.runtime import ProcessInfo
from trpc_sdk.runtime import WorkerInfo

if TYPE_CHECKING:
    from trpc_sdk.server import ServerOption


class TrpcServerAdapter(BaseTrpcServerAdapter):
    """Options and listen-endpoint tables for the tRPC server process.

    Holds ``process_info`` and ``opts`` for the worker to read. Does not own
    listen sockets, does not run handlers, and does not fork children; those
    live on the worker chosen by ``server.mode``.
    """

    def __init__(self, opts: Optional[ServerOption] = None) -> None:
        """Record server options; no child is forked, no socket is bound.

        Args:
            opts: Process-model options. ``service_num`` sizes
                ``registry_semaphore``.
        """
        self.opts: Optional[ServerOption] = opts

        # Pre-fork records a custom mode can fill so ``send_msg_to_worker``
        # has somewhere to write. ``merge`` and ``separate`` leave this empty.
        self.worker_process_list: List[WorkerInfo] = []

        # Listen/handler metadata appended by ``add_listen_endpoint`` during
        # ``ServiceAdapter.init_transport``, before the worker binds anything.
        self.process_info: ProcessInfo = ProcessInfo()

        # Each service ``close()`` releases once after deregister.
        # ``close()`` acquires ``service_num`` times before returning.
        self.registry_semaphore: threading.Semaphore = threading.Semaphore(self.opts.service_num)

    def set_process_info(self, process_type: ProcessType) -> None:
        """Satisfy :class:`BaseTrpcServerAdapter`; merge workers ignore this.

        The master / dispatcher / executor topology that used to stamp a
        process role here is gone. The method stays so the ABC can be
        instantiated; nothing reads the argument.

        Args:
            process_type: Role of the current process.
        """
        del process_type

    @property
    def on_worker_message(self) -> Optional[Callable]:
        """The removed ``ServerOption.on_worker_message``; nothing calls it any more."""
        if not self.opts:
            return None
        return self.opts.on_worker_message

    def add_listen_endpoint(self, transport: BaseServerTransport) -> None:
        """Check one listen endpoint, then append it to ``process_info``.

        Called from ``ServiceAdapter.init_transport`` right after ``transport.bind(opts)``, so
        still before fork. Nothing is bound here; the opts / transport lists
        stay index-aligned so the worker can read them later. Which addresses
        are valid is the transport's business: :meth:`~BaseServerTransport.check_endpoint`
        logs the reason, and the process exits with status 1.

        Args:
            transport: A transport whose :meth:`~BaseServerTransport.bind`
                already ran. Every per-endpoint field (address, handler,
                parser, timeout, ...) lives on ``transport.opts``; the
                transport itself is kept as well because the opts do not
                point back to it.
        """
        opts: BaseServerTransportOption = transport.opts
        if not transport.check_endpoint():
            logger.error("[server adapter] cannot serve %s (network %s)", opts.address, opts.network)
            sys.exit(1)

        self.process_info.add_transport_opt(opts)
        self.process_info.add_transport(transport)

    def send_msg_to_worker(self, from_id: int, to: int, data: str) -> Optional[bool]:
        """Send an inter-worker message.

        Only available when this process actually forked workers, which means
        ``worker_process_list`` is filled: the message pipes it holds are the
        transport. ``merge`` and ``separate`` leave the list empty, so there is
        no peer to address and the call is dropped with a warning.

        A negative or out-of-range ``to`` returns ``False`` and sends
        nothing; broadcast is not supported. A successful send returns
        ``None``.

        Args:
            from_id: Sending worker id, written into the frame.
            to: Target worker id, in ``[0, len(worker_process_list))``.
            data: Payload; encoded as UTF-8 on the write end of the
                target's message pipe.

        Returns:
            ``False`` if this process has no worker children or ``to`` is out
            of range; otherwise ``None``.
        """
        if not self.worker_process_list:
            logger.warning("[server adapter] drop worker msg from %s to %s: mode %s forks no worker process", from_id,
                           to, self.opts.mode if self.opts else "")
            return False

        if to < 0 or len(self.worker_process_list) <= to:
            return False

        self.worker_process_list[to].send_msg(from_id, data)

    def close(self) -> None:
        """Wait for service deregister.

        Acquires ``registry_semaphore`` once per configured service (10s
        timeout each) so ``ServiceAdapter.close`` can finish registry logout.
        Children are stopped by the worker, not here.
        """
        for _ in range(self.opts.service_num):
            self.registry_semaphore.acquire(True, 10)
