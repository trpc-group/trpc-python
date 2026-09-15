# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Options and listen-endpoint tables the worker reads at startup.

:class:`TrpcServerEngine` lives in the same process as the worker. It does
not listen for clients, does not run service handlers, and does not fork
anyone. ``Service.run`` appends listen endpoints onto ``process_info``;
:meth:`~trpc_sdk.server.Server.serve` then builds the worker for ``server.mode``
and that worker binds the addresses itself.

The master / dispatcher / executor topology that used to live here is gone.
What remains is the data the worker still needs:

* ``opts`` and ``process_info`` — process-model options and the per-endpoint
  tables filled during ``Service.run``.
* ``registry_semaphore`` — one permit per service, so shutdown can wait for
  naming-service logout.
* ``worker_process_list`` and :meth:`send_msg_to_worker` — the inter-worker
  message pipe. ``merge`` and ``separate`` leave the list empty;
  :func:`trpc_sdk.server.send_msg_worker` still works for a custom mode that
  fills it.

``GLOBAL_SERVER_ENGINE`` is set before any fork so children inherit the
reference. That is why :func:`trpc_sdk.server.send_msg_worker` can reach
``worker_process_list`` from inside a worker.
"""

from __future__ import annotations

import threading
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseService
from trpc_sdk.abc import BaseTrpcServerEngine
from trpc_sdk.abc import Parser
from trpc_sdk.log import logger
from trpc_sdk.utils import is_tcp_port_available
from trpc_sdk.utils import is_udp_port_available
from trpc_sdk.utils import is_unix_socket
from trpc_sdk.utils import unix_path_valid
from ._constants import Network
from ._option import ServerOption
from ._process_info import ProcessInfo
from ._types import WorkerInfo


GLOBAL_SERVER_ENGINE: Optional[TrpcServerEngine] = None


class TrpcServerEngine(BaseTrpcServerEngine):
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
        # ``Service.run``, before the worker binds anything.
        self.process_info: ProcessInfo = ProcessInfo()

        # Each service ``close()`` releases once after deregister.
        # ``close()`` acquires ``service_num`` times before returning.
        self.registry_semaphore: threading.Semaphore = threading.Semaphore(self.opts.service_num)

    @property
    def on_worker_message(self) -> Optional[Callable]:
        """Callback invoked when a worker receives an inter-worker message.

        Copied from ``ServerOption`` so child processes can read it after
        fork without holding the option object themselves.
        """
        if not self.opts:
            return None
        return self.opts.on_worker_message

    def add_listen_endpoint(self,
                            address: Tuple[str, int],
                            socket_type: str,
                            handler: Callable,
                            checker: Parser,
                            timeout: int,
                            network: str,
                            service: BaseService,
                            tran_opt: BaseServerTransportOption,
                            target: str = '') -> None:
        """Append one listen endpoint the worker will bind itself.

        Called from ``transport.listen_and_serve`` during ``Service.run``.
        Nothing is bound here; the nine ``process_info`` lists stay
        index-aligned so the worker can copy them later. TCP/HTTP and UDP
        binds are both accepted.

        Args:
            address: ``(host, port)`` to listen on.
            socket_type: Socket family (unix vs net).
            handler: Per-request handler for this listen endpoint.
            checker: Frame-integrity parser for inbound connections.
            timeout: Idle/request timeout in milliseconds.
            network: Network protocol (tcp/http/udp).
            service: Service adapter bound to that endpoint.
            tran_opt: Transport options for that endpoint.
            target: Optional custom address stored beside the bind address.
        """
        # check socket info valid
        if is_unix_socket(socket_type):
            if not unix_path_valid(address[0]):
                logger.error("[Master Engine] cannot unlink unix address %s", address[0])
                exit()
        else:
            if network.find(Network.TCP) != -1 or network.find(Network.HTTP) != -1:
                if not is_tcp_port_available(address[0], address[1]):
                    logger.error("[Master Engine] tcp address %s invalid", address)
                    exit()
            elif network.find(Network.UDP) != -1:
                if not is_udp_port_available(address[0], address[1]):
                    logger.error("[Master Engine] udp address %s invalid", address)
                    exit()
            else:
                logger.error("[Master Engine] unsupported network %s", network)
                exit()

        self.process_info.add_address(address)
        self.process_info.add_socket_type(socket_type)
        self.process_info.add_handler(handler)
        self.process_info.add_checker(checker)
        self.process_info.add_timeout(timeout)
        self.process_info.add_network(network)
        self.process_info.add_service(service)
        self.process_info.add_transport_opt(tran_opt)
        self.process_info.add_target(target)

    def send_msg_to_worker(self, from_id: int, to: int, data: str) -> Optional[bool]:
        """Send an inter-worker message, or broadcast when ``to`` is negative.

        Out-of-range unicast (``to`` past the last worker) returns ``False``
        and sends nothing. A successful unicast or broadcast returns
        ``None``.

        Args:
            from_id: Sending worker id, written into the frame.
            to: Target worker id, or a negative value to broadcast to every
                worker except the sender.
            data: Payload; encoded as UTF-8 on the write end of the
                target's message pipe.

        Returns:
            ``False`` if ``to`` is out of range; otherwise ``None``.
        """
        if len(self.worker_process_list) <= to:
            return False

        if to < 0:
            self.broadcast_msg_to_worker(from_id, data)
        else:
            self.worker_process_list[to].send_msg(from_id, data)

    def broadcast_msg_to_worker(self, from_id: int, data: str) -> None:
        """Write ``data`` to every worker except the sender.

        Args:
            from_id: Sending worker id; that worker is skipped.
            data: Payload forwarded through each remaining worker's
                message pipe.
        """
        for worker in self.worker_process_list:
            if worker.id == from_id:
                continue
            worker.send_msg(from_id, data)

    def close(self) -> None:
        """Wait for service deregister.

        Acquires ``registry_semaphore`` once per configured service (10s
        timeout each) so ``Service.close`` can finish registry logout.
        Children are stopped by the worker, not here.
        """
        for _ in range(self.opts.service_num):
            self.registry_semaphore.acquire(True, 10)
