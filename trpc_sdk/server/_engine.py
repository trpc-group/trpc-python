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

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseTrpcServerEngine
from trpc_sdk.abc import ProcessType
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

    def set_process_info(self, process_type: ProcessType) -> None:
        """Satisfy :class:`BaseTrpcServerEngine`; merge workers ignore this.

        The master / dispatcher / executor topology that used to stamp a
        process role here is gone. The method stays so the ABC can be
        instantiated; nothing reads the argument.

        Args:
            process_type: Role of the current process.
        """
        del process_type

    @property
    def on_worker_message(self) -> Optional[Callable]:
        """Callback invoked when a worker receives an inter-worker message.

        Copied from ``ServerOption`` so child processes can read it after
        fork without holding the option object themselves.
        """
        if not self.opts:
            return None
        return self.opts.on_worker_message

    def add_listen_endpoint(self, opts: BaseServerTransportOption, network: str) -> None:
        """Append one listen endpoint the worker will bind itself.

        Called from ``transport.listen_and_serve`` during ``Service.run``.
        Nothing is bound here; the nine ``process_info`` lists stay
        index-aligned so the worker can copy them later. TCP/HTTP and UDP
        binds are both accepted.

        Args:
            opts: Transport options of this endpoint. Every field the
                ``process_info`` tables need is read off this object, and the
                object itself is stored as the endpoint's transport options.
            network: Network protocol (tcp/http/udp). Passed separately
                because it is not always ``opts.network``: an HTTP service is
                configured as ``network: tcp`` plus ``protocol: http``, and
                the worker picks its front end by this value, so the HTTP
                transport has to register the endpoint as ``http``.
        """
        address = (opts.address, opts.port)

        # check socket info valid
        if is_unix_socket(opts.socket_type):
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
        self.process_info.add_socket_type(opts.socket_type)
        self.process_info.add_handler(opts.handle)
        self.process_info.add_checker(opts.checker)
        self.process_info.add_timeout(opts.idle_timeout)
        self.process_info.add_network(network)
        self.process_info.add_service(opts.service)
        self.process_info.add_transport_opt(opts)
        self.process_info.add_target(opts.target)

    def send_msg_to_worker(self, from_id: int, to: int, data: str) -> Optional[bool]:
        """Send an inter-worker message, or broadcast when ``to`` is negative.

        Only available when this process actually forked workers, which means
        ``worker_process_list`` is filled: the message pipes it holds are the
        transport. ``merge`` and ``separate`` leave the list empty, so there is
        no peer to address and the call is dropped with a warning.

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
            ``False`` if this process has no worker children or ``to`` is out
            of range; otherwise ``None``.
        """
        if not self.worker_process_list:
            logger.warning("[server engine] drop worker msg from %s to %s: mode %s forks no worker process", from_id,
                           to, self.opts.mode if self.opts else "")
            return False

        if len(self.worker_process_list) <= to:
            return False

        if to < 0:
            self.broadcast_msg_to_worker(from_id, data)
        else:
            self.worker_process_list[to].send_msg(from_id, data)

    def broadcast_msg_to_worker(self, from_id: int, data: str) -> None:
        """Write ``data`` to every worker except the sender.

        Like :meth:`send_msg_to_worker`, this needs a mode that forked workers;
        an empty ``worker_process_list`` is reported rather than silently
        iterating over nothing.

        Args:
            from_id: Sending worker id; that worker is skipped.
            data: Payload forwarded through each remaining worker's
                message pipe.
        """
        if not self.worker_process_list:
            logger.warning("[server engine] drop worker broadcast from %s: mode %s forks no worker process", from_id,
                           self.opts.mode if self.opts else "")
            return

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
