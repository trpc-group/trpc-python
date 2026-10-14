# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""What the runtime tells a built-in server transport when it listens.

:meth:`~trpc_sdk.abc.BaseServerTransport.listen` takes no arguments and serves
on the calling thread's loop. The built-in transports add an optional
:class:`ListenOption` so the runtime can hand them what depends on the process
and thread model: which loops serve, ``SO_REUSEPORT``, the shared handler pool.

:class:`ListenOption` and :class:`~trpc_sdk.abc.BaseServerTransportOption` are
filled at different times. ``bind`` stores the transport option before fork
and it records the endpoint. ``ListenOption`` is built after fork, once per
``listen`` call.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional
from typing import Sequence


@dataclass
class ListenOption:
    """Where and how one ``listen`` call of a built-in transport serves.

    Built by the runtime after fork, once per endpoint and serving process.
    ``bind`` has already stored
    :class:`~trpc_sdk.abc.BaseServerTransportOption` on the transport; this
    object is only the argument of this call.

    Attributes:
        loop: The calling thread's loop. Not running yet, so the transport
            drives it with ``run_until_complete``. An acceptor watches the
            listen socket on it.
        serving_loops: Loops that serve the accepted connections or bind the
            datagram sockets, each with ``index``, ``loop``,
            ``submit(func, timeout)`` and ``attach_socket(sock, factory,
            ssl_context)``. Empty means serve on ``loop`` itself.
        reuse_port: Set ``SO_REUSEPORT`` on every socket this call creates.
        shared_pool: ``SharedHandlerPool`` of ``io_handle_type: separate``;
            the connections then run handlers on handle threads.
        protocol_factory: Replaces the transport's own connection class, for
            a runtime that serves the bytes differently (the separate-mode I/O
            process forwards frames to rpc tasks).
        http_runner_factory: Coroutine factory returning a started aiohttp
            ``AppRunner``; awaited on the loop that will serve the runner.
        schedule_target: ``SchedulerParams.target`` an acceptor passes to the
            connection selector.
        ssl_context: Server ``SSLContext``; ``None`` lets the transport build
            one from its own options.
        ready_timeout: Seconds a ``submit`` onto a serving loop may take;
            ``None`` waits forever.
        worker: The serving process. Its ``loop`` is the loop a user protocol
            starter serves on.
        index: Position of the endpoint in the worker's tables.
    """

    loop: asyncio.AbstractEventLoop
    serving_loops: Sequence[Any] = ()
    reuse_port: bool = False
    shared_pool: Optional[Any] = None
    protocol_factory: Optional[Callable[[], asyncio.BaseProtocol]] = None
    http_runner_factory: Optional[Callable[[], Awaitable[Any]]] = None
    schedule_target: str = ""
    ssl_context: Optional[Any] = None
    ready_timeout: Optional[float] = None
    worker: Any = None
    index: int = 0
