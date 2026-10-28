# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Handlers and import-time registrations for ``server.mode: peer``.

Imported by ``server.py`` before ``serve()``, so the fork copies every
registration into worker-0 and the workers it spawns:

* unary ``SayHello`` on the tRPC services
* one server-stream method per tRPC-over-TCP service
* an HTTP ``@route``
* a user-protocol UDP starter (``SO_REUSEPORT``, one socket per worker)
* an unnamed timer (every worker) and one named ``worker:0``

Hooks and the exit callbacks are installed from ``server.py`` rather than
at import, so a checker process can import this module without arming
them. A Task, a dispatcher scheduler and a synchronous handler are also
installed from ``server.py``, and only when the matching environment
variable is set: registering a Task unconditionally would make every
peer start fail before the fork.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from typing import List

from trpc_sdk.abc import TaskInfo
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.http import route
from trpc_sdk.plugin import register_task
from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.runtime import register_handler_thread_hook
from trpc_sdk.runtime import set_signal_handle
from trpc_sdk.server import Method
from trpc_sdk.server import ServiceDesc
from trpc_sdk.server import StreamDesc
from trpc_sdk.server import register_protocol_handle
from trpc_sdk.stream import create_stream_dispatcher
from trpc_sdk.transport import register_dispatcher_worker_func
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import create_new_socket
from trpc_sdk.utils import register_timer_task

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

# Long enough that a 1s ready_timeout fires while this worker is still
# inside on_start, and that "workers bound" reports a wait of at least ~4s.
SLOW_HOOK_SECONDS = 5

TASK_NAME = "echo"
HTTP_PATH = "/peer"
USER_PROTOCOL = "user"
STREAM_METHOD = "StreamSay"


def _on_main() -> int:
    """Return 1 when the caller is this process's main thread.

    Peer runs handlers on the worker's main thread. A checker compares
    this with 1.

    Returns:
        ``1`` on the main thread, ``0`` otherwise.
    """
    return int(threading.get_ident() == threading.main_thread().ident)


def _rpc_text(ctx: TrpcContext) -> str:
    """Describe the worker serving ``ctx``.

    ``index`` comes from :func:`current_handler_thread`. In peer mode that
    is the worker index, and ``count`` is ``worker_num``. ``service`` is
    the listen address's service name.

    Returns:
        ``rpc:<index>:<count>:<pid>:<on_main>:<service>``.
    """
    info = current_handler_thread()
    index = info.index if info is not None else -1
    count = info.count if info is not None else 0
    service = get_message(ctx).callee_service_name
    return f"rpc:{index}:{count}:{os.getpid()}:{_on_main()}:{service}"


class PeerGreeter(rpc.GreeterServicer):
    """Unary greeter whose reply names the worker that ran it.

    ``info:<seconds>`` sleeps first, so a checker can SIGTERM with the
    request still in flight. ``SayHi`` stays the generated stub, which
    raises if called; the checks only use ``SayHello``.
    """

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply with this worker's identity, after an optional sleep.

        Args:
            ctx: Request context. The callee on its message is this listen
                address's service name.
            request: ``msg`` selects the behavior; anything else replies at once.

        Returns:
            ``HelloReply`` whose ``msg`` is :func:`_rpc_text`.
        """
        msg = request.msg or ""
        if msg.startswith("info:"):
            raw = msg[5:]
            if raw:
                await asyncio.sleep(float(raw))
        return pb.HelloReply(msg=_rpc_text(ctx))


async def _say_stream(stream) -> None:
    """Send one frame naming this worker and the stream id, then return.

    Both streams opened on one connection must report the same worker:
    the kernel assigned that connection to one process.

    Args:
        stream: Server stream for this call.
    """
    info = current_handler_thread()
    index = info.index if info is not None else -1
    await stream.send_msg(pb.HelloReply(msg=f"stream:{index}:{stream.stream_id}"))


def register_streams(server) -> None:
    """Register :func:`_say_stream` on every tRPC-over-TCP service.

    One dispatcher per service. Called after :func:`create_server` and
    before :meth:`Server.serve`, so the fork copies the dispatchers.

    Args:
        server: Server whose services were just built from the YAML.
    """
    for service in server.service_adapters.values():
        option = service.opt
        if option.protocol != "trpc" or "tcp" not in option.transport_option.network:
            continue
        name = option.service_name
        desc = ServiceDesc(service_name="/" + name,
                           methods=[],
                           streams=[
                               StreamDesc(name=f"/{name}/{STREAM_METHOD}",
                                          impl_func=_say_stream,
                                          server_stream=None)
                           ],
                           stream_handler=create_stream_dispatcher())
        service.register(desc, False)


@route(HTTP_PATH)
async def _http_rpc(ctx: TrpcContext, request) -> str:
    """HTTP ``@route`` that returns the same text as unary ``SayHello``."""
    del request
    return _rpc_text(ctx)


# Datagram transports created by :func:`_start_user`. The runtime's
# ``worker._user_transports`` is a dict of framework transports and is
# destroyed on shutdown with a no-op ``destroy``. Appending to it raises.
# Each peer worker is its own process, so this list is the sockets that
# process opened. The framework does not close a user protocol's socket.
_HELD: List[asyncio.BaseTransport] = []


class _UserEcho(asyncio.DatagramProtocol):
    """UDP protocol that answers ``user:<index>:<pid>`` to any datagram."""

    def connection_made(self, transport) -> None:
        """Keep the transport used to send the reply.

        Args:
            transport: The endpoint this protocol was bound with.
        """
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        """Reply with this worker's index and pid.

        Args:
            data: Ignored. Any payload gets the same reply.
            addr: Where to send the reply.
        """
        del data
        info = current_handler_thread()
        index = info.index if info is not None else -1
        self.transport.sendto(f"user:{index}:{os.getpid()}".encode(), addr)


def _start_user(worker, index: int) -> None:
    """Bind this worker's ``SO_REUSEPORT`` UDP socket for the user protocol.

    Called once per worker, on that worker's loop, before the loop runs.
    The transport is kept in :data:`_HELD` for the life of the process.
    User protocols are not closed by the framework.

    Args:
        worker: Peer worker serving the address.
        index: Position of the address in ``worker.addresses``.
    """
    sock = create_new_socket(worker.addresses[index], worker.socket_types[index], True, False)
    sock.setblocking(False)
    endpoint = worker.loop.create_datagram_endpoint(lambda: _UserEcho(), sock=sock)
    transport, _protocol = worker.loop.run_until_complete(endpoint)
    _HELD.append(transport)


def _on_start(info) -> None:
    """Print one line when this worker's hook runs, then maybe fail or stall.

    The line is printed before the sleep or the raise, so a checker can
    see the hook start even when startup is about to be aborted.
    ``FAIL_HOOK=<index>`` raises. ``SLOW_HOOK=<index>`` blocks, which
    keeps that worker's ready flag clear.

    Args:
        info: This worker's serving-loop identity. ``index`` is the worker
            index and ``count`` is ``worker_num``.
    """
    print(f"HOOK_START {info.index} {info.count} {os.getpid()} {_on_main()}", flush=True)
    if os.environ.get("FAIL_HOOK") == str(info.index):
        raise RuntimeError("hook failed on purpose")
    if os.environ.get("SLOW_HOOK") == str(info.index):
        time.sleep(SLOW_HOOK_SECONDS)


def _on_stop(info) -> None:
    """Print one line when this worker stops.

    Args:
        info: This worker's serving-loop identity.
    """
    print(f"HOOK_STOP {info.index} {os.getpid()}", flush=True)


def install_hooks() -> None:
    """Register the start and stop prints. Call this before ``Server.serve``."""
    register_handler_thread_hook(on_start=_on_start, on_stop=_on_stop)


def _worker_exit() -> None:
    """Print when this worker runs its ``worker_handle``."""
    print(f"WORKER_EXIT {os.getpid()}", flush=True)


def _master_exit() -> None:
    """Print when worker-0 runs ``master_handle``."""
    print(f"MASTER_EXIT {os.getpid()}", flush=True)


def install_signals() -> None:
    """Register the exit prints. Call this before ``Server.serve``."""
    set_signal_handle(master_handle=_master_exit, worker_handle=_worker_exit)


def _pin_first(_worker_conn_num: int) -> int:
    """Scheduler body. Peer ignores it; the warning is what the check wants.

    Args:
        _worker_conn_num: Unused. Separate mode would pass the worker count.

    Returns:
        Always ``0``.
    """
    return 0


def install_scheduler() -> None:
    """Register a dispatcher scheduler when ``SCHEDULER=1``.

    Call this before ``Server.serve``. Peer warns and does not use it.
    """
    if os.environ.get("SCHEDULER") == "1":
        register_dispatcher_worker_func(_pin_first)


def _task_handler(reqs: list) -> list:
    """Echo the batch. Peer rejects the registration before this can run.

    Args:
        reqs: One batch of request bodies.

    Returns:
        The same list.
    """
    return list(reqs)


def install_task() -> None:
    """Register a Task when ``TASK=1``.

    Call this before ``Server.serve``. Peer raises before the fork, so
    the initializer is never entered. Left unregistered otherwise: a Task
    in the default import would make every start fail.
    """
    if os.environ.get("TASK") != "1":
        return
    register_task(TASK_NAME, TaskInfo(name=TASK_NAME, handler=_task_handler, initializer=lambda: None))


def _sync_hello(ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
    """Synchronous handler. Peer only warns; the check does not call it.

    Args:
        ctx: Unused.
        request: Unused.

    Returns:
        An empty reply. Callers never get here if the framework refuses
        to await a plain function.
    """
    del ctx, request
    return pb.HelloReply(msg="sync")


def install_sync(server) -> None:
    """Register :func:`_sync_hello` when ``SYNC=1``.

    ``Server.register`` warns that the handler is not ``async def``. The
    method is added beside ``SayHello``; it does not replace it.

    Args:
        server: Server that already has the async greeter registered.
    """
    if os.environ.get("SYNC") != "1":
        return
    server.register(
        ServiceDesc(service_name="sync",
                    methods=[
                        Method(name="/sync/SyncHello",
                               req_cls=pb.HelloRequest,
                               rsp_cls=pb.HelloReply,
                               impl_func=_sync_hello)
                    ]))


register_protocol_handle(USER_PROTOCOL, _start_user)


@register_timer_task(type=TimerType.ONCE, interval=0)
class AllPeerWorkers(TimerTask):
    """Fire once on every worker. The missing name is what peer warns about."""

    async def handle(self) -> None:
        """Print this worker's index and pid."""
        info = current_handler_thread()
        index = info.index if info is not None else -1
        print(f"TIMER_ALL {index} {os.getpid()}", flush=True)


@register_timer_task(name="worker:0", type=TimerType.ONCE, interval=0)
class WorkerZeroTimer(TimerTask):
    """Fire once, and only on worker-0."""

    async def handle(self) -> None:
        """Print worker-0's index and pid."""
        info = current_handler_thread()
        index = info.index if info is not None else -1
        print(f"TIMER_NAMED {index} {os.getpid()}", flush=True)
