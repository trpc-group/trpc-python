# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Handlers and import-time registrations for ``server.mode: separate``.

Imported by ``server.py`` before ``serve()``, so the fork copies every
registration into the I/O process and its children:

* unary ``SayHello`` on the tRPC services
* one server-stream method per tRPC-over-TCP service
* an HTTP ``@route``
* a user-protocol UDP starter (``SO_REUSEPORT``, one socket per rpc task)
* one Task process, whose initializer is what ``server.io_wait_count: 1`` waits for
* an unnamed timer (every rpc task) and one named ``worker:0``

The I/O process does not run handlers, so it prints none of ``HOOK_START``.
Hooks, the SIGTERM callbacks and the optional pin scheduler are installed
from ``server.py`` rather than at import, so a checker process can import
this module without arming them.
"""

from __future__ import annotations

import asyncio
import os
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
from trpc_sdk.server import ServiceDesc
from trpc_sdk.server import StreamDesc
from trpc_sdk.server import register_protocol_handle
from trpc_sdk.stream import create_stream_dispatcher
from trpc_sdk.task import send_to_task
from trpc_sdk.transport import register_dispatcher_worker_func
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import create_new_socket
from trpc_sdk.utils import inc_io_wait_count
from trpc_sdk.utils import register_timer_task

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

# Long enough that a ready_timeout of 1s kills the process (after the
# child-stop grace) before this returns, and that a checker can see TCP
# refuse while it is still running.
SLOW_HOOK_SECONDS = 8
SLOW_TASK_SECONDS = 4

TASK_NAME = "echo"
HTTP_PATH = "/separate"
USER_PROTOCOL = "user"
STREAM_METHOD = "StreamSay"


def _rpc_text(ctx: TrpcContext) -> str:
    """Describe the rpc task serving ``ctx``.

    ``index`` comes from :func:`current_handler_thread`, which each rpc task
    sets to its own position before it serves. ``service`` is the listen
    address's service name: the framework writes that onto the message, not
    the callee from the request head.

    Returns:
        ``rpc:<index>:<count>:<pid>:<service>``.
    """
    info = current_handler_thread()
    index = info.index if info is not None else -1
    count = info.count if info is not None else 0
    service = get_message(ctx).callee_service_name
    return f"rpc:{index}:{count}:{os.getpid()}:{service}"


class SeparateGreeter(rpc.GreeterServicer):
    """Unary greeter whose reply names the rpc task that ran it.

    ``info:<seconds>`` sleeps first, so a checker can SIGTERM with the
    request still in flight. ``task:<text>`` is forwarded to the Task
    process and the reply carries that process's pid.
    """

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply with this rpc task's identity, after an optional sleep or task call.

        Args:
            ctx: Request context. The callee on its message is this listen
                address's service name.
            request: ``msg`` selects the behavior; anything else replies at once.

        Returns:
            ``HelloReply`` whose ``msg`` is :func:`_rpc_text`, or
            ``task:<text>:<task_pid>`` when ``msg`` starts with ``task:``.
        """
        msg = request.msg or ""
        if msg.startswith("info:"):
            raw = msg[5:]
            if raw:
                await asyncio.sleep(float(raw))
        elif msg.startswith("task:"):
            body = await send_to_task(ctx, TASK_NAME, msg[5:])
            return pb.HelloReply(msg=f"task:{body}")
        return pb.HelloReply(msg=_rpc_text(ctx))


async def _say_stream(stream) -> None:
    """Send one frame naming this rpc task and the stream id, then return.

    The handler starts when the INIT frame is accepted, so the client does
    not have to send a DATA frame first. The id is the one on the frame,
    which is what the I/O process hashed with the session id.

    Args:
        stream: Server stream for this call.
    """
    info = current_handler_thread()
    index = info.index if info is not None else -1
    await stream.send_msg(pb.HelloReply(msg=f"stream:{index}:{stream.stream_id}"))


def register_streams(server) -> None:
    """Register :func:`_say_stream` on every tRPC-over-TCP service.

    One dispatcher per service: a dispatcher keeps the service option it was
    initialized with, and two services must not share that option. Called
    after :func:`create_server`, before :meth:`Server.serve`, so the fork
    copies the registered dispatchers.

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
# ``worker._user_transports`` is a dict of framework transports on a merge
# worker and is destroyed on shutdown with a no-op ``destroy``. Appending
# to it raises. Each rpc task is its own process, so this list is the
# sockets that process opened. The framework does not close a user
# protocol's socket.
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
        """Reply with this rpc task's index and pid.

        Args:
            data: Ignored. Any payload gets the same reply.
            addr: Where to send the reply.
        """
        del data
        info = current_handler_thread()
        index = info.index if info is not None else -1
        self.transport.sendto(f"user:{index}:{os.getpid()}".encode(), addr)


def _start_user(worker, index: int) -> None:
    """Bind this rpc task's ``SO_REUSEPORT`` UDP socket for the user protocol.

    Called once per rpc task, on that task's loop, before the loop runs.
    The transport is kept in :data:`_HELD` for the life of the process.
    User protocols are not closed by the framework on exit.

    Args:
        worker: Rpc task serving the address.
        index: Position of the address in ``worker.addresses``.
    """
    sock = create_new_socket(worker.addresses[index], worker.socket_types[index], True, False)
    sock.setblocking(False)
    endpoint = worker.loop.create_datagram_endpoint(lambda: _UserEcho(), sock=sock)
    transport, _protocol = worker.loop.run_until_complete(endpoint)
    _HELD.append(transport)


def _on_start(info) -> None:
    """Print one line when an rpc task is ready to serve, or fail/stall startup.

    ``FAIL_HOOK=1`` raises, which aborts this child with status 1.
    ``SLOW_HOOK=1`` blocks in the hook so the ready flag stays clear and the
    I/O process has not bound TCP yet. The sleep is longer than a 1s
    ``ready_timeout`` plus the time the parent spends stopping children.

    Args:
        info: This rpc task's serving-loop identity.
    """
    if os.environ.get("FAIL_HOOK") == "1":
        raise RuntimeError("FAIL_HOOK")
    if os.environ.get("SLOW_HOOK") == "1":
        time.sleep(SLOW_HOOK_SECONDS)
    print(f"HOOK_START {info.index} {info.count} {os.getpid()}", flush=True)


def _on_stop(info) -> None:
    """Print one line when an rpc task stops.

    Args:
        info: This rpc task's serving-loop identity.
    """
    print(f"HOOK_STOP {info.index} {os.getpid()}", flush=True)


def install_hooks() -> None:
    """Register the start and stop prints. Call this before ``Server.serve``."""
    register_handler_thread_hook(on_start=_on_start, on_stop=_on_stop)


def _worker_exit() -> None:
    """Print when an rpc task runs its SIGTERM worker callback."""
    print(f"WORKER_EXIT {os.getpid()}", flush=True)


def _master_exit() -> None:
    """Print when the I/O process runs its server exit callback."""
    print(f"MASTER_EXIT {os.getpid()}", flush=True)


def install_signals() -> None:
    """Register the exit prints. Call this before ``Server.serve``."""
    set_signal_handle(master_handle=_master_exit, worker_handle=_worker_exit)


def _pin_first(_worker_conn_num: int) -> int:
    """Send every unary frame to rpc task 0.

    Runs on the I/O process's event loop. Stream frames do not call it.

    Args:
        _worker_conn_num: How many rpc tasks are alive. Unused; the pin is fixed.

    Returns:
        Always ``0``.
    """
    return 0


def install_scheduler() -> None:
    """Pin unary frames to rpc task 0 when ``SCHEDULER=pin0``.

    Call this before ``Server.serve``. The I/O process is the one that
    forks, so the hook is installed in the process that will run it.
    """
    if os.environ.get("SCHEDULER") == "pin0":
        register_dispatcher_worker_func(_pin_first)


def _task_initializer() -> None:
    """Stall on ``SLOW_TASK=1``, then let the I/O process start listening.

    ``inc_io_wait_count`` is what ``server.io_wait_count: 1`` waits for, so
    TCP stays closed until this returns. Printed before the increment so a
    checker can treat the line as "the wait is about to end".
    """
    if os.environ.get("SLOW_TASK") == "1":
        time.sleep(SLOW_TASK_SECONDS)
    print(f"TASK_READY {os.getpid()}", flush=True)
    inc_io_wait_count()


def _task_handler(reqs: list) -> list:
    """Echo each item with this Task process's pid appended.

    Args:
        reqs: One batch of request bodies.

    Returns:
        ``<item>:<pid>`` for each item, same length as ``reqs``.
    """
    pid = os.getpid()
    return [f"{item}:{pid}" for item in reqs]


register_task(TASK_NAME, TaskInfo(name=TASK_NAME, handler=_task_handler, initializer=_task_initializer))
register_protocol_handle(USER_PROTOCOL, _start_user)


@register_timer_task(type=TimerType.ONCE, interval=0)
class AllRpcTasks(TimerTask):
    """Fire once on every rpc task. The missing name is what the I/O process warns about."""

    async def handle(self) -> None:
        """Print this rpc task's index and pid."""
        info = current_handler_thread()
        index = info.index if info is not None else -1
        print(f"TIMER_ALL {index} {os.getpid()}", flush=True)


@register_timer_task(name="worker:0", type=TimerType.ONCE, interval=0)
class NamedRpcTask(TimerTask):
    """Fire once, and only on rpc task 0 (``worker:0``)."""

    async def handle(self) -> None:
        """Print rpc task 0's index and pid."""
        info = current_handler_thread()
        index = info.index if info is not None else -1
        print(f"TIMER_NAMED {index} {os.getpid()}", flush=True)
