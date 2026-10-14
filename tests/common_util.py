# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Unittest helper tool"""
import asyncio
import os
import signal
import socket
import threading

from functools import partial
from threading import Thread
from typing import Callable
from typing import Tuple

from trpc_sdk.utils import close_socket
from trpc_sdk.utils import get_async_lib
from trpc_sdk.utils import is_valid_socket
from trpc_sdk.utils import unlink_unix_socket_file


def unused_port() -> int:
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class BaseTestProtocol(asyncio.Protocol):
    """Test protocol"""

    def __init__(self, on_con_lost: asyncio.Future):
        self.transport = None
        self.on_con_lost: asyncio.Future = on_con_lost

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        assert is_valid_socket(sock)

    def connection_lost(self, exc):
        # The socket has been closed
        self.on_con_lost.set_result(True)


async def create_test_connection(handler, test_cb: Callable[[asyncio.BaseEventLoop, socket.socket], None]):
    """Create conn"""
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()
    on_con_lost = loop.create_future()

    # Create a pair of connected sockets
    rsock, wsock = socket.socketpair()

    # Register the socket to wait for data.
    handler = partial(handler, on_con_lost=on_con_lost)
    transport, protocol = await loop.create_connection(handler, sock=rsock)
    await test_cb(loop, wsock)
    try:
        await protocol.on_con_lost
    finally:
        transport.close()
        sock = transport.get_extra_info("socket")
        assert not is_valid_socket(sock)
        close_socket(sock)
        wsock.close()


class ThreadUnixServer:
    """Unix server in thread"""

    def __init__(self, addr: Tuple[str, int], handle: Callable = None) -> None:
        self.addr = addr
        self.handle = handle
        self._unix_srv = None
        self.__thread = Thread(target=self.run, daemon=True)
        self.loop = None
        self.tid = 0
        self.is_start = False

    async def _test_checker(self):
        await asyncio.sleep(0.1)
        self.is_start = True

    def run(self):
        """run"""
        self.loop = get_async_lib().new_event_loop()
        asyncio.set_event_loop(self.loop)
        handle = partial(self.handle, loop=self.loop)
        coros = self.loop.create_unix_server(handle, path=self.addr[0])
        self._unix_srv = self.loop.run_until_complete(coros)
        self.tid = threading.currentThread().ident

        def handle_kill_signal():
            if self._unix_srv is not None:
                print("handle_kill_signal")
                self._unix_srv.close()
                unlink_unix_socket_file(self.addr[0])
                self._unix_srv = None
            self.loop.close()

        try:
            if threading.current_thread() is threading.main_thread():
                self.loop.add_signal_handler(signal.SIGUSR2, handle_kill_signal)
            self.loop.create_task(self._test_checker())
            self.loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            if self._unix_srv is not None:
                self._unix_srv.close()
                # 删除套接字
                unlink_unix_socket_file(self.addr[0])
                self._unix_srv = None

    def start(self):
        """Start server"""
        self.__thread.start()

    def stop(self):
        """Stop server"""
        self._unix_srv.close()
        unlink_unix_socket_file(self.addr[0])
        if self.tid:
            signal.pthread_kill(self.tid, signal.SIGUSR2)
        if self.__thread:
            self.__thread.join()

        os.remove(self.addr[0])
        self.is_start = False
