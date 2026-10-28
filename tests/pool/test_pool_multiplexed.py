# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import socket

from threading import Event
from threading import Thread

import pytest

from tests.common_util import unused_port
from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.config import SocketType
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.transport import ConnPoolOptions
from trpc_sdk.transport import ConnectionPoolManager
from trpc_sdk.transport._pool import ConnectionPool


class ThreadTcpServer(object):

    def __init__(self, port, handle=None):
        self.addr = ('127.0.0.1', port)
        self.handle = handle
        self.thread = Thread(target=self.running, daemon=False)
        self.ready = Event()
        self._running = False
        self.thread.start()
        self.ready.wait(timeout=3)
        self.actives = []

    def running(self):
        self._running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(0.1)
        self.sock.bind(self.addr)
        self.sock.listen()
        self.ready.set()

        while self._running:
            try:
                client, addr = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if self.handle:
                self.handle(client, addr)
            self.actives.append(client)

    def stop(self):
        self._running = False
        self.thread.join(timeout=1)
        for client in self.actives:
            client.close()
        self.sock.close()


class TestMultiplexed:
    """Pooled connections replaced the removed multiplexed pool."""

    def setup_class(self):
        self.port = unused_port()
        self.tcpsrv = ThreadTcpServer(self.port)
        print("start simple tcpserver")

    def teardown_class(self):
        print("stop simple tcpserver")
        self.tcpsrv.stop()

    def _pool(self) -> ConnectionPool:
        trans_opt = BaseClientTransportOption()
        trans_opt.address = ("127.0.0.1", self.port)
        trans_opt.socket_type = SocketType.NET
        options = ConnPoolOptions(max_idle=2, max_active=4)
        return ConnectionPool(dial=ConnectionPoolManager.dial, trans_opt=trans_opt, options=options)

    @pytest.mark.asyncio
    async def test_get_conn(self):
        pool = self._pool()
        conn = await pool.get(3.0)
        assert conn
        conn.write(bytes('hi： ', encoding="utf8"))

        try:
            conn.write("hi: ")
        except Exception as err:  # pylint: disable=broad-except
            assert "data argument must be" in str(err)
            assert "not 'str'" in str(err)
        finally:
            conn.close()
            pool.close()

    @pytest.mark.asyncio
    async def test_read_after_close(self):
        pool = self._pool()
        conn = await pool.get(3.0)
        conn.force_close()
        with pytest.raises(ClientPoolConnClosedError):
            await conn.read(1)
        pool.close()

    @pytest.mark.asyncio
    async def test_close(self):
        pool = self._pool()
        conn = await pool.get(3.0)
        conn.close()
        assert pool.idle_conns_num == 1
        pool.close()

    @pytest.mark.asyncio
    async def test_write_empty_payload(self):
        """An empty payload is still accepted by the pooled stream."""
        pool = self._pool()
        conn = await pool.get(3.0)
        conn.write(b"")
        conn.close()
        pool.close()
