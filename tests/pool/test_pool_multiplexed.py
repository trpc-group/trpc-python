# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import asyncio
import socket
import time

from asyncio import Future
from threading import Event
from threading import Thread

import pytest

from tests.common_util import unused_port
from trpc_sdk.client.options import *
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.pool.multiplexed import Connection
from trpc_sdk.pool.multiplexed import Connections
from trpc_sdk.pool.multiplexed import VirtualConnection
from trpc_sdk.pool.multiplexed import default_multiplexed_pool
from trpc_sdk.utils import get_or_create_event_loop


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
                ss, addr = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            print("accept", ss, addr)
            if self.handle:
                self.handle(ss, addr)
            # ss.send('byebye')
            # ss.recv(512)
            self.actives.append(ss)

    def stop(self):
        self._running = False
        self.thread.join(timeout=1)
        for s in self.actives:
            s.close()
        self.sock.close()
        print("after tcp server sock close")


class TestMultiplexed():

    def get_conn(self):
        opts = Options()
        o = with_protocol('trpc')
        o(opts)
        conns = Connections(("127.0.0.1", self.port), default_multiplexed_pool, opts.call_options.parser, opts.codec)
        conn = Connection(("127.0.0.1", self.port), conns, opts.call_options.parser, opts.codec,
                          default_multiplexed_pool.opts)
        vconn = VirtualConnection(1, conn, None)
        return conns, conn, vconn

    def setup_class(self):

        def HandlerRead(sock, addr):
            pass

        self.port = unused_port()
        self.tcpsrv = ThreadTcpServer(self.port, HandlerRead)
        print("start simple tcpserver")

    def teardown_class(self):
        print("stop simple tcpserver")
        self.tcpsrv.stop()
        print("stop simple tcpserver end")

    @pytest.mark.asyncio
    async def test_get_conn(self):
        opts = Options()
        o = with_protocol('trpc')
        o(opts)
        res = await default_multiplexed_pool.get_conn("", ("127.0.0.1", self.port), 1, None, opts.call_options.parser,
                                                      opts.codec, 3.0)
        assert res
        res.write(bytes('hi： ', encoding="utf8"))

        try:
            res.write("hi: ")
        except Exception as e:  # pylint: disable=broad-except
            assert "data argument must be" in str(e)
            assert "not 'str'" in str(e)

        res.close()

    async def send_future(self, f):
        f.done()
        f.set_result((None, None, ClientPoolConnClosedError("err")))

    @pytest.mark.asyncio
    async def test_read(self):
        conns, conn, vconn = self.get_conn()
        future = Future()
        vconn.future = future
        get_or_create_event_loop().create_task(self.send_future(future))
        try:
            await vconn.read()
        except Exception as e:  # pylint: disable=broad-except
            assert str(e) == "err"

    @pytest.mark.asyncio
    async def test_close(self):
        conns, conn, vconn = self.get_conn()
        vconn.future = Future()
        conn.future_dict[1] = vconn
        conns.cs = []
        await conns.get_connection()
        conn.close()

    @pytest.mark.asyncio
    async def test_send_rsp(self):
        conns, conn, vconn = self.get_conn()
        vconn.future = Future()
        conn.future_dict[1] = vconn
        await conn.send_rsp(None, 1, "", None)
        await conn.send_rsp(None, 2, "", None)

    @pytest.mark.asyncio
    async def test_start_conns(self):
        conns, conn, vconn = self.get_conn()
        try:
            conns.cs.append(None)
            await conns.start_connections()
        except Exception as e:  # pylint: disable=broad-except
            assert "'NoneType' object has no attribute 'state'" in str(e)
