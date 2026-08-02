# -*- coding: utf-8 -*-
"""Test transport util module"""

import asyncio

from typing import Callable

from trpc_sdk.abc.transport import BaseClientTransportOptions
from trpc_sdk.codec import TrpcServerParser
from trpc_sdk.transport.server_conn_info import Parser


class TestChecker(Parser):
    """Test checker"""

    def __init__(self, f: Callable = None):
        self.f = f or TrpcServerParser

    def create(self):
        """Create server parser"""
        return self.f()

    async def read_frame(self, conn) -> bytes:
        """Read frame"""
        raise asyncio.TimeoutError('')


class MockSocket:
    """Mock socket"""

    def fileno(self):
        """File Descriptor"""
        return 1


class TestClientConn:
    """Test client conn"""

    def __init__(self):
        self.buf = b''
        self.metadata = {"socket": MockSocket(), "peername": ("127.0.0.1", 8080), "sockname": ("127.0.0.1", 8080)}
        self._extra = {}
        self.closing = False

    async def read(self, length):
        """Read data"""
        return self.buf

    def write(self, buf):
        """Write data"""
        self.buf = buf

    def sendto(self, buf, addr):
        """Sendto data"""
        self.buf = buf

    def close(self):
        """Close conn"""
        self.closing = True

    def get_extra_info(self, key):
        """Get extra info"""
        return self.metadata[key]

    def is_closing(self):
        """Closing"""
        return self.closing


async def _mock_receive_return():
    """Mock receive_return"""
    return b'hi: hello: ', "test"


class MockTransport:

    def __init__(self):
        self._data = {'sockname': ("192.168.1.1", 8080), 'peername': ("10.0.0.1", 9090)}

    @property
    def transport(self):
        return self

    def get_extra_info(self, key: str):
        return self._data[key]


# pylint: disable=unnecessary-pass
class TestConn:
    """Conn for test"""

    async def read(self, length: int):
        """Read 'length' data"""
        if length == 16:
            return b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '[:16]
        return b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '[16:]

    async def receive(self):
        """Recv data"""
        return await _mock_receive_return()

    def write(self, buf):
        """Write buff"""
        pass

    def close(self, force: bool = False):
        """Close conn"""
        pass

    @property
    def stream_conn(self):
        return MockTransport()


class TestPool:
    """Conn pool for test"""

    async def get(self, trans_opt: BaseClientTransportOptions, timeout: float):
        """Get conn"""
        return TestConn()


class TestPipeSocket:
    """Test PipeSocket"""

    __pipe_buf = b''

    def __init__(self, id):
        self.id = id

    @classmethod
    def get_buf(cls):
        return cls.__pipe_buf

    @classmethod
    def set_buf(cls, buf):
        cls.__pipe_buf = buf

    def sendall(self, buf):
        self.set_buf(buf)

    def recv(self, length):
        return self.get_buf()

    def fileno(self):
        return self.id

    def close(self):
        pass
