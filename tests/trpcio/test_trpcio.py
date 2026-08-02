# -*- coding: utf-8 -*-

import asyncio
import socket
import time

import pytest

from trpc_sdk.codec import new_message
from trpc_sdk.driver_trpcio_ext.ipc_codec import inner_encode_req
from trpc_sdk.server import Context
from trpc_sdk.transport.server_worker_trpcio import CWorkerConnectionTrpcio
from trpc_sdk.utils import get_or_create_event_loop


def create_valid_package():
    """Create a valid test package."""
    # Create a properly encoded IPC request package
    # fd=1, server_port=8000 (matches the addresses in worker_connection fixture),
    # session_id=12345, remote_ip=127.0.0.1, data=test_request
    # Note: server_port must match a key in addresses dict {8000: 0, 8001: 1}
    return inner_encode_req(
        client_conn_id=1,
        client_session_id=12345,
        client_port=9000,  # Client port (arbitrary)
        server_port=8000,  # Server port must match addresses dict key
        client_ip_binary=b'\x7f\x00\x00\x01',  # 127.0.0.1 in binary (4 bytes for IPv4)
        client_domain=socket.AddressFamily.AF_INET,
        server_ip_binary=b'\x7f\x00\x00\x01',  # 127.0.0.1 in binary (4 bytes for IPv4)
        server_domain=socket.AddressFamily.AF_INET,
        data=b'test_request_data')


class MockPipeConnection:

    def __init__(self) -> None:
        self.__is_closing = True
        self.data = b''
        self.is_send = False

    def send(self, data):
        self.data = data
        self.is_send = True

    def is_closing(self):
        return self.__is_closing

    def close(self):
        pass


class MockWorker:

    def __init__(self, conn) -> None:
        self.id = 0
        self.pipe_conn = conn


class MockServerEngine:

    def __init__(self, worker) -> None:
        self.worker_process_list = {0: worker}


class TestApp:

    def __init__(self, worker) -> None:
        self.worker = worker
        self.server_engine = MockServerEngine(worker)
        self.loop = get_or_create_event_loop()


async def mock_handle(ctx: Context, req_buf: bytes) -> bytes:
    return req_buf


@pytest.mark.asyncio
async def test_trpcio():
    conn = MockPipeConnection()
    worker = MockWorker(conn)
    app = TestApp(worker)
    addresses = {8000: 0}
    handlers = {0: mock_handle}
    timeouts = [1000, 1000]
    c_worker_conn = CWorkerConnectionTrpcio(app, addresses, handlers, timeouts)
    c_worker_conn.connection_made(conn)
    c_worker_conn.connection_lost(Exception())

    ctx = Context()
    ctx, msg = new_message(ctx)

    msg.timeout = 1000
    ctx.timeout = 1000
    ctx.recv_time = time.time() * 1000
    data = create_valid_package()

    c_worker_conn.data_received(data)
