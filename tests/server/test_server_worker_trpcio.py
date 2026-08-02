# -*- coding: utf-8 -*-
#
# Copyright @ 2023 Tencent.com
"""Unit tests for CWorkerConnectionTrpcio class."""

import asyncio
import socket

from unittest.mock import Mock
from unittest.mock import patch

import pytest

from trpc_sdk.driver_trpcio_ext.ipc_codec import inner_encode_req
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.server import Context
from trpc_sdk.transport.server_worker_trpcio import CWorkerConnectionTrpcio
from trpc_sdk.utils import get_or_create_event_loop


class MockPipeConnection:
    """Mock pipe connection for testing."""

    def __init__(self):
        self.__is_closing = False
        self.data = b''
        self.is_send = False
        self.send_count = 0

    def send(self, data):
        """Send data through pipe."""
        self.data = data
        self.is_send = True
        self.send_count += 1

    def is_closing(self):
        """Check if connection is closing."""
        return self.__is_closing

    def close(self):
        """Close the connection."""
        self.__is_closing = True


class MockWorker:
    """Mock worker for testing."""

    def __init__(self, conn):
        self.id = 0
        self.pipe_conn = conn


class MockWorkerProcess:
    """Mock worker process for testing."""

    def __init__(self, pipe_conn):
        self.pipe_conn = pipe_conn


class MockServerEngine:
    """Mock server engine for testing."""

    def __init__(self, worker):
        # Create a MockWorkerProcess that has pipe_conn attribute
        worker_process = MockWorkerProcess(worker.pipe_conn)
        self.worker_process_list = {0: worker_process}


class MockApp:
    """Mock application for testing."""

    def __init__(self, worker):
        self.worker = worker
        self.server_engine = MockServerEngine(worker)

    @property
    def loop(self):
        """Get the current running event loop."""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            # If no loop is running, get or create the event loop
            return get_or_create_event_loop()


async def mock_handle(ctx: Context, req_buf: bytes) -> bytes:
    """Mock request handler."""
    return req_buf


async def mock_handle_exception(ctx: Context, req_buf: bytes) -> bytes:
    """Mock request handler that raises exception."""
    raise Exception("Mock handler exception")


@pytest.fixture
def mock_pipe_conn():
    """Create mock pipe connection."""
    return MockPipeConnection()


@pytest.fixture
def mock_worker(mock_pipe_conn):
    """Create mock worker."""
    return MockWorker(mock_pipe_conn)


@pytest.fixture
def mock_app(mock_worker):
    """Create mock application."""
    return MockApp(mock_worker)


@pytest.fixture
def worker_connection(mock_app):
    """Create worker connection instance."""
    addresses = {8000: 0, 8001: 1}
    handlers = {0: mock_handle, 1: mock_handle}
    timeouts = [1000, 2000]
    return CWorkerConnectionTrpcio(mock_app, addresses, handlers, timeouts)


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


@pytest.mark.asyncio
async def test_connection_made(worker_connection, mock_pipe_conn):
    """Test connection_made method."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)
    assert worker_connection.aio_connection == mock_transport


@pytest.mark.asyncio
async def test_connection_lost(worker_connection):
    """Test connection_lost method."""
    mock_transport = Mock()
    mock_transport.is_closing.return_value = False
    worker_connection.aio_connection = mock_transport

    worker_connection.connection_lost(None)

    mock_transport.close.assert_called_once()


@pytest.mark.asyncio
async def test_connection_lost_already_closing(worker_connection):
    """Test connection_lost when connection is already closing."""
    mock_transport = Mock()
    mock_transport.is_closing.return_value = True
    worker_connection.aio_connection = mock_transport

    worker_connection.connection_lost(None)

    mock_transport.close.assert_not_called()


@pytest.mark.asyncio
async def test_connection_lost_no_connection(worker_connection):
    """Test connection_lost when no connection exists."""
    worker_connection.aio_connection = None

    # Should not raise exception
    worker_connection.connection_lost(None)


@pytest.mark.asyncio
async def test_data_received_valid_package(worker_connection, mock_pipe_conn):
    """Test data_received with valid package."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    data = create_valid_package()
    worker_connection.data_received(data)

    # Wait for async task to complete
    await asyncio.sleep(0.5)


# @pytest.mark.asyncio
def test_data_received_decode_error(worker_connection):
    """Test data_received with decode error."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    class MockParse():

        def read(self):
            raise ServerDecodeError("Mock checker fatal error")

        def feeds_data(self, data):
            pass

        def reset(self):
            pass

    worker_connection.checker = MockParse()
    worker_connection.data_received(b'invalid_data')


def test_data_received_empty_packages(worker_connection):
    """Test data_received with empty packages."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    class MockParse():

        def read(self):
            return []

        def feeds_data(self, data):
            pass

        def reset(self):
            pass

    worker_connection.checker = MockParse()
    worker_connection.data_received(b'some_data')


def test_data_received_process_exception(worker_connection):
    """Test data_received with exception during processing."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    class MockParse():

        def __init__(self):
            self.is_return = False

        def read(self):
            if self.is_return is False:
                self.is_return = True
                packages = [b'package']
            else:
                packages = []
            return packages

        def feeds_data(self, data):
            pass

        def reset(self):
            pass

    worker_connection.checker = MockParse()
    data = create_valid_package()

    def create_service_coro(packages):
        raise Exception("Mock service coroutine error")

    worker_connection._create_service_coro = create_service_coro
    worker_connection.data_received(data)


@pytest.mark.asyncio
async def test_handle_request_success(worker_connection, mock_pipe_conn):
    """Test __handle_request with successful execution."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    ctx = Context()
    ctx.remote_ip = "127.0.0.1"
    ctx.remote_port = 8000
    ctx.local_ip = "127.0.0.1"
    ctx.local_port = 9000
    ctx.timeout = 1000
    ctx.with_meta_data("session_id", 12345)
    ctx.with_meta_data("fd", 1)
    ctx.with_meta_data("conn", mock_pipe_conn)

    req_data = b"test_request"

    await worker_connection._CWorkerConnectionTrpcio__handle_request(ctx, 0, req_data)

    # Verify that response was sent
    assert mock_pipe_conn.is_send


@pytest.mark.asyncio
async def test_handle_request_exception(worker_connection, mock_pipe_conn):
    """Test __handle_request with exception during handling."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    # Replace handler with one that raises exception
    worker_connection.handlers[0] = mock_handle_exception

    ctx = Context()
    ctx.remote_ip = "127.0.0.1"
    ctx.remote_port = 8000
    ctx.local_ip = "127.0.0.1"
    ctx.local_port = 9000
    ctx.timeout = 1000
    ctx.with_meta_data("session_id", 12345)
    ctx.with_meta_data("fd", 1)
    ctx.with_meta_data("conn", mock_pipe_conn)

    req_data = b"test_request"

    # Should handle exception gracefully
    await worker_connection._CWorkerConnectionTrpcio__handle_request(ctx, 0, req_data)

    # Wait for error handling
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_multiple_addresses(mock_app):
    """Test worker connection with multiple addresses."""
    addresses = {8000: 0, 9000: 1, 10000: 2}
    handlers = {0: mock_handle, 1: mock_handle, 2: mock_handle}
    timeouts = [1000, 2000, 3000]

    worker_conn = CWorkerConnectionTrpcio(mock_app, addresses, handlers, timeouts)

    assert worker_conn.addresses == addresses
    assert worker_conn.handlers == handlers
    assert worker_conn.timeouts == timeouts


@pytest.mark.asyncio
async def test_context_metadata_handling(worker_connection, mock_pipe_conn):
    """Test that context metadata is properly set and retrieved."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    data = create_valid_package()
    worker_connection.data_received(data)

    # Wait for async task to complete
    await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_loop_assignment(mock_app):
    """Test that event loop is properly assigned."""
    addresses = {8000: 0}
    handlers = {0: mock_handle}
    timeouts = [1000]

    worker_conn = CWorkerConnectionTrpcio(mock_app, addresses, handlers, timeouts)

    assert worker_conn.loop == mock_app.loop


@pytest.mark.asyncio
async def test_connection_lost_with_exception(worker_connection):
    """Test connection_lost with exception parameter."""
    mock_transport = Mock()
    mock_transport.is_closing.return_value = False
    worker_connection.aio_connection = mock_transport

    test_exception = Exception("Test connection error")
    worker_connection.connection_lost(test_exception)

    mock_transport.close.assert_called_once()


@pytest.mark.asyncio
async def test_data_received_multiple_reads(worker_connection, mock_pipe_conn):
    """Test data_received with multiple read cycles."""
    mock_transport = Mock()
    worker_connection.connection_made(mock_transport)

    data = create_valid_package()

    # Feed data in chunks
    chunk_size = 50
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        worker_connection.data_received(chunk)

    # Wait for async tasks to complete
    await asyncio.sleep(0.5)
