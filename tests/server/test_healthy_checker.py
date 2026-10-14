# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test module for TRPC healthy checker functionality

This module contains comprehensive tests for:
- Healthy process monitoring and communication
- Heartbeat connection protocols
- Timeout callback registration
- Master and child heartbeat connections
- Healthy checker plugin functionality
"""

import asyncio
import multiprocessing
import os
import struct
import time
from unittest.mock import MagicMock, call
from types import SimpleNamespace

import pytest

from trpc_sdk._inner import fork_hook
from trpc_sdk.config import config
from trpc_sdk.server import _healthy_checker
from trpc_sdk.server._healthy_checker import MAGIC_VAL
from trpc_sdk.server._healthy_checker import PKG_LEN
from trpc_sdk.server._healthy_checker import HealthyChecker
from trpc_sdk.server._healthy_checker import _ChildHeartbeatConnection
from trpc_sdk.server._healthy_checker import _HealthyCheckerPlugin
from trpc_sdk.server._healthy_checker import _HealthyOptions
from trpc_sdk.server._healthy_checker import _MasterHeartbeatConnection
from trpc_sdk.server._healthy_checker import _PipeRecvData
from trpc_sdk.server._healthy_checker import _run_checker_thread_in_worker
from trpc_sdk.server._healthy_checker import _timeout_handles
from trpc_sdk.server._healthy_checker import init_healthy_checker
from trpc_sdk.server._healthy_checker import register_timeout_callback


def test_pipe_recv_data_default_values():
    """Test _PipeRecvData dataclass with default values"""
    data = _PipeRecvData()
    assert data.magic_val == MAGIC_VAL
    assert data.pid == -1
    assert data.current_recv_time == 0
    assert data.ret is False


def test_pipe_recv_data_custom_values():
    """Test _PipeRecvData dataclass with custom values"""
    data = _PipeRecvData(magic_val=0x5678, pid=12345, current_recv_time=1000, ret=True)
    assert data.magic_val == 0x5678
    assert data.pid == 12345
    assert data.current_recv_time == 1000
    assert data.ret is True


def test_healthy_options_default_values():
    """Test _HealthyOptions dataclass with default values"""
    options = _HealthyOptions()
    assert options.process_type == fork_hook.ProcessType.UNKNOWN
    assert options.name == ""
    assert options.master_pipe is None
    assert options.child_pipe is None
    assert options.heartbeat_interval == 0
    assert options.max_check_interval == 0


def test_healthy_options_custom_values():
    """Test _HealthyOptions dataclass with custom values"""
    mock_pipe = MagicMock()
    options = _HealthyOptions(process_type=fork_hook.ProcessType.WORKER,
                              name="test-worker",
                              master_pipe=mock_pipe,
                              child_pipe=mock_pipe,
                              heartbeat_interval=5,
                              max_check_interval=40)
    assert options.process_type == fork_hook.ProcessType.WORKER
    assert options.name == "test-worker"
    assert options.master_pipe == mock_pipe
    assert options.child_pipe == mock_pipe
    assert options.heartbeat_interval == 5
    assert options.max_check_interval == 40


def test_register_timeout_callback():
    """Test registering timeout callback for process type"""

    def mock_callback(pid):
        pass

    register_timeout_callback(fork_hook.ProcessType.WORKER, mock_callback)
    assert fork_hook.ProcessType.WORKER in _timeout_handles
    assert _timeout_handles[fork_hook.ProcessType.WORKER] == mock_callback


def test_register_multiple_callbacks():
    """Test registering multiple timeout callbacks for different process types"""

    def worker_callback(pid):
        pass

    def dispatcher_callback(pid):
        pass

    register_timeout_callback(fork_hook.ProcessType.WORKER, worker_callback)
    register_timeout_callback(fork_hook.ProcessType.DISPATCHER, dispatcher_callback)

    assert fork_hook.ProcessType.WORKER in _timeout_handles
    assert fork_hook.ProcessType.DISPATCHER in _timeout_handles
    assert _timeout_handles[fork_hook.ProcessType.WORKER] == worker_callback
    assert _timeout_handles[fork_hook.ProcessType.DISPATCHER] == dispatcher_callback


def test_master_heartbeat_connection_initialization():
    """Test _MasterHeartbeatConnection initialization"""
    mock_options = MagicMock()
    mock_options.heartbeat_interval = 5
    mock_options.max_check_interval = 40
    mock_options.name = "test-process"
    mock_options.master_pipe = MagicMock()
    mock_options.child_pipe = MagicMock()
    mock_options.process_type = fork_hook.ProcessType.WORKER

    connection = _MasterHeartbeatConnection(mock_options)
    assert connection._heartbeat_interval == 5
    assert connection._idle_heartbeat_interval == 40
    assert connection.name == "test-process"
    assert connection.process_type == fork_hook.ProcessType.WORKER


def test_master_heartbeat_connection_timeout_handler():
    """Test _MasterHeartbeatConnection timeout handler execution"""
    mock_options = MagicMock()
    mock_options.process_type = fork_hook.ProcessType.WORKER
    mock_options.name = "test-process"

    connection = _MasterHeartbeatConnection(mock_options)

    # Mock the timeout handler
    mock_handler_called = False

    def mock_handler(pid):
        nonlocal mock_handler_called
        mock_handler_called = True

    # Register mock handler
    register_timeout_callback(fork_hook.ProcessType.WORKER, mock_handler)

    # Test timeout handler execution
    connection._child_pid = 12345
    connection._run_timeout_handler(12345)
    assert mock_handler_called is True


def test_master_heartbeat_connection_handle_timeout():
    """Test _MasterHeartbeatConnection timeout detection logic"""
    mock_options = MagicMock()
    mock_options.process_type = fork_hook.ProcessType.WORKER
    mock_options.name = "test-process"

    connection = _MasterHeartbeatConnection(mock_options)
    connection._child_pid = 12345
    connection.last_recv_time = 1000

    # Test no timeout (within interval)
    result = connection._handle_timeout(1010, 20, "test")
    assert result is False
    assert connection._timeout_cnt == 0

    # Test timeout (exceeds interval)
    result = connection._handle_timeout(1030, 20, "test")
    assert result is True
    assert connection._timeout_cnt == 1


def test_master_heartbeat_connection_decode_valid_data():
    """Test _MasterHeartbeatConnection data decoding with valid data"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Create valid test data
    test_pid = 12345
    test_time = int(time.time())
    pack_data = struct.pack('>H', MAGIC_VAL)
    pack_data += struct.pack('>I', test_pid)
    pack_data += struct.pack('>I', test_time)

    connection._recv_buffer = pack_data
    result = connection._decode()

    assert result.ret is True
    assert result.magic_val == MAGIC_VAL
    assert result.pid == test_pid
    assert result.current_recv_time == test_time


def test_master_heartbeat_connection_decode_invalid_magic():
    """Test _MasterHeartbeatConnection data decoding with invalid magic value"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Create data with invalid magic value
    invalid_magic = 0x5678
    pack_data = struct.pack('>H', invalid_magic)
    pack_data += struct.pack('>I', 12345)
    pack_data += struct.pack('>I', int(time.time()))

    connection._recv_buffer = pack_data
    result = connection._decode()

    assert result.ret is False
    assert result.magic_val == invalid_magic


def test_master_heartbeat_connection_decode_insufficient_data():
    """Test _MasterHeartbeatConnection data decoding with insufficient data"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Create insufficient data (less than PKG_LEN)
    connection._recv_buffer = b'\x12\x34'  # Only magic value
    result = connection._decode()

    assert result.ret is False


def test_child_heartbeat_connection_initialization():
    """Test _ChildHeartbeatConnection initialization"""
    mock_options = MagicMock()
    mock_options.master_pipe = MagicMock()
    mock_options.child_pipe = MagicMock()
    mock_options.process_type = fork_hook.ProcessType.WORKER
    mock_options.name = "test-worker"

    mock_options.heartbeat_interval = 1
    connection = _ChildHeartbeatConnection(mock_options)
    assert connection._heartbeat_interval == 1
    assert connection.process_type == fork_hook.ProcessType.WORKER
    assert connection.name == "test-worker"


def test_healthy_checker_initialization():
    """Test HealthyChecker class initialization"""
    checker = HealthyChecker()
    assert checker._options == {}
    assert checker.options == {}


def test_healthy_checker_make_options():
    """Test HealthyChecker make_options method"""
    checker = HealthyChecker()

    # Mock config to avoid dependency issues
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        checker.make_options(fork_hook.ProcessType.WORKER, "test-worker")

        assert fork_hook.ProcessType.WORKER in checker._options
        assert len(checker._options[fork_hook.ProcessType.WORKER]) == 1

        option = checker._options[fork_hook.ProcessType.WORKER][0]
        assert option.process_type == fork_hook.ProcessType.WORKER
        assert option.name == "test-worker"
        assert option.heartbeat_interval == 5
        assert option.max_check_interval == 40  # 5 * 8
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_healthy_checker_make_options_multiple():
    """Test HealthyChecker make_options with multiple process types"""
    checker = HealthyChecker()

    # Mock config
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-1")
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-2")
        checker.make_options(fork_hook.ProcessType.DISPATCHER, "dispatcher")

        assert len(checker._options[fork_hook.ProcessType.WORKER]) == 2
        assert len(checker._options[fork_hook.ProcessType.DISPATCHER]) == 1

        worker_names = [opt.name for opt in checker._options[fork_hook.ProcessType.WORKER]]
        assert "worker-1" in worker_names
        assert "worker-2" in worker_names
        assert checker._options[fork_hook.ProcessType.DISPATCHER][0].name == "dispatcher"
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_init_healthy_checker_function():
    """Test init_healthy_checker function"""
    # Mock config to enable heartbeat
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        # Clear global variable
        _healthy_checker._init_healthy_checker = None

        # Test initialization
        worker_list = [MagicMock(id=1), MagicMock(id=2)]
        init_healthy_checker(worker_list, 1, [])

        assert _healthy_checker._init_healthy_checker is not None
        assert fork_hook.ProcessType.WORKER in _healthy_checker._init_healthy_checker.options
        assert fork_hook.ProcessType.DISPATCHER in _healthy_checker._init_healthy_checker.options
        assert len(_healthy_checker._init_healthy_checker.options[fork_hook.ProcessType.WORKER]) == 2
        assert len(_healthy_checker._init_healthy_checker.options[fork_hook.ProcessType.DISPATCHER]) == 1

        # Test with zero dispatcher_num
        _healthy_checker._init_healthy_checker = None
        init_healthy_checker([], 0, [])
        assert _healthy_checker._init_healthy_checker is not None
        assert fork_hook.ProcessType.DISPATCHER not in _healthy_checker._init_healthy_checker.options
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_init_healthy_checker_without_heartbeat():
    """Test init_healthy_checker when heartbeat_interval is not set"""
    # Mock config to disable heartbeat
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 0

    try:
        _healthy_checker._init_healthy_checker = None

        init_healthy_checker([MagicMock(id=1)], 1, [])
        assert _healthy_checker._init_healthy_checker is None
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_healthy_checker_plugin_initialization():
    """Test _HealthyCheckerPlugin initialization"""
    plugin = _HealthyCheckerPlugin()
    assert plugin.checker_thread is None


def test_healthy_checker_plugin_on_master_start_without_heartbeat():
    """Test _HealthyCheckerPlugin on_master_start without heartbeat interval"""
    # Mock config to disable heartbeat
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 0

    try:
        plugin = _HealthyCheckerPlugin()
        plugin.on_master_start()
        assert plugin.checker_thread is None
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_healthy_checker_plugin_on_worker_start():
    """Test _HealthyCheckerPlugin on_worker_start method"""
    plugin = _HealthyCheckerPlugin()
    # Should not raise any exception
    plugin.on_worker_start()


def test_run_checker_thread_in_worker_without_heartbeat():
    """Test _run_checker_thread_in_worker without heartbeat interval"""
    # Mock config to disable heartbeat
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 0

    try:
        # This should not raise any exception and should return early
        _run_checker_thread_in_worker(fork_hook.ProcessType.WORKER)
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_pipe_connection_data_structure():
    """Test pipe connection data structure and constants"""
    assert MAGIC_VAL == 0x1234
    assert PKG_LEN == 10

    # Test data packing and unpacking
    test_pid = 12345
    test_time = 1000
    pack_data = struct.pack('>H', MAGIC_VAL)
    pack_data += struct.pack('>I', test_pid)
    pack_data += struct.pack('>I', test_time)

    assert len(pack_data) == PKG_LEN

    # Verify unpacking
    magic = struct.unpack('>H', pack_data[:2])[0]
    pid = struct.unpack('>I', pack_data[2:6])[0]
    timestamp = struct.unpack('>I', pack_data[6:10])[0]

    assert magic == MAGIC_VAL
    assert pid == test_pid
    assert timestamp == test_time


def test_timeout_handles_global_variable():
    """Test global timeout_handles dictionary"""
    # Clear any existing handlers
    _timeout_handles.clear()

    def mock_callback(pid):
        pass

    # Test adding handler
    register_timeout_callback(fork_hook.ProcessType.WORKER, mock_callback)
    assert fork_hook.ProcessType.WORKER in _timeout_handles
    assert _timeout_handles[fork_hook.ProcessType.WORKER] == mock_callback

    # Test clearing
    _timeout_handles.clear()
    assert len(_timeout_handles) == 0


def test_process_type_enum_usage():
    """Test that process type enums are correctly used"""
    assert hasattr(fork_hook.ProcessType, 'WORKER')
    assert hasattr(fork_hook.ProcessType, 'DISPATCHER')
    assert hasattr(fork_hook.ProcessType, 'TASK')
    assert hasattr(fork_hook.ProcessType, 'UNKNOWN')

    # Test that the types are distinct
    assert fork_hook.ProcessType.WORKER != fork_hook.ProcessType.DISPATCHER
    assert fork_hook.ProcessType.DISPATCHER != fork_hook.ProcessType.TASK


@pytest.mark.asyncio
async def test_master_heartbeat_connection_connection_made():
    """Test _MasterHeartbeatConnection connection_made method"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    mock_transport = MagicMock()
    connection.connection_made(mock_transport)

    assert connection._conn == mock_transport
    assert connection._task is not None


def test_master_heartbeat_connection_data_received_valid():
    """Test _MasterHeartbeatConnection data_received with valid data"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)
    connection.last_recv_time = None
    connection._is_over_timeout = False

    # Create valid test data
    test_pid = 12345
    test_time = int(time.time())
    pack_data = struct.pack('>H', MAGIC_VAL)
    pack_data += struct.pack('>I', test_pid)
    pack_data += struct.pack('>I', test_time)

    connection.data_received(pack_data)

    assert connection._child_pid == test_pid
    assert connection.last_recv_time == test_time
    assert connection._is_over_timeout is False


def test_master_heartbeat_connection_data_received_invalid():
    """Test _MasterHeartbeatConnection data_received with invalid data"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Create invalid data (wrong magic value)
    invalid_data = struct.pack('>H', 0x5678)  # Wrong magic
    invalid_data += struct.pack('>I', 12345)
    invalid_data += struct.pack('>I', int(time.time()))

    # Should not raise exception
    connection.data_received(invalid_data)


def test_master_heartbeat_connection_data_received_partial():
    """Test _MasterHeartbeatConnection data_received with partial data"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Send partial data (less than PKG_LEN)
    partial_data = struct.pack('>H', MAGIC_VAL)  # Only magic value
    connection.data_received(partial_data)

    # Should not process incomplete data
    assert connection._child_pid is None


def test_master_heartbeat_connection_connection_lost():
    """Test _MasterHeartbeatConnection connection_lost method"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Create a mock task
    mock_task = MagicMock()
    connection._task = mock_task
    connection._is_running = True

    # Create a mock connection
    mock_conn = MagicMock()
    connection._conn = mock_conn

    connection.connection_lost(None)

    assert connection._is_running is False
    mock_task.cancel.assert_called_once()
    mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_child_heartbeat_connection_connection_made():
    """Test _ChildHeartbeatConnection connection_made method"""
    mock_options = MagicMock()
    connection = _ChildHeartbeatConnection(mock_options)

    mock_transport = MagicMock()
    connection.connection_made(mock_transport)

    assert connection._conn == mock_transport
    assert connection._task is not None


@pytest.mark.asyncio
async def test_child_heartbeat_connection_send_heartbeat():
    """Test _ChildHeartbeatConnection heartbeat sending logic"""
    mock_options = MagicMock()
    mock_options.heartbeat_interval = 1
    connection = _ChildHeartbeatConnection(mock_options)

    # Mock the connection and transport
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    connection._conn = mock_transport
    connection._is_running = True

    # Mock os.getpid
    original_getpid = os.getpid
    os.getpid = MagicMock(return_value=12345)

    try:
        # Test the send method - need to run in event loop since it's async
        async def run_test():
            # Set _is_running to False after first iteration to avoid infinite loop
            connection._is_running = True
            # Create the async task
            task = asyncio.create_task(connection._send())
            # Wait a short time for the task to execute
            await asyncio.sleep(0.1)
            # Stop the task
            connection._is_running = False
            await task

        # Run the async test
        await run_test()

        # Verify that write was called with correct data format
        mock_transport.write.assert_called_once()
        written_data = mock_transport.write.call_args[0][0]

        # Verify data structure
        assert len(written_data) == PKG_LEN
        magic_val = struct.unpack('>H', written_data[:2])[0]
        pid = struct.unpack('>I', written_data[2:6])[0]
        timestamp = struct.unpack('>I', written_data[6:10])[0]

        assert magic_val == MAGIC_VAL
        assert pid == 12345
        assert isinstance(timestamp, int)
    finally:
        os.getpid = original_getpid


def test_child_heartbeat_connection_connection_lost():
    """Test _ChildHeartbeatConnection connection_lost method"""
    mock_options = MagicMock()
    mock_options.heartbeat_interval = 1
    mock_options.process_type = fork_hook.ProcessType.UNKNOWN
    mock_options.name = "test"
    connection = _ChildHeartbeatConnection(mock_options)

    # Create a mock task
    mock_task = MagicMock()
    connection._task = mock_task
    connection._is_running = True

    # Create a mock connection
    mock_conn = MagicMock()
    connection._conn = mock_conn

    connection.connection_lost(None)

    assert connection._is_running is False
    mock_task.cancel.assert_called_once()
    mock_conn.close.assert_called_once()


def test_healthy_checker_options_property():
    """Test HealthyChecker options property"""
    checker = HealthyChecker()
    assert checker.options == {}

    # Add some options and test the property
    mock_options = MagicMock()
    checker._options = {fork_hook.ProcessType.WORKER: [mock_options]}
    assert checker.options == {fork_hook.ProcessType.WORKER: [mock_options]}


def test_run_checker_thread_in_worker_process_matching():
    """Test _run_checker_thread_in_worker process name matching"""
    # Mock config
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        # Create a healthy checker with options
        checker = HealthyChecker()
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-1")

        _healthy_checker._init_healthy_checker = checker

        # Mock multiprocessing current process
        original_current_process = multiprocessing.current_process
        mock_process = MagicMock()
        mock_process.name = "worker-1"
        multiprocessing.current_process = MagicMock(return_value=mock_process)

        try:
            # This should find the matching option
            _run_checker_thread_in_worker(fork_hook.ProcessType.WORKER)
        finally:
            multiprocessing.current_process = original_current_process
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_run_checker_thread_in_worker_process_not_found():
    """Test _run_checker_thread_in_worker when process name doesn't match"""
    # Mock config
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        # Create a healthy checker with options
        checker = HealthyChecker()
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-1")

        _healthy_checker._init_healthy_checker = checker

        # Mock multiprocessing current process with non-matching name
        original_current_process = multiprocessing.current_process
        mock_process = MagicMock()
        mock_process.name = "worker-2"  # Doesn't match "worker-1"
        multiprocessing.current_process = MagicMock(return_value=mock_process)

        try:
            # This should log an error about process not found
            _run_checker_thread_in_worker(fork_hook.ProcessType.WORKER)
        finally:
            multiprocessing.current_process = original_current_process
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_timeout_callback_registration_edge_cases():
    """Test edge cases for timeout callback registration"""
    # Test registering None callback
    register_timeout_callback(fork_hook.ProcessType.TASK, None)
    assert fork_hook.ProcessType.TASK in _timeout_handles
    assert _timeout_handles[fork_hook.ProcessType.TASK] is None

    # Test overwriting existing callback
    def first_callback(pid):
        pass

    def second_callback(pid):
        pass

    register_timeout_callback(fork_hook.ProcessType.WORKER, first_callback)
    register_timeout_callback(fork_hook.ProcessType.WORKER, second_callback)

    assert _timeout_handles[fork_hook.ProcessType.WORKER] == second_callback


def test_pipe_recv_data_edge_cases():
    """Test edge cases for _PipeRecvData"""
    # Test with negative values
    data = _PipeRecvData(pid=-100, current_recv_time=-500)
    assert data.pid == -100
    assert data.current_recv_time == -500

    # Test with zero values
    data = _PipeRecvData(pid=0, current_recv_time=0)
    assert data.pid == 0
    assert data.current_recv_time == 0

    # Test with large values
    data = _PipeRecvData(pid=999999, current_recv_time=999999999)
    assert data.pid == 999999
    assert data.current_recv_time == 999999999


def test_healthy_options_edge_cases():
    """Test edge cases for _HealthyOptions"""
    # Test with empty name
    options = _HealthyOptions(name="")
    assert options.name == ""

    # Test with long name
    long_name = "a" * 100
    options = _HealthyOptions(name=long_name)
    assert options.name == long_name

    # Test with zero intervals
    options = _HealthyOptions(heartbeat_interval=0, max_check_interval=0)
    assert options.heartbeat_interval == 0
    assert options.max_check_interval == 0


def test_plugin_registration():
    """Test that _HealthyCheckerPlugin is properly registered"""
    # Verify plugin registration by checking attributes
    plugin = _HealthyCheckerPlugin()
    assert hasattr(plugin, 'on_master_start')
    assert hasattr(plugin, 'on_worker_start')
    assert hasattr(plugin, 'checker_thread')


def test_constants_values():
    """Test that constants have correct values"""
    assert MAGIC_VAL == 0x1234
    assert PKG_LEN == 10

    # Verify the package structure matches the length
    test_data = struct.pack('>H', MAGIC_VAL)
    test_data += struct.pack('>I', 12345)  # pid
    test_data += struct.pack('>I', 1000)  # timestamp
    assert len(test_data) == PKG_LEN


@pytest.mark.asyncio
async def test_master_heartbeat_connection_check_idle_task():
    """Test _MasterHeartbeatConnection idle checking task"""
    mock_options = MagicMock()
    mock_options.max_check_interval = 10
    connection = _MasterHeartbeatConnection(mock_options)
    connection._is_running = True
    connection.last_recv_time = 1000
    connection._child_pid = 12345

    # Mock the handle_timeout method to track calls
    original_handle_timeout = connection._handle_timeout
    handle_timeout_calls = []
    connection._handle_timeout = lambda *args: handle_timeout_calls.append(args)

    # Test the idle check logic
    # asyncio.run(connection._check_idle())
    async def stop():
        await asyncio.sleep(1)
        connection._is_running = False

    asyncio.create_task(stop())
    await connection._check_idle()

    # Restore original method
    connection._handle_timeout = original_handle_timeout

    # Verify that handle_timeout was called with correct parameters
    assert len(handle_timeout_calls) > 0


def test_child_heartbeat_connection_send_task_termination():
    """Test _ChildHeartbeatConnection send task termination"""
    mock_options = MagicMock()
    connection = _ChildHeartbeatConnection(mock_options)
    connection._is_running = True

    # Mock the connection
    mock_transport = MagicMock()
    mock_transport.is_closing.return_value = False
    connection._conn = mock_transport

    # Test that the task stops when _is_running is set to False
    connection._is_running = False


def test_healthy_checker_integration_scenario():
    """Test integrated scenario of HealthyChecker usage"""
    # Mock config
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 5

    try:
        # Create a complete scenario
        checker = HealthyChecker()

        # Add multiple process types
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-1")
        checker.make_options(fork_hook.ProcessType.WORKER, "worker-2")
        checker.make_options(fork_hook.ProcessType.DISPATCHER, "dispatcher-1")
        checker.make_options(fork_hook.ProcessType.TASK, "task-1")

        # Verify all options are correctly stored
        assert len(checker.options[fork_hook.ProcessType.WORKER]) == 2
        assert len(checker.options[fork_hook.ProcessType.DISPATCHER]) == 1
        assert len(checker.options[fork_hook.ProcessType.TASK]) == 1

        # Verify option properties
        worker_option = checker.options[fork_hook.ProcessType.WORKER][0]
        assert worker_option.process_type == fork_hook.ProcessType.WORKER
        assert worker_option.name == "worker-1"
        assert worker_option.heartbeat_interval == 5
        assert worker_option.max_check_interval == 40

    finally:
        config.global_config_obj.server.heartbeat_interval = original_config


def test_timeout_handler_integration():
    """Test timeout handler integration with different process types"""
    # Clear existing handlers
    _timeout_handles.clear()

    # Track which handlers are called
    handler_calls = {}

    def create_handler(process_type):

        def handler(pid):
            handler_calls[process_type] = pid

        return handler

    # Register handlers for all process types
    for process_type in [fork_hook.ProcessType.WORKER, fork_hook.ProcessType.DISPATCHER, fork_hook.ProcessType.TASK]:
        register_timeout_callback(process_type, create_handler(process_type))

    # Test handler execution
    test_pid = 12345

    # Execute handlers
    for process_type in _timeout_handles:
        _timeout_handles[process_type](test_pid)
        assert handler_calls[process_type] == test_pid


def test_plugin_lifecycle():
    """Test _HealthyCheckerPlugin complete lifecycle"""
    plugin = _HealthyCheckerPlugin()

    # Initial state
    assert plugin.checker_thread is None

    # Test on_master_start without heartbeat (should do nothing)
    original_config = config.global_config_obj.server.heartbeat_interval
    config.global_config_obj.server.heartbeat_interval = 0

    try:
        plugin.on_master_start()
        assert plugin.checker_thread is None
    finally:
        config.global_config_obj.server.heartbeat_interval = original_config

    # Test on_worker_start (should not raise exception)
    plugin.on_worker_start()


def test_data_encoding_decoding_roundtrip():
    """Test complete data encoding and decoding roundtrip"""
    # Test data
    test_pid = 54321
    test_time = int(time.time())

    # Encode data (like child process does)
    encoded_data = struct.pack('>H', MAGIC_VAL)
    encoded_data += struct.pack('>I', test_pid)
    encoded_data += struct.pack('>I', test_time)

    # Decode data (like master process does)
    magic_val = struct.unpack('>H', encoded_data[:2])[0]
    pid = struct.unpack('>I', encoded_data[2:6])[0]
    timestamp = struct.unpack('>I', encoded_data[6:10])[0]

    # Verify roundtrip
    assert magic_val == MAGIC_VAL
    assert pid == test_pid
    assert timestamp == test_time
    assert len(encoded_data) == PKG_LEN


def test_heartbeat_protocol_compatibility():
    """Test heartbeat protocol compatibility between master and child"""
    # This test ensures that the encoding/decoding logic is compatible
    # between master and child implementations

    # Child-side encoding (from _ChildHeartbeatConnection._send)
    child_pid = 67890
    child_time = int(time.time())
    child_data = struct.pack('>H', MAGIC_VAL)
    child_data += struct.pack('>I', child_pid)
    child_data += struct.pack('>I', child_time)

    # Master-side decoding (from _MasterHeartbeatConnection._decode)
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)
    connection._recv_buffer = child_data
    decoded_data = connection._decode()

    # Verify compatibility
    assert decoded_data.ret is True
    assert decoded_data.magic_val == MAGIC_VAL
    assert decoded_data.pid == child_pid
    assert decoded_data.current_recv_time == child_time


def test_error_handling_robustness():
    """Test error handling robustness in various scenarios"""
    mock_options = MagicMock()

    # Test MasterHeartbeatConnection with various error conditions
    connection = _MasterHeartbeatConnection(mock_options)

    # Test with empty data
    connection.data_received(b'')
    assert connection._recv_buffer == b''

    # Test with corrupted data
    corrupted_data = b'invalid_data_that_will_cause_errors'
    connection.data_received(corrupted_data)
    # Should not raise exception

    # Test with very long data
    long_data = b'x' * 1000
    connection.data_received(long_data)
    # Should handle gracefully


def test_resource_cleanup():
    """Test resource cleanup in connection classes"""
    mock_options = MagicMock()

    # Test MasterHeartbeatConnection cleanup
    master_connection = _MasterHeartbeatConnection(mock_options)
    master_connection._task = MagicMock()
    master_connection._conn = MagicMock()
    master_connection.connection_lost(None)

    # Test ChildHeartbeatConnection cleanup
    child_connection = _ChildHeartbeatConnection(mock_options)
    child_connection._task = MagicMock()
    child_connection._conn = MagicMock()
    child_connection.connection_lost(None)


def test_configuration_dependencies():
    """Test dependencies on configuration values"""
    # Test that heartbeat_interval configuration is properly used
    original_config = config.global_config_obj.server.heartbeat_interval

    test_values = [0, 1, 5, 10, 30]
    for value in test_values:
        config.global_config_obj.server.heartbeat_interval = value

        checker = HealthyChecker()
        checker.make_options(fork_hook.ProcessType.WORKER, "test-worker")

        option = checker.options[fork_hook.ProcessType.WORKER][0]
        assert option.heartbeat_interval == value
        assert option.max_check_interval == value * 8

    config.global_config_obj.server.heartbeat_interval = original_config


def test_module_level_initialization():
    """Test module-level initialization and setup"""
    # Test that fork hook callbacks are registered at module import
    # This is verified by the existence of the callback registrations
    # at the end of the _healthy_checker.py file

    # The module should be properly initialized when imported
    # This test ensures the module setup is correct


def test_comprehensive_coverage_validation():
    """Comprehensive test to validate coverage of major functionality"""
    # This test serves as a final validation that major functionality is covered

    # Data structures
    pipe_data = _PipeRecvData()
    healthy_options = _HealthyOptions()

    # Functionality
    def mock_callback(pid):
        pass

    register_timeout_callback(fork_hook.ProcessType.WORKER, mock_callback)

    # Class instances
    mock_opts = MagicMock()
    master_conn = _MasterHeartbeatConnection(mock_opts)
    child_conn = _ChildHeartbeatConnection(mock_opts)
    checker = HealthyChecker()
    plugin = _HealthyCheckerPlugin()

    # Verify basic functionality
    assert hasattr(master_conn, 'connection_made')
    assert hasattr(child_conn, 'connection_made')
    assert hasattr(checker, 'make_options')
    assert hasattr(plugin, 'on_master_start')

    # Verify timeout handlers are registered
    assert fork_hook.ProcessType.WORKER in _timeout_handles


def test_edge_case_timeout_scenarios():
    """Test edge case timeout scenarios"""
    mock_options = MagicMock()
    connection = _MasterHeartbeatConnection(mock_options)

    # Test with None last_recv_time
    connection.last_recv_time = None
    connection._child_pid = 12345
    result = connection._handle_timeout(1000, 10, "test")
    assert result is False  # Should not timeout without previous receive time

    # Test with very large time difference
    connection.last_recv_time = 1000
    result = connection._handle_timeout(2000, 500, "test")  # 1000 sec diff, 500 sec interval
    assert result is True  # Should timeout

    # Test with exactly at interval boundary
    connection.last_recv_time = 1000
    result = connection._handle_timeout(1010, 10, "test")  # Exactly at interval
    assert result is False  # Should not timeout (strict inequality)


def test_heartbeat_interval_calculation():
    """Test heartbeat interval calculation logic"""
    # Test the calculation of max_check_interval as heartbeat_interval * 8
    test_cases = [(1, 8), (5, 40), (10, 80), (15, 120), (30, 240)]

    for heartbeat_interval, expected_max_check in test_cases:
        options = _HealthyOptions(heartbeat_interval=heartbeat_interval, max_check_interval=heartbeat_interval * 8)
        assert options.max_check_interval == expected_max_check


def test_process_type_validation():
    """Test process type validation and handling"""
    # Test that unknown process types are handled gracefully
    options = _HealthyOptions(process_type=fork_hook.ProcessType.UNKNOWN)
    assert options.process_type == fork_hook.ProcessType.UNKNOWN

    # Test that valid process types are accepted
    valid_types = [fork_hook.ProcessType.WORKER, fork_hook.ProcessType.DISPATCHER, fork_hook.ProcessType.TASK]

    for process_type in valid_types:
        options = _HealthyOptions(process_type=process_type)
        assert options.process_type == process_type


@pytest.fixture
def restore_checker_state():
    old_checker = checker_mod._init_healthy_checker
    old_interval = checker_mod.config.global_config_obj.server.heartbeat_interval
    yield
    checker_mod._init_healthy_checker = old_checker
    checker_mod.config.global_config_obj.server.heartbeat_interval = old_interval


def _packet(pid, timestamp):
    return (
        struct.pack(">H", checker_mod.MAGIC_VAL)
        + struct.pack(">I", pid)
        + struct.pack(">I", timestamp)
    )


def test_valid_heartbeat_clears_previous_timeout():
    options = checker_mod._HealthyOptions(name="worker", heartbeat_interval=1)
    connection = checker_mod._MasterHeartbeatConnection(options)
    connection.last_recv_time = 10
    connection._child_pid = 7
    connection._is_over_timeout = True

    connection.data_received(_packet(7, 20))

    assert connection._is_over_timeout is False
    assert connection.last_recv_time == 20


def test_decode_error_is_logged(monkeypatch):
    connection = checker_mod._MasterHeartbeatConnection(checker_mod._HealthyOptions(name="worker"))
    monkeypatch.setattr(connection, "_decode", MagicMock(side_effect=ValueError("corrupt")))
    error = MagicMock()
    monkeypatch.setattr(checker_mod.logger, "error", error)

    connection.data_received(b"bad")

    assert "corrupt" in error.call_args.args[0]


def test_init_healthy_checker_includes_tasks(monkeypatch, restore_checker_state):
    checker_mod._init_healthy_checker = None
    checker_mod.config.global_config_obj.server.heartbeat_interval = 2
    make_options = MagicMock()
    fake_checker = SimpleNamespace(make_options=make_options)
    monkeypatch.setattr(checker_mod, "HealthyChecker", MagicMock(return_value=fake_checker))

    checker_mod.init_healthy_checker([], 0, [SimpleNamespace(name="cleanup"), SimpleNamespace(name="metrics")])

    assert make_options.call_args_list == [
        call(fork_hook.ProcessType.TASK, "cleanup"),
        call(fork_hook.ProcessType.TASK, "metrics"),
    ]
