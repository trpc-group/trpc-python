# -*- coding: utf-8 -*-
"""Test module for TRPC server functionality

This module contains tests for:
- Server signal handling
- Service management
- Server engine operations
- Various server configurations and edge cases
"""
import asyncio
import threading
import time
from unittest import mock
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from aiohttp import web

import trpc_sdk.server.server_engine as engine_mod
from trpc_sdk.trpc import new as create_new_server
from trpc_sdk.codec import new_message
from trpc_sdk.config import config
from trpc_sdk.filter import get_server_filter
from trpc_sdk.plugin import setup, plugin
from trpc_sdk.server import (
    Context,
    Server,
    ServerOption,
    Service,
    ServiceOption,
    server_engine,
)
from trpc_sdk.server.signal_handle import run_server_signal_handle, run_worker_signal_handle, set_signal_handle
from trpc_sdk.utils.loop_lib import asynclib
from ..examples.helloworld.greeter import GreeterServicer
from ..examples.helloworld.features import filter as _
from ..examples.helloworld.stub.trpc_trpc_python_group_helloworld.rpc import register_GreeterServicer_server
from trpc_sdk.utils import get_or_create_event_loop

config.load_global_config("./examples/helloworld/trpc_python.yaml", 'utf-8')
setup()


def empty_sleep_func():
    """Empty function that sleeps for 2 seconds"""
    time.sleep(2)


_test_server = Server(ServerOption())
_test_server.wait = empty_sleep_func

# admin服务
for service_conf in config.global_config_obj.server.service:
    service_option = ServiceOption()

    service_option.set_address((service_conf.ip, service_conf.port))
    service_option.set_timeout(float(service_conf.timeout) / 1000.0)
    service_option.set_network(service_conf.network)
    service_option.set_protocol(service_conf.protocol)

    for filer_name in config.global_config_obj.server.filter:
        service_option.add_filter(get_server_filter(filer_name))

    for filer_name in service_conf.filter:
        service_option.add_filter(get_server_filter(filer_name))

    service = Service(service_option=service_option)
    service.opt.set_handler(service.handle)
    service.opt.set_service_name(service_conf.name)
    _test_server.add_service(service_conf.name, service)

register_GreeterServicer_server(_test_server, GreeterServicer())


@pytest.mark.asyncio
async def test_handle():
    """Test the server request handling functionality
    
    Tests various scenarios including:
    - Normal request handling
    - Function not found case
    - HTTP callback handling
    - Different timeout scenarios
    """
    ctx = Context()
    ctx, msg = new_message(ctx)

    msg.timeout = 1000
    ctx.timeout = 1000
    ctx.recv_time = time.time() * 1000

    data = b'\t0\x00\x00\x00\x00\x00g\x00P\x00\x00\x00\x00\x00\x00\x18\x01*\x17trpc_sdk.app.server.service:3/' \
           b'trpc.trpc_python_group.helloworld.Greeter/SayHello\n\x05hello'
    await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").handle(ctx, data)

    data = b'\t0\x00\x00\x00\x00\x00i\x00R\x00\x00\x00\x00\x00\x00\x18\x01 \xe8\x07*\x17trpc_sdk.app.server.service:' \
           b'2trpc.trpc_python_group.helloworld.Greeter/SayHello\n\x05hello'
    ret = await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").handle(ctx, data)
    assert ret == b'\t0\x00\x00\x00\x00\x00$\x00\x14\x00\x00\x00\x00\x00\x00\x18\x01 \x0c2\x0efunc not exist'

    ctx.meta_data["request"] = None
    try:

        async def http_cb(ctx, request):
            return web.Response()

        ret = await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").http_handle(ctx, http_cb)
    except:  # pylint: disable=bare-except
        assert False

    try:
        msg.timeout = 1000
        ctx.timeout = 1
        await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").handle(ctx, data)
    except:  # pylint: disable=bare-except
        assert False
    try:
        msg.timeout = 1000
        ctx.timeout = -1
        await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").handle(ctx, data)
    except:  # pylint: disable=bare-except
        assert False

    try:
        data = b'\t0\x00\x00\x00\x00\x00g\x00P\x00\x00\x00\x00\x00\x00\x18\x01*\x17trpc_sdk.app' \
               b'.server.service:3/trpc.trpc_python_group.helloworld.Greeter/SayHello\n\x05hello'
        msg.timeout = 1000
        ctx.timeout = 1000
        ctx.recv_time = (time.time() - 1) * 1000  # ms
        await _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter").handle(ctx, data)
    except:  # pylint: disable=bare-except
        assert False


@pytest.mark.asyncio
async def test_avalanche():
    """Test avalanche protection functionality
    
    Verifies that timeout checking works correctly 
    when server is in dispatcher mode
    """
    config.global_config_obj.server.check_timeout = True
    config.global_config_obj.server.mode = "dispatcher"
    ctx = Context()
    ctx, msg = new_message(ctx)

    data = b'\t0\x00\x00\x00\x00\x00i\x00R\x00\x00\x00\x00\x00\x00\x18\x01 \xe8\x07*\x17trpc_sdk.app.server.service:' \
           b'2trpc.trpc_python_group.helloworld.Greeter/SayHello\n\x05hello'
    msg.timeout = 1000
    ctx.timeout = 1
    ctx.recv_time = (time.time() - 1) * 1000  # ms
    rpc_service = _test_server.get_service("trpc.trpc_python_group.helloworld.Greeter")
    ret = await rpc_service.handle(ctx, data)
    assert ret == b'\t0\x00\x00\x00\x00\x00%\x00\x15\x00\x00\x00\x00\x00\x00\x18\x01 \x152\x0fpackage timeout'


def test_get_service():
    """Test getting service from server
    
    Tests:
        1. Getting non-existent service returns None
        2. Adding and getting a service with None option
    """
    ret = _test_server.get_service("trpc_sdk.test.helloworld.Greeter_no")
    assert ret is None

    _test_server.add_service("test", Service(None))
    assert _test_server.get_service("test").opt is None


class FakeService(Service):
    """Mock service class for testing basic service functionality"""

    def run(self):
        """Run the fake service (prints confirmation message)
        
        This is a simple mock implementation that just prints
        a confirmation message when the service runs.
        """
        print("service run ok")


async def mock_stop():
    """Helper function to stop event loop after 1 second
    
    This is used in tests to gracefully stop the event loop
    after a short delay.
    """
    await asyncio.sleep(1)
    loop = get_or_create_event_loop()
    loop.stop()


def test_add_service():
    """Test adding a service to the server
    
    Tests:
    - Adding a fake service to the server
    - Starting the server in a separate thread
    - Graceful shutdown of the test server
    """
    _test_server.services = {}
    _test_server.add_service("test", FakeService(None))

    async def _exit_loop(loop):
        await asyncio.sleep(1)
        loop.stop()

    def _thread_entry():
        loop = asynclib.new_event_loop()
        _test_server.opt.loop = loop
        asyncio.set_event_loop(loop)
        loop.create_task(_exit_loop(loop))
        _test_server.serve()
        _test_server.opt.loop = None

    with mock.patch('multiprocessing.Process.start') as m:
        m.return_value = None
        test_thread = threading.Thread(target=_thread_entry, daemon=True)
        test_thread.start()
        test_thread.join()


class TestProcess():
    """Mock process class for testing process management"""

    def is_alive(self):
        """Always returns False to simulate dead process
        
        Returns:
            bool: Always returns False
        """
        return False


class MockInstance():
    """Mock instance class containing a test process"""

    def __init__(self):
        """Initialize with a TestProcess instance"""
        self.process_instance = TestProcess()


def restart_executor_process(_):
    """Mock function for restarting executor process
    
    Args:
        process_info: Information about the process to restart
        
    Returns:
        MockInstance: A mock process instance
    """
    return MockInstance()


def test_engine():
    """Test server engine functionality
    
    Tests:
    - Engine initialization 
    - Process setup for different types (dispatcher, worker, executor)
    - Process management operations
    - Error handling during process startup
    """

    engine = server_engine.TrpcServerEngine(ServerOption())

    plugin.TaskHandlers.append(plugin.Task(None, None, use_c_task=False))

    engine.init_processes()
    engine.set_dispatcher_process(('./.test_dispatcher_unix', 0), config.SocketType.UNIX, None, 10, 'tcp')
    engine.set_worker_process(None, None, None)
    engine.set_executor_process(('127.0.0.1', 12345), config.SocketType.NET, None, None, 10, 'tcp', None, None)
    engine.set_executor_process(('127.0.0.1', 13579), config.SocketType.NET, None, None, 10, 'udp', None, None)
    engine.set_executor_process(('./.test_dispatcher_unix', 0), config.SocketType.UNIX, None, None, 10, 'tcp', None,
                                None)
    try:
        engine.start_processes()
    except Exception:  # pylint: disable=broad-except
        assert False

    engine.restart_child_process = restart_executor_process
    engine.sub_dispatcher_process[100] = MockInstance()
    engine.sub_worker_processes[100] = MockInstance()
    engine.sub_worker_task_processes[100] = MockInstance()

    try:
        engine.restart_child_process(100)
        engine.close()
    except Exception:  # pylint: disable=broad-except
        assert False


def empty_func():
    """Empty function that does nothing"""
    return


def empty_child_pid_100000():
    """Mock function that returns fixed PID 100000"""
    return 100000


def empty_child_pid_0():
    """Mock function that returns fixed PID 0"""
    return 0


async def empty_async_func():
    """Empty async function that does nothing"""
    return


def test_empty_server():
    """Test server with minimal/empty configuration
    
    Tests:
    - Server initialization with empty config
    - Service registration
    - Server lifecycle operations
    - Child process exit handling scenarios
    - Reboot behavior with different child PIDs
    """
    create_new_server("./tests/test_config_data/trpc_python_unix.yaml")

    empty_server = create_new_server("./tests/test_config_data/test_server_empty.yaml")
    register_GreeterServicer_server(empty_server, GreeterServicer())
    loop = get_or_create_event_loop()
    loop.create_task(mock_stop())
    empty_server.server_engine.init_processes = empty_func
    empty_server.server_engine.start_processes = empty_func
    empty_server.wait = empty_sleep_func
    empty_server.kill = empty_func
    with mock.patch('multiprocessing.Process.start') as m:
        m.return_value = None
        empty_server.serve()

    empty_server.get_service("Greeter")
    empty_server.get_service("trpc.trpc_python_group.helloworld.Greeter")
    loop = get_or_create_event_loop()
    loop.run_until_complete(empty_server.wait_for_close(loop.create_task(empty_async_func())))

    empty_server.close = False
    try:
        empty_server.handle_child_exit(0, None)
    except Exception:  # pylint: disable=broad-except
        pass

    empty_server.close = False
    empty_server.opt.reboot_child = True
    empty_server.get_exit_child_pid = empty_child_pid_0
    try:
        empty_server.handle_child_exit(0, None)
    except Exception:  # pylint: disable=broad-except
        pass

    empty_server.close = False
    empty_server.opt.reboot_child = True
    empty_server.get_exit_child_pid = empty_child_pid_100000
    try:
        empty_server.handle_child_exit(0, None)
    except Exception:  # pylint: disable=broad-except
        pass


class TestRegistry:
    """Mock registry class for service registration testing"""

    async def register(self, _, __):
        """Mock service registration (no operation)"""
        return

    async def deregister(self, _, __):
        """Mock service deregistration (no operation)"""
        return


def test_empty_service():
    """Test service with empty configuration
    
    Tests:
    - Service initialization with minimal config
    - Registry operations
    - Service lifecycle (startup and shutdown)
    """
    empty_server = create_new_server("./tests/test_config_data/test_service_empty.yaml")
    register_GreeterServicer_server(empty_server, GreeterServicer())

    test_service = empty_server.services["trpc.trpc_python_group.helloworld.Greeter"]
    test_service.opt.registry = TestRegistry()
    test_service.opt.registry_delay = 10
    test_service.run()
    time.sleep(1)
    test_service.close()
    empty_server.server_engine.close()


def master_exit():
    """Mock function for master process exit handler"""
    return


def worker_exit():
    """Mock function for worker process exit handler"""
    return


def test_set_signal_handle():
    """Test signal handler registration and execution
    
    Tests:
        1. Successful registration and execution of master/worker signal handlers
        2. Error cases with invalid handler types
        3. Proper error messages for failed registrations
    """
    set_signal_handle(master_handle=master_exit, worker_handle=worker_exit)
    run_server_signal_handle()
    run_worker_signal_handle()
    try:
        set_signal_handle("1", None)
    except Exception as e:  # pylint: disable=broad-except
        assert str(e) == 'register signal master handle failed'

    try:
        set_signal_handle(None, "1")
    except Exception as e:  # pylint: disable=broad-except
        assert str(e) == 'register signal worker handle failed'


def test_close_wait_time_config():
    """Test close_wait_time configuration loading
    
    Tests:
        1. close_wait_time can be loaded from config
        2. Default value is 0 when not configured
    """
    # Test with explicit close_wait_time value
    from trpc_sdk.config.config import Server as ServerConfig
    server_config = ServerConfig(close_wait_time=5000)
    assert server_config.close_wait_time == 5000

    # Test default value
    server_config_default = ServerConfig()
    assert server_config_default.close_wait_time == 0


def test_close_wait_time_in_handle_master_exit():
    """Test close_wait_time functionality in handle_master_exit
    
    Tests:
        1. Sleep is called with correct duration when close_wait_time > 0
        2. Sleep is not called when close_wait_time = 0
        3. Proper logging when waiting
    """
    import signal

    # Test with close_wait_time > 0
    server_opt = ServerOption()
    server_opt.close_wait_time = 2000  # 2 seconds
    test_server = Server(server_opt)

    # Mock the dependencies
    test_server.services = {}
    mock_loop = mock.Mock()
    test_server.opt.loop = mock_loop

    with mock.patch.object(test_server.server_engine, 'close'):
        # with mock.patch('time.sleep') as mock_sleep:
        with mock.patch('trpc_sdk.server.server.time') as mock_time:
            with mock.patch.object(test_server, 'kill'):
                # Call handle_master_exit
                test_server.handle_master_exit(signal.SIGTERM, None)

                # Verify sleep was called with 2.0 seconds
                # mock_sleep.assert_called_once_with(2.0)
                mock_time.sleep.assert_called_once_with(2.0)

    # Test with close_wait_time = 0
    server_opt_zero = ServerOption()
    server_opt_zero.close_wait_time = 0
    test_server_zero = Server(server_opt_zero)

    test_server_zero.services = {}
    test_server_zero.opt.loop = mock_loop

    with mock.patch.object(test_server_zero.server_engine, 'close'):
        # with mock.patch('time.sleep') as mock_sleep:
        with mock.patch('trpc_sdk.server.server.time') as mock_time:
            with mock.patch.object(test_server_zero, 'kill'):
                # Call handle_master_exit
                test_server_zero.handle_master_exit(signal.SIGTERM, None)

                # Verify sleep was not called
                # mock_sleep.assert_not_called()
                mock_time.sleep.assert_not_called()


def test_close_wait_time_execution_order():
    """Test that close_wait_time wait happens before server_engine.close()
    
    Tests:
        1. Verify execution order: service.close() -> wait -> server_engine.close()
        2. Ensure wait happens at the correct time in the shutdown sequence
    """
    import signal

    server_opt = ServerOption()
    server_opt.close_wait_time = 1000  # 1 second
    test_server = Server(server_opt)

    test_server.services = {}
    mock_loop = mock.Mock()
    test_server.opt.loop = mock_loop

    # Track execution order
    execution_order = []

    def mock_sleep(duration):
        execution_order.append(('sleep', duration))

    def mock_engine_close():
        execution_order.append(('engine_close', ))

    def mock_kill():
        execution_order.append(('kill', ))

    with mock.patch('time.sleep', side_effect=mock_sleep):
        with mock.patch.object(test_server.server_engine, 'close', side_effect=mock_engine_close):
            with mock.patch.object(test_server, 'kill', side_effect=mock_kill):
                test_server.handle_master_exit(signal.SIGTERM, None)

                # Verify execution order
                assert len(execution_order) == 3
                assert execution_order[0] == ('sleep', 1.0)
                assert execution_order[1] == ('engine_close', )
                assert execution_order[2] == ('kill', )


def _engine():
    opts = ServerOption()
    opts.worker_num = 1
    opts.service_num = 0
    return engine_mod.TrpcServerEngine(opts)


def test_check_server_rejects_illegal_mode_and_worker_count():
    engine = _engine()
    engine.engine_mode = object()
    with pytest.raises(Exception, match="invalid engine mode"):
        engine.check_server()

    engine.engine_mode = engine_mod.ServerMode.DISPATCHER
    engine.opts.worker_num = 0
    with pytest.raises(Exception, match="invalid worker number"):
        engine.check_server()


@pytest.mark.parametrize(
    ("method", "network", "validator"),
    [
        ("set_dispatcher_process", "tcp", "tcp_port_valid"),
        ("set_executor_process", "tcp", "tcp_port_valid"),
        ("set_executor_process", "udp", "udp_port_valid"),
    ],
)
def test_address_validation_exits(monkeypatch, method, network, validator):
    engine = _engine()
    monkeypatch.setattr(engine_mod, "is_unix_socket", lambda _: False)
    monkeypatch.setattr(engine_mod, validator, lambda *_: False)

    with pytest.raises(SystemExit):
        if method == "set_dispatcher_process":
            engine.set_dispatcher_process(("bad", -1), "net", None, 1, network)
        else:
            engine.set_executor_process(("bad", -1), "net", None, None, 1, network, None, None)


def test_unix_address_validation_exits(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(engine_mod, "is_unix_socket", lambda _: True)
    monkeypatch.setattr(engine_mod, "unix_path_valid", lambda _: False)
    with pytest.raises(SystemExit):
        engine.set_dispatcher_process(("/bad", 0), "unix", None, 1, "tcp")


def test_send_msg_to_worker_bounds_unicast_and_broadcast():
    engine = _engine()
    workers = [SimpleNamespace(id=i, send_msg=MagicMock()) for i in range(3)]
    engine.worker_process_list = workers

    assert engine.send_msg_to_worker(0, 3, b"x") is False
    engine.send_msg_to_worker(0, 2, b"one")
    workers[2].send_msg.assert_called_once_with(0, b"one")

    engine.send_msg_to_worker(1, -1, b"all")
    workers[0].send_msg.assert_called_once_with(1, b"all")
    workers[1].send_msg.assert_not_called()
    workers[2].send_msg.assert_called_with(1, b"all")


def test_close_terminates_all_live_process_kinds(monkeypatch):
    engine = _engine()
    live = lambda: SimpleNamespace(process_instance=SimpleNamespace(is_alive=lambda: True))
    engine.sub_dispatcher_process = {1: live()}
    engine.sub_worker_processes = {2: live()}
    engine.sub_worker_task_processes = {3: live()}
    engine.sub_timer_processes = {4: live()}
    close_child = MagicMock()
    monkeypatch.setattr(engine, "close_child", close_child)

    engine.close()

    assert [call.args[0] for call in close_child.call_args_list] == [1, 2, 3, 4]


def test_close_child_swallows_os_exception(monkeypatch):
    monkeypatch.setattr(engine_mod.os, "kill", MagicMock(side_effect=OSError("gone")))
    warning = MagicMock()
    monkeypatch.setattr(engine_mod.logger, "warning", warning)

    engine_mod.TrpcServerEngine.close_child(123)

    warning.assert_called_once()
