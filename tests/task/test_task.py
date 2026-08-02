# -*- coding: utf-8 -*-
import asyncio
import queue
import time

from asyncio import Future
from unittest import mock

from trpc_sdk import context
from trpc_sdk import task
from trpc_sdk import utils
from trpc_sdk.exceptions import TaskHandleError
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import Task
from trpc_sdk.plugin import register_plugin
from trpc_sdk.task import task_util
from trpc_sdk.task import task_util_c
from trpc_sdk.task.consumer import TaskConsumer
from trpc_sdk.task.task import TRPC_DEEP_INFER_TASK
from trpc_sdk.task.task import send_to_task_with_serializer
from trpc_sdk.transport import server_task
from trpc_sdk.transport import server_worker


def handler(reqs):
    rsps = []
    for req in reqs:
        rsps.append("task handle ok-" + str(req))
    return rsps


async def co_handler(reqs):
    rsps = []
    for req in reqs:
        rsps.append("task handle ok-" + str(req))
    return rsps


def handler_not_list(reqs):
    return "task handle ok-123"


def handler_wrong_list(reqs):
    return []


def handler_exception(reqs):
    raise Exception("test exception")


def initializer():
    pass


@register_plugin(plugin_type=PluginType.USER_DEFINED,
                 plugin_name="task_test",
                 task=Task(handler, initializer, use_c_task=False))
def load_task():
    pass


class TestConnection:

    def __init__(self):
        self.buf = b''

    def write(self, buf):
        self.buf = buf


class TestPipeConnection:

    def __init__(self):
        self.buf = b''

    def send(self, buf):
        self.buf = buf


async def _wait_consumer_response(consumer, task_conn_obj, timeout=5):
    """Run consumer until task response bytes are produced."""
    consumer.start()
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    try:
        while not task_conn_obj.buf:
            task_util_c.consume_batch_rsp()
            if task_conn_obj.buf:
                break
            if loop.time() >= deadline:
                raise AssertionError("timed out waiting for task response")
            await asyncio.sleep(0.01)
    finally:
        consumer.terminate()
        consumer.join(timeout=timeout)


@utils.trpc_async_pytest
async def test_send_to_task():
    try:
        task.send_to_task(context.Context(), "test", None)
    except TaskHandleError:
        assert True
    else:
        assert False

    def serialize(obj):
        return b"test_data"

    def deserialize(buf):
        return None

    try:
        await send_to_task_with_serializer(context.Context(), 1, "test", None, serialize, deserialize)
    except TaskHandleError:
        assert True
    else:
        assert False


@utils.trpc_async_pytest
async def test_send_to_task_with_serialize():
    conn = TestConnection()
    task_util.register_task_conn(TRPC_DEEP_INFER_TASK, conn)

    def serialize(obj):
        return b"test_data"

    def deserialize(buf):
        return None

    future = Future()
    future.set_result("test")
    backup = task_util.send_to_task_c
    task_util.send_to_task_c = mock.Mock(return_value=future)
    try:
        await send_to_task_with_serializer(context.Context(), 1, "test", None, serialize, deserialize)
    except TaskHandleError:
        assert False
    else:
        assert True
    task_util.send_to_task_c = backup


@utils.trpc_async_pytest
async def test_send_to_task_worker():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    ret = await asyncio.wait_for(task_coro, timeout=5)
    assert "task handle ok-" + req == ret


@utils.trpc_async_pytest
async def test_send_to_task_executor():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    ret = await asyncio.wait_for(task_coro, timeout=5)
    assert "task handle ok-" + req == ret


@utils.trpc_async_pytest
async def test_task_timeout():
    ctx = context.Context()
    ctx.recv_time = (int(time.time() * 1000 - 1000))
    ctx.timeout = 100

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    try:
        await asyncio.wait_for(task_coro, timeout=5)
    except TaskHandleError as err:
        assert err.msg == "error: task wait timeout!"
    else:
        assert False


@utils.trpc_async_pytest
async def test_task_rsp_not_list():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()
    task_util.TASK_QUEUE = queue.Queue()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler_not_list, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    try:
        await asyncio.wait_for(task_coro, timeout=5)
    except TaskHandleError as err:
        assert err.msg == "error: get non-list type from function return!"
    else:
        assert False


@utils.trpc_async_pytest
async def test_task_rsp_wrong_list():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()
    task_util.TASK_QUEUE = queue.Queue()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler_wrong_list, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    try:
        await asyncio.wait_for(task_coro, timeout=5)
    except TaskHandleError as err:
        assert err.msg == "error: get wrong length from function return!"
    else:
        assert False


@utils.trpc_async_pytest
async def test_task_rsp_handle_fail():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()
    task_util.TASK_QUEUE = queue.Queue()

    task_util.register_task_conn("task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(handler_exception, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    try:
        await asyncio.wait_for(task_coro, timeout=5)
    except TaskHandleError as err:
        assert "test exception" in err.msg
    else:
        assert False


@utils.trpc_async_pytest
async def test_register_ctask():
    task_name = "t1"
    assert not task_util.is_ctask(task_name)
    task_util.register_ctask_name(task_name)
    assert task_util.is_ctask(task_name)

    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000
    req = "123"

    client_conn_obj = TestConnection()
    task_util.register_task_conn(task_name, client_conn_obj)
    task_coro = task.send_to_task(ctx, task_name, req)
    assert isinstance(task_coro, Future)


@utils.trpc_async_pytest
async def test_task_num():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()

    task_util.register_task_num("t2", 1)
    task_util.register_task_conn("t2:0", client_conn_obj)
    task_coro = task.send_to_task(ctx, "t2", req)
    assert isinstance(task_coro, Future)


@utils.trpc_async_pytest
async def test_send_to_task_co_worker():
    ctx = context.Context()
    ctx.recv_time = int(time.time() * 1000)
    ctx.timeout = 30000

    # init params
    req = '123'
    client_conn_obj = TestConnection()
    task_conn_obj = TestConnection()

    task_util.register_task_conn("co_task_test", client_conn_obj)
    task_coro = task.send_to_task(ctx, "co_task_test", req)
    assert isinstance(task_coro, Future)

    task_conn = server_task.TaskConnection(None, 1)
    task_conn.connection_made(task_conn_obj)
    task_conn.data_received(client_conn_obj.buf)

    consumer = TaskConsumer(co_handler, 100, 5, True)
    await _wait_consumer_response(consumer, task_conn_obj)

    worker_task_conn = server_worker.TaskConnection()
    worker_task_conn.connection_made(client_conn_obj)
    worker_task_conn.data_received(task_conn_obj.buf)

    # end
    ret = await asyncio.wait_for(task_coro, timeout=5)
    assert "task handle ok-" + req == ret
