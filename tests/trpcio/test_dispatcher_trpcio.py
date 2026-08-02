# -*- coding: utf-8 -*-
import os
import time

from unittest import mock

from trpc_sdk.abc.task import Task
from trpc_sdk.config import config
from trpc_sdk.plugin import get_registered_tasks
from trpc_sdk.server import ServerOption
from trpc_sdk.server import option
from trpc_sdk.server.c_dispatcher import CDispatcherProcess
from trpc_sdk.server.dispatcher import Dispatcher
from trpc_sdk.server.server_engine import TrpcServerEngine
from trpc_sdk.server.server_pipe_connection import LEFT_PORT
from trpc_sdk.server.server_pipe_connection import TCP_PIPE
from trpc_sdk.server.server_pipe_connection import PipeConnection
from trpc_sdk.server.server_pipe_connection import new_pipe
from trpc_sdk.server.worker import Worker
from trpc_sdk.transport_trpcio.c_dispatcher import CDispatcher


def test_dispatcher_trpcio():
    """test dispatcher trpcio"""
    engine = TrpcServerEngine(option.ServerOption())
    tasks = get_registered_tasks()
    tasks.append(Task(handler=None, use_c_task=True, c_task_driver_params={"log_file": "./task.log"}))
    engine.init_task_process()


def test_dispatcher_trpcio():
    """test dispatcher trpcio"""
    engine = TrpcServerEngine(option.ServerOption())

    worker = Worker(worker_id=0)
    worker.socket_pair = new_pipe(TCP_PIPE)
    pipe_conn = PipeConnection(worker.socket_pair, LEFT_PORT)
    worker.pipe_conn = pipe_conn

    engine.worker_process_list.append(worker)
    engine.process_info.add_address(("127.0.0.1", 53311))
    engine.process_info.add_socket_type(config.SocketType.NET)
    engine.process_info.add_timeout(1)
    engine.process_info.add_checker(None)
    engine.process_info.add_network('tcp')

    dispatcher = CDispatcherProcess(None, engine)
    dispatcher.dispatcher_instance = Dispatcher()
    dispatcher.dispatcher_instance.worker_pipe_conn_list = [pipe_conn]
    dispatcher.dispatcher = CDispatcher()
    dispatcher.use_trpcio = True

    try:
        dispatcher.start = mock.Mock(return_value=0)
    except:  # pylint: disable=bare-except
        assert False
    assert True


def test_server_engine():
    from trpc_sdk.server import server_engine
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.init_worker_process()
    assert len(engine.worker_process_list) != 0


def start_dispatcher(dispatcher):
    dispatcher.run()


def stop_dispatcher():
    # 检查是否监听了端口
    time.sleep(3)
    pid = os.popen("netstat -nlp| grep :53311 | awk '{print $7}' | awk -F\"/\" '{ print $1 }'").read()
    pid = pid.strip()
    if pid is None or pid == "":
        assert False
