# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test server_process_info module"""

from tests.transport_util import TestParser
from trpc_sdk.abc.codec import Parser
from trpc_sdk.abc.server import BaseService
from trpc_sdk.server.server_process_info import ProcessInfo
from trpc_sdk.server.server_process_info import ProcessTaskInfo
from trpc_sdk.transport import BaseServerTransportOption


def test_server_process_info():
    """Test server conn info"""

    task_info = ProcessTaskInfo()
    assert len(task_info.task_name) == 0
    assert len(task_info.task_pipe_conn) == 0

    process_info = ProcessInfo()
    assert process_info.process is None
    assert process_info.process_task_info is None
    assert process_info.pipe_conn is None
    assert process_info.dispatcher_pipe_conn_list is None
    assert process_info.networks is None
    assert process_info.parsers is None
    assert process_info.handlers is None
    assert process_info.timeouts is None
    assert process_info.services is None
    assert process_info.targets is None
    assert len(process_info.addresses) == 0
    assert len(process_info.socket_types) == 0
    assert len(process_info.transport_opt) == 0

    new_process_info = process_info.deep_copy(process_info)
    assert len(new_process_info.addresses) == 0
    assert len(new_process_info.socket_types) == 0
    assert len(new_process_info.transport_opt) == 0

    process_info.add_address(("127.0.0.1", 8000))
    process_info.add_address(("127.0.0.1", 8001))
    assert len(process_info.addresses) == 2
    assert len(new_process_info.addresses) == 0

    process_info.add_socket_type('net')
    process_info.add_socket_type('unix')
    assert len(process_info.socket_types) == 2
    assert len(new_process_info.socket_types) == 0

    process_info.add_network('tcp')
    process_info.add_network('udp')
    assert len(process_info.networks) == 2
    assert new_process_info.targets is None

    process_info.add_parser(TestParser())
    process_info.add_parser(TestParser())
    assert len(process_info.parsers) == 2
    assert new_process_info.parsers is None

    def handler():
        print("handler")

    process_info.add_handler(handler)
    process_info.add_handler(handler)
    assert len(process_info.handlers) == 2
    assert new_process_info.handlers is None

    process_info.add_timeout(1000)
    process_info.add_timeout(1000)
    assert len(process_info.timeouts) == 2
    assert new_process_info.timeouts is None

    process_info.add_service(BaseService())
    process_info.add_service(BaseService())
    assert len(process_info.services) == 2
    assert new_process_info.services is None

    process_info.add_transport_opt(BaseServerTransportOption())
    assert len(process_info.transport_opt) == 1
    assert len(new_process_info.transport_opt) == 0

    process_info.add_target("ip://127.0.0.1")
    process_info.add_target("ip://127.0.0.2")
    assert len(process_info.targets) == 2
    assert new_process_info.targets is None
