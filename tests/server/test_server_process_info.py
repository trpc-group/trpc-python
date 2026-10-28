# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test process info records shared by server workers."""

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.runtime import ProcessInfo
from trpc_sdk.runtime import ProcessTaskInfo


def test_server_process_info():
    """Listen fields live on ``transport_opt``; the copy has its own lists."""
    task_info = ProcessTaskInfo()
    assert task_info.task_name == []
    assert task_info.task_pipe_conn == []

    process_info = ProcessInfo()
    assert process_info.process is None
    assert process_info.process_task_info is None
    assert process_info.pipe_conn is None
    assert process_info.dispatcher_pipe_conn_list == []
    assert process_info.transport_opt == []
    assert process_info.transports == []

    copied = ProcessInfo.deep_copy(process_info)
    assert copied.transport_opt == []
    assert copied.transports == []
    assert copied.dispatcher_pipe_conn_list == []

    first = BaseServerTransportOption()
    first.set_address(("127.0.0.1", 8000))
    first.set_network("tcp")
    first.set_socket_type("net")
    first.set_target("ip://127.0.0.1")
    second = BaseServerTransportOption()
    second.set_address(("127.0.0.1", 8001))
    second.set_network("udp")
    second.set_socket_type("unix")
    second.set_target("ip://127.0.0.2")
    process_info.add_transport_opt(first)
    process_info.add_transport_opt(second)
    process_info.add_transport(object())

    assert len(copied.transport_opt) == 0
    assert len(copied.transports) == 0
    assert [opt.address for opt in process_info.transport_opt] == [("127.0.0.1", 8000), ("127.0.0.1", 8001)]
    assert [opt.network for opt in process_info.transport_opt] == ["tcp", "udp"]
    assert [opt.socket_type for opt in process_info.transport_opt] == ["net", "unix"]
    assert [opt.target for opt in process_info.transport_opt] == ["ip://127.0.0.1", "ip://127.0.0.2"]
    assert len(process_info.transports) == 1

    # List containers are copied; the option objects themselves stay shared.
    first.set_address(("10.0.0.1", 1))
    again = ProcessInfo.deep_copy(process_info)
    assert again.transport_opt[0].address == ("10.0.0.1", 1)
    process_info.add_transport_opt(BaseServerTransportOption())
    assert len(again.transport_opt) == 2
    assert len(process_info.transport_opt) == 3
