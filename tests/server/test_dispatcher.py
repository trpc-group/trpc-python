# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Python replacement for the removed C dispatcher process.

Separate mode forwards frames with ``get_next_worker`` / ``select_rpc_task``.
Merge mode has no dispatcher process; ``MergeWorker`` serves the endpoints
itself.
"""

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.runtime import MergeWorker
from trpc_sdk.server import ServerOption
from trpc_sdk.server import TrpcServerAdapter
from trpc_sdk.transport import SchedulerParams
from trpc_sdk.transport import get_next_worker
from trpc_sdk.transport import register_dispatcher_worker_func
from trpc_sdk.transport import select_rpc_task


def _clear_hooks():
    register_dispatcher_worker_func(None)
    register_dispatcher_worker_func(None, True)


def test_merge_worker_serves_without_a_dispatcher():
    """Merge mode is the process that used to be split into dispatcher and executor."""
    _clear_hooks()
    opt = BaseServerTransportOption()
    opt.set_address(("127.0.0.1", 8000))
    opt.set_network("tcp")
    opt.set_socket_type("net")
    adapter = TrpcServerAdapter(ServerOption())
    adapter.process_info.add_transport_opt(opt)
    adapter.process_info.add_transport(object())

    worker = MergeWorker(adapter)

    assert worker.addresses == [("127.0.0.1", 8000)]
    assert worker.networks == ["tcp"]
    assert worker.socket_types == ["net"]
    assert worker.block is True
    assert worker.children == []


def test_unary_hook_and_stream_stickiness():
    """Unary frames use the registered hook; stream frames stay on one worker."""
    _clear_hooks()
    try:
        unary = SchedulerParams(worker_conn_num=3, remote_ip="127.0.0.1", session_id=1, stream_id=0)
        register_dispatcher_worker_func(lambda worker_conn_num: worker_conn_num - 1)
        assert get_next_worker(unary) == 2

        register_dispatcher_worker_func(lambda schedule: schedule.worker_conn_num, True)
        assert get_next_worker(unary) == 3

        sticky = SchedulerParams(
            worker_conn_num=4,
            remote_ip="",
            session_id=1,
            stream_id=2,
            frame_type=trpc_pb2.TRPC_STREAM_FRAME,
        )
        assert get_next_worker(sticky) == (1 + 2) % 4
        # A stream frame stays on its hashed worker and is dropped when that worker is dead.
        dead = SchedulerParams(
            worker_conn_num=4,
            remote_ip="",
            session_id=2,
            stream_id=0,
            frame_type=trpc_pb2.TRPC_STREAM_FRAME,
        )
        assert select_rpc_task(dead, [True, True, False, True]) is None
    finally:
        _clear_hooks()
