# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Shared fixtures for the process-model tests: a fake engine and role cleanup."""

from __future__ import annotations

import multiprocessing
from types import SimpleNamespace
from typing import List
from typing import Optional
from typing import Tuple

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.http import HttpServerTransport
from trpc_sdk.runtime import ProcessInfo
from trpc_sdk.runtime import install_thread_model
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import resolve_server_transport


class _ParserFactory:
    """Stands in for a protocol parser factory; only ``create`` is looked at."""

    def create(self):
        return object()


def make_engine(endpoints: List[Tuple[str, str]], worker_num: int = 2, worker_thread_num: int = 1, **opts):
    """Build an engine whose ``process_info`` describes ``endpoints``.

    Args:
        endpoints: ``(network, protocol)`` per listen address. Ports start at
            20000 and go up by one. Each gets the transport ``ServiceAdapter.run``
            would give it: HTTP for ``http``, the registered starter's for a
            protocol with one, tRPC otherwise.
        worker_num: Separate-mode rpc task count, or peer-mode worker count.
        worker_thread_num: Merge-mode loop thread count.
        **opts: Overrides for the option fields ``MergeWorker`` and peer mode
            read; each has a default below.
    """
    info = ProcessInfo()
    for i, (network, protocol) in enumerate(endpoints):
        transport_opt = BaseServerTransportOption()
        transport_opt.set_address(("127.0.0.1", 20000 + i))
        transport_opt.set_network(network)
        transport_opt.set_parser(_ParserFactory() if protocol == "trpc" else None)
        transport_opt.set_idle_timeout(1000)
        transport_opt.set_service_adapter(SimpleNamespace(opt=SimpleNamespace(protocol=protocol,
                                                                              service_name=f"svc{i}")))
        transport = resolve_server_transport(protocol,
                                             HttpServerTransport() if protocol == "http" else TrpcServerTransport())
        transport.bind(transport_opt)
        info.transport_opt.append(transport_opt)
        info.transports.append(transport)
    fields = dict(io_thread_num=1,
                  handler_max_concurrency=0,
                  handler_queue_size=1024,
                  tcp_reuse_port=False,
                  ready_timeout=300000,
                  close_wait_time=0,
                  io_handle_type="")
    fields.update(opts)
    return SimpleNamespace(process_info=info,
                           opts=SimpleNamespace(worker_num=worker_num, worker_thread_num=worker_thread_num,
                                                **fields))


@pytest.fixture
def engine_factory():
    return make_engine


@pytest.fixture(autouse=True)
def uninstall_thread_model():
    """Drop the thread model a worker under test installed, so the next test starts without one."""
    yield
    install_thread_model(None)


@pytest.fixture
def restore_process_type():
    """Undo any ``type`` stamp a test puts on ``current_process()``."""
    process = multiprocessing.current_process()
    missing = object()
    saved: Optional[object] = getattr(process, "type", missing)
    yield process
    if saved is missing:
        if hasattr(process, "type"):
            delattr(process, "type")
    else:
        process.type = saved
