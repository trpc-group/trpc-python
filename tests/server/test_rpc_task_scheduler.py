# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Separate-mode rpc task selection: sticky streams, hooks, dead rpc tasks."""

from __future__ import annotations

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.transport import _scheduler
from trpc_sdk.transport import register_dispatcher_worker_func
from trpc_sdk.transport import select_rpc_task
from trpc_sdk.transport._types import SchedulerParams


@pytest.fixture(autouse=True)
def clean_scheduler(monkeypatch):
    monkeypatch.setattr(_scheduler, "_send_worker_index", 0)
    monkeypatch.setattr(_scheduler, "_warned_bad_hook_index", False)
    monkeypatch.setattr(_scheduler, "_worker_scheduler_func", None)
    monkeypatch.setattr(_scheduler, "_dispatcher_worker_func", None)


def _params(count, frame_type=trpc_pb2.TRPC_UNARY_FRAME, session_id=0, stream_id=0):
    return SchedulerParams(worker_conn_num=count, remote_ip="", session_id=session_id, stream_id=stream_id,
                           frame_type=frame_type, target="rpc_task")


def test_round_robin_skips_dead_rpc_tasks():
    alive = [True, False, True]

    picks = [select_rpc_task(_params(3), alive) for _ in range(4)]

    assert picks == [2, 0, 2, 0]


def test_no_live_rpc_task_returns_none():
    assert select_rpc_task(_params(2), [False, False]) is None
    assert select_rpc_task(_params(0), []) is None


def test_stream_frames_stay_sticky_and_never_move():
    stream = dict(frame_type=trpc_pb2.TRPC_STREAM_FRAME, session_id=3, stream_id=2)

    assert select_rpc_task(_params(4, **stream), [True] * 4) == 1
    assert select_rpc_task(_params(4, **stream), [True, False, True, True]) is None


def test_hook_result_is_used_when_live():
    register_dispatcher_worker_func(lambda params: 1, is_scheduler=True)

    assert select_rpc_task(_params(3), [True, True, True]) == 1


@pytest.mark.parametrize("picked", [5, -1, 1, "x"])
def test_bad_hook_result_falls_back_and_warns_once(monkeypatch, picked):
    warned = []
    monkeypatch.setattr(_scheduler.logger, "warning", lambda *args: warned.append(args))
    register_dispatcher_worker_func(lambda count: picked)

    first = select_rpc_task(_params(3), [True, False, True])
    second = select_rpc_task(_params(3), [True, False, True])

    assert first in (0, 2) and second in (0, 2)
    assert len(warned) == 1
