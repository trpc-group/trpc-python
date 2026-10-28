# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""How many children build_task_infos expands each registered task into."""

from __future__ import annotations

from typing import Dict

from trpc_sdk.abc import TaskInfo
from trpc_sdk.runtime._process import _task_process
from trpc_sdk.runtime._process._task_process import build_task_infos


def _handler(reqs):
    return reqs


def test_task_without_task_num_uses_the_default(monkeypatch):
    registered: Dict[str, int] = {}
    tasks = [TaskInfo(name="dflt", handler=_handler), TaskInfo(name="own", handler=_handler, task_num=2)]
    monkeypatch.setattr(_task_process, "get_registered_tasks", lambda: tasks)
    monkeypatch.setattr(_task_process, "register_task_num", registered.__setitem__)

    infos = build_task_infos(default_task_num=3)

    assert [info.name for info in infos] == ["dflt:0", "dflt:1", "dflt:2", "own:0", "own:1"]
    assert registered == {"dflt": 3, "own": 2}


def test_default_task_num_is_one(monkeypatch):
    monkeypatch.setattr(_task_process, "get_registered_tasks", lambda: [TaskInfo(name="t", handler=_handler)])
    monkeypatch.setattr(_task_process, "register_task_num", lambda name, num: None)
    assert [info.name for info in build_task_infos()] == ["t:0"]
