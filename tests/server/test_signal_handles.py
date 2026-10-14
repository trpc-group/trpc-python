# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from __future__ import annotations

import functools

import pytest

from trpc_sdk.runtime._common import _signals


@pytest.fixture(autouse=True)
def empty_handles(monkeypatch):
    monkeypatch.setattr(_signals, "_master_handles", [])
    monkeypatch.setattr(_signals, "_worker_handles", [])


def test_failing_callback_does_not_skip_the_rest():
    ran = []
    _signals.set_signal_handle(master_handle=lambda: 1 / 0, worker_handle=lambda: 1 / 0)
    _signals.set_signal_handle(master_handle=lambda: ran.append("master"),
                               worker_handle=lambda: ran.append("worker"))

    _signals.run_server_signal_handle()
    _signals.run_worker_signal_handle()

    assert ran == ["master", "worker"]


class _Owner:

    def close(self) -> None:
        pass


def _with_default(timeout: int = 1) -> None:
    pass


def _needs_arg(value) -> None:
    pass


@pytest.mark.parametrize("handle", [_Owner().close, functools.partial(_needs_arg, 1), _with_default])
def test_zero_arg_callables_are_accepted(handle):
    _signals.set_signal_handle(master_handle=handle, worker_handle=handle)

    assert _signals._master_handles == [handle]
    assert _signals._worker_handles == [handle]


@pytest.mark.parametrize("handle", ["1", _needs_arg])
def test_handles_that_need_arguments_are_rejected(handle):
    with pytest.raises(Exception, match="register signal master handle failed"):
        _signals.set_signal_handle(master_handle=handle)
    with pytest.raises(Exception, match="register signal worker handle failed"):
        _signals.set_signal_handle(worker_handle=handle)
