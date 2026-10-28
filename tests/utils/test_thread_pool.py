# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from trpc_sdk.runtime._threadmodel import LoopThread


def _work():
    return 1


def test_submit_to_loop_thread():
    """A loop thread is the executor that replaced the global thread pool."""
    thread = LoopThread(0)
    thread.start()
    thread.wait_ready(3)
    try:
        assert thread.submit(_work) == 1
    finally:
        assert thread.stop(3) is True
