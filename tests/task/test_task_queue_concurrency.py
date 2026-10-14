# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Regression tests for the native task request queue."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path


def test_multiple_consumers_do_not_dequeue_from_empty_queue():
    """Eight consumers must safely compete for one request at a time.

    Run the scenario in a subprocess because the original bug calls abort()
    inside libstdc++ instead of raising a Python exception.
    """
    script = textwrap.dedent(
        """
        import threading
        import time

        from trpc_sdk.task._consumer import TaskConsumer
        from trpc_sdk.task._cython._task_frame import inner_encode_task_req
        from trpc_sdk.task._cython._task_queue import consume_batch_rsp
        from trpc_sdk.task._cython._task_queue import receive_pkg_from_worker
        from trpc_sdk.task._cython._task_queue import task_init
        from trpc_sdk.task import TASK_CONN


        class ResponseSink:
            def __init__(self):
                self.response_count = 0

            def write(self, _data):
                self.response_count += 1


        condition = threading.Condition()
        start_barrier = threading.Barrier(9)
        handled_count = 0


        class SynchronizedTaskConsumer(TaskConsumer):
            async def consume(self):
                start_barrier.wait(timeout=5)
                await super().consume()


        def handler(requests):
            global handled_count
            with condition:
                handled_count += len(requests)
                condition.notify_all()
            return requests


        request_count = 2000
        task_init(100)
        sink = ResponseSink()
        TASK_CONN[0] = sink

        consumers = [
            SynchronizedTaskConsumer(handler, wait_interval=1, batch_size=20)
            for _ in range(8)
        ]
        for consumer in consumers:
            consumer.start()

        # Start producing only after every consumer has entered consume().
        start_barrier.wait(timeout=5)
        try:
            for pkg_id in range(request_count):
                package = inner_encode_task_req(
                    pkg_id,
                    int(time.time() * 1000),
                    30000,
                    pkg_id,
                )
                receive_pkg_from_worker(0, package)

                with condition:
                    consumed = condition.wait_for(
                        lambda: handled_count >= pkg_id + 1,
                        timeout=5,
                    )
                if not consumed:
                    raise AssertionError(
                        f"request {pkg_id} was not consumed; "
                        f"handled_count={handled_count}"
                    )
        finally:
            for consumer in consumers:
                consumer.terminate()
            for consumer in consumers:
                consumer.join(timeout=5)

        if any(consumer.is_alive() for consumer in consumers):
            raise AssertionError("task consumer failed to stop")

        consume_batch_rsp()
        if sink.response_count != request_count:
            raise AssertionError(
                f"expected {request_count} responses, "
                f"got {sink.response_count}"
            )
        """
    )

    project_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, (
        "multi-consumer task queue subprocess failed "
        f"(returncode={result.returncode})\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
