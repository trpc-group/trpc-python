# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import time

from trpc_sdk.server._types import QueueWindow


def test_report_queue_time():
    qw = QueueWindow()
    now_ms = int(time.time() * 1000)
    ret = qw.report_queue_time(now_ms, True)
    assert ret is True

    assert qw.windows_list[qw.cur % 2].total == 1
    assert qw.windows_list[qw.cur % 2].failed == 0

    qw.report_queue_time(now_ms, False)

    assert qw.windows_list[qw.cur % 2].total == 2
    assert qw.windows_list[qw.cur % 2].failed == 1
