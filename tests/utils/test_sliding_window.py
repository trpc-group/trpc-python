# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import time

from trpc_sdk.server.sliding_window import QueueWindow


def test_report_queue_time():
    qw = QueueWindow()
    ret = qw.report_queue_time(int(time.time()), True)
    assert ret is True

    assert qw.windows_list[qw.cur % 2].total == 1
    assert qw.windows_list[qw.cur % 2].failed == 0

    qw.report_queue_time(int(time.time()), False)

    assert qw.windows_list[qw.cur % 2].total == 2
    assert qw.windows_list[qw.cur % 2].failed == 1
