# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Multi-process example for ``server.mode: separate``.

``features/thread/conf/separate.yaml`` is a different thing: one process,
``mode: merge``, with ``handle_thread_num`` (I/O threads vs handle
threads). This directory forks ``worker_num`` processes to run handlers.
The I/O process only accepts tRPC-over-TCP and forwards frames.

``check.py`` starts ``server.py`` itself.
"""
