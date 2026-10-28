# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Single-process thread-model examples: merge and the separate thread model.

``conf/separate.yaml`` is still ``server.mode: merge``. ``handle_thread_num``
only splits I/O threads from handle threads inside that one process. The multi-process mode, ``server.mode: separate``, lives in
``features/separate``.

Run ``check.py`` in this directory for TCP and UDP, and
``features/http/thread/check.py`` for HTTP. Both launch ``server.py``
themselves.
"""
