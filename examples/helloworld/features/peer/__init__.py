# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Multi-process example for ``server.mode: peer``.

``worker_num`` processes (the process you started is worker-0) each bind
every endpoint with ``SO_REUSEPORT`` and serve it themselves. Nothing is
forwarded between them. ``check.py`` starts ``server.py`` and checks that
against ``_docs/server_peer_mode_design.md``.

Restarting one worker, preloading a model before the fork, Task, and
refusing a second instance on the same ports are not in this example.
Those are the "not this version" items in design sections 9.4 and 15.
"""
