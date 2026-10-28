# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Admin example for merge, peer and separate modes.

``server.py`` registers a ``whoami`` command on both sides of the admin
plugin. ``conf/`` gives each process mode its own ports. ``check.py``
starts the server for a mode and curls the built-in commands plus
``/cmds/whoami``.
"""
