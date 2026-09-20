# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for the codec package.

Frame-head length and ``trans_info`` keys are the single source for
both the Python modules and the module-level ``cdef`` copies that
``_cython/_codec.pyx`` / ``_cython/_codec_stream.pyx`` bind at import time.
``_cython/_codec_parser.pyx`` is the exception: it hardcodes its own
``FRAME_HEAD_LEN`` so the packet-scanning loop reads a C constant
instead of a module attribute.
"""

from __future__ import annotations

from trpc_sdk._inner import trpc_pb2


# tRPC wire frame head (stx + type + state + package_len + head_len +
# stream_id + reserved). Not the transport IPC envelope, which is 6 bytes.
FRAME_HEAD_LEN = 16

# RequestProtocol / StreamInit request_meta trans_info keys.
DYEING_KEY = "trpc-dyeing-key"
# Historical PascalCase name; callers import EnvTransfer as the key string.
EnvTransfer = "trpc-env"

# Task-stream frames reuse the tRPC magic even though task_stream_codec
# packs them by hand instead of going through c_codec_stream.
TASK_STREAM_MAGIC_VALUE = trpc_pb2.TRPC_MAGIC_VALUE
# Plugin name the task-stream codecs and checker register under; also the
# protocol name trpc.stream passes to get_client_codec / get_server_codec.
INNER_TASK_STREAM = "inner_task_stream"
