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


# tRPC wire frame head (stx + type + state + package_len + head_len +
# stream_id + reserved). Not the transport IPC envelope, which is 6 bytes.
FRAME_HEAD_LEN = 16

# RequestProtocol / StreamInit request_meta trans_info keys.
DYEING_KEY = "trpc-dyeing-key"
ENV_TRANSFER_KEY = "trpc-env"
