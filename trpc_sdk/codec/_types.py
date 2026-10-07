# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the codec package."""

from __future__ import annotations

from typing import NewType

from trpc_sdk._inner import trpc_pb2

# Annotation alias for INIT-frame proto instances cached by session/stream id.
# Runtime NewType is identity; stored values are ``TrpcStreamInitMeta()``.
StreamInitMeta = NewType("StreamInitMeta", trpc_pb2.TrpcStreamInitMeta)
