# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython per-service handler path and service factory."""

from ._service import Service  # pyright: ignore[reportMissingModuleSource]
from ._service import create_service  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "Service",
    "create_service",
]
