# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP-specific exceptions.

Framework-wide errors live in :mod:`trpc_sdk.exceptions`. This module only
holds the URL-parse failure used by :class:`~trpc_sdk.http.UrlParser` and
the HTTP client.
"""

from __future__ import annotations

from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException


class InvalidURL(TrpcException):
    """Raised when a URL cannot be parsed into scheme plus netloc.

    Args:
        msg: Human-readable description of what was wrong with the URL.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(exception_type=TrpcErrorType.FRAMEWORK, code=-1, msg=msg)
