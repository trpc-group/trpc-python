# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pure-Python contracts for frame-body compression."""

from abc import abstractmethod

from ._plugin import BasePlugin


class BaseCompress(BasePlugin):
    """Plugin contract for frame-body compression.

    Both methods take and return raw ``bytes``. Implementations decide
    whether empty input is a no-op (gzip/zlib/snappy) or passed through
    (noop).
    """

    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compress ``data`` and return the encoded body.

        Args:
            data: Raw body bytes.
        """
        return bytes()

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress ``data`` and return the original body.

        Args:
            data: Compressed body bytes.
        """
        return bytes()
