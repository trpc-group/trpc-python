# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the length-only tRPC frame splitter.

``TrpcServerProtocolChecker`` checks magic ``2352`` and ``package_len``
only; protocol decode happens later.
"""

from __future__ import annotations

from typing import List

from trpc_sdk.abc import BaseServerProtocolChecker


class TrpcServerProtocolChecker(BaseServerProtocolChecker):
    """Split a TCP stream into complete tRPC frames.

    A frame is 16-byte head plus ``package_len - 16`` more bytes.
    ``read`` returns every complete package currently buffered; an
    incomplete tail stays in the buffer across ``feed_data``.
    """

    def feed_data(self, data: bytes) -> None:
        """Append ``data`` to the growable parse buffer.

        Args:
            data: Incremental stream bytes from the client TCP.
        """
        ...

    def read(self) -> List[bytes]:
        """Return every complete frame and compact leftover bytes.

        Raises ``ServerDecodeError`` on magic mismatch, a
        ``package_len`` smaller than 16, or a ``package_len`` above
        ``TrpcConfig.server_config().max_rbuf_size``.

        Returns:
            Complete packages including the 16-byte head. Empty when
            the buffer has no full frame yet.
        """
        ...

    def reset(self) -> None:
        """Drop buffered bytes without freeing the allocation."""
        ...

    def resize(self, new_number: int) -> None:
        """Reallocate the parse buffer to ``new_number`` bytes.

        Args:
            new_number: New capacity in bytes.
        """
        ...
