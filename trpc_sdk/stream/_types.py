# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the stream package.

:class:`ClientStreamDesc` describes one client-side streaming method.
"""

from __future__ import annotations

from typing import NamedTuple


class ClientStreamDesc(NamedTuple):
    """Describes one client-side streaming method.

    Attributes:
        stream_name: RPC name written onto the INIT / DATA frames.
        client_streams: Unused by this package; kept for stub generators.
    """

    stream_name: str = ""
    client_streams: bool = False
