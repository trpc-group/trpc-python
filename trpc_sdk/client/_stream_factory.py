# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client stream factories.

Each outbound stream call needs a fresh object: :attr:`Stream.opts` stays
``None`` until :meth:`~trpc_sdk.client.Stream.start` fills in the per-call
transport, codec and deadline. A shared instance would leak one call's
options into the next.
"""

from __future__ import annotations

from ._stream import Stream


def create_stream() -> Stream:
    """Build a client stream for the public ``trpc_sdk.stream`` protocol.

    Every call needs its own object: ``Stream.opts`` stays ``None`` until
    ``start()`` fills in the per-call transport, codec and deadline, so a
    shared instance would leak one call's options into the next.
    """
    return Stream()
