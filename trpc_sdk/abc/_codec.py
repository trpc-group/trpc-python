# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pure-Python contracts for compression, serialization, and the task body codec.

These interfaces are distinct from the Cython context-first protocol
codec in :mod:`trpc_sdk.abc._cython` (``BaseCodec`` /
``BaseClientProtocolChecker`` / ``BaseServerProtocolChecker``).

Concrete compressors register with ``register_plugin(PluginType.COMPRESS,
...)``; serializers register with ``register_plugin(
PluginType.SERIALIZATION, ...)``. The default ``return bytes()`` bodies
exist so a subclass that calls ``super()`` still gets a defined result;
they are not used on the production path. ``TaskCodec`` is not a
registered plugin; the process-wide default lives in :mod:`trpc_sdk.task`.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod


class TaskCodec(ABC):
    """Base class for the business payload codec of a task.

    The framing between a worker and a task child is fixed, but the body
    is opaque to it: business code chooses how its own objects become
    bytes by installing a codec. Both processes must install the same
    one. Unlike the two contracts above this is not a registered plugin;
    the process-wide default lives in :mod:`trpc_sdk.task`.
    """

    @abstractmethod
    def encode(self, obj) -> bytes:
        """Serialize one request or response body.

        Args:
            obj: Object handed to, or returned by, the task handler.

        Returns:
            The body bytes to put in the frame.
        """

    @abstractmethod
    def decode(self, data: bytes) -> object:
        """Deserialize one request or response body.

        Args:
            data: Body bytes taken from the frame.

        Returns:
            The object to hand to the task handler or the caller.
        """
