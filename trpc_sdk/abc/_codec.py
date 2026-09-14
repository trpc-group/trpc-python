# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pure-Python plugin contracts for compression and serialization.

These interfaces are distinct from the Cython context-first protocol
codec in :mod:`trpc_sdk.abc._cython` (``BaseCodec`` /
``BaseClientProtocolChecker`` / ``BaseServerProtocolChecker``).

Concrete compressors register with ``register_plugin(PluginType.COMPRESS,
...)``; serializers register with ``register_plugin(
PluginType.SERIALIZATION, ...)``. The default ``return bytes()`` bodies
exist so a subclass that calls ``super()`` still gets a defined result;
they are not used on the production path.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any


class BaseCompress(ABC):
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


class BaseSerializer(ABC):
    """Plugin contract for RPC-body serialization.

    ``serialize`` turns an application object into body bytes.
    ``deserialize`` writes those bytes back into a caller-supplied
    object (protobuf message, ``JsonData``, ``Body``, ...).
    """

    @abstractmethod
    def serialize(self, body: Any) -> bytes:
        """Serialize ``body`` and return the encoded bytes.

        Args:
            body: Application object for this ContentType.
        """
        return bytes()

    @abstractmethod
    def deserialize(self, data: bytes, body: Any) -> None:
        """Populate ``body`` from serialized ``data``.

        Args:
            data: Serialized body bytes.
            body: Object to mutate in place.
        """
