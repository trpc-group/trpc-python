# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pure-Python contracts for serialization
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

from ._plugin import BasePlugin


class BaseSerializer(BasePlugin):
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
