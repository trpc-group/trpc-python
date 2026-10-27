# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython implementations of the core tRPC interfaces."""

from ._codec import BaseClientProtocolParser  # pyright: ignore[reportMissingModuleSource]
from ._codec import BaseCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec import BaseServerProtocolParser  # pyright: ignore[reportMissingModuleSource]
from ._context import RpczData  # pyright: ignore[reportMissingModuleSource]
from ._context import TrpcContext  # pyright: ignore[reportMissingModuleSource]
from ._message import TrpcFrameHead  # pyright: ignore[reportMissingModuleSource]
from ._message import TrpcMessage  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import clone_client_message  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import clone_context_and_message  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import clone_message  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import create_frame_head  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import create_message  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import get_message  # pyright: ignore[reportMissingModuleSource]
from ._message_builder import parse_frame_head  # pyright: ignore[reportMissingModuleSource]
from ._server import BaseServiceAdapter  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "BaseClientProtocolParser",
    "BaseCodec",
    "BaseServerProtocolParser",
    "BaseServiceAdapter",
    "RpczData",
    "TrpcContext",
    "TrpcFrameHead",
    "TrpcMessage",
    "clone_client_message",
    "clone_context_and_message",
    "clone_message",
    "create_frame_head",
    "create_message",
    "get_message",
    "parse_frame_head",
]
