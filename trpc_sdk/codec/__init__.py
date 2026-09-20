# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python codec package: what a frame means, not how it travels.

Public symbols are re-exported here. Implementation lives in ``_``-prefixed
modules; callers should only ``from trpc_sdk.codec import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.codec._codec` — Python ``TrpcClientCodec`` / ``TrpcServerCodec`` plugins (``BaseCodec`` subclasses) that
  hold a unary and a stream Cython codec and dispatch between them; the server INIT meta cache lives here
- :mod:`~trpc_sdk.codec._cython._codec` / :mod:`~trpc_sdk.codec._cython._codec_stream` — private Cython unary /
  stream codecs, both derived from the C-level ``_Codec`` root in :mod:`trpc_sdk.abc`
- :mod:`~trpc_sdk.codec._cython._codec_parser` — TCP sticky-packet splitter (``TrpcServerProtocolChecker``)
- :mod:`~trpc_sdk.codec._factory` — plugin lookups and protocol checkers

The message type and its clone helpers now live in :mod:`trpc_sdk.abc`. The
``Message`` / ``CFrameHead`` / ``FrameHead`` names below are aliases kept so
existing callers and generated stubs keep working; new code should import
``TrpcMessage`` / ``TrpcFrameHead`` from ``trpc_sdk.abc`` directly.
"""

from __future__ import annotations

from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import clone_client_message
from trpc_sdk.abc import clone_context_and_message
from trpc_sdk.abc import clone_message
from trpc_sdk.abc import create_frame_head
from trpc_sdk.abc import create_message
from trpc_sdk.abc import get_message
from trpc_sdk.abc import parse_frame_head
from ._codec import TrpcClientCodec
from ._codec import TrpcServerCodec
from ._constants import FRAME_HEAD_LEN
from ._cython import TrpcServerProtocolChecker
from ._factory import TrpcClientParser
from ._factory import get_client_checker
from ._factory import get_client_codec
from ._factory import get_server_checker
from ._factory import get_server_codec
from ._utils import get_request_attachment
from ._utils import get_response_attachment
from ._utils import set_request_attachment
from ._utils import set_response_attachment

# Historical spellings of the message types; identical objects, not subclasses.
Message = TrpcMessage
CFrameHead = TrpcFrameHead
FrameHead = TrpcFrameHead

__all__ = [
    # Codec
    "TrpcClientCodec",
    "TrpcServerCodec",
    "TrpcClientParser",
    "TrpcServerProtocolChecker",
    "get_client_checker",
    "get_client_codec",
    "get_server_checker",
    "get_server_codec",
    "set_request_attachment",
    "get_request_attachment",
    "set_response_attachment",
    "get_response_attachment",
    "FRAME_HEAD_LEN",
    # Message
    "CFrameHead",
    "FrameHead",
    "Message",
    "clone_client_message",
    "clone_message",
    "clone_context_and_message",
    "create_frame_head",
    "create_message",
    "get_message",
    "parse_frame_head",
]
