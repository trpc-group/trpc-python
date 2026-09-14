# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python codec package: what a frame means, not how it travels.

Public symbols are re-exported here. Implementation lives in ``_``-prefixed
modules; callers should only ``from trpc_sdk.codec import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.codec._message` — ``Message``, 16-byte ``CFrameHead``, clone helpers
- :mod:`~trpc_sdk.codec._codec` — Python ``ClientCodec`` / ``ServerCodec`` dispatch
- :mod:`~trpc_sdk.codec._c_codec` / :mod:`~trpc_sdk.codec._c_codec_stream` — Cython unary / stream pack
- :mod:`~trpc_sdk.codec._codec_parser` — TCP sticky-packet splitter (``TrpcServerParser``)
- :mod:`~trpc_sdk.codec._factory` — plugin lookups and protocol checkers
- :mod:`~trpc_sdk.codec._compress` / :mod:`~trpc_sdk.codec._serialization` — body plugins
- :mod:`~trpc_sdk.codec._task_stream_codec` — inner worker-to-task stream protocol
- :mod:`~trpc_sdk.codec._exceptions` — task-stream encode / decode errors

``_message`` is imported first so ``trpc_sdk.abc`` can take ``Message``
from this package without cycling through ``_factory``.
"""

from __future__ import annotations

from ._message import CFrameHead
from ._message import FrameHead
from ._message import Message
from ._message import clone_client_message
from ._message import clone_context_and_message
from ._message import clone_message
from ._message import create_frame_head
from ._message import create_message
from ._message import get_message
from ._message import parse_frame_head
from ._codec import ClientCodec
from ._codec import ServerCodec
from ._compress import CompressGZip
from ._compress import CompressNoop
from ._compress import CompressSnappy
from ._compress import CompressStreamSnappy
from ._compress import CompressZlib
from ._constants import FRAME_HEAD_LEN
from ._constants import INNER_TASK_STREAM
from ._exceptions import TaskStreamClientDecodeError
from ._exceptions import TaskStreamClientEncodeError
from ._exceptions import TaskStreamClientReadFrameError
from ._exceptions import TaskStreamServerDecodeError
from ._exceptions import TaskStreamServerEncodeError
from ._factory import TrpcClientParser
from ._factory import TrpcServerParser
from ._factory import get_client_checker
from ._factory import get_client_codec
from ._factory import get_server_checker
from ._factory import get_server_codec
from ._serialization import JceSerializer
from ._serialization import JsonSerializer
from ._serialization import NoopSerializer
from ._serialization import ProtoSerializer
from ._serialization import WupSerializer
from ._serialization import register_pb_json_format_kwargs
from ._task_stream_codec import TaskStreamClientCodec
from ._task_stream_codec import TaskStreamClientParser
from ._task_stream_codec import TaskStreamServerCodec
from ._task_stream_codec import TaskStreamServerParser
from ._types import Body
from ._types import CompressType
from ._types import JsonData
from ._types import SerializationType
from ._utils import compress
from ._utils import decompress
from ._utils import deserialize
from ._utils import get_compress
from ._utils import get_request_attachment
from ._utils import get_response_attachment
from ._utils import get_serializer
from ._utils import serialize
from ._utils import set_request_attachment
from ._utils import set_response_attachment

__all__ = [
    # Compress
    "CompressType",
    "compress",
    "decompress",
    "get_compress",
    "CompressNoop",
    "CompressGZip",
    "CompressZlib",
    "CompressSnappy",
    "CompressStreamSnappy",
    # Codec
    "ClientCodec",
    "ServerCodec",
    "TrpcClientParser",
    "TrpcServerParser",
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
    # Serialization
    "SerializationType",
    "serialize",
    "deserialize",
    "get_serializer",
    "JsonSerializer",
    "JsonData",
    "Body",
    "register_pb_json_format_kwargs",
    "ProtoSerializer",
    "NoopSerializer",
    "JceSerializer",
    "WupSerializer",
    # Task stream
    "INNER_TASK_STREAM",
    "TaskStreamClientCodec",
    "TaskStreamServerCodec",
    "TaskStreamClientParser",
    "TaskStreamServerParser",
    "TaskStreamClientReadFrameError",
    "TaskStreamClientEncodeError",
    "TaskStreamClientDecodeError",
    "TaskStreamServerEncodeError",
    "TaskStreamServerDecodeError",
]
