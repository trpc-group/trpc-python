# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3

# Relative cimports inside a package __init__.pxd resolve against the parent
# package, so absolute module paths are required here. Only Cython-level
# symbols may be re-exported; pure-Python classes such as BaseCodec and
# BaseClientProtocolParser must be imported with a regular Python import.
# _Codec is cimport-only: it is the cross-package base for the Cython codecs
# in trpc_sdk.codec._cython and is deliberately absent from __init__.py.

from trpc_sdk.abc._cython._codec cimport _Codec
from trpc_sdk.abc._cython._codec cimport BaseServerProtocolParser
from trpc_sdk.abc._cython._server cimport BaseService
from trpc_sdk.abc._cython._context cimport RpczData
from trpc_sdk.abc._cython._context cimport TrpcContext
from trpc_sdk.abc._cython._message cimport TrpcFrameHead
from trpc_sdk.abc._cython._message cimport TrpcMessage
from trpc_sdk.abc._cython._message_builder cimport clone_client_message
from trpc_sdk.abc._cython._message_builder cimport clone_context_and_message
from trpc_sdk.abc._cython._message_builder cimport clone_message
from trpc_sdk.abc._cython._message_builder cimport create_frame_head
from trpc_sdk.abc._cython._message_builder cimport create_message
from trpc_sdk.abc._cython._message_builder cimport get_message
from trpc_sdk.abc._cython._message_builder cimport parse_frame_head
