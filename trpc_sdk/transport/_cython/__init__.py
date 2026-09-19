# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython inner-IPC framing used across dispatcher / worker / task pipes."""

from ._ipc_codec import InnerDatagramParser  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import InnerStreamParser  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import PipeDataHeader  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import decode_worker_msg_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import encode_worker_msg_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_decode_data_header_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_decode_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_decode_rsp  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_encode_data_header_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_encode_data_header_rsp  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_encode_req  # pyright: ignore[reportMissingModuleSource]
from ._ipc_codec import inner_encode_rsp  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "InnerDatagramParser",
    "InnerStreamParser",
    "PipeDataHeader",
    "decode_worker_msg_req",
    "encode_worker_msg_req",
    "inner_decode_data_header_req",
    "inner_decode_req",
    "inner_decode_rsp",
    "inner_encode_data_header_req",
    "inner_encode_data_header_rsp",
    "inner_encode_req",
    "inner_encode_rsp",
]
