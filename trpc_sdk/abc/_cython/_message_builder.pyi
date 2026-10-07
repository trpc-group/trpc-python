# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from typing import Tuple

from ._context import TrpcContext
from ._message import TrpcFrameHead
from ._message import TrpcMessage


def create_frame_head(
    frame_type: int,
    frame_state: int,
    stream_id: int,
) -> TrpcFrameHead: ...

def parse_frame_head(data: bytes) -> TrpcFrameHead: ...

def create_message(
    context: TrpcContext,
) -> Tuple[TrpcContext, TrpcMessage]: ...

def get_message(context: TrpcContext) -> TrpcMessage: ...

def clone_context_and_message(
    context: TrpcContext,
) -> Tuple[TrpcContext, TrpcMessage]: ...

def clone_message(
    context: TrpcContext,
) -> Tuple[TrpcContext, TrpcMessage]: ...

def clone_client_message(
    context: TrpcContext,
) -> Tuple[TrpcContext, TrpcMessage]: ...
