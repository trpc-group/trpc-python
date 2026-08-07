# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from ._context import TrpcContext
from ._message import TrpcFrameHead
from ._message import TrpcMessage


def new_frame_head(
    frame_type: int,
    frame_state: int,
    stream_id: int,
) -> TrpcFrameHead: ...

def parse_frame_head(data: bytes) -> TrpcFrameHead: ...

def create_new_context(
    context: TrpcContext,
) -> TrpcContext: ...

def create_client_context_from_server(
    context: TrpcContext,
) -> TrpcContext: ...

def get_or_create_message(context: TrpcContext) -> TrpcMessage: ...

def create_new_message_for_context(
    context: TrpcContext,
) -> TrpcMessage: ...
