# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from typing import Any


class TrpcFrameHead:
    """Mutable tRPC transport frame header."""

    stx: int
    frame_type: int
    frame_state: int
    package_len: int
    head_len: int
    stream_id: int
    frame_reserved: int


class TrpcMessage:
    """Protocol-independent RPC message data without context ownership."""

    frame_head: TrpcFrameHead
    timeout: int
    serialization_type: int
    compress_type: int
    stream_id: int
    dyeing: bool
    dyeing_key: str
    server_meta_data: Any
    client_meta_data: Any
    server_rsp_err: Any
    client_rsp_err: Any
    server_req_head: Any
    server_rsp_head: Any
    client_req_head: Any
    client_rsp_head: Any
    caller_app: str
    caller_server: str
    caller_service: str
    caller_method: str
    callee_app: str
    callee_server: str
    callee_service: str
    callee_method: str
    local_addr: str
    remote_addr: str
    logger: str
    namespace: str
    env_name: str
    set_name: str
    env_transfer: str
    callee_container_name: str
    callee_set_name: str
    stream_frame: Any
    request_id: int
    session_id: int
    request_attachment: bytes
    response_attachment: bytes

    @property
    def server_rpc_name(self) -> str:
        """RPC name observed by the server."""
        ...

    @server_rpc_name.setter
    def server_rpc_name(self, value: str) -> None: ...

    @property
    def client_rpc_name(self) -> str:
        """RPC name selected by the client."""
        ...

    @client_rpc_name.setter
    def client_rpc_name(self, value: str) -> None: ...

    @property
    def caller_service_name(self) -> str:
        """Fully qualified upstream service name."""
        ...

    @caller_service_name.setter
    def caller_service_name(self, value: str) -> None: ...

    @property
    def callee_service_name(self) -> str:
        """Fully qualified downstream service name."""
        ...

    @callee_service_name.setter
    def callee_service_name(self, value: str) -> None: ...

    def __repr__(self) -> str: ...
