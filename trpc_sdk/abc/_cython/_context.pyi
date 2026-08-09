# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from typing import Any
from typing import Optional

from ._message import TrpcMessage


class RpczData:
    """RPC scheduling and I/O timestamps measured in microseconds."""

    server_post_sched_recv_msg_us: int
    server_pre_sched_send_msg_us: int
    server_post_sched_send_msg_us: int
    server_pre_io_send_msg_us: int
    server_post_io_send_msg_us: int
    client_pre_io_send_msg_us: int
    client_post_io_send_msg_us: int
    client_post_sched_send_msg_us: int
    client_post_sched_recv_msg_us: int


class TrpcContext:
    """Runtime state and message owned by one RPC invocation."""

    deadline: int
    timeout: int
    msg: Optional[TrpcMessage]
    meta_data: Any
    remote_ip: str
    remote_port: int
    local_ip: str
    local_port: int
    recv_time: float
    rpcz_data: RpczData

    def with_meta_data(self, key: Any, value: Any) -> None: ...

    def get_meta_data(self, key: Any, default: Any = None) -> Any: ...

    def pop_meta_data(self, key: Any, default: Any = None) -> Any: ...

    def __copy__(self) -> "TrpcContext": ...
