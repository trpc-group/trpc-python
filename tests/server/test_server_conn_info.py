# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Inner IPC framing used between the I/O process and an rpc task."""

import pytest

from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.runtime import ProcessTaskInfo
from trpc_sdk.transport import InnerStreamParser
from trpc_sdk.transport._cython import inner_decode_req
from trpc_sdk.transport._cython import inner_encode_req


def test_server_conn_info():
    """A complete frame round-trips; a short or corrupt buffer does not."""
    body = b"123"
    addr = b"127.0.0.1:8000|127.0.0.1:9000"
    encoded = inner_encode_req(1, 2, 3, 4, addr, body)
    fd, index, session_id, recv_time, addr_info, decoded = inner_decode_req(encoded)
    assert (fd, index, session_id, recv_time, addr_info, decoded) == (1, 2, 3, 4, addr, body)

    task_info = ProcessTaskInfo()
    assert task_info.task_name == []
    assert task_info.task_pipe_conn == []

    parser = InnerStreamParser()
    assert parser.read() == []

    parser.feeds_data(encoded[:1])
    assert parser.read() == []

    parser.feeds_data(encoded[1:])
    assert parser.read() == [encoded]
    assert parser.read() == []

    bad = InnerStreamParser()
    bad.feeds_data(b"\x00" * 32)
    with pytest.raises(ServerDecodeError):
        bad.read()
