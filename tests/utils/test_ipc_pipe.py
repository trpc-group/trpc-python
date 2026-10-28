# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""PipeConnection and create_pipe, the pre-fork IPC socketpair."""

import pytest

from trpc_sdk.utils.ipc import PipeConnection
from trpc_sdk.utils.ipc import PipeEnd
from trpc_sdk.utils.ipc import PipeKind
from trpc_sdk.utils.ipc import create_pipe


def test_create_pipe_stream_and_datagram():
    stream = create_pipe(PipeKind.TCP)
    datagram = create_pipe()
    udp = create_pipe(PipeKind.UDP)
    try:
        assert stream[0].type == stream[1].type
        assert datagram[0].type == datagram[1].type
        left = PipeConnection(stream, PipeEnd.LEFT)
        right = PipeConnection(stream, PipeEnd.RIGHT)
        left.send(b"ping")
        assert right.recv(16) == b"ping"
        assert left.fileno() == stream[0].fileno()
        assert isinstance(udp[0].fileno(), int)
    finally:
        for pair in (stream, datagram, udp):
            for sock in pair:
                sock.close()


def test_pipe_rejects_a_bad_pair_or_end():
    pair = create_pipe()
    try:
        with pytest.raises(ValueError):
            PipeConnection(None, PipeEnd.LEFT)
        with pytest.raises(ValueError):
            PipeConnection((pair[0],), PipeEnd.LEFT)
        with pytest.raises(ValueError):
            PipeConnection(pair, 0)
    finally:
        pair[0].close()
        pair[1].close()


def test_missing_socket_and_blocking_and_close():
    pair = create_pipe()
    conn = PipeConnection(pair, PipeEnd.LEFT)
    peer = PipeConnection(pair, PipeEnd.RIGHT)
    conn.process_socket = None
    with pytest.raises(Exception, match="None process_socket"):
        conn.send(b"x")
    with pytest.raises(Exception, match="None process_socket"):
        conn.recv(4)
    with pytest.raises(Exception, match="None process_socket"):
        conn.fileno()
    with pytest.raises(Exception, match="None process_socket"):
        conn.close()

    conn.process_socket = pair[0]
    assert conn.get_socket() is pair[0]

    class _WouldBlock:
        def sendall(self, data):
            raise BlockingIOError()

    conn.process_socket = _WouldBlock()
    conn.send(b"blocked")
    conn.process_socket = pair[0]
    conn.process_socket.setblocking(False)
    assert conn.recv(4) is None

    peer.close_extra_conn()
    peer.close()
    conn.close_extra_conn()
    conn.close()

    class _Broken:
        def close(self):
            raise OSError("already closed")

    conn.socket_pair = (conn.socket_pair[0], _Broken())
    conn.process_type = PipeEnd.LEFT
    conn.close_extra_conn()
