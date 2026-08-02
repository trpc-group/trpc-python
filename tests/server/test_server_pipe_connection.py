# -*- coding: utf-8 -*-
"""Test transport module"""
import socket

from tests.transport_util import TestPipeSocket
from trpc_sdk.server import server_pipe_connection


def test_new_pipe():
    """Test new pipe"""
    socket_pair = server_pipe_connection.new_pipe()
    assert socket_pair is not None
    lsock, rsock = socket_pair
    assert lsock.family == socket.AF_UNIX
    assert rsock.family == socket.AF_UNIX


def test_server_pipe_connection():
    """Test server_pipe_connection"""
    socket_pair = (TestPipeSocket("left"), TestPipeSocket("right"))

    left_pipe = server_pipe_connection.PipeConnection(socket_pair, server_pipe_connection.LEFT_PORT)
    left_pipe.close_extra_conn()
    right_pipe = server_pipe_connection.PipeConnection(socket_pair, server_pipe_connection.RIGHT_PORT)
    right_pipe.close_extra_conn()

    left_pipe.send(b'1')
    assert right_pipe.recv(1) == b'1'

    assert left_pipe.fileno() == "left"

    assert left_pipe.get_socket() == socket_pair[0]

    left_pipe.close()
