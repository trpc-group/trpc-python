# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test server_conn_info module"""

from trpc_sdk.server.server_process_info import ProcessTaskInfo
from trpc_sdk.transport.server_conn_info import InnerParser
from trpc_sdk.transport.server_conn_info import inner_decode
from trpc_sdk.transport.server_conn_info import inner_encode


def test_server_conn_info():
    """Test server conn info"""
    data = b'123'
    encoded_data = inner_encode(data)
    decoded_data = inner_decode(encoded_data)
    assert decoded_data == data

    task_info = ProcessTaskInfo()
    assert len(task_info.task_name) == 0
    assert len(task_info.task_pipe_conn) == 0

    parser = InnerParser()
    assert parser.read() is None
    parser = InnerParser()
    parser.feeds_data(b'1')
    assert parser.read() is None
    parser = InnerParser()
    parser.feeds_data(b'\x05"\x00\x00\x00\t1')
    assert parser.read() is None
    parser = InnerParser()
    parser.feeds_data(encoded_data)
    assert parser.read().content == encoded_data

    try:
        parser = InnerParser()
        parser.feeds_data(b'\x04"\x00\x00\x00\t123123')
        parser.read()
        assert False
    except Exception:  # pylint: disable=broad-except
        assert True

    try:
        parser = InnerParser()
        parser.feeds_data(b'\x05"\x00\x00\x00\t123123')
        parser.read()
        assert False
    except Exception as err:  # pylint: disable=broad-except
        print(err)
        assert True

    try:
        inner_decode(b'\x05"\x00\x00\x00\t1')
        assert False
    except Exception as err:  # pylint: disable=broad-except
        print(err)
        assert True
