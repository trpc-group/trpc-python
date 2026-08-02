# -*- coding: utf-8 -*-
"""Test server task"""

import asyncio
import os
import socket
import traceback

from functools import partial

import pytest

from trpc_sdk.transport.net_util import new_inet_socket
from trpc_sdk.transport.net_util import new_server_socket
from trpc_sdk.transport.net_util import new_unix_socket
from trpc_sdk.utils.network import unix_path_valid


def test_new_server_socket():
    """Test new server socket"""
    sock = new_server_socket(("127.0.0.1", 33312), "net")
    assert sock
    sock.close()
    sock = None

    sock = new_server_socket(("127.0.0.1", 33313), "net", stream=False)
    assert sock
    sock.close()
    sock = None

    path = '.__test_new_unix_socket'
    sock = new_server_socket((path, 33313), "unix", stream=False)
    assert sock
    sock.close()
    sock = None
    assert unix_path_valid(path)
    if os.path.exists(path):
        os.remove(path)
