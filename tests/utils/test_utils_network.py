# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test network"""
import socket
import os
import sys
from unittest.mock import MagicMock

import pytest

from trpc_sdk.utils._network import *
from trpc_sdk.utils import _network as network


def test_int2str_for_ip():
    """Test int to str"""
    assert int2ip(2130706433) == "127.0.0.1"
    assert ip2int("127.0.0.1") == 2130706433


def test_bytes2str_for_ip():
    """Test bytes to str for ip"""
    assert bytes_to_ip(ip_to_bytes("::1")) == "::1"
    assert bytes_to_ip(ip_to_bytes("127.0.0.1")) == "127.0.0.1"


def test_ip_type():
    """Test ip type"""
    assert not is_ipv4("::1")
    assert is_ipv4("127.0.0.1")
    assert is_ipv6("::1")
    assert not is_ipv6("127.0.0.1")
    assert not is_unix_socket("127.0.0.1")
    assert is_unix_socket("unix")


def test_port():
    """Test port"""
    assert port_valid(22345)
    assert not port_valid(2234500)
    assert not tcp_port_valid('127.0.0.1', 2234500)
    assert tcp_port_valid("127.0.0.1", 12345)
    assert tcp_port_valid("::1", 12345)
    assert udp_port_valid("127.0.0.1", 12345)
    assert not udp_port_valid("::1", 1234500)
    assert real_tcp_port_valid("127.0.0.1", 12345)
    assert not real_tcp_port_valid("127..0.0.1", 12345)
    assert real_udp_port_valid("127.0.0.1", 12345)
    assert not real_udp_port_valid("127..0.0.1", 12345)
    port = find_free_port()
    assert port_valid(port)


def test_unix():
    """Test unix"""
    assert unix_path_valid(".test_tcp_socket")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(".test_tcp_socket")
    assert unix_path_valid(".test_tcp_socket")


def test_get_ip_with_nic():
    """Test ip with nic"""
    assert get_ip_with_nic("") == ""
    get_ip_with_nic("en0")
    get_ip_with_nic("eth0")
    get_ip_with_nic("eth1")


def test_join_host_port():
    """Test join host port"""
    assert join_host_port("::1", 0) == "[::1]:0"
    assert join_host_port("127.0.0.1", 0) == "127.0.0.1:0"


def test_socket_state():
    """Test socket state"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    assert is_valid_socket(sock)
    assert not is_valid_socket(None)
    assert not is_close_wait(sock)
    assert "TCP_CLOSE" == get_tcp_state(sock)
    set_close_wait(sock, 2)
    set_default_close_wait(sock)
    set_no_close_wait(sock)
    set_tcp_no_delay(sock)
    set_tcp_keep_alive(sock)
    close_socket(sock)
    assert not is_valid_socket(sock)

    class MockSocket:

        def close(a: int):
            pass

    close_socket(MockSocket())


def test_close_wait_at_mac():
    """Test close wait return succ at mac"""
    platform = sys.platform
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    sys.platform = "darwin"
    assert not is_close_wait(sock)
    sys.platform = platform

# ---- merged from test_utils_network_extra.py ----


def test_is_ipv4_ipv6_invalid_addr():
    assert network.is_ipv4("not-an-ip") is False
    assert network.is_ipv6("also-not-an-ip") is False


def test_unix_path_valid_when_unlink_fails(tmp_path, monkeypatch):
    sock_path = tmp_path / "occupied.sock"
    sock_path.write_text("x")

    def fake_unlink(path):
        raise OSError("busy")

    monkeypatch.setattr(os, "unlink", fake_unlink)
    assert network.unix_path_valid(str(sock_path)) is False
    assert network.unlink_unix_socket_file(str(sock_path)) is False


def test_get_ip_with_nic_ipv6_and_error(monkeypatch):
    monkeypatch.setattr(network.netifaces, "interfaces", lambda: ["fake0"])

    def fake_ifaddresses(nic):
        if nic == "fake0":
            return {
                network.netifaces.AF_INET6: [{"addr": "fe80::1"}],
            }
        raise RuntimeError("boom")

    monkeypatch.setattr(network.netifaces, "ifaddresses", fake_ifaddresses)
    assert network.get_ip_with_nic("fake0") == "fe80::1"

    monkeypatch.setattr(
        network.netifaces,
        "ifaddresses",
        lambda nic: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert network.get_ip_with_nic("fake0") == ""


def test_close_socket_transport_socket_and_unknown(monkeypatch):
    class FakeTransportSocket:
        def __init__(self):
            self._sock = MagicMock()

    monkeypatch.setattr(network, "TransportSocket", FakeTransportSocket)
    sock = FakeTransportSocket()
    network.close_socket(sock)
    sock._sock.close.assert_called_once()

    network.close_socket(object())


@pytest.mark.skipif(sys.platform == "darwin", reason="TCP_INFO is Linux-only")
def test_tcp_info_paths_linux():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    try:
        network.is_close_wait(sock)
        network.get_tcp_state(sock)
    finally:
        sock.close()


def test_set_close_wait_helpers():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        network.set_close_wait(sock, 1)
        network.set_default_close_wait(sock)
    finally:
        sock.close()

