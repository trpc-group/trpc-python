# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the built-in IP discovery plugin."""

import asyncio

from trpc_sdk.naming._discovery import IpDiscovery


def test_ip_discovery_can_be_instantiated():
    assert isinstance(IpDiscovery(), IpDiscovery)


def test_select_node_returns_none():
    assert asyncio.run(IpDiscovery().select_node("127.0.0.1:8000", None)) is None


def test_list_returns_single_node():
    nodes = asyncio.run(IpDiscovery().list("127.0.0.1:8000", ""))

    assert len(nodes) == 1
    assert nodes[0].ip == "127.0.0.1"
    assert nodes[0].port == 8000
