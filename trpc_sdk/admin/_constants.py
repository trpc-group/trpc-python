# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants shared by the admin HTTP server and the worker WS client."""

from __future__ import annotations

RPCZ_FILTER_PLUGIN_NAME: str = "rpcz"
"""Filter name whose presence in the config enables the admin rpcz collector."""

WS_PATH: str = "/_/ws"
"""Admin server path that worker WS clients connect to."""

WS_USER_AGENT_PREFIX: str = "trpc-admin/"
"""User-Agent prefix the admin server requires before upgrading to WS."""

WS_RECONNECT_INTERVAL_S: float = 10
"""Seconds a worker WS client waits before reconnecting after a failure."""
