# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Error-path tests for the core client invocation code."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from trpc_sdk.client import client as client_mod
from trpc_sdk.client.options import Options
from trpc_sdk.exceptions import ClientConnectionError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.transport import RequestType


class _Context:
    def __init__(self):
        self.message = SimpleNamespace(client_rsp_err=None)
        self.deadline = 0
        self.timeout = 0

    def get_message(self):
        return self.message


def _direct_options():
    opts = Options()
    opts.codec = MagicMock()
    opts.codec.encode.return_value = b"request"
    opts.transport = SimpleNamespace(round_trip=AsyncMock())
    return opts


@pytest.mark.asyncio
async def test_client_func_translates_connection_error(monkeypatch):
    opts = _direct_options()
    opts.transport.round_trip.side_effect = ConnectionError("closed")
    ctx = _Context()
    ctx.message.serialization_type = 0
    ctx.message.compress_type = 0
    monkeypatch.setattr(client_mod, "serialize", lambda *_: b"body")
    monkeypatch.setattr(client_mod, "compress", lambda *_: b"body")

    with pytest.raises(ClientConnectionError, match="bad connection"):
        await client_mod._client_func(ctx, object(), object(), opts)


@pytest.mark.asyncio
async def test_client_func_send_only_returns_before_decode(monkeypatch):
    opts = _direct_options()
    opts.call_options.req_type = RequestType.SendOnly
    opts.transport.round_trip.return_value = b"ignored"
    ctx = _Context()
    ctx.message.serialization_type = 0
    ctx.message.compress_type = 0
    monkeypatch.setattr(client_mod, "serialize", lambda *_: b"body")
    monkeypatch.setattr(client_mod, "compress", lambda *_: b"body")

    assert await client_mod._client_func(ctx, object(), object(), opts) is None
    opts.codec.decode.assert_not_called()


@pytest.mark.asyncio
async def test_select_node_rejects_none(monkeypatch):
    opts = Options()
    opts.selector = SimpleNamespace(select=AsyncMock(return_value=None))

    with pytest.raises(ClientRouteError, match="cannot find downstream node"):
        await client_mod.select_node(SimpleNamespace(), opts)


def _prepare_invoke(monkeypatch, filter_error=None, codec=object()):
    opts = Options()
    opts.codec = codec
    opts.selector = SimpleNamespace(report=AsyncMock())
    opts.filters = SimpleNamespace(handle=AsyncMock())
    if filter_error is None:
        opts.filters.handle.return_value = (None, None)
    else:
        opts.filters.handle.side_effect = filter_error
    node = SimpleNamespace(address="127.0.0.1:1", service_name="svc")
    monkeypatch.setattr(client_mod, "get_options", lambda *_: opts)
    monkeypatch.setattr(client_mod, "update_message", lambda *_: None)
    monkeypatch.setattr(client_mod, "select_node", AsyncMock(return_value=node))
    monkeypatch.setattr(client_mod, "set_timeout", lambda *_: client_mod.ClientTimeoutReason.DownstreamTimeout)
    monkeypatch.setattr(client_mod, "now_ms", lambda: 100)
    return opts


@pytest.mark.asyncio
async def test_invoke_empty_codec_raises_encode_error(monkeypatch):
    opts = _prepare_invoke(monkeypatch, codec=None)

    with pytest.raises(ClientEncodeError, match="client codec empty"):
        await client_mod.Client().invoke(_Context(), object(), SimpleNamespace, [])

    opts.selector.report.assert_awaited_once()


@pytest.mark.asyncio
async def test_invoke_timeout_is_wrapped(monkeypatch):
    opts = _prepare_invoke(monkeypatch, asyncio.TimeoutError())

    with pytest.raises(ClientTimeoutError, match="client request timeout") as raised:
        await client_mod.Client().invoke(_Context(), object(), SimpleNamespace, [])

    assert raised.value.timeout_type == client_mod.ClientTimeoutReason.DownstreamTimeout
    opts.selector.report.assert_awaited_once()


@pytest.mark.asyncio
async def test_invoke_unknown_exception_is_reraised(monkeypatch):
    opts = _prepare_invoke(monkeypatch, ValueError("unexpected"))
    monkeypatch.setattr(client_mod.logger, "error", MagicMock())

    with pytest.raises(ValueError, match="unexpected"):
        await client_mod.Client().invoke(_Context(), object(), SimpleNamespace, [])

    opts.selector.report.assert_awaited_once()


def test_invoke_sync_creates_loop_when_thread_has_none(monkeypatch):
    created = []
    original_new_event_loop = asyncio.new_event_loop
    client = client_mod.Client()

    def new_loop():
        loop = original_new_event_loop()
        created.append(loop)
        return loop

    monkeypatch.setattr(client_mod, "get_or_create_event_loop", MagicMock(side_effect=RuntimeError))
    monkeypatch.setattr(client_mod.asynclib, "new_event_loop", new_loop)
    monkeypatch.setattr(client, "invoke", AsyncMock(return_value="done"))
    result = {}

    def run():
        result["value"] = client.invoke_sync(_Context(), object(), object, [])
        asyncio.set_event_loop(None)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    try:
        assert result["value"] == "done"
        assert len(created) == 1
    finally:
        for loop in created:
            loop.close()
