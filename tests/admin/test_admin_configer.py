"""Unit coverage for admin server configuration dispatch."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call

import pytest

from trpc_sdk.admin import server as server_mod
from trpc_sdk.admin.server import Configer


@pytest.mark.asyncio
async def test_config_multiple_keys_sends_combined_payload():
    cfg = Configer()
    cfg.getters = {
        "first": AsyncMock(side_effect=lambda value=None: f"one:{value}"),
        "second": AsyncMock(side_effect=lambda value=None: f"two:{value}"),
    }
    ws = SimpleNamespace(send_str=AsyncMock(), closed=False)

    await cfg.config({"first": "a", "second": "b"}, [ws])

    assert json.loads(ws.send_str.call_args.args[0].removeprefix("config ")) == {
        "first": "one:a",
        "second": "two:b",
    }


@pytest.mark.asyncio
async def test_debug_send_exception_cleans_pending(monkeypatch):
    cfg = Configer()
    ws = SimpleNamespace(send_str=AsyncMock(side_effect=OSError("closed")), closed=False)
    cfg.clients = [ws]
    monkeypatch.setattr(server_mod.uuid, "uuid4", lambda: "request-1")

    assert await cfg.debug({"type": "heap_profile"}) is None
    assert cfg.debug_pending == {}


@pytest.mark.asyncio
async def test_rpcz_illegal_child_index_uses_round_robin(monkeypatch):
    cfg = Configer()
    first = SimpleNamespace(send_str=AsyncMock(side_effect=OSError("stop")), closed=False)
    second = SimpleNamespace(send_str=AsyncMock(side_effect=OSError("stop")), closed=False)
    cfg.clients = [first, second]
    monkeypatch.setattr(server_mod.uuid, "uuid4", MagicMock(side_effect=["r1", "r2"]))

    assert await cfg.rpcz({}, child_index=99) is None
    assert await cfg.rpcz({}, child_index=-1) is None

    first.send_str.assert_awaited_once()
    second.send_str.assert_awaited_once()
    assert cfg.rr_idx == 2


def test_on_worker_debug_unknown_request_id_logs(monkeypatch):
    cfg = Configer()
    warning = MagicMock()
    monkeypatch.setattr(server_mod.trpc_log, "warning", warning)

    cfg.on_worker_debug("missing", {"errorcode": 0})

    warning.assert_called_once()


def test_decode_span_dict_and_json_paths(monkeypatch):
    cfg = Configer()
    decoded = [SimpleNamespace(span_id=1), SimpleNamespace(span_id=2)]
    from_dict = MagicMock(side_effect=decoded)
    monkeypatch.setattr(server_mod.Span, "from_dict", from_dict)

    assert cfg._decode_span({"span_id": 1}) is decoded[0]
    assert cfg._decode_span('{"span_id": 2}') is decoded[1]
    assert from_dict.call_args_list == [call({"span_id": 1}), call({"span_id": 2})]


@pytest.mark.asyncio
async def test_collect_rpcz_once_dispatches_each_client(monkeypatch):
    cfg = Configer()
    cfg.clients = [
        SimpleNamespace(closed=False),
        SimpleNamespace(closed=False),
    ]
    rpcz = AsyncMock(return_value=None)
    monkeypatch.setattr(cfg, "rpcz", rpcz)

    await cfg.collect_rpcz_once(timeout=1.5)

    assert rpcz.await_args_list == [
        call(payload={"type": "collect"}, timeout=1.5, child_index=0),
        call(payload={"type": "collect"}, timeout=1.5, child_index=1),
    ]


@pytest.mark.asyncio
async def test_worker_response_handlers_cover_invalid_and_valid_json(monkeypatch):
    error = MagicMock()
    subprocess = MagicMock()
    monkeypatch.setattr(server_mod.trpc_log, "error", error)
    monkeypatch.setattr(server_mod.configer, "on_subprocess_rpcz", subprocess)

    await server_mod.handle_worker_debug(None, "{bad")
    await server_mod.handle_subprocess_rpcz(
        None,
        json.dumps({"request_id": "r5", "child_index": 1, "spans": []}),
    )

    error.assert_called_once()
    subprocess.assert_called_once_with(
        "r5",
        {"request_id": "r5", "child_index": 1, "spans": []},
    )
