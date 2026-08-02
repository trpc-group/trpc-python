# -*- coding: utf-8 -*-
import json
import socket
import time
from urllib import request
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import trpc_sdk
from trpc_sdk import admin
from trpc_sdk.admin import client as client_mod


@admin.on_master_start
def admin_master(routes, configer):
    from aiohttp import web

    @configer.reg_cfg('test')
    async def test(arg):
        return arg

    @routes.get('/hello')
    async def hello(request):
        msg = 'Hello World!'
        await configer.config({'test': msg})
        return web.Response(text=msg)


@admin.on_worker_start
def admin_worker(configer):
    import logging

    @configer.reg_cfg('test')
    async def test(ws, arg):
        return logging.debug('from master msg: %s', arg)


def find_avail_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    res = sock.getsockname()
    sock.close()
    return res


@pytest.fixture(scope='module')
def server():
    from trpc_sdk import admin
    from trpc_sdk import config
    config.load_global_config('./examples/helloworld/trpc_python.yaml', 'utf-8')
    plug = admin.AdminPlugin()
    addr = plug.get_config(False)
    addr = find_avail_port()
    plug.get_config = lambda m: addr

    plug.on_master_start()
    time.sleep(0.5)
    plug.on_worker_start()
    time.sleep(0.5)
    return addr


def test_version(server):
    url = 'http://%s:%d/version' % server
    rsp = json.loads(request.urlopen(url).read())
    assert rsp['version'] == trpc_sdk.__version__


def test_cmds(server):
    url = 'http://%s:%d/cmds' % server
    rsp = json.loads(request.urlopen(url).read())
    assert rsp['cmds']


def test_loglevel(server):
    url = 'http://%s:%d/cmds/loglevel' % server
    rsp = json.loads(request.urlopen(url).read())
    assert rsp['level']


def test_loglevel_set(server):
    url = 'http://%s:%d/cmds/loglevel' % server
    rsp = json.loads(request.urlopen(request.Request(url, method='PUT')).read())
    assert rsp['errorcode'] == -1
    rsp = json.loads(request.urlopen(request.Request(url, b'value=error', method='PUT')).read())
    assert rsp['level'] == 'error'


def test_config(server):
    url = 'http://%s:%d/cmds/config' % server
    rsp = json.loads(request.urlopen(url).read())
    assert rsp['content']


def test_language(server):
    url = 'http://%s:%d/cmds/debug/language' % server
    rsp = json.loads(request.urlopen(url).read())
    assert rsp['language'] == 'python'
    assert rsp['errorcode'] == 0


def test_cpu_profile_bad_param(server):
    """duration_ms 参数缺失时应立即返回 errorcode=-1，不触发采集"""
    url = 'http://%s:%d/cmds/debug/profile/cpu' % server
    req = request.Request(url, data=b'', method='POST')
    rsp = json.loads(request.urlopen(req).read())
    assert rsp['errorcode'] == -1
    assert 'duration_ms' in rsp['message']


def test_cpu_profile_no_child(server):
    """mock 掉子进程查找返回 None，应立即返回 errorcode=-1"""
    from unittest.mock import patch

    from trpc_sdk.admin.debug_handler.cpu_profiler_handler import cpu_profiler_handler as handler
    url = 'http://%s:%d/cmds/debug/profile/cpu?duration_ms=100' % server
    with patch.object(handler, '_pick_child_pid', return_value=None):
        req = request.Request(url, data=b'', method='POST')
        rsp = json.loads(request.urlopen(req).read())
    assert rsp['errorcode'] == -1
    assert 'no child' in rsp['message']


def test_heap_profile_bad_param(server):
    """duration_ms 参数缺失时应立即返回 errorcode=-1，不分发给 worker"""
    url = 'http://%s:%d/cmds/debug/profile/heap' % server
    req = request.Request(url, data=b'', method='POST')
    rsp = json.loads(request.urlopen(req).read())
    assert rsp['errorcode'] == -1
    assert 'duration_ms' in rsp['message']


def test_heap_profile_no_worker(server):
    """mock configer.debug 返回 None，模拟无可用 worker，应立即返回 errorcode=-1"""
    from unittest.mock import AsyncMock
    from unittest.mock import patch

    from trpc_sdk.admin import server as admin_server
    url = 'http://%s:%d/cmds/debug/profile/heap?duration_ms=100' % server
    with patch.object(admin_server.configer, 'debug', new=AsyncMock(return_value=None)):
        req = request.Request(url, data=b'', method='POST')
        rsp = json.loads(request.urlopen(req).read())
    assert rsp['errorcode'] == -1
    assert 'no worker' in rsp['message']


def test_heap_profile_success(server):
    """worker 正常响应时应返回 errorcode=0，并包含 tracemalloc 数据"""
    url = 'http://%s:%d/cmds/debug/profile/heap?duration_ms=100' % server
    req = request.Request(url, data=b'', method='POST')
    rsp = json.loads(request.urlopen(req).read())
    assert rsp['errorcode'] == 0
    assert rsp.get('data_type') == 'tracemalloc'
    assert isinstance(rsp.get('data'), list)


def test_heap_profile_child_index_passed_to_debug(server):
    """child_index 应从 query 参数透传给 configer.debug()。"""
    from unittest.mock import AsyncMock
    from unittest.mock import patch

    from trpc_sdk.admin import server as admin_server
    url = 'http://%s:%d/cmds/debug/profile/heap?duration_ms=100&child_index=0' % server
    mock_debug = AsyncMock(return_value=None)
    with patch.object(admin_server.configer, 'debug', new=mock_debug):
        req = request.Request(url, data=b'', method='POST')
        request.urlopen(req).read()
    _, kwargs = mock_debug.call_args
    assert kwargs.get('child_index') == 0


def test_heap_profile_invalid_child_index_passed_as_none(server):
    """非数字的 child_index 应被转为 None 传给 configer.debug()，退避 round-robin。"""
    from unittest.mock import AsyncMock
    from unittest.mock import patch

    from trpc_sdk.admin import server as admin_server
    url = 'http://%s:%d/cmds/debug/profile/heap?duration_ms=100&child_index=abc' % server
    mock_debug = AsyncMock(return_value=None)
    with patch.object(admin_server.configer, 'debug', new=mock_debug):
        req = request.Request(url, data=b'', method='POST')
        request.urlopen(req).read()
    _, kwargs = mock_debug.call_args
    assert kwargs.get('child_index') is None


@pytest.mark.asyncio
async def test_handle_debug_unknown_type_returns_error():
    ws = SimpleNamespace(send_str=AsyncMock())

    await client_mod.handle_debug(ws, json.dumps({"request_id": "r1", "type": "unknown"}))

    response = ws.send_str.call_args.args[0]
    assert response.startswith("debug ")
    payload = json.loads(response.removeprefix("debug "))
    assert payload["request_id"] == "r1"
    assert payload["errorcode"] == -1
    assert "unknown debug command" in payload["message"]


@pytest.mark.asyncio
async def test_handle_debug_handler_exception(monkeypatch):
    from trpc_sdk.admin.debug_handler.heap_profiler_handler import heap_profiler_handler

    monkeypatch.setattr(heap_profiler_handler, "handle", AsyncMock(side_effect=RuntimeError("profile failed")))
    ws = SimpleNamespace(send_str=AsyncMock())

    await client_mod.handle_debug(ws, json.dumps({"request_id": "r2", "type": "heap_profile"}))

    payload = json.loads(ws.send_str.call_args.args[0].removeprefix("debug "))
    assert payload == {"request_id": "r2", "errorcode": -1, "message": "profile failed"}


@pytest.mark.asyncio
async def test_handle_rpcz_serializes_websocket_response(monkeypatch):
    spans = [SimpleNamespace(to_dict=MagicMock(return_value={"span_id": 3}))]
    monkeypatch.setattr(client_mod, "pop_all_spans", lambda: spans)
    ws = SimpleNamespace(send_str=AsyncMock())

    await client_mod.handle_rpcz(ws, json.dumps({"request_id": "r3", "child_index": 2}))

    payload = json.loads(ws.send_str.call_args.args[0].removeprefix("rpcz "))
    assert payload == {"request_id": "r3", "child_index": 2, "spans": [{"span_id": 3}]}


@pytest.mark.asyncio
async def test_run_stops_on_keyboard_interrupt(monkeypatch):
    session = object()
    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", MagicMock(return_value=session))
    websocket = AsyncMock(side_effect=KeyboardInterrupt)
    monkeypatch.setattr(client_mod, "websocket", websocket)

    await client_mod.run("127.0.0.1", 8080)

    websocket.assert_awaited_once_with(session, "http://127.0.0.1:8080/_/ws")
