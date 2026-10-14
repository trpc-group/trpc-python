# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import json
import socket
import time
from urllib import request
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from trpc_sdk import admin
from trpc_sdk.admin import _client as client_mod
from trpc_sdk.version import __version__


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
    from trpc_sdk.config import TrpcConfig
    TrpcConfig.load('./examples/helloworld/trpc_python.yaml', 'utf-8')
    plug = admin.AdminPlugin()
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
    assert rsp['version'] == __version__


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
