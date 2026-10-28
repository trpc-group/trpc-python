# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import pytest

from trpc_sdk.abc import BaseCircuitBreaker
from trpc_sdk.abc import BaseClient
from trpc_sdk.abc import BaseClientStream
from trpc_sdk.abc import BaseClientStreamConnection
from trpc_sdk.abc import BaseConfig
from trpc_sdk.abc import BaseConfigDecoder
from trpc_sdk.abc import BaseConfigProvider
from trpc_sdk.abc import BaseDiscovery
from trpc_sdk.abc import BaseLoadBalance
from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import BaseServiceRegistry
from trpc_sdk.abc import Node
from trpc_sdk.abc import SelectorOption
from trpc_sdk.abc import TrpcContext


def test_registry():
    with pytest.raises(TypeError):
        registry = BaseServiceRegistry()
        registry.register("", ())
        registry.deregister("", ())


class _Client(BaseClient):

    async def invoke(self, ctx, req, rsp_cls, options):
        return await super().invoke(ctx, req, rsp_cls, options)


class _ClientStream(BaseClientStream):

    async def recv_msg(self, msg):
        return await super().recv_msg(msg)

    async def send_msg(self, msg):
        return await super().send_msg(msg)

    async def close_send(self):
        return await super().close_send()

    def context(self):
        return super().context()


class _Stream(BaseClientStreamConnection):

    async def recv(self, ctx):
        return await super().recv(ctx)

    async def send(self, ctx, req):
        return await super().send(ctx, req)

    def close(self, ctx):
        return super().close(ctx)

    async def start(self, ctx, options):
        return await super().start(ctx, options)


class _Config(BaseConfig):

    def load(self):
        return super().load()

    def get(self, key, default):
        return super().get(key, default)

    def parse(self, object_dict):
        return super().parse(object_dict)


class _Provider(BaseConfigProvider):

    def load(self, file_path, options=None):
        return super().load(file_path, options)

    def hot_reload(self, key, callback, options=None):
        return super().hot_reload(key, callback, options)

    def set_options(self, options):
        return super().set_options(options)


class _Decoder(BaseConfigDecoder):

    def decode(self, config_data, options=None):
        return super().decode(config_data, options)

    def set_options(self, options):
        return super().set_options(options)


class _Discovery(BaseDiscovery):

    async def list(self, service_name, namespace):
        return await super().list(service_name, namespace)

    async def select_node(self, service_name, opt):
        return await super().select_node(service_name, opt)


class _Selector(BaseSelector):

    async def select(self, service_name, opt):
        return await super().select(service_name, opt)

    async def report(self, node, cost, code):
        return await super().report(node, cost, code)


class _Registry(BaseServiceRegistry):

    def register(self, service, address):
        return super().register(service, address)

    def deregister(self, service, address):
        return super().deregister(service, address)


class _LoadBalance(BaseLoadBalance):

    async def select(self, service_name, node_list, namespace):
        return await super().select(service_name, node_list, namespace)


class _CircuitBreaker(BaseCircuitBreaker):

    def available(self, node):
        return super().available(node)

    def report(self, node, cost, code):
        return super().report(node, cost, code)


@pytest.mark.asyncio
async def test_base_client_abstract_bodies():
    client = _Client()
    assert await client.invoke(TrpcContext(), None, None, []) is None

    stream = _ClientStream()
    assert await stream.recv_msg(None) is None
    assert await stream.send_msg(None) is None
    assert await stream.close_send() is None
    assert stream.context() is None

    connection = _Stream()
    assert await connection.recv(TrpcContext()) is None
    assert await connection.send(TrpcContext(), None) is None
    assert connection.close(TrpcContext()) is None
    assert await connection.start(TrpcContext(), []) is None


def test_base_config_abstract_and_set_config():
    cfg = _Config("ds", "dec", {}, {})
    assert cfg.load() is None
    assert cfg.get("k", None) is None
    assert cfg.parse({}) is None
    cfg.set_raw_config({"a": 1})
    assert cfg._raw_config_dict == {"a": 1}

    provider = _Provider()
    assert provider.load("", {}) is None
    assert provider.hot_reload("k", lambda: None, {}) is None
    assert provider.set_options({}) is None

    decoder = _Decoder()
    assert decoder.decode(b"", {}) is None
    assert decoder.set_options({}) is None


@pytest.mark.asyncio
async def test_base_discovery_list_body():
    assert await _Discovery().list("svc", "ns") is None
    assert await _Discovery().select_node("svc", None) is None


@pytest.mark.asyncio
async def test_base_selector_registry_lb_cb_bodies():
    node = Node(service_name="s", ip="127.0.0.1", port=1)
    sel = _Selector()
    assert await sel.select("s", SelectorOption()) is None
    assert await sel.report(node, 1, 1) is None

    reg = _Registry()
    assert reg.register("s", ("127.0.0.1", 1)) is None
    assert reg.deregister("s", ("127.0.0.1", 1)) is None

    assert await _LoadBalance().select("s", [node], "ns") is None

    cb = _CircuitBreaker()
    assert cb.available(node) is None
    assert cb.report(node, 1, 1) is None
