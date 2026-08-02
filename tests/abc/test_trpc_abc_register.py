# -*- coding: utf-8 -*-

import pytest

from trpc_sdk.context import Context
from trpc_sdk.naming.option import SelectorOption
from trpc_sdk.abc.circuitbreaker import BaseCircuitBreaker
from trpc_sdk.abc.client import BaseClient, BaseClientStream, BaseStream
from trpc_sdk.abc.config import BaseConfig, BaseDataProvider, BaseDecoder
from trpc_sdk.abc.discovery import BaseDiscovery
from trpc_sdk.abc.loadbalance import BaseLoadBalance
from trpc_sdk.abc.node import Node
from trpc_sdk.abc.registry import *
from trpc_sdk.abc.registry import Registry
from trpc_sdk.abc.selector import BaseSelector

def test_registry():
    with pytest.raises(TypeError):
        r = Registry()
        r.register("", ())
        r.deregister("", ())

# ---- merged from test_trpc_abc_extra.py ----


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


class _Stream(BaseStream):
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

    def watch(self, callback, interval, event_loop):
        return super().watch(callback, interval, event_loop)

    def unmarshal(self, object_dict):
        return super().unmarshal(object_dict)


class _Provider(BaseDataProvider):
    def load(self, file_path, options):
        return super().load(file_path, options)

    def watch(self, callback, interval, event_loop):
        return super().watch(callback, interval, event_loop)

    def set_options(self, options):
        return super().set_options(options)


class _Decoder(BaseDecoder):
    def decode(self, config_data, options):
        return super().decode(config_data, options)

    def set_options(self, options):
        return super().set_options(options)


class _Discovery(BaseDiscovery):
    def list(self, service_name, namespace):
        return super().list(service_name, namespace)


class _Selector(BaseSelector):
    async def select(self, service_name, opt):
        return await super().select(service_name, opt)

    async def report(self, node, cost, success):
        return await super().report(node, cost, success)


class _Registry(Registry):
    def register(self, service, address):
        return super().register(service, address)

    def deregister(self, service, address):
        return super().deregister(service, address)


class _LoadBalance(BaseLoadBalance):
    def select(self, service_name, node_list, namespace):
        return super().select(service_name, node_list, namespace)


class _CircuitBreaker(BaseCircuitBreaker):
    def available(self, node):
        return super().available(node)

    def report(self, node, cost, success):
        return super().report(node, cost, success)


@pytest.mark.asyncio
async def test_base_client_abstract_bodies():
    client = _Client()
    assert await client.invoke(Context(), None, None, []) is None

    stream = _ClientStream()
    assert await stream.recv_msg(None) is None
    assert await stream.send_msg(None) is None
    assert await stream.close_send() is None
    assert stream.context() is None

    s = _Stream()
    assert await s.recv(Context()) is None
    assert await s.send(Context(), None) is None
    assert s.close(Context()) is None
    assert await s.start(Context(), []) is None


def test_base_config_abstract_and_set_config():
    cfg = _Config("ds", "dec", {}, {})
    with pytest.raises(NotImplementedError):
        cfg.load()
    with pytest.raises(NotImplementedError):
        cfg.get("k", None)
    with pytest.raises(NotImplementedError):
        cfg.watch(lambda: None, 1, None)
    with pytest.raises(NotImplementedError):
        cfg.unmarshal({})
    cfg.set_config({"a": 1})
    assert cfg.config == {"a": 1}

    provider = _Provider()
    with pytest.raises(NotImplementedError):
        provider.load("", {})
    with pytest.raises(NotImplementedError):
        provider.watch(lambda: None, 1, None)
    with pytest.raises(NotImplementedError):
        provider.set_options({})

    decoder = _Decoder()
    with pytest.raises(NotImplementedError):
        decoder.decode(b"", {})
    with pytest.raises(NotImplementedError):
        decoder.set_options({})


def test_base_discovery_list_body():
    assert _Discovery().list("svc", "ns") is None


@pytest.mark.asyncio
async def test_base_selector_registry_lb_cb_bodies():
    node = Node(service_name="s", ip="127.0.0.1", port=1)
    sel = _Selector()
    assert await sel.select("s", SelectorOption()) is None
    assert await sel.report(node, 1, 1) is None

    reg = _Registry()
    assert reg.register("s", ("127.0.0.1", 1)) is None
    assert reg.deregister("s", ("127.0.0.1", 1)) is None

    assert _LoadBalance().select("s", [node], "ns") is None

    cb = _CircuitBreaker()
    assert cb.available(node) is None
    assert cb.report(node, 1, 1) is None

