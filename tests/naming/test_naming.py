# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import socket

import pytest

from trpc_sdk.naming.circuitbreaker import NoopCircuitBreaker
from trpc_sdk.naming.discovery import IpDiscovery, resolve_host, set_default_discovery
from trpc_sdk.naming.ip_selector import IpSelector, DNSSelector, get_selector
from trpc_sdk.naming.loadbalance import RandomLoadBalance, set_default_loadbalance
from trpc_sdk.naming.option import SelectorOption
from trpc_sdk.naming.trpc_selector import TrpcSelector
from trpc_sdk.abc.node import Node
from trpc_sdk.naming import discovery as discovery_mod
from trpc_sdk.naming import loadbalance as lb_mod


@pytest.mark.asyncio
async def test_discovery():
    discovery = IpDiscovery()
    node_list = await discovery.list("192.0.0.1:8000", "namespace")
    assert node_list[0].service_name == "192.0.0.1:8000"
    assert node_list[0].address == "192.0.0.1:8000"


@pytest.mark.asyncio
async def test_discovery_dns():
    discovery = IpDiscovery()
    node_list = await discovery.list("baidu.com:8000", "namespace")
    assert node_list[0].service_name == "baidu.com:8000"
    assert node_list[0].address != "baidu.com:8000"
    assert node_list[0].address is not None


@pytest.mark.asyncio
async def test_loadbalance():
    node_list = [Node(service_name="demo", ip="192.0.0.1", port=10065)]
    loadbalance = RandomLoadBalance()
    node = await loadbalance.select("demo", node_list, "namespace")
    assert node.address == "192.0.0.1:10065"


@pytest.mark.asyncio
async def test_circuitbreaker():
    node = Node("demo", "192.0.0.1")
    circuitbreaker = NoopCircuitBreaker()
    is_available = await circuitbreaker.available(node)
    await circuitbreaker.report(node, 1, 0)
    assert is_available is True


@pytest.mark.asyncio
async def test_selector():
    options = SelectorOption()
    options.set_discovery("IPDiscovery")
    options.set_loadbalance("RandomLB")
    options.set_circuitbreaker("NoopCB")
    trpc_selector = TrpcSelector()
    node = await trpc_selector.select("192.168.0.1:10086", options)
    assert node.address == "192.168.0.1:10086"

    await trpc_selector.report(node, 1, 0)

    ip_selector = IpSelector()
    node = await ip_selector.select("192.168.0.1:10086", options)
    assert node.address == "192.168.0.1:10086"

    dns_selector = DNSSelector()
    node = await dns_selector.select("baidu.com:80", options)
    assert node.address != "baidu.com:80"

@pytest.fixture(autouse=True)
def _restore_naming_defaults():
    old_discovery = discovery_mod.DefaultDiscovery
    old_lb = lb_mod.DefaultLoadBalance
    yield
    discovery_mod.DefaultDiscovery = old_discovery
    lb_mod.DefaultLoadBalance = old_lb


def test_set_default_discovery_and_loadbalance():
    set_default_discovery("CustomDiscovery")
    assert discovery_mod.DefaultDiscovery == "CustomDiscovery"
    set_default_loadbalance("CustomLB")
    assert lb_mod.DefaultLoadBalance == "CustomLB"


def test_resolve_host_ipv6_bracket():
    ip, port = resolve_host("[::1]:8080")
    assert ip == "::1"
    assert port == 8080


def test_resolve_host_ipv6_wrong_address():
    with pytest.raises(Exception, match="wrong address"):
        resolve_host("[::1]")


def test_resolve_host_strips_query():
    ip, port = resolve_host("127.0.0.1:9000?foo=1", resolve_dns=False)
    assert ip == "127.0.0.1"
    assert port == 9000


def test_safe_gethostbyname_gaierror(monkeypatch):
    def boom(_host):
        raise socket.gaierror("fail")

    monkeypatch.setattr(socket, "gethostbyname", boom)
    assert discovery_mod._safe_gethostbyname("no.such.host") is None


@pytest.mark.asyncio
async def test_ip_selector_empty_service_name():
    opt = SelectorOption()
    with pytest.raises(Exception, match="service_name empty"):
        await IpSelector().select("", opt)
    with pytest.raises(Exception, match="service_name empty"):
        await DNSSelector().select("", opt)


@pytest.mark.asyncio
async def test_ip_selector_multi_addr_and_report():
    opt = SelectorOption()
    selector = IpSelector()
    node = await selector.select("127.0.0.1:8000,127.0.0.1:8001", opt)
    assert node.ip == "127.0.0.1"
    assert node.port in (8000, 8001)
    await selector.report(node, 1, 1)
    await DNSSelector().report(node, 1, 0)


def test_get_selector_plugin():
    assert get_selector("ip") is not None


@pytest.mark.asyncio
async def test_random_loadbalance_empty_nodes():
    with pytest.raises(Exception, match="ErrNoServerAvailable"):
        await RandomLoadBalance().select("svc", [], "ns")


def test_selector_option_setters():
    opt = SelectorOption()
    opt.set_key("hash-key")
    assert opt.key == "hash-key"

    opt.set_env_key("env")
    assert opt.env_key == "env"
    assert opt.servicerouter_opt.env_key == "env"

    opt.set_servicerouter("polaris")
    assert opt.servicerouter == "polaris"

    opt.set_disable_service_router(True)
    assert opt.disable_service_router is True
    assert opt.servicerouter_opt.disable_service_router is True
