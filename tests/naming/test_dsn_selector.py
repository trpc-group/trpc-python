# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import asyncio
import threading
import time

from typing import NoReturn
from unittest import mock

import pytest

from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.abc import ServiceNameExtractor
from trpc_sdk.naming import ResolveSelector
from trpc_sdk.naming import get_default_selector
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER


@pytest.mark.asyncio
async def test_dns_selector_report():
    selector = get_default_selector()
    ret = await selector.report(Node(service_name="test"), 1, 1)
    assert ret is None


@pytest.mark.asyncio
async def test_dns_selector_select_error():
    selector = get_default_selector()
    opts = SelectorOption()
    with pytest.raises(Exception) as e:
        await selector.select("", opts)


@pytest.mark.asyncio
async def test_dns_selector_select():
    selector = get_default_selector()
    opts = SelectorOption()
    node = await selector.select("test", opts)
    assert node.service_name == "test"
    assert node.address == "test"


class FakeSelector(BaseSelector):
    """
    FakeSelector a fake selector for test
    """

    def __init__(self):
        self.addrs = {"abc": "127.0.0.1:8080"}

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        if service_name == "":
            raise Exception("service name can not be empty")
        addr = self.addrs[service_name]
        if addr is None:
            raise Exception("addr not found")
        return Node(service_name=service_name, address=addr)

    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        pass


class service_name_extractor(ServiceNameExtractor):

    def extract(self, dsn: str) -> tuple:
        idx = dsn.find('@')
        if idx + 1 >= len(dsn):
            return (0, 0)
        post = idx + 1
        length = len(dsn) - post
        return (post, length)


@pytest.mark.asyncio
async def test_resolve_selector():
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_SELECTOR, "empty",
                                        ResolveSelector("", service_name_extractor()))
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_SELECTOR, "noextractor", ResolveSelector("", None))
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_SELECTOR, "fake", FakeSelector())
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_SELECTOR, "dsn+fake",
                                        ResolveSelector("fake", service_name_extractor()))

    s = DEFAULT_PLUGIN_MANAGER.get_instance(PluginType.NAMING_SELECTOR, "dsn+fake")
    assert s is not None
    node = await s.select("@abc", SelectorOption())
    assert node.address == "@127.0.0.1:8080"

    ret = await s.report(Node(service_name="test"), 1, 1)
    assert ret is None

    empty_s = DEFAULT_PLUGIN_MANAGER.get_instance(PluginType.NAMING_SELECTOR, "empty")
    assert empty_s is not None
    with pytest.raises(ValueError, match="not instantiated"):
        node = await empty_s.select("@abc", SelectorOption())

    noextractor_s = DEFAULT_PLUGIN_MANAGER.get_instance(PluginType.NAMING_SELECTOR, "noextractor")
    assert noextractor_s is not None
    with pytest.raises(ValueError, match="extractor is None"):
        node = await noextractor_s.select("@abc", SelectorOption())
