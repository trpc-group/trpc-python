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

from trpc_sdk.abc.node import Node
from trpc_sdk.abc.selector import BaseSelector
from trpc_sdk.naming.dsn_selector import ResolveSelector
from trpc_sdk.naming.dsn_selector import ServiceNameExtractor
from trpc_sdk.naming.dsn_selector import default_selector
from trpc_sdk.naming.option import SelectorOption
from trpc_sdk.plugin import DefaultPluginManager
from trpc_sdk.plugin import PluginType


@pytest.mark.asyncio
async def test_dns_selector_report():
    selector = default_selector()
    ret = await selector.report(Node(), 1, 1)
    assert ret is None


@pytest.mark.asyncio
async def test_dns_selector_select_error():
    selector = default_selector()
    opts = SelectorOption()
    with pytest.raises(Exception) as e:
        await selector.select("", opts)


@pytest.mark.asyncio
async def test_dns_selector_select():
    selector = default_selector()
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
    DefaultPluginManager.set_instance(PluginType.NAMING_SELECTOR, "empty",
                                      ResolveSelector("", service_name_extractor()))
    DefaultPluginManager.set_instance(PluginType.NAMING_SELECTOR, "noextractor", ResolveSelector("", None))
    DefaultPluginManager.set_instance(PluginType.NAMING_SELECTOR, "fake", FakeSelector())
    DefaultPluginManager.set_instance(PluginType.NAMING_SELECTOR, "dsn+fake",
                                      ResolveSelector("fake", service_name_extractor()))

    s = DefaultPluginManager.get_instance(PluginType.NAMING_SELECTOR, "dsn+fake")
    assert s is not None
    node = await s.select("@abc", SelectorOption())
    assert node.address == "@127.0.0.1:8080"

    ret = await s.report(Node(), 1, 1)
    assert ret is None

    empty_s = DefaultPluginManager.get_instance(PluginType.NAMING_SELECTOR, "empty")
    assert empty_s is not None
    with pytest.raises(Exception) as excinfo:
        node = await empty_s.select("@abc", SelectorOption())
    assert str(excinfo.value) == "resolver selector name can not be empty"

    noextractor_s = DefaultPluginManager.get_instance(PluginType.NAMING_SELECTOR, "noextractor")
    assert noextractor_s is not None
    with pytest.raises(Exception) as excinfo:
        node = await noextractor_s.select("@abc", SelectorOption())
    assert str(excinfo.value) == "service extractor can not be empty"
