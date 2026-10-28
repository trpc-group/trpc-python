# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test config repair and dict-to-object assignment."""

import sys
from typing import List
from typing import Optional
from typing import Union

from trpc_sdk.config import Admin
from trpc_sdk.config import ClientConfig
from trpc_sdk.config import ClientServiceConfig
from trpc_sdk.config import Config
from trpc_sdk.config import GlobalConfig
from trpc_sdk.config import ServerConfig
from trpc_sdk.config import ServerService
from trpc_sdk.config import TrpcConfigParser
from trpc_sdk.config._defaults import _repair_client_config
from trpc_sdk.config._defaults import repair_config


class _Child:
    def __init__(self, name: str = ""):
        self.name = name


class _Holder:
    def __init__(
        self,
        child: Optional[_Child] = None,
        mapping: dict = None,
        items: List[_Child] = None,
        tags: List[str] = None,
        raw_list: list = None,
        count: int = 0,
    ):
        self.child = child
        self.mapping = mapping
        self.items = items
        self.tags = tags
        self.raw_list = raw_list
        self.count = count


def test_assign_dict_optional_and_list_branches():
    holder = _Holder()
    TrpcConfigParser.populate_from_dict(
        {
            "child": {"name": "a"},
            "mapping": {"k": "v"},
            "items": [{"name": "x"}, ["ignored"], "str-item"],
            "tags": ["t1"],
            "raw_list": [1, 2],
            "count": None,
        },
        holder,
    )
    assert holder.child.name == "a"
    assert holder.mapping == {"k": "v"}
    assert len(holder.items) == 2
    assert holder.items[0].name == "x"
    assert holder.items[1] == "str-item"
    assert holder.tags == ["t1"]
    assert holder.raw_list == [1, 2]
    assert holder.count == 0


def test_assign_dict_union_optional_unpack():
    class OptHolder:
        def __init__(self, value: Union[int, None] = None):
            self.value = value

    obj = OptHolder()
    TrpcConfigParser.populate_from_dict({"value": 7}, obj)
    assert obj.value == 7


def test_repair_config_fills_defaults(monkeypatch):
    cfg_module = sys.modules["trpc_sdk.config._defaults"]
    monkeypatch.setattr(cfg_module, "get_ip_with_nic", lambda nic: f"10.0.0.{nic or '0'}")
    svc = ServerService(ip="", nic="1", port=8080, protocol="", network="", address="")
    # Admin IP is filled only when nic is empty.
    admin = Admin(ip="", nic="", port=9028)
    server = ServerConfig(service=[svc], admin=admin, protocol="trpc", network="tcp")
    client = ClientConfig(
        namespace="",
        network="tcp",
        protocol="trpc",
        discovery="IPDiscovery",
        load_balance="RandomLB",
        circuit_breaker="NoopCB",
        filters=["f1"],
        service=[],
    )
    backend = ClientServiceConfig(
        callee="",
        service_name="demo",
        namespace="",
        network="",
        protocol="",
        target="",
        discovery="",
        load_balance="",
        circuit_breaker="",
        filters=["f0"],
    )
    client.service = [backend]
    cfg = Config(
        global_config=GlobalConfig(namespace="Production", local_ip=""),
        server=server,
        client=client,
    )

    repair_config(cfg)

    assert svc.protocol == "trpc"
    assert svc.network == "tcp"
    assert svc.ip == "10.0.0.1"
    assert svc.address == "10.0.0.1:8080"
    assert cfg.global_config.local_ip == "10.0.0.1"
    assert admin.ip == "10.0.0.0"
    assert cfg.client.namespace == "Production"
    assert backend.callee == "demo"
    assert backend.namespace == "Production"
    assert backend.network == "tcp"
    assert backend.protocol == "trpc"
    assert backend.discovery == "IPDiscovery"
    assert backend.load_balance == "RandomLB"
    assert backend.circuit_breaker == "NoopCB"


def test_repair_client_config_callee_from_empty_service_name():
    client = ClientConfig(namespace="ns", network="udp", protocol="http")
    conf = ClientServiceConfig(callee="only-callee", service_name="", namespace="x", target="ip://1")
    _repair_client_config(conf, client)
    assert conf.service_name == "only-callee"
