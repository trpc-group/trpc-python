# -*- coding: utf-8 -*-
"""Test config.repair_config and assign_dict_to_object branches."""

import sys
from typing import List, Optional, Union

from trpc_sdk.config.client_config import BackendConfig, Client
from trpc_sdk.config.config import (
    Admin,
    Config,
    Global,
    Service,
    Server,
    TRPCConfig,
    repair_client_config,
    repair_config,
)


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
    TRPCConfig.assign_dict_to_object(
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
    TRPCConfig.assign_dict_to_object({"value": 7}, obj)
    assert obj.value == 7


def test_repair_config_fills_defaults(monkeypatch):
    cfg_module = sys.modules["trpc_sdk.config.config"]
    monkeypatch.setattr(cfg_module, "get_ip_with_nic", lambda nic: f"10.0.0.{nic or '0'}")
    svc = Service(ip="", nic="1", port=8080, protocol="", network="", address="")
    admin = Admin(ip="", nic="2", port=9028)
    server = Server(service=[svc], admin=admin, protocol="trpc", network="tcp")
    client = Client(
        namespace="",
        network="tcp",
        protocol="trpc",
        discovery="IPDiscovery",
        loadbalance="RandomLB",
        circuitbreaker="NoopCB",
        filter=["f1"],
        service=[],
    )
    backend = BackendConfig(
        callee="",
        service_name="demo",
        namespace="",
        network="",
        protocol="",
        target="",
        discovery="",
        loadbalance="",
        circuitbreaker="",
        filter=["f0"],
    )
    client.service = [backend]
    cfg = Config(
        global_config=Global(namespace="Production", local_ip=""),
        server=server,
        client=client,
    )

    repair_config(cfg)

    assert svc.protocol == "trpc"
    assert svc.network == "tcp"
    assert svc.ip == "10.0.0.1"
    assert svc.address == "10.0.0.1:8080"
    assert cfg.global_config.local_ip == "10.0.0.1"
    assert admin.ip == "10.0.0.2"
    assert cfg.client.namespace == "Production"
    assert backend.callee == "demo"
    assert backend.namespace == "Production"
    assert backend.network == "tcp"
    assert backend.protocol == "trpc"
    assert backend.discovery == "IPDiscovery"
    assert backend.loadbalance == "RandomLB"
    assert backend.circuitbreaker == "NoopCB"


def test_repair_client_config_callee_from_empty_service_name():
    client = Client(namespace="ns", network="udp", protocol="http")
    conf = BackendConfig(callee="only-callee", service_name="", namespace="x", target="ip://1")
    repair_client_config(conf, client)
    assert conf.service_name == "only-callee"
