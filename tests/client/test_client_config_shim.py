# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client config helpers forward to TrpcConfig."""

from trpc_sdk.client._config import get_client_service_config
from trpc_sdk.client._config import parse_client_config
from trpc_sdk.client._config import set_client_service_config
from trpc_sdk.client._config import set_default_client_config
from trpc_sdk.config import ClientConfig
from trpc_sdk.config import ClientServiceConfig
from trpc_sdk.config import TrpcConfig
from trpc_sdk.config import merge_client_service_config


def test_client_config_helpers_forward(monkeypatch):
    calls = {}

    def fake_client_config(cls, service_name):
        calls["client_config"] = service_name
        return "backend"

    def fake_parse(cls, conf):
        calls["parse"] = conf
        return "parsed"

    def fake_set_all(cls, conf):
        calls["set_all"] = conf

    def fake_set_one(cls, name, conf):
        calls["set_one"] = (name, conf)

    monkeypatch.setattr(TrpcConfig, "client_service_config", classmethod(fake_client_config))
    monkeypatch.setattr(TrpcConfig, "parse_client_config", classmethod(fake_parse))
    monkeypatch.setattr(TrpcConfig, "set_client_service_configs", classmethod(fake_set_all))
    monkeypatch.setattr(TrpcConfig, "set_client_service_config", classmethod(fake_set_one))

    assert get_client_service_config("svc") == "backend"
    assert parse_client_config({"a": 1}) == "parsed"
    backend = ClientServiceConfig(callee="svc")
    set_default_client_config({"svc": backend})
    set_client_service_config("svc", backend)

    assert calls["client_config"] == "svc"
    assert calls["parse"] == {"a": 1}
    assert calls["set_all"] == {"svc": backend}
    assert calls["set_one"] == ("svc", backend)


def test_merge_client_service_config_fills_unset_namespace():
    parent = ClientConfig(namespace="Production")
    service = ClientServiceConfig(callee="demo")
    assert merge_client_service_config(parent, service) is None
    assert service.namespace == "Production"
    assert service.callee == "demo"
