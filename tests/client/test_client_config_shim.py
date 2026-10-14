# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test trpc.client.config shim forwards to client_config."""

from trpc_sdk.client import config as client_config_shim
from trpc_sdk.config import client_config as cli_config


def test_client_config_shim_forwards(monkeypatch):
    calls = {}

    def fake_client_config(service_name):
        calls["client_config"] = service_name
        return "backend"

    def fake_unmarshal(conf):
        calls["unmarshal"] = conf
        return "unmarshaled"

    def fake_merge(client_conf, service):
        calls["merge"] = (client_conf, service)
        return service

    def fake_register_config(conf):
        calls["register_config"] = conf

    def fake_register_client_config(name, conf):
        calls["register_client_config"] = (name, conf)

    monkeypatch.setattr(cli_config, "client_config", fake_client_config)
    monkeypatch.setattr(cli_config, "unmarshal_client_config", fake_unmarshal)
    monkeypatch.setattr(cli_config, "merge_backend_config", fake_merge)
    monkeypatch.setattr(cli_config, "register_config", fake_register_config)
    monkeypatch.setattr(cli_config, "register_client_config", fake_register_client_config)

    assert client_config_shim.client_config("svc") == "backend"
    assert client_config_shim.unmarshal_client_config({"a": 1}) == "unmarshaled"
    backend = cli_config.BackendConfig()
    assert client_config_shim.merge_backend_config(None, backend) is backend
    client_config_shim.register_config({"svc": backend})
    client_config_shim.register_client_config("svc", backend)

    assert calls["client_config"] == "svc"
    assert calls["unmarshal"] == {"a": 1}
    assert calls["merge"][1] is backend
    assert calls["register_config"] == {"svc": backend}
    assert calls["register_client_config"] == ("svc", backend)
