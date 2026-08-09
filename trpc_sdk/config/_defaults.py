# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Post-load repair of config values.

The config file lets a value be declared once at a parent level and inherited
by its children: a service inherits protocol and network from ``server``, a
backend inherits namespace, discovery and filters from ``client``. Nothing in
the file format expresses that, so the inheritance is applied here after
deserializing, by filling in every field the child left empty.

Service IPs are resolved from NIC names in the same pass.
"""

from __future__ import annotations

from typing import Any
from typing import List
from typing import Optional

from trpc_sdk.utils import get_ip_with_nic
from trpc_sdk.utils import join_host_port
from ._types import ClientConfig
from ._types import ClientServiceConfig
from ._types import Config


def _set_default_val(obj: Any, attr: str, default: Any) -> None:
    """Fill ``attr`` on ``obj`` when it is still empty.

    Empty means ``None`` or ``""``. ``0`` and ``False`` are kept, so numeric
    and boolean fields are not overwritten.
    """
    if getattr(obj, attr) in (None, ""):
        setattr(obj, attr, default)


def _repair_config(cfg: Config) -> None:
    """Fill in missing service/client config fields from parent defaults.

    Args:
        cfg: The top-level config to repair.
    """
    _repair_service_ip_with_nic(cfg)

    for service in cfg.server.service:
        _set_default_val(service, "protocol", cfg.server.protocol)
        _set_default_val(service, "network", cfg.server.network)
        _set_default_val(service, "ip", cfg.global_config.local_ip)
        _set_default_val(service, "address", join_host_port(service.ip, int(service.port)))

    _set_default_val(cfg.client, "namespace", cfg.global_config.namespace)

    for backend_conf in cfg.client.service:
        _repair_client_config(backend_conf, cfg.client)


def _repair_service_ip_with_nic(cfg: Config) -> None:
    """Resolve service IPs from NIC names when IP is not explicitly set.

    Also populates ``cfg.global_config.local_ip`` from the first service
    if it is empty.

    Args:
        cfg: The top-level config to repair.
    """
    for service in cfg.server.service:
        _set_default_val(service, "ip", get_ip_with_nic(service.nic))
        _set_default_val(cfg.global_config, "local_ip", service.ip)

    if cfg.server.admin is not None and cfg.server.admin.nic == "":
        _set_default_val(cfg.server.admin, "ip", get_ip_with_nic(cfg.server.admin.nic))


def _repair_client_config(conf: ClientServiceConfig, cfg: ClientConfig) -> None:
    """Fill in missing backend config fields from client-level defaults.

    Args:
        conf: A single backend service config to repair.
        cfg: The client-level config providing defaults.
    """
    _set_default_val(conf, "callee", conf.service_name)
    _set_default_val(conf, "service_name", conf.callee)
    _set_default_val(conf, "namespace", cfg.namespace)
    _set_default_val(conf, "network", cfg.network)
    _set_default_val(conf, "protocol", cfg.protocol)

    if conf.target == "":
        _set_default_val(conf, "discovery", cfg.discovery)
        _set_default_val(conf, "load_balance", cfg.load_balance)
        _set_default_val(conf, "circuit_breaker", cfg.circuit_breaker)

    if conf.filters is not None:
        prepend_client_filters(conf, cfg.filters)


def merge_client_service_config(client_conf, service: ClientServiceConfig):
    """Apply the client-level shared parameters to a client service config.

    Only fields the backend has not set itself are filled in.

    Args:
        client_conf: The client-level config holding the shared parameters.
        service: The backend config to fill in.
    """
    for key in dir(client_conf):
        if not key.startswith("_") and hasattr(service, key):
            value = getattr(client_conf, key)
            _apply_default_field(key, service, value)


def _apply_default_field(key, service: ClientServiceConfig, value):
    """Apply one client-level shared parameter to a backend config.

    Lists are merged with the client-level values first; other fields are only
    filled in when the backend has not set them.

    Args:
        key: Name of the shared parameter.
        service: The backend config to fill in.
        value: Value of the shared parameter.
    """
    if isinstance(value, list) and isinstance(getattr(service, key), list):
        setattr(service, key, value + getattr(service, key))
    if getattr(service, key) is None:
        setattr(service, key, value)


def prepend_client_filters(conf: ClientServiceConfig, parent_filters: Optional[List[str]]):
    """Insert filters in front of the backend's existing ones, de-duplicating.

    Args:
        conf: The backend config whose filters are updated.
        parent_filters: The filters to insert first.
    """
    if parent_filters is None:
        return

    filters = []
    filters.extend(parent_filters)

    if conf.filters is not None:
        for item in conf.filters:
            if item not in filters:
                filters.append(item)

    conf.filters = filters
 