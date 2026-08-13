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

from ._types import ClientConfig
from ._types import ClientServiceConfig
from ._types import Config
from ._utils import get_ip_with_nic
from ._utils import join_host_port


def _set_default_val(obj: Any, attr: str, default: Any) -> None:
    """Fill attr on obj when it is still empty.

    Empty means ``None`` or ``""``. ``0`` and ``False`` are kept, so numeric
    and boolean fields are not overwritten.

    Args:
        obj: The object to fill the attribute on.
        attr: The attribute to fill.
        default: The default value to fill.
    """
    if getattr(obj, attr) in (None, ""):
        setattr(obj, attr, default)


def _repair_service_ip_with_nic(cfg: Config) -> None:
    """Resolve service IPs from NIC names when IP is not explicitly set.

    Also populates cfg.global_config.local_ip from the first service
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


def repair_config(cfg: Config) -> None:
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

    for client_service_conf in cfg.client.service:
        _repair_client_config(client_service_conf, cfg.client)


def _apply_default_field(client_service_conf: ClientServiceConfig, key: str, value: Any) -> None:
    """Apply one client-level shared parameter to a client service config.

    Lists are merged with the client-level values first; other fields are only
    filled in when the client service has not set them.

    Args:
        key: Name of the shared parameter.
        client_service_conf: The client service config to fill in.
        value: Value of the shared parameter.
    """
    if isinstance(value, list) and isinstance(getattr(client_service_conf, key), list):
        setattr(client_service_conf, key, value + getattr(client_service_conf, key))
    if getattr(client_service_conf, key) is None:
        setattr(client_service_conf, key, value)


def merge_client_service_config(client_conf: ClientConfig, client_service_conf: ClientServiceConfig):
    """Apply the client-level shared parameters to a client service config.

    Only fields the client service has not set itself are filled in.

    Args:
        client_conf: The client-level config holding the shared parameters.
        client_service_conf: The client service config to fill in.
    """
    for key in dir(client_conf):
        if not key.startswith("_") and hasattr(client_service_conf, key):
            value = getattr(client_conf, key)
            _apply_default_field(client_service_conf, key, value)


def prepend_client_filters(client_service_conf: ClientServiceConfig, parent_filters: Optional[List[str]]):
    """Insert filters in front of the client service's existing ones, de-duplicating.

    Args:
        client_service_conf: The client service config whose filters are updated.
        parent_filters: The filters to insert first.
    """
    if parent_filters is None:
        return

    filters = []
    filters.extend(parent_filters)

    if client_service_conf.filters is not None:
        for item in client_service_conf.filters:
            if item not in filters:
                filters.append(item)

    client_service_conf.filters = filters
