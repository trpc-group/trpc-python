# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Deprecated client-config shim over :class:`~trpc_sdk.config.TrpcConfig`.

Prefer the corresponding :class:`~trpc_sdk.config.TrpcConfig` classmethods.
"""

from __future__ import annotations

from typing import Dict

from trpc_sdk.config import ClientServiceConfig
from trpc_sdk.config import TrpcConfig


def get_client_service_config(service_name: str) -> ClientServiceConfig:
    """Return the backend config registered under ``service_name``.

    Deprecated: use :meth:`TrpcConfig.client_service_config`.

    Args:
        service_name: Callee service name from the peer's protocol file.

    Returns:
        The registered :class:`ClientServiceConfig`, or the fallback one.
    """
    return TrpcConfig.client_service_config(service_name)


def parse_client_config(conf):
    """Parse client config into the process-wide registry.

    Deprecated: use :meth:`TrpcConfig.parse_client_config`.

    Args:
        conf: Config parser or mapping passed through to ``TrpcConfig``.
    """
    return TrpcConfig.parse_client_config(conf)


def set_default_client_config(conf: Dict[str, ClientServiceConfig]):
    """Replace the whole backend-config registry after the user parsed config.

    Deprecated: use :meth:`TrpcConfig.set_client_service_configs`.

    Args:
        conf: Backend configs keyed by callee service name.
    """
    return TrpcConfig.set_client_service_configs(conf)


def set_client_service_config(callee_service_name: str, conf: ClientServiceConfig):
    """Register a single backend config after the user parsed config.

    Not safe for concurrent calls.

    Deprecated: use :meth:`TrpcConfig.set_client_service_config`.

    Args:
        callee_service_name: Callee service name to register under.
        conf: Backend config to register.
    """
    return TrpcConfig.set_client_service_config(callee_service_name, conf)
