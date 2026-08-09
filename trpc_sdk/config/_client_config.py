# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Deprecated client config helpers.

Prefer the corresponding :class:`~trpc_sdk.config.TrpcConfig` classmethods.
"""

from __future__ import annotations

from typing import Dict

from ._config import TrpcConfig
from ._parser import TrpcConfigParser
from ._types import ClientServiceConfig


def parse_client_config(config_manager: TrpcConfigParser) -> None:
    """Parse client config.

    Deprecated: use :meth:`TrpcConfig.parse_client_config`.
    """
    TrpcConfig.parse_client_config(config_manager)


def get_client_service_config(service_name: str) -> ClientServiceConfig:
    """Get a backend config by the callee service name from the peer's protocol file.

    Deprecated: use :meth:`TrpcConfig.client_service_config`.

    Args:
        service_name: The callee service name to look up.

    Returns:
        The registered backend config, or the fallback one.
    """
    return TrpcConfig.client_service_config(service_name)


def set_default_client_config(config: Dict[str, ClientServiceConfig]) -> None:
    """Replace the whole backend config registry after the user parsed config.

    Deprecated: use :meth:`TrpcConfig.set_client_backend_configs`.

    Args:
        config: The backend configs keyed by callee service name.
    """
    TrpcConfig.set_client_backend_configs(config)


def set_client_service_config(callee_service_name: str, config: ClientServiceConfig) -> None:
    """Register a single backend config after the user parsed config.

    Not safe for concurrent calls.

    Deprecated: use :meth:`TrpcConfig.set_client_service_config`.

    Args:
        callee_service_name: The callee service name to register under.
        config: The backend config to register.
    """
    TrpcConfig.set_client_service_config(callee_service_name, config)
