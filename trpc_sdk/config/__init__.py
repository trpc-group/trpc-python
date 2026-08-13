# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""tRPC-Python configuration module.

The package is split by responsibility:

- :mod:`~trpc_sdk.config._types` — dataclasses describing the config file
- :mod:`~trpc_sdk.config._constants` — default values for config models and the decoder
- :mod:`~trpc_sdk.config._provider` — data source plugins and the named-provider registry
- :mod:`~trpc_sdk.config._decoder` — codecs that turn raw bytes into Python objects
- :mod:`~trpc_sdk.config._parser` — loading, decoding and dict-to-object mapping
- :mod:`~trpc_sdk.config._defaults` — post-load inheritance of parent-level values
- :mod:`~trpc_sdk.config._config` — :class:`~trpc_sdk.config.TrpcConfig`, the
  process-wide config and registries
- :mod:`~trpc_sdk.config._exceptions` — config subsystem exceptions
- :mod:`~trpc_sdk.config._options` — ``with_*`` helpers for config loaders
- :mod:`~trpc_sdk.config._loader` — loading business config files via
  :class:`~trpc_sdk.config.ConfigLoaderFactory`

:class:`~trpc_sdk.config.TrpcConfig` is the entry point: it loads the startup
YAML and hands out the global, server, client and plugins sections through
classmethods.

Config files use :class:`~trpc_sdk.config.ConfigLoaderFactory`
with the ``with_*`` option helpers from :mod:`~trpc_sdk.config._options`.
Deprecated module-level client helpers (``get_client_service_config`` and
friends) remain for call-site migration.
"""

from __future__ import annotations

from ._config import TrpcConfig
from ._decoder import JsonDecoder
from ._decoder import NoopDecoder
from ._decoder import YamlDecoder
from ._defaults import merge_client_service_config
from ._defaults import prepend_client_filters
from ._exceptions import ConfigError
from ._exceptions import ProviderError
from ._exceptions import ProviderNotExist
from ._exceptions import ConfigDecoderError
from ._exceptions import GetConfigKeyError
from ._loader import ConfigLoader
from ._loader import ConfigLoaderFactory
from ._options import with_codec
from ._options import with_provider
from ._parser import TrpcConfigParser
from ._provider import FileConfigProvider
from ._types import Admin
from ._types import ClientConfig
from ._types import ClientServiceConfig
from ._types import Config
from ._types import GlobalConfig
from ._types import MemoryInspection
from ._types import QueueCircuitBreakerConfig
from ._types import Rpcz
from ._types import SaveBindInfo
from ._types import ServerConfig
from ._types import ServerService
from ._types import SocketType

__all__ = [
    # Config models
    "Admin",
    "ClientConfig",
    "ClientServiceConfig",
    "Config",
    "GlobalConfig",
    "MemoryInspection",
    "QueueCircuitBreakerConfig",
    "Rpcz",
    "SaveBindInfo",
    "ServerConfig",
    "ServerService",
    "SocketType",
    # Loading and deserializing
    "TrpcConfig",
    "TrpcConfigParser",
    # Post-load repair
    "merge_client_service_config",
    "prepend_client_filters",
    # Global state and registries
    # config loading
    "ConfigLoader",
    "ConfigLoaderFactory",
    "with_codec",
    "with_provider",
    # Exceptions
    "ConfigError",
    "GetConfigKeyError",
    "ConfigDecoderError",
    "ProviderError",
    "ProviderNotExist",
    # Plugins
    "JsonDecoder",
    "NoopDecoder",
    "YamlDecoder",
    "FileConfigProvider",
]

get_client_service_config = TrpcConfig.client_service_config
parse_client_config = TrpcConfig.parse_client_config
set_client_service_config = TrpcConfig.set_client_service_config
set_client_service_configs = TrpcConfig.set_client_service_configs
get_plugins_config = TrpcConfig.plugins_config
get_global_config = TrpcConfig.global_config
get_server_config = TrpcConfig.server_config
get_client_config = TrpcConfig.client_config
