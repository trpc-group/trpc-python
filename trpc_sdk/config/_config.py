# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Process-wide tRPC configuration.

:class:`TrpcConfig` owns everything the framework used to keep in module-level
globals: the config loaded from the startup YAML file, the plugins subtree, and
the client backend registry other packages write into. The state lives in class
variables and every entry point is a classmethod, so there is no instance to
pass around and no singleton to fetch first.
"""

from __future__ import annotations

from typing import ClassVar
from typing import Dict
from typing import Optional

from ._defaults import repair_config
from ._defaults import merge_client_service_config
from ._parser import TrpcConfigParser
from ._types import ClientConfig
from ._types import ClientServiceConfig
from ._types import Config
from ._types import GlobalConfig
from ._types import ServerConfig


class TrpcConfig:
    """The framework configuration of the current process.

    This is deliberately not a singleton: the state is held in class variables
    and reached through classmethod, so callers use the class itself rather
    than an instance handed out by an accessor.
    """

    # Parser that loads, decodes, and deserializes the framework configuration.
    _config_parser: ClassVar[Optional[TrpcConfigParser]] = None
    # Typed process-wide tRPC configuration populated by the manager.
    _config: ClassVar[Config] = Config()
    # Global plugins config, used to store the global plugins config.
    _plugins_config: ClassVar[Dict] = {}
    # Fallback client config used when a callee service has no registered
    # service. Populated in place by parse_client_config from client-level defaults.
    _fallback_client_config: ClassVar[ClientServiceConfig] = ClientServiceConfig()
    # Client service configuration, parsed and populated from the user's config
    # and then read back by the framework.
    _client_service_configs: ClassVar[Dict[str, ClientServiceConfig]] = {}

    def __init__(self) -> None:
        raise TypeError("TrpcConfig keeps the process-wide config in class variables; "
                        "call its classmethod instead of instantiating it.")

    @classmethod
    def load(cls, config_path: str, file_code: str = "utf-8") -> Config:
        """Load the framework YAML into the process-wide configuration.

        Every section is loaded in one pass: the client backend registry, the
        typed global/server/client config, and the plugins subtree.

        Args:
            config_path: Path to the YAML config file.
            file_code: File encoding.

        Returns:
            The configuration that was just loaded.
        """
        cls._config_parser = TrpcConfigParser('file', 'yaml', {'file_path': config_path},
                                              {'code': file_code})
        # The parser retains the decoded dictionary for lookup and deserializing.
        cls._config_parser.load()
        if cls._config_parser.get('client') is not None:
            cls.parse_client_config(cls._config_parser)

        cls._config = cls._parse_config()

        cls._plugins_config = cls._config_parser.get("plugins")

        repair_config(cls._config)

        return cls._config

    @classmethod
    def parse_client_config(cls, config_parser: TrpcConfigParser) -> None:
        """Parse the client section into the backend registry and the fallback.

        Args:
            config_parser: The parser holding the decoded config file.
        """
        client_config = ClientConfig()
        config_parser.parse({"client": client_config})

        client_service_config = {}

        for service_conf in client_config.service:
            merge_client_service_config(client_config, service_conf)
            if service_conf.callee:
                client_service_config[service_conf.callee] = service_conf
            else:
                client_service_config[service_conf.service_name] = service_conf

        cls.set_client_service_configs(client_service_config)

        if client_config.filters is not None:
            cls._fallback_client_config.filters = client_config.filters

        if client_config.timeout > 0:
            cls._fallback_client_config.timeout = client_config.timeout

        if len(client_config.namespace) > 0:
            cls._fallback_client_config.namespace = client_config.namespace

    @classmethod
    def config(cls) -> Config:
        """Return the whole typed configuration."""
        return cls._config

    @classmethod
    def global_config(cls) -> GlobalConfig:
        """Return the ``global`` section of the configuration."""
        return cls._config.global_config

    @classmethod
    def server_config(cls) -> ServerConfig:
        """Return the ``server`` section of the configuration."""
        return cls._config.server

    @classmethod
    def client_config(cls) -> ClientConfig:
        """Return the ``client`` section of the configuration."""
        return cls._config.client

    @classmethod
    def plugins_config(cls) -> Dict:
        """Return the ``plugins`` section of the startup YAML."""
        return cls._plugins_config

    @classmethod
    def config_parser(cls) -> Optional[TrpcConfigParser]:
        """Return the loaded framework config parser, or ``None`` before load."""
        return cls._config_parser

    @classmethod
    def client_service_config(cls, service_name: str) -> ClientServiceConfig:
        """Get a service config by the callee service name from the peer's protocol file.

        Args:
            service_name: The callee service name to look up.

        Returns:
            The registered service config, or the fallback one.
        """
        return cls._client_service_configs.get(service_name, cls._fallback_client_config)

    @classmethod
    def set_client_service_config(cls, callee_service_name: str, config: ClientServiceConfig) -> None:
        """Register a single service config after the user parsed config.

        Not safe for concurrent calls.

        Args:
            callee_service_name: The callee service name to register under.
            config: The service config to register.
        """
        cls._client_service_configs[callee_service_name] = config

    @classmethod
    def set_client_service_configs(cls, configs: Dict[str, ClientServiceConfig]) -> None:
        """Replace the whole service config registry after the user parsed config.

        Args:
            configs: The service configs keyed by callee service name.
        """
        cls._client_service_configs = configs

    @classmethod
    def _parse_config(cls) -> Config:
        """Create and populate the typed process-wide tRPC configuration.

        Raises:
            RuntimeError: If :meth:`load` has not been called yet.
        """
        if cls._config_parser is None:
            raise RuntimeError("Global config is not loaded. Call TrpcConfig.load() first.")
        config = Config()
        cls._config_parser.parse(config)
        return config


get_plugins_config = TrpcConfig.plugins_config
get_global_config = TrpcConfig.global_config
get_server_config = TrpcConfig.server_config
get_client_config = TrpcConfig.client_config
