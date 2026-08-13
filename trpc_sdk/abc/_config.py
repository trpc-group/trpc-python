# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Configuration extension interfaces.

This module defines the abstract contracts for configuration loading, data
providers, and decoders used by pluggable configuration backends.
"""
from __future__ import annotations

from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Optional
from typing import Dict

from ._plugin import PluginType
from ._plugin import BasePlugin

class BaseConfig(BasePlugin):
    """Define configuration loading and value access.

    A concrete implementation loads configuration from a named data source,
    decodes it with a named decoder, and exposes typed lookups and object
    hydration helpers.
    """

    def __init__(
        self,
        provider_name: str,
        decoder_name: str,
        provider_options: Dict,
        decoder_options: Dict
    ) -> None:
        """Initialize the configuration backend identity and options.

        Args:
            provider_name: Registered config provider name.
            decoder_name: Registered config decoder name.
            provider_options: Options passed to the config provider.
            decoder_options: Options passed to the config decoder.
        """
        super().__init__()
        self._type = PluginType.CONFIG
        self._provider_name = provider_name
        self._decoder_name = decoder_name
        self._provider_options = provider_options
        self._decoder_options = decoder_options
        self._raw_config_dict: Dict = {}

    @abstractmethod
    def load(self) -> Dict:
        """Load and decode the complete configuration.

        Returns:
            Decoded configuration dictionary.
        """
        ...

    @abstractmethod
    def get(self, key: str, default: Any) -> Any:
        """Return one configuration value by key.

        Args:
            key: Configuration key to look up.
            default: Value returned when the key is absent.

        Returns:
            Stored configuration value, or ``default`` when missing.
        """
        ...

    @abstractmethod
    def parse(self, object_dict: Dict) -> None:
        """Hydrate nested objects from decoded configuration.

        Args:
            object_dict: Nested mapping that mirrors the configuration
                hierarchy. Leaf values are mutable objects that receive
                decoded fields.
        """
        ...

    def set_raw_config(self, raw_config_dict: Dict) -> None:
        """Replace the in-memory raw configuration dictionary.

        Args:
            raw_config_dict: Decoded configuration to store.
        """
        self._raw_config_dict = raw_config_dict


class BaseConfigProvider(BasePlugin):
    """Define config provider access and hot-reload."""

    def __init__(self) -> None:
        super().__init__()
        self._type = PluginType.CONFIG_PROVIDER

    @abstractmethod
    def load(self, file_path: str, options: Optional[Dict] = None) -> bytes:
        """Load raw config bytes from the provider.

        Args:
            file_path: Path or identifier of the config resource.
            options: Optional provider-specific load options.

        Returns:
            Raw config bytes.
        """
        ...

    @abstractmethod
    def hot_reload(self, key: str, callback: Callable, options: Optional[Dict] = None) -> None:
        """Hot-reload the provider and invoke a callback on changes.

        Args:
            key: Key of the config to hot-reload.
            callback: Callable invoked when the config changes.
            options: Optional provider-specific hot-reload options.
        """
        ...

    @abstractmethod
    def set_options(self, options: Dict) -> None:
        """Update config provider options.

        Args:
            options: Config provider-specific options.
        """
        ...


class BaseConfigDecoder(BasePlugin):
    """Define decoding of raw config bytes into dictionaries."""

    def __init__(self) -> None:
        super().__init__()
        self._type = PluginType.CONFIG_DECODER

    @abstractmethod
    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> Dict:
        """Decode raw config bytes into a dictionary.

        Args:
            config_data: Raw config payload.
            options: Optional decoder-specific options.

        Returns:
            Decoded config dictionary.
        """
        ...

    @abstractmethod
    def set_options(self, options: Dict) -> None:
        """Update config decoder options.

        Args:
            options: Config decoder-specific options.
        """
        ...
