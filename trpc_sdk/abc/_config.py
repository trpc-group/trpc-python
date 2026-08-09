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

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Callable
from typing import Optional
from typing import Dict


class BaseConfig(ABC):
    """Define configuration loading and value access.

    A concrete implementation loads configuration from a named data source,
    decodes it with a named decoder, and exposes typed lookups and object
    hydration helpers.
    """

    def __init__(
        self,
        data_source_name: str,
        decoder_name: str,
        ds_options: Dict,
        de_options: Dict
    ) -> None:
        """Initialize the configuration backend identity and options.

        Args:
            data_source_name: Registered data provider name.
            decoder_name: Registered decoder name.
            ds_options: Options passed to the data provider.
            de_options: Options passed to the decoder.
        """
        self._data_source_name = data_source_name
        self._decoder_name = decoder_name
        self._ds_options = ds_options
        self._de_options = de_options
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
    def deserialize(self, object_dict: Dict) -> None:
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


class BaseDataProvider(ABC):
    """Define configuration data source access and change watching."""

    @abstractmethod
    def load(self, file_path: str, options: Optional[Dict] = None) -> bytes:
        """Load raw configuration bytes from the data source.

        Args:
            file_path: Path or identifier of the configuration resource.
            options: Optional provider-specific load options.

        Returns:
            Raw configuration bytes.
        """
        ...

    @abstractmethod
    def watch(self, key: str, callback: Callable, options: Optional[Dict] = None) -> None:
        """Watch the data source and invoke a callback on changes.

        Args:
            key: Key of the configuration to watch.
            callback: Callable invoked when the configuration changes.
            options: Optional provider-specific watch options.
        """
        ...

    @abstractmethod
    def set_options(self, options: Dict) -> None:
        """Update provider options.

        Args:
            options: Provider-specific configuration options.
        """
        ...


class BaseDecoder(ABC):
    """Define decoding of raw configuration bytes into dictionaries."""

    @abstractmethod
    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> Dict:
        """Decode raw configuration bytes into a dictionary.

        Args:
            config_data: Raw configuration payload.
            options: Optional decoder-specific options.

        Returns:
            Decoded configuration dictionary.
        """
        ...

    @abstractmethod
    def set_options(self, options: Dict) -> None:
        """Update decoder options.

        Args:
            options: Decoder-specific configuration options.
        """
        ...
