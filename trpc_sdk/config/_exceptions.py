# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Configuration subsystem exception definitions."""

from __future__ import annotations

from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException


class ConfigError(TrpcException):
    """Base class for every error raised by the configuration subsystem."""

    def __init__(self, msg: str = ""):
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CONFIG_ERR, msg)

    def __str__(self) -> str:
        return self.msg

    def __repr__(self) -> str:
        return self.__str__()


class GetConfigKeyError(ConfigError):
    """Raised when a config key cannot be read.

    Args:
        key: The config key that failed to resolve.
        msg: Human readable description of the failure.
    """

    def __init__(self, key: str, msg: str = ""):
        super().__init__(msg)
        self.key = key

    def __str__(self) -> str:
        return f'Config key {self.key} error: {self.msg}'


class ConfigDecoderError(ConfigError):
    """Raised when a config decoder fails to turn raw config data into a dict.

    Args:
        decoder_name: Name of the config decoder that failed.
        msg: Human readable description of the failure.
    """

    def __init__(self, decoder_name: str, msg: str = ''):
        super().__init__(msg)
        self.decoder_name = decoder_name

    def __str__(self) -> str:
        return f'Decoder {self.decoder_name} decode error: {self.msg}'


class ProviderError(ConfigError):
    """Raised when a provider fails to read the raw config data.

    Args:
        provider: Name of the provider that failed.
        msg: Human readable description of the failure.
    """

    def __init__(self, provider: str, msg: str = ""):
        super().__init__(msg)
        self.provider = provider


class ProviderNotExist(ConfigError):
    """Raised when the requested provider is not registered.

    Args:
        provider: Name of the missing provider.
        msg: Human readable description of the failure.
    """

    def __init__(self, provider: str, msg: str = ""):
        super().__init__(msg)
        self.provider = provider

    def __str__(self) -> str:
        return f'provider: {self.provider} not exist'
