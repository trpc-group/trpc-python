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
        super(ConfigError, self).__init__(
            TrpcErrorType.FRAMEWORK,
            TrpcErrorCode.TRPC_CONFIG_KEY_ERR,
            msg,
        )
        self.key = key

    def __str__(self) -> str:
        return f'Config key {self.key} error: {self.msg}'


class DecodeError(ConfigError):
    """Raised when a decoder fails to turn raw config data into a dict.

    Args:
        decoder: Name of the decoder that failed.
        msg: Human readable description of the failure.
    """

    def __init__(self, decoder: str, msg: str = ''):
        super(ConfigError, self).__init__(
            TrpcErrorType.FRAMEWORK,
            TrpcErrorCode.TRPC_CONFIG_DECODE_ERR,
            msg,
        )
        self.decoder = decoder

    def __str__(self) -> str:
        return f'Decoder {self.decoder} decode error: {self.msg}'


class DataSourceError(ConfigError):
    """Raised when a data provider fails to read the raw config data.

    Args:
        data_source: Name of the data source that failed.
        msg: Human readable description of the failure.
    """

    def __init__(self, data_source: str, msg: str = ""):
        super(ConfigError, self).__init__(
            TrpcErrorType.FRAMEWORK,
            TrpcErrorCode.TRPC_CONFIG_DATASOURCE_ERR,
            msg,
        )
        self.data_source = data_source


class DataSourceNotExist(ConfigError):
    """Raised when the requested data source is not registered.

    Args:
        data_source: Name of the missing data source.
        msg: Human readable description of the failure.
    """

    def __init__(self, data_source: str, msg: str = ""):
        super(ConfigError, self).__init__(
            TrpcErrorType.FRAMEWORK,
            TrpcErrorCode.TRPC_CONFIG_DATASOURCE_NOT_EXIST_ERR,
            msg,
        )
        self.data_source = data_source

    def __str__(self) -> str:
        return f'datasource: {self.data_source} not exist'
