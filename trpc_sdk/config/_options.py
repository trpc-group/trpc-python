# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Option builders for :class:`~trpc_sdk.config.ConfigLoader`.

These ``with_*`` helpers are passed to
:meth:`~trpc_sdk.config.ConfigLoaderFactory.create` (and the load wrappers)
to select a decoder or data provider::

    from trpc_sdk.config import ConfigLoaderFactory, with_codec, with_provider

    cfg = ConfigLoaderFactory.sync_load(
        "my.yaml", with_provider("file"), with_codec("YamlDecoder")
    )
"""

from __future__ import annotations

from typing import Callable

from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin

from ._exceptions import ConfigDecoderError
from ._exceptions import ProviderNotExist
from ._loader import ConfigLoader


def with_codec(name: str) -> Callable:
    """Build an option selecting the decoder plugin to use.

    Args:
        name: Registered ``CONFIG_DECODER`` plugin name.

    Returns:
        An option callable accepted by
        :meth:`~trpc_sdk.config.ConfigLoaderFactory.create`.
    """

    def option(config: ConfigLoader) -> None:
        decoder = get_plugin(PluginType.CONFIG_DECODER, name)
        if decoder is None:
            raise ConfigDecoderError(name)
        config.set_decoder(decoder)

    return option


def with_provider(name: str) -> Callable:
    """Build an option selecting the data provider to use.

    Args:
        name: Data provider name registered via ``set_provider``.

    Returns:
        An option callable accepted by
        :meth:`~trpc_sdk.config.ConfigLoaderFactory.create`.
    """

    def option(config: ConfigLoader) -> None:

        provider = get_plugin(PluginType.CONFIG_PROVIDER, name)
        if provider is None:
            raise ProviderNotExist(name)
        config.set_provider(provider)

    return option
