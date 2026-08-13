# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Config decoder plugins.

Decoders turn the raw bytes returned by a data provider into a Python object,
usually a ``dict``. They are registered as ``CONFIG_DECODER`` plugins and are
selected by name through :class:`~trpc_sdk.config._parser.TrpcConfigParser` or
:func:`~trpc_sdk.config._options.with_codec`.

The ``code`` decoder option selects the text encoding of the raw bytes and
defaults to ``utf8``.
"""

from __future__ import annotations

import json
from typing import Optional
from typing import Dict

import yaml

from trpc_sdk.abc import BaseConfigDecoder
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_ENCODING


class _OptionsDecoder(BaseConfigDecoder):
    """Base for decoders that only need to remember their options."""

    def __init__(self):
        super().__init__()
        self._options: Optional[Dict] = None

    def set_options(self, options: Dict) -> None:
        self._options = options

    def _resolve_encoding(self, options: Optional[dict]) -> str:
        """Read the text encoding from the decoder options.
    
        Args:
            options: Decoder options, may be ``None``.
    
        Returns:
            The configured encoding, or ``utf8`` when unset.
        """
        if options is not None and 'code' in options:
            return options.get('code') or DEFAULT_ENCODING
        return DEFAULT_ENCODING


@register_plugin(PluginType.CONFIG_DECODER, 'yaml')
class YamlDecoder(_OptionsDecoder):
    """Decodes YAML config data into a dict."""

    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> Dict:
        config_data_str = config_data.decode(self._resolve_encoding(options))
        return yaml.load(config_data_str, Loader=yaml.FullLoader)


@register_plugin(PluginType.CONFIG_DECODER, 'json')
class JsonDecoder(_OptionsDecoder):
    """Decodes JSON config data into a dict."""

    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> Dict:
        config_data_str = config_data.decode(self._resolve_encoding(options))
        return json.loads(config_data_str)


@register_plugin(PluginType.CONFIG_DECODER, 'noop')
class NoopDecoder(BaseConfigDecoder):
    """Returns the raw config data unchanged.

    Note: because raw bytes are returned rather than a dict, the
    ``ConfigLoader.get()`` interface is not supported with this decoder.
    """

    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> bytes:
        return config_data

    def set_options(self, options: Dict) -> None:
        return None
