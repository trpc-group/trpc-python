# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Configuration adapters for the logging subsystem."""

from typing import Any
from typing import Dict
from typing import List

from trpc_sdk.abc import FormatConfig
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import WriterConfig
from trpc_sdk.config import get_plugins_config


def _to_dataclass(value: Any, cls: type) -> Any:
    """Build ``cls`` from a YAML mapping; pass other values through.

    Args:
        value: A raw mapping from YAML, an already-built instance, or ``None``.
        cls: Dataclass to construct when ``value`` is a mapping.

    Returns:
        A ``cls`` instance when ``value`` is a dict, otherwise ``value``.
    """
    return cls(**value) if isinstance(value, dict) else value


def parse_output_configs(output_dict_configs: List[Dict]) -> List[OutputConfig]:
    """Convert raw output dict dictionaries into typed configurations.

    ``OutputConfig(**raw)`` does not recurse, so the nested ``format_config``
    and ``writer_config`` mappings are converted here; writers read them by
    attribute.

    Args:
        output_dict_configs: Output configuration dictionaries.

    Returns:
        Parsed output dict configurations.
    """
    output_configs = []
    for raw_config in output_dict_configs:
        fields = dict(raw_config)
        if "format_config" in fields:
            fields["format_config"] = _to_dataclass(fields["format_config"], FormatConfig)
        if "writer_config" in fields:
            fields["writer_config"] = _to_dataclass(fields["writer_config"], WriterConfig)
        output_configs.append(OutputConfig(**fields))
    return output_configs


def get_log_config() -> Dict[str, List[Dict]]:
    """Return the ``plugins.log`` configuration section.

    Returns:
        A mapping from logger names to output configuration lists.
    """
    return get_plugins_config().get("log") or {}
