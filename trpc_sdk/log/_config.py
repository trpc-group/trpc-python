# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Configuration adapters for the logging subsystem."""

from typing import Dict
from typing import List

from trpc_sdk.abc import FormatConfig
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import WriterConfig


def parse_output_configs(plugin_config: List[Dict]) -> List[OutputConfig]:
    """Convert raw output dictionaries into typed configurations.

    Args:
        plugin_config: Output configuration dictionaries.

    Returns:
        Parsed output configurations.
    """
    output_configs = []
    for raw_config in plugin_config:
        if isinstance(raw_config, OutputConfig):
            output_configs.append(raw_config)
            continue

        values = dict(raw_config)
        writer_config = values.get("writer_config")
        if isinstance(writer_config, dict):
            values["writer_config"] = WriterConfig(**writer_config)

        format_config = values.get("format_config")
        if isinstance(format_config, dict):
            values["format_config"] = FormatConfig(**format_config)

        output_configs.append(OutputConfig(**values))
    return output_configs


def get_log_config() -> Dict:
    """Return the ``plugins.log`` configuration section.

    Returns:
        A mapping from logger names to output configuration lists.
    """
    # if _GLOBAL_CONFIG is None:
    #     return {}
    # return _GLOBAL_CONFIG.global_plugins_config.get("log") or {}
    return {}


def get_log_backend_config() -> Dict:
    """Return the ``plugins.log_backend`` configuration section.

    Returns:
        A mapping from backend names to backend-specific options.
    """
    # if _GLOBAL_CONFIG is None:
    #     return {}
    # return _GLOBAL_CONFIG.global_plugins_config.get("log_backend") or {}
    return {}
