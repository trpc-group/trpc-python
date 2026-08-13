# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Configuration adapters for the logging subsystem."""

from typing import Dict
from typing import List

from trpc_sdk.abc import OutputConfig
from trpc_sdk.config import get_plugins_config


def parse_output_configs(output_dict_configs: List[Dict]) -> List[OutputConfig]:
    """Convert raw output dict dictionaries into typed configurations.

    Args:
        output_dict_configs: Output configuration dictionaries.

    Returns:
        Parsed output dict configurations.
    """
    output_configs = []
    for raw_config in output_dict_configs:
        output_configs.append(OutputConfig(**raw_config))
    return output_configs


def get_log_config() -> Dict[str, List[Dict]]:
    """Return the ``plugins.log`` configuration section.

    Returns:
        A mapping from logger names to output configuration lists.
    """
    return get_plugins_config().get("log") or {}
