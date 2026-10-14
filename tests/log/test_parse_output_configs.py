# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for converting YAML log outputs into typed configurations."""

from trpc_sdk.abc import FormatConfig
from trpc_sdk.abc import WriterConfig
from trpc_sdk.log._config import parse_output_configs


def test_nested_dicts_become_dataclasses():
    [config] = parse_output_configs([{
        "writer": "file",
        "level": "info",
        "format_config": {
            "fmt": "[%(levelname)s][%(message)s]",
            "time_fmt": "%Y-%m-%d %H:%M:%S",
        },
        "writer_config": {
            "filename": "./app.log",
            "max_size": 10,
        },
    }])

    assert isinstance(config.format_config, FormatConfig)
    assert config.format_config.fmt == "[%(levelname)s][%(message)s]"
    assert config.format_config.time_fmt == "%Y-%m-%d %H:%M:%S"
    assert isinstance(config.writer_config, WriterConfig)
    assert config.writer_config.filename == "./app.log"
    assert config.writer_config.max_size == 10


def test_existing_dataclasses_are_kept():
    format_config = FormatConfig(fmt="%(message)s")
    writer_config = WriterConfig(filename="x.log")

    [config] = parse_output_configs([{
        "writer": "file",
        "format_config": format_config,
        "writer_config": writer_config,
    }])

    assert config.format_config is format_config
    assert config.writer_config is writer_config


def test_missing_nested_configs_stay_none():
    [config] = parse_output_configs([{"writer": "console", "level": "debug"}])

    assert config.format_config is None
    assert config.writer_config is None


def test_input_dict_is_not_mutated():
    raw = {"writer": "console", "format_config": {"fmt": "%(message)s"}}

    parse_output_configs([raw])

    assert raw["format_config"] == {"fmt": "%(message)s"}
