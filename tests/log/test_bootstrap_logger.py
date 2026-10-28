# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the logger used before ``plugins.log`` is initialized."""

import inspect
import io
import re
import sys

import pytest
from loguru import logger as loguru_logger

from trpc_sdk.abc import OutputConfig
from trpc_sdk.log import DEFAULT_LOG_NAME
from trpc_sdk.log import create_bootstrap_logger
from trpc_sdk.log import create_loguru_logger
from trpc_sdk.log import flush_logs
from trpc_sdk.log import logger as log
from trpc_sdk.log._logger import _LoggerManager
from trpc_sdk.log._loguru import reset_state


def _line_pattern(level: str, message: str, lineno: int) -> str:
    """Return the regex of one line in the framework default console layout."""
    return (r"^\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\]\[%s\]\[test_bootstrap_logger\.py:%d\]\[\d+\]\[%s\]$" %
            (level, lineno, re.escape(message)))


def _next_lineno() -> int:
    """Return the line number right after the caller's line."""
    return inspect.currentframe().f_back.f_lineno + 1


@pytest.fixture(autouse=True)
def _clean_sinks(monkeypatch):
    """Start every test without configured sinks and with a fresh default logger."""
    reset_state()
    bootstrap = create_bootstrap_logger()
    monkeypatch.setattr(_LoggerManager, "_default_logger", bootstrap)
    monkeypatch.setitem(_LoggerManager._loggers, DEFAULT_LOG_NAME, bootstrap)
    yield
    flush_logs()
    reset_state()


def test_module_function_uses_framework_layout_and_call_site(capsys):
    lineno = _next_lineno()
    log.warning("hello %s", "world")

    err = capsys.readouterr().err.splitlines()
    assert len(err) == 1
    assert re.match(_line_pattern("WARNING", "hello world", lineno), err[0])


def test_direct_logger_call_reports_call_site(capsys):
    bootstrap = log.get_logger(DEFAULT_LOG_NAME)
    lineno = _next_lineno()
    bootstrap.info("direct")

    err = capsys.readouterr().err.splitlines()
    assert len(err) == 1
    assert re.match(_line_pattern("INFO", "direct", lineno), err[0])


def test_exception_is_appended(capsys):
    try:
        1 / 0
    except ZeroDivisionError:
        log.error("boom", exc_info=True)

    err = capsys.readouterr().err
    assert "[ERROR]" in err
    assert "ZeroDivisionError" in err


def test_pre_init_records_skip_loguru(capsys):
    stream = io.StringIO()
    sink_id = loguru_logger.add(stream, format="{message}")
    try:
        log.warning("not in loguru")
        loguru_logger.complete()
    finally:
        loguru_logger.remove(sink_id)

    assert stream.getvalue() == ""
    assert "not in loguru" in capsys.readouterr().err


def test_cached_reference_routes_to_configured_default_sinks(monkeypatch):
    cached = create_bootstrap_logger()
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    create_loguru_logger(DEFAULT_LOG_NAME, [OutputConfig(writer="console", level="debug")])

    lineno = _next_lineno()
    cached.info("cached reference")
    flush_logs()

    lines = [line for line in stream.getvalue().splitlines() if "cached reference" in line]
    assert len(lines) == 1
    assert re.match(_line_pattern("INFO", "cached reference", lineno), lines[0])


def test_missing_default_keeps_writing_stderr(monkeypatch):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    create_loguru_logger("worker", [OutputConfig(writer="console", level="debug")])

    lineno = _next_lineno()
    log.info("no default logger")
    flush_logs()

    lines = [line for line in stream.getvalue().splitlines() if "no default logger" in line]
    assert len(lines) == 1
    assert re.match(_line_pattern("INFO", "no default logger", lineno), lines[0])
