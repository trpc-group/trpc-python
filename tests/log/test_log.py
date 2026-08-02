"""Test log module.

This module contains unit tests for the TRPC logging functionality, including:
- Logger registration and retrieval
- Log level methods (debug, info, warning, error, fatal)
- Context-aware logging
- Log formatters
- Concurrent rotating file handler
"""

import io
import json
import logging
import os
import traceback

import pytest

from trpc_sdk import context
from trpc_sdk.config import config
from trpc_sdk.log import CTX_FIELDS_IN_EXTRA_KEY
from trpc_sdk.log import NoopFormatterWithCtx
from trpc_sdk.log import TRPCFormatterWithCtx
from trpc_sdk.log import get_logger_formatter
from trpc_sdk.log import logger
from trpc_sdk.log import logger_format


def test_get_default_logger():
    """Test retrieving the default logger instance."""
    default_logger = logger.get_logger("default")
    assert default_logger is not None, "Default logger should exist"


def test_get_master_logger():
    """Test retrieving the master logger instance."""
    master_logger = logger.get_logger("master")
    assert master_logger is not None, "Master logger should exist"


def test_get_worker_logger():
    """Test retrieving the worker logger instance."""
    worker_logger = logger.get_logger("worker")
    assert worker_logger is not None, "Worker logger should exist"


def test_log_debug():
    """Test debug level logging."""
    logger.debug("debug")


def test_log_info():
    """Test info level logging."""
    logger.info("info")


def test_log_warning():
    """Test warning level logging."""
    logger.warning("warning")


def test_log_error():
    """Test error level logging."""
    logger.error("error")


def test_log_fatal():
    """Test fatal level logging."""
    logger.fatal("error")


def test_new_logging_logger():
    """Test creating a new logging logger from configuration."""
    config.load_global_config('./examples/helloworld/trpc_python.yaml', 'utf-8')
    log_config = config.global_plugins_config["log"]["default"]
    new_logger = logger.new_logging_logger(log_config)
    assert new_logger is not None, "New logger should be created successfully"


class MockLogger:
    """Mock logger for testing context-aware logging methods."""

    def debug_context(self, ctx: context.Context, format_str: str, *args, **kwargs) -> None:
        """Mock debug logging with context.

        Args:
            ctx: TRPC context object.
            format_str: Format string for the log message.
            *args: Arguments for format string.
            **kwargs: Additional keyword arguments.
        """
        pass

    def info_context(self, ctx: context.Context, format_str: str, *args, **kwargs) -> None:
        """Mock info logging with context.

        Args:
            ctx: TRPC context object.
            format_str: Format string for the log message.
            *args: Arguments for format string.
            **kwargs: Additional keyword arguments.
        """
        pass

    def warning_context(self, ctx: context.Context, format_str: str, *args, **kwargs) -> None:
        """Mock warning logging with context.

        Args:
            ctx: TRPC context object.
            format_str: Format string for the log message.
            *args: Arguments for format string.
            **kwargs: Additional keyword arguments.
        """
        pass

    def error_context(self, ctx: context.Context, format_str: str, *args, **kwargs) -> None:
        """Mock error logging with context.

        Args:
            ctx: TRPC context object.
            format_str: Format string for the log message.
            *args: Arguments for format string.
            **kwargs: Additional keyword arguments.
        """
        pass


def mock_set_log_name(name: str) -> MockLogger:
    """Create a mock logger instance for testing.

    Args:
        name: Logger name (unused in mock).

    Returns:
        MockLogger instance.
    """
    return MockLogger()


def test_with_context_fields():
    """Test adding custom fields to context for logging."""
    ctx = context.TrpcContext()

    # Test exception when fields is not a dict
    with pytest.raises(Exception, match="with_context_fields param fields must be type of dict"):
        logger.with_context_fields(ctx, None)

    # Test normal case
    expected_fields = {"uin": "123"}
    logger.with_context_fields(ctx, expected_fields)
    actual_fields = ctx.get_meta_data(context.META_DATA_KEY_LOG_FIELDS, None)
    assert expected_fields == actual_fields, "Context fields should match expected fields"


def test_get_ctx_fields():
    """Test retrieving custom log fields from context."""
    expected_fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, expected_fields)
    actual_fields = logger.get_ctx_fields(ctx)
    assert actual_fields == expected_fields, "Retrieved fields should match set fields"


def test_debug_context(mocker):
    """Test debug level logging with context fields."""
    fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, fields)
    msg = "hello"

    # Test normal case
    logger.debug_context(ctx, msg)

    # Test case when default logger has debug_context method
    mocker.patch("trpc_sdk.log._logger.set_log_name", mock_set_log_name)
    logger.debug_context(ctx, msg)


def test_info_context(mocker):
    """Test info level logging with context fields."""
    fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, fields)
    msg = "hello"

    # Test normal case
    logger.info_context(ctx, msg)

    # Test case when default logger has info_context method
    mocker.patch("trpc_sdk.log._logger.set_log_name", mock_set_log_name)
    logger.info_context(ctx, msg)


def test_warning_context(mocker):
    """Test warning level logging with context fields."""
    fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, fields)
    msg = "hello"

    # Test normal case
    logger.warning_context(ctx, msg)

    # Test case when default logger has warning_context method
    mocker.patch("trpc_sdk.log._logger.set_log_name", mock_set_log_name)
    logger.warning_context(ctx, msg)


def test_error_context(mocker):
    """Test error level logging with context fields."""
    fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, fields)
    msg = "hello"

    # Test normal case
    logger.error_context(ctx, msg)

    # Test case when default logger has error_context method
    mocker.patch("trpc_sdk.log._logger.set_log_name", mock_set_log_name)
    logger.error_context(ctx, msg)


def test_fatal_context():
    """Test fatal level logging with context fields."""
    fields = {"uin": "123"}
    ctx = context.TrpcContext()
    logger.with_context_fields(ctx, fields)
    msg = "hello"
    logger.fatal_context(ctx, msg)


def test_trpc_formatter_with_ctx():
    """Test TRPC formatter with context fields."""
    record = logging.LogRecord("", logging.INFO, "", 0, "test log", None, None)
    setattr(record, CTX_FIELDS_IN_EXTRA_KEY, {"uin": "124"})

    formatter = logger_format.TRPCFormatterWithCtx()
    formatted_msg = formatter.format(record)

    expected_msg = 'test log[{"uin": "124"}]'
    assert formatted_msg == expected_msg, f"Formatted message should be: {expected_msg}"


def test_trpc_json_formatter_with_ctx():
    """Test TRPC JSON formatter with context fields and exception info."""
    # Create exception info for testing
    exc_info = None
    try:
        1 / 0
    except ZeroDivisionError as e:
        exc_info = (type(e), e, e.__traceback__)

    # Create log record with exception and stack info
    record = logging.LogRecord(
        name="",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test log",
        args=None,
        exc_info=exc_info,
        sinfo='Stack trace info',
    )

    setattr(record, CTX_FIELDS_IN_EXTRA_KEY, {"uin": "124"})
    formatter = logger_format.TRPCJsonFormatterWithCtx()
    formatted_msg = formatter.format(record)
    parsed_msg = json.loads(formatted_msg)

    # Format expected exception message
    sio = io.StringIO()
    traceback.print_exception(exc_info[0], exc_info[1], exc_info[2], None, sio)
    expected_exception_str = sio.getvalue()
    sio.close()

    # Assertions
    assert parsed_msg["Level"] == "INFO", "Log level should be INFO"
    assert parsed_msg["Message"] == f"test log\n{expected_exception_str}Stack trace info", \
        "Message should contain log text, exception, and stack info"
    assert parsed_msg["uin"] == "124", "Context field 'uin' should be present"
    assert "Timestamp" in parsed_msg, "Timestamp field should be present"
    assert "Process" in parsed_msg, "Process field should be present"


def test_logger_formatter():
    """Test logger formatter retrieval and instantiation."""
    # Test default TRPC formatter
    json_formatter = get_logger_formatter()
    assert issubclass(json_formatter, TRPCFormatterWithCtx), \
        "Default formatter should be subclass of TRPCFormatterWithCtx"
    assert isinstance(json_formatter(), TRPCFormatterWithCtx), \
        "Default formatter instance should be TRPCFormatterWithCtx"

    # Test noop formatter
    noop_formatter = get_logger_formatter('noop')
    assert issubclass(noop_formatter, NoopFormatterWithCtx), \
        "Noop formatter should be subclass of NoopFormatterWithCtx"
    assert isinstance(noop_formatter(), NoopFormatterWithCtx), \
        "Noop formatter instance should be NoopFormatterWithCtx"
