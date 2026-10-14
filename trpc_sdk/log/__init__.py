# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Public logging API for tRPC-Python."""

from ._constants import CTX_FIELDS_IN_EXTRA_KEY
from ._constants import DEFAULT_LOG_BACKEND_NAME
from ._constants import DEFAULT_LOG_NAME
from ._constants import LEVEL_NAME_TO_LOGGING_LEVEL
from ._constants import LEVEL_TO_LOGGING_LEVEL
from ._constants import META_DATA_KEY_LOG_FIELDS
from ._format import NoopFormatterWithCtx
from ._format import TrpcJsonFormatterWithCtx
from ._format import TrpcFormatterWithCtx
from ._format import TrpcJsonFormatterWithCtx
from ._format import get_logger_formatter
from ._format import register_logger_formatter
from ._logger import DefaultLoggerInitPlugin
from ._logger import create_logging_logger
from ._logger import debug
from ._logger import debug_context
from ._logger import ensure_filepath_and_lineno
from ._logger import error
from ._logger import error_context
from ._logger import fatal
from ._logger import fatal_context
from ._logger import get_ctx_fields
from ._logger import get_log_factory
from ._logger import get_logger
from ._logger import info
from ._logger import info_context
from ._logger import register_logger
from ._logger import set_default_logger
from ._logger import warning
from ._logger import warning_context
from ._logger import with_context_fields
from ._loguru import LoguruFactoryPlugin
from ._loguru import LoguruLogger
from ._loguru import capture_all_outputs
from ._loguru import create_bootstrap_logger
from ._loguru import create_loguru_logger
from ._loguru import flush_logs
from ._loguru import install_third_party_capture
from ._loguru import loguru_available
from . import _logger as logger

__all__ = [
    "CTX_FIELDS_IN_EXTRA_KEY",
    "DEFAULT_LOG_BACKEND_NAME",
    "DEFAULT_LOG_NAME",
    "DefaultLoggerInitPlugin",
    "LEVEL_NAME_TO_LOGGING_LEVEL",
    "LEVEL_TO_LOGGING_LEVEL",
    "LoguruFactoryPlugin",
    "LoguruLogger",
    "META_DATA_KEY_LOG_FIELDS",
    "NoopFormatterWithCtx",
    "TrpcFormatterWithCtx",
    "TrpcJsonFormatterWithCtx",
    "capture_all_outputs",
    "create_bootstrap_logger",
    "create_logging_logger",
    "create_loguru_logger",
    "debug",
    "debug_context",
    "ensure_filepath_and_lineno",
    "error",
    "flush_logs",
    "error_context",
    "fatal",
    "fatal_context",
    "get_ctx_fields",
    "get_log_factory",
    "get_logger",
    "get_logger_formatter",
    "info",
    "info_context",
    "install_intercept",
    "install_third_party_capture",
    "loguru_available",
    "register_logger",
    "register_logger_formatter",
    "set_default_logger",
    "warning",
    "warning_context",
    "with_context_fields",
    "logger",
]
