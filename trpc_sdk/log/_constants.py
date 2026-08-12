# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Constants shared by the logging facade and backend implementations."""

from __future__ import annotations

import logging
from typing import Dict

from trpc_sdk.abc import LogLevelType

LEVEL_NAME_TO_LOGGING_LEVEL: Dict[str, int] = {
    "trace": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.CRITICAL,
}
LEVEL_TO_LOGGING_LEVEL: Dict[LogLevelType, int] = {
    LogLevelType.TRACE: logging.DEBUG,
    LogLevelType.DEBUG: logging.DEBUG,
    LogLevelType.INFO: logging.INFO,
    LogLevelType.WARNING: logging.WARNING,
    LogLevelType.ERROR: logging.ERROR,
    LogLevelType.FATAL: logging.CRITICAL,
}

# Map trpc level names to loguru level names.  ``trace`` renders as DEBUG, like
# the stdlib backend, which maps it to logging.DEBUG for lack of a TRACE level.
LEVEL_NAME_TO_LOGURU_LEVEL_NAME = {
    "trace": "DEBUG",
    "debug": "DEBUG",
    "info": "INFO",
    "warning": "WARNING",
    "error": "ERROR",
    "fatal": "CRITICAL",
}

# Map LogLevelType enum to trpc level names (for set_level()).
ENUM_TO_LEVEL_NAME = {
    LogLevelType.TRACE: "trace",
    LogLevelType.DEBUG: "debug",
    LogLevelType.INFO: "info",
    LogLevelType.WARNING: "warning",
    LogLevelType.ERROR: "error",
    LogLevelType.FATAL: "fatal",
}

# Context fields in extra key
CTX_FIELDS_IN_EXTRA_KEY: str = "trpc_ctx_fields"
# Default log name
DEFAULT_LOG_NAME: str = "default"

# Default log backend name
DEFAULT_LOG_BACKEND_NAME: str = "loguru"

# Context suffix key
CTX_SUFFIX_KEY = "trpc_ctx_suffix"

# Extra fields key
EXTRA_FIELDS_KEY = "trpc_extra"

# Meta data key log fields
META_DATA_KEY_LOG_FIELDS = "__trpc_log_fields__"

# Record name key
RECORD_NAME_KEY = "trpc_record_name"

# Third party key
THIRD_PARTY_KEY = "trpc_third_party"

# TRPC log backend
TRPC_LOG_BACKEND = "TRPC_LOG_BACKEND"
