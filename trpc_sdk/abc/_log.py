# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Logging extension interfaces and shared configuration models.

This module defines severity levels, output configuration dataclasses, and the
abstract logger contract used by pluggable logging backends.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from enum import unique
from typing import Any


@unique
class LogLevelType(IntEnum):
    """Define supported log severity levels."""

    NIL = 0
    TRACE = 1
    DEBUG = 2
    INFO = 3
    WARNING = 4
    ERROR = 5
    FATAL = 6


LOG_LEVEL_NAMES = {
    "trace": LogLevelType.TRACE,
    "debug": LogLevelType.DEBUG,
    "info": LogLevelType.INFO,
    "warning": LogLevelType.WARNING,
    "error": LogLevelType.ERROR,
    "fatal": LogLevelType.FATAL,
}
"""Map lowercase severity names to ``LogLevelType`` values."""


@dataclass
class WriterConfig:
    """Describe file-backed log writer options.

    Attributes:
        filename: Base log file name.
        log_path: Directory that stores rolled log files.
        max_size: Maximum size of one log file in megabytes.
        max_backups: Maximum number of retained backup files.
        max_age: Maximum age of retained files in days.
        compress: Whether rotated files should be compressed.
        roll_type: Rolling strategy identifier.
        time_unit: Time unit used by time-based rolling.
    """

    filename: str = ""
    log_path: str = ""
    max_size: int = 0
    max_backups: int = 0
    max_age: int = 0
    compress: bool = False
    roll_type: str = ""
    time_unit: str = ""


@dataclass
class FormatConfig:
    """Describe structured log field formatting options.

    Attributes:
        time_fmt: Timestamp format string.
        time_key: Field name used for the timestamp.
        level_key: Field name used for the severity level.
        name_key: Field name used for the logger name.
        caller_key: Field name used for the caller location.
        message_key: Field name used for the log message.
        stacktrace_key: Field name used for stack traces.
        fmt: Complete format template string.
    """

    time_fmt: str = ""
    time_key: str = ""
    level_key: str = ""
    name_key: str = ""
    caller_key: str = ""
    message_key: str = ""
    stacktrace_key: str = ""
    fmt: str = ""


@dataclass
class OutputConfig:
    """Describe one logger output destination.

    Attributes:
        writer: Writer backend name.
        level: Minimum severity accepted by this output.
        formatter: Formatter backend name.
        caller_skip: Number of stack frames to skip when recording callers.
        writer_config: Writer-specific options.
        format_config: Formatter-specific options.
        remote_config: Remote sink options, if any.
    """

    writer: str = ""
    level: str = ""
    formatter: str = ""
    caller_skip: int = 2
    writer_config: WriterConfig = None
    format_config: FormatConfig = None
    remote_config: dict = None
    capture_third_party: bool = False
    third_party_level: str = ""

    def effective_third_party_level(self) -> str:
        """Return the level name third-party logs must reach on this output.

        Each backend maps the name onto its own level table, but the fallback
        chain is the same everywhere.
        """
        return (self.third_party_level or self.level or "debug").lower()


@dataclass
class BaseWriter:
    """Hold runtime state for one concrete log writer.

    Attributes:
        output_config: Output configuration that created this writer.
        fmt: Message format template.
        time_fmt: Timestamp format string.
        handler: Backend-specific handler instance.
        level: Effective severity threshold for this writer.
    """

    output_config: OutputConfig
    fmt: str = (
        "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s]"
        "[%(process)d][%(message)s]"
    )
    time_fmt: str = "%Y-%m-%d %H:%M:%S"
    handler: Any = None
    level: Any = None


class BaseLogger(ABC):
    """Define the pluggable logger API used by the framework."""

    @abstractmethod
    def debug(self, format_str: str, *args, **kwargs):
        """Log a debug-severity message.

        Args:
            format_str: printf-style or backend-specific format string.
            *args: Positional values interpolated into ``format_str``.
            **kwargs: Backend-specific structured fields or options.
        """
        ...

    @abstractmethod
    def info(self, format_str: str, *args, **kwargs):
        """Log an info-severity message.

        Args:
            format_str: printf-style or backend-specific format string.
            *args: Positional values interpolated into ``format_str``.
            **kwargs: Backend-specific structured fields or options.
        """
        ...

    @abstractmethod
    def warning(self, format_str: str, *args, **kwargs):
        """Log a warning-severity message.

        Args:
            format_str: printf-style or backend-specific format string.
            *args: Positional values interpolated into ``format_str``.
            **kwargs: Backend-specific structured fields or options.
        """
        ...

    @abstractmethod
    def error(self, format_str: str, *args, **kwargs):
        """Log an error-severity message.

        Args:
            format_str: printf-style or backend-specific format string.
            *args: Positional values interpolated into ``format_str``.
            **kwargs: Backend-specific structured fields or options.
        """
        ...

    @abstractmethod
    def fatal(self, format_str: str, *args, **kwargs):
        """Log a fatal-severity message.

        Args:
            format_str: printf-style or backend-specific format string.
            *args: Positional values interpolated into ``format_str``.
            **kwargs: Backend-specific structured fields or options.
        """
        ...

    @abstractmethod
    def set_level(self, level: LogLevelType):
        """Update the minimum severity accepted by this logger.

        Args:
            level: New severity threshold.
        """
        ...

    @abstractmethod
    def with_fields(self, **kwargs: Any):
        """Return a logger enriched with structured fields.

        Args:
            **kwargs: Field names and values attached to subsequent log lines.

        Returns:
            Logger instance that includes the supplied fields.
        """
        ...
