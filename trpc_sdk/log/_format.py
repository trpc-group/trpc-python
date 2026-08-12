# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Formatter registry and context-aware logging formatters."""

from __future__ import annotations

import json
import logging
from typing import Any
from typing import Dict
from typing import Type

from ._constants import CTX_FIELDS_IN_EXTRA_KEY

_formatter_map: Dict[str, Type[logging.Formatter]] = {}


def get_logger_formatter(name: str = "json") -> Type[logging.Formatter]:
    """Return a registered formatter class.

    Args:
        name: Formatter name.

    Returns:
        The requested formatter class, or the default JSON formatter class.
    """
    return _formatter_map.get(name, _formatter_map["json"])


def register_logger_formatter(name: str):
    """Create a decorator that registers a formatter class.

    Args:
        name: Formatter name.

    Returns:
        A class decorator that registers and returns the formatter class.
    """

    def decorator(cls):
        """Register and return a formatter class."""
        _formatter_map[name] = cls
        return cls

    return decorator


@register_logger_formatter("json")
class TrpcFormatterWithCtx(logging.Formatter):
    """Append context fields as JSON to a standard formatted log line."""

    __LOG_WITH_FIELDS_FORMAT = "{}[{}]"

    @staticmethod
    def _get_attributes(record: logging.LogRecord) -> Dict[str, Any]:
        """Return context fields attached to a log record."""
        return vars(record).get(CTX_FIELDS_IN_EXTRA_KEY, {})

    def format(self, record: logging.LogRecord) -> str:
        """Format a record and append its context fields.

        Args:
            record: Record to format.

        Returns:
            The formatted log line.
        """
        msg = super().format(record)
        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            fields_json = json.dumps(ctx_attrs)
            msg = self.__LOG_WITH_FIELDS_FORMAT.format(msg, fields_json)
        return msg


@register_logger_formatter("noop")
class NoopFormatterWithCtx(TrpcFormatterWithCtx):
    """Render only context fields when they are present."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a record without the standard prefix when context exists.

        Args:
            record: Record to format.

        Returns:
            The formatted message or serialized context fields.
        """
        msg = super().format(record)
        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            fields_json = json.dumps(ctx_attrs)
            msg = f"{fields_json} "
        return msg


@register_logger_formatter("full_json")
class TrpcJsonFormatterWithCtx(logging.Formatter):
    """Render a complete log record as a JSON object."""

    NORMAL_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def _get_attributes(record: logging.LogRecord) -> Dict[str, Any]:
        """Return context fields attached to a log record."""
        return vars(record).get(CTX_FIELDS_IN_EXTRA_KEY, {})

    @staticmethod
    def _add_with_newline(msg: str, content: str) -> str:
        """Append content to a message on a new line."""
        if msg[-1:] != "\n":
            msg += "\n"
        return msg + content

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON.

        Args:
            record: Record to format.

        Returns:
            A JSON-encoded log line.
        """
        record.message = record.getMessage()
        msg = record.message
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            msg = self._add_with_newline(msg, record.exc_text)
        if record.stack_info:
            msg = self._add_with_newline(msg, self.formatStack(record.stack_info))

        datefmt = self.datefmt or self.NORMAL_DATE_FORMAT
        record.asctime = self.formatTime(record, datefmt)

        log_fields = {
            "Level": record.levelname,  # cSpell:ignore levelname
            "Timestamp": "%s.%03d" % (record.asctime, record.msecs),  # cSpell:ignore msecs
            "Line": f"{record.filename}: {record.lineno}",
            "Process": record.process,
            "Message": msg,
        }

        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            log_fields.update(ctx_attrs)
        return json.dumps(log_fields, ensure_ascii=False)
