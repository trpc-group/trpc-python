# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Utility functions shared by logging writers and formatters."""

from __future__ import annotations

import logging
from typing import Any
from typing import Dict

from trpc_sdk.abc import WriterConfig

from ._constants import CTX_FIELDS_IN_EXTRA_KEY
from ._constants import DEFAULT_LOG_NAME
from ._constants import EXTRA_FIELDS_KEY
from ._constants import RECORD_NAME_KEY

# cSpell:ignore trpc


def resolve_file_path(writer_config: WriterConfig) -> str:
    """Return the path a file output writes to.

    Spelled exactly like the stdlib ``FileWriter`` does, so that both backends
    name the same output the same way.

    Args:
        writer_config: The output's writer configuration.

    Returns:
        The file path, prefixed with ``log_path`` when one is configured.
    """
    path = writer_config.filename
    if writer_config.log_path:
        path = writer_config.log_path + "/" + path
    return path


def _std_level_from_loguru(level_no: int) -> int:
    """Convert a loguru level number to the closest stdlib level number.

    trpc's own levels share stdlib's scale, but a record intercepted from
    ``logging`` may carry a level loguru knows and stdlib does not, such as
    loguru's own TRACE at 5.

    Args:
        level_no: Numeric Loguru level.

    Returns:
        The closest standard-library logging level.
    """
    return logging.DEBUG if level_no <= logging.DEBUG else level_no


def to_std_record(raw_record) -> logging.LogRecord:
    """Convert a loguru record into a stdlib LogRecord.

    Used both to render lines with trpc's ``logging.Formatter`` based formatters
    and to feed bridged ``LOG_WRITER`` handlers. Every field is taken from
    the loguru record captured at call time, so the caller location, pid and
    timestamp stay correct even when the sink runs in the master process.

    Args:
        raw_record: The ``dict`` carried by a loguru message.

    Returns:
        An equivalent logging.LogRecord.
    """
    extra: Dict[str, Any] = raw_record["extra"]
    exception = raw_record["exception"]
    exc_info = None
    if exception:
        exc_info = (exception.type, exception.value, exception.traceback)

    record = logging.LogRecord(
        name=extra.get(RECORD_NAME_KEY) or raw_record["name"] or DEFAULT_LOG_NAME,
        level=_std_level_from_loguru(raw_record["level"].no),  # cSpell:ignore no
        pathname=raw_record["file"].path,
        lineno=extra.get("trpc_line") or raw_record["line"],
        msg=raw_record["message"],
        args=(),
        exc_info=exc_info,
        func=raw_record["function"],
    )
    record.levelname = raw_record["level"].name  # cSpell:ignore levelname
    record.filename = extra.get("trpc_file") or raw_record["file"].name
    record.process = raw_record["process"].id
    record.processName = raw_record["process"].name
    record.thread = raw_record["thread"].id
    record.threadName = raw_record["thread"].name
    record.created = raw_record["time"].timestamp()
    record.msecs = (record.created - int(record.created)) * 1000  # cSpell:ignore msecs
    record.__dict__[CTX_FIELDS_IN_EXTRA_KEY] = extra.get(CTX_FIELDS_IN_EXTRA_KEY) or {}
    # Last, like logging_logger.makeRecord() does: the caller's own fields win.
    record.__dict__.update(extra.get(EXTRA_FIELDS_KEY) or {})
    return record
