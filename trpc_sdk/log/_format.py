# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Logger format"""
import json
import logging

from typing import Dict

__all__ = ("get_logger_formatter", "register_logger_formatter", "CTX_FIELDS_IN_EXTRA_KEY", "TRPCFormatterWithCtx",
           "NoopFormatterWithCtx", "TRPCJsonFormatterWithCtx")

_formatter_map: Dict[str, logging.Formatter] = {}


def get_logger_formatter(name: str = "json"):
    """Get logger formatter"""
    return _formatter_map.get(name, _formatter_map.get("json"))


def register_logger_formatter(name: str):
    """Register format"""

    def decorator(cls):

        _formatter_map[name] = cls
        return cls

    return decorator


# 在log extra中保存context fields 的key
CTX_FIELDS_IN_EXTRA_KEY = "trpc_ctx_fields"


@register_logger_formatter("json")
class TRPCFormatterWithCtx(logging.Formatter):
    """Trpc format with ctx"""
    __LOG_WITH_FIELDS_FORMAT = "{}[{}]"

    @staticmethod
    def _get_attributes(record):
        return vars(record).get(CTX_FIELDS_IN_EXTRA_KEY, {})

    def format(self, record):
        msg = super().format(record)
        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            fields_json = json.dumps(ctx_attrs)
            msg = self.__LOG_WITH_FIELDS_FORMAT.format(msg, fields_json)
        return msg


@register_logger_formatter("noop")
class NoopFormatterWithCtx(TRPCFormatterWithCtx):
    """Noop format"""

    def format(self, record):
        msg = super().format(record)
        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            fields_json = json.dumps(ctx_attrs)
            msg = f"{fields_json} "
        return msg


@register_logger_formatter("full_json")
class TRPCJsonFormatterWithCtx(logging.Formatter):
    """Trpc format with ctx, full json format"""
    NORMALDATEFORMAT = '%Y-%m-%d %H:%M:%S'

    @staticmethod
    def _get_attributes(record):
        return vars(record).get(CTX_FIELDS_IN_EXTRA_KEY, {})

    def _add_with_newline(self, msg, content):
        if msg[-1:] != "\n":
            msg = msg + "\n"
        msg = msg + content
        return msg

    def format(self, record):
        """Format log message in full json form.

        :param record: log record.
        :return: log record in json form.
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

        datefmt = self.datefmt or self.NORMALDATEFORMAT
        record.asctime = self.formatTime(record, datefmt)

        log_fields = {
            "Level": record.levelname,
            "Timestamp": '%s.%03d' % (record.asctime, record.msecs),
            "Line": f"{record.filename}: {record.lineno}",
            "Process": record.process,
            "Message": msg,
        }

        ctx_attrs = self._get_attributes(record)
        if ctx_attrs:
            for k, v in ctx_attrs.items():
                log_fields[k] = v
        return json.dumps(log_fields, ensure_ascii=False)
