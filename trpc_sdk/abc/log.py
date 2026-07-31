# -*- coding: utf-8 -*-
from abc import ABCMeta
from enum import Enum
from enum import unique
from typing import List


@unique
class LogLevel(Enum):
    LevelNil = 0
    LevelTrace = 1
    LevelDebug = 2
    LevelInfo = 3
    LevelWarning = 4
    LevelError = 5
    LevelFatal = 6


LevelNames = {
    "trace": LogLevel.LevelTrace,
    "debug": LogLevel.LevelDebug,
    "info": LogLevel.LevelInfo,
    "warning": LogLevel.LevelWarning,
    "error": LogLevel.LevelError,
    "fatal": LogLevel.LevelFatal,
}


class WriterConfig:

    def __init__(self,
                 filename: str = '',
                 log_path: str = '',
                 max_size: int = 0,
                 max_backups: int = 0,
                 max_age: int = 0,
                 compress: bool = False,
                 roll_type: str = '',
                 time_unit: str = ''):
        self.filename = filename
        self.log_path = log_path
        self.max_size = max_size
        self.max_backups = max_backups
        self.max_age = max_age
        self.compress = compress
        self.roll_type = roll_type
        self.time_unit = time_unit


class FormatConfig:

    def __init__(self,
                 time_fmt: str = '',
                 time_key: str = '',
                 level_key: str = '',
                 name_key: str = '',
                 caller_key: str = '',
                 message_key: str = '',
                 stacktrace_key: str = '',
                 fmt: str = ''):
        self.time_fmt = time_fmt
        self.time_key = time_key
        self.level_key = level_key
        self.name_key = name_key
        self.caller_key = caller_key
        self.message_key = message_key
        self.stacktrace_key = stacktrace_key
        self.fmt = fmt


class OutputConfig:

    def __init__(self,
                 writer: str = '',
                 level: str = '',
                 formatter: str = '',
                 caller_skip: int = 2,
                 writer_config: WriterConfig = None,
                 format_config: FormatConfig = None,
                 remote_config: dict = None):
        self.writer = writer
        self.level = level
        self.formatter = formatter
        self.writer_config = writer_config
        self.format_config = format_config
        self.remote_config = remote_config
        self.caller_skip = caller_skip


class BaseWriter:

    def __init__(self, output_config: OutputConfig):
        self.output_config = output_config
        self.fmt = "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(process)d][%(message)s]"
        self.time_fmt = "%Y-%m-%d %H:%M:%S"
        self.handler = None
        self.level = None


class BaseLogger(metaclass=ABCMeta):

    def __init__(self, writers: List[BaseWriter]):
        self.filename = ""
        self.writers = writers
        self.min_level = None

    def debug(self, format_str: str, *args, **kwargs):
        pass

    def info(self, format_str: str, *args, **kwargs):
        pass

    def warning(self, format_str: str, *args, **kwargs):
        pass

    def error(self, format_str: str, *args, **kwargs):
        pass

    def fatal(self, format_str: str, *args, **kwargs):
        pass

    def set_level(self, level: LogLevel):
        pass

    def with_fields(self, **kwargs):
        pass
