# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Provide the Loguru-based logging backend for tRPC-Python.

The module owns physical sinks, routes records from logical tRPC loggers, and
bridges standard-library and custom writer plugins into Loguru.
"""

import atexit
import copy
import inspect
import json
import logging
import os
import sys
import threading
import time
import traceback
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseLogger
from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import LogLevelType
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin

from ._constants import CTX_FIELDS_IN_EXTRA_KEY
from ._constants import CTX_SUFFIX_KEY
from ._constants import DEFAULT_LOG_BACKEND_NAME
from ._constants import DEFAULT_LOG_NAME
from ._constants import ENUM_TO_LEVEL_NAME
from ._constants import EXTRA_FIELDS_KEY
from ._constants import LEVEL_NAME_TO_LOGGING_LEVEL
from ._constants import LEVEL_NAME_TO_LOGURU_LEVEL_NAME
from ._constants import RECORD_NAME_KEY
from ._constants import THIRD_PARTY_KEY
from ._types import LevelHolder
from ._types import PhysicalSink
from ._types import SinkBinding
from ._utils import resolve_file_path
from ._writer import LoggerWriterPlugin
from ._writer import build_bridged_sink_kwargs

try:
    from loguru import logger as _LOGURU_LOGGER

    _LOGURU_AVAILABLE = True
except ImportError:
    _LOGURU_LOGGER = None
    _LOGURU_AVAILABLE = False

# cSpell:ignore trpc

# Fields of ``extra`` which are already carried by a dedicated key.
_CONSUMED_EXTRA_KEYS = frozenset({"filename", "lineno", CTX_FIELDS_IN_EXTRA_KEY})

# Attribute names any LogRecord owns, used to tell a third-party record's own
# fields from the ones logging puts there.
_LOG_RECORD_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


def loguru_available() -> bool:
    """Return True if the optional loguru dependency is importable."""
    return _LOGURU_AVAILABLE


class _BootstrapLoguruLogger(BaseLogger):
    """Small import-time logger used before the YAML sinks are registered.

    Until the ``default`` logger has configured sinks, records are written to
    stderr in the framework's default console layout without going through
    loguru, so loguru's built-in sink keeps its usual lifetime. Once those
    sinks exist, records are routed to them like ``LoguruLogger("default")``
    does, which keeps references cached before initialization working.
    """

    _write_lock = threading.Lock()

    def __init__(self):
        """Initialize the bootstrap logger with the default severity."""
        super().__init__()
        self._bound_fields: Dict = {}
        self.filename = DEFAULT_LOG_NAME
        self.min_level = logging.DEBUG

    @classmethod
    def _reset_write_lock(cls) -> None:
        """Recreate the stderr lock in a forked child."""
        cls._write_lock = threading.Lock()

    def _log(self, level_name: str, format_str: str, args, kwargs) -> None:
        """Write one bootstrap record to stderr or the configured default sinks."""
        if (not _LOGURU_AVAILABLE or LEVEL_NAME_TO_LOGGING_LEVEL[level_name] < self.min_level):
            return
        extra = {**self._bound_fields, **(kwargs.get("extra") or {})}
        trpc_file = extra.get("filename")
        trpc_line = extra.get("lineno")
        if not trpc_file or not trpc_line:
            caller = sys._getframe(2)  # cSpell:ignore _getframe
            trpc_file = trpc_file or os.path.basename(caller.f_code.co_filename)
            trpc_line = trpc_line or caller.f_lineno
        message = _LogRecordHelper.format_message(format_str, args)
        exc_info = kwargs.get("exc_info")

        if _LoguruSinkManager.bindings_for(DEFAULT_LOG_NAME):
            bound = _LOGURU_LOGGER.bind(
                **_LogRecordHelper.bound_fields(DEFAULT_LOG_NAME, self.filename, extra, trpc_file, trpc_line))
            bound.opt(depth=2, exception=exc_info).log(LEVEL_NAME_TO_LOGURU_LEVEL_NAME[level_name], message)
            return

        line = "[%s][%s][%s:%s][%d][%s]%s%s\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"),
            LEVEL_NAME_TO_LOGURU_LEVEL_NAME[level_name],
            trpc_file,
            trpc_line,
            os.getpid(),
            message,
            self._format_exception(exc_info),
            _LogRecordHelper.context_suffix(extra.get(CTX_FIELDS_IN_EXTRA_KEY) or {}),
        )
        with self._write_lock:
            try:
                sys.stderr.write(line)
                sys.stderr.flush()
            except (OSError, ValueError, AttributeError):
                pass

    @staticmethod
    def _format_exception(exc_info) -> str:
        """Render ``exc_info`` like the ``{exception}`` field of the console layout."""
        if not exc_info:
            return ""
        if isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        elif not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()
        if exc_info[0] is None:
            return ""
        return "\n" + "".join(traceback.format_exception(*exc_info)).rstrip("\n")

    def trace(self, format_str: str, *args, **kwargs):
        """Log a trace-severity message."""
        self._log("trace", format_str, args, kwargs)

    def debug(self, format_str: str, *args, **kwargs):
        """Log a debug-severity message."""
        self._log("debug", format_str, args, kwargs)

    def info(self, format_str: str, *args, **kwargs):
        """Log an info-severity message."""
        self._log("info", format_str, args, kwargs)

    def warning(self, format_str: str, *args, **kwargs):
        """Log a warning-severity message."""
        self._log("warning", format_str, args, kwargs)

    def error(self, format_str: str, *args, **kwargs):
        """Log an error-severity message."""
        self._log("error", format_str, args, kwargs)

    def fatal(self, format_str: str, *args, **kwargs):
        """Log a fatal-severity message."""
        self._log("fatal", format_str, args, kwargs)

    def set_level(self, level: LogLevelType):
        """Set the minimum severity from a framework level."""
        level_name = ENUM_TO_LEVEL_NAME.get(level, "debug")
        self.min_level = LEVEL_NAME_TO_LOGGING_LEVEL[level_name]

    def set_min_level(self, level: int) -> None:
        """Set the minimum standard-library logging level."""
        self.min_level = level

    def get_min_level(self) -> int:
        """Return the current minimum standard-library logging level."""
        return self.min_level

    def with_fields(self, **kwargs) -> BaseLogger:
        """Return a shallow logger copy with additional structured fields.

        Args:
            **kwargs: Structured fields bound to subsequent records.

        Returns:
            A logger carrying the combined fields.
        """
        logger = copy.copy(self)
        logger._bound_fields = {**self._bound_fields, **kwargs}
        return logger


def create_bootstrap_logger() -> _BootstrapLoguruLogger:
    """Create the logger used until configured sinks are available."""
    if not _LOGURU_AVAILABLE:
        raise RuntimeError("loguru is required by the trpc logging runtime")
    return _BootstrapLoguruLogger()


class _LogRecordHelper:
    """Stateless helpers used while forwarding records to loguru."""

    @staticmethod
    def context_suffix(fields: Dict) -> str:
        """Render context fields once for the native format fast path."""
        return "[%s]" % json.dumps(fields) if fields else ""

    @staticmethod
    def own_fields(record: logging.LogRecord) -> Dict:
        """Return fields a stdlib record carries beyond LogRecord's own ones."""
        return {
            key: value
            for key, value in record.__dict__.items()
            if key not in _LOG_RECORD_RESERVED and key not in _CONSUMED_EXTRA_KEYS
        }

    @staticmethod
    def bound_fields(logger_name: str, record_name: str, extra: Dict, trpc_file: str, trpc_line: int) -> Dict:
        """Return the ``extra`` fields a trpc logger binds to one loguru record."""
        ctx_fields = extra.get(CTX_FIELDS_IN_EXTRA_KEY) or {}
        return {
            "trpc_logger": logger_name,
            "trpc_file": trpc_file,
            "trpc_line": trpc_line,
            RECORD_NAME_KEY: record_name,
            CTX_FIELDS_IN_EXTRA_KEY: ctx_fields,
            CTX_SUFFIX_KEY: _LogRecordHelper.context_suffix(ctx_fields),
            EXTRA_FIELDS_KEY: {
                k: v
                for k, v in extra.items() if k not in _CONSUMED_EXTRA_KEYS
            },
        }

    @staticmethod
    def format_message(format_str: str, args) -> str:
        """Apply stdlib ``%`` formatting without loguru re-parsing the result."""
        if args:
            try:
                return format_str % args
            except Exception:  # pylint: disable=broad-except
                return format_str
        return format_str


class _LoguruSinkManager:
    """Own all mutable sink registration state.

    Registration is performed during logger setup rather than for every log
    record. Keeping the state and its operations on one class avoids mutable
    module globals without adding work to the emit path.
    """

    # File targets share one physical sink. Streams and bridged handlers receive
    # unique keys and are never shared.
    _sinks_by_target: Dict[str, PhysicalSink] = {}
    _bindings_by_logger: Dict[str, List[SinkBinding]] = {}
    _sink_ids_by_logger: Dict[str, List[int]] = {}
    _target_key_seq = 0
    _default_sink_removed = False

    @classmethod
    def ensure_default_sink_removed(cls) -> None:
        """Remove loguru's built-in stderr sink exactly once."""
        if cls._default_sink_removed or not _LOGURU_AVAILABLE:
            return
        try:
            _LOGURU_LOGGER.remove(0)
        except (ValueError, Exception):  # pylint: disable=broad-except
            pass
        cls._default_sink_removed = True

    @staticmethod
    def build_filter(bindings: List[SinkBinding]):
        """Build a sink filter over the sink's live binding list."""

        def _filter(record) -> bool:
            """Return whether a record belongs to this physical sink."""
            extra: Dict = record["extra"]
            level_no: int = record["level"].no
            if extra.get(THIRD_PARTY_KEY):
                return any(binding.capture_third_party and level_no >= binding.third_party_level
                           for binding in bindings)

            name = extra.get("trpc_logger")
            for binding in bindings:
                if binding.logger_name != name:
                    continue
                if level_no >= max(binding.gate.level_no, binding.level_holder.level_no):
                    return True
            return False

        return _filter

    @staticmethod
    def build_sink_format(bindings: List[SinkBinding]):
        """Build a sink formatter over the sink's live binding list."""

        def _format(record) -> str:
            """Return the format configured for a routed record."""
            extra: Dict = record["extra"]
            third_party: bool = extra.get(THIRD_PARTY_KEY)
            name: str = extra.get("trpc_logger")
            for binding in bindings:
                matches = (binding.capture_third_party if third_party else binding.logger_name == name)
                if matches:
                    line_format = binding.line_format
                    return line_format(record) if callable(line_format) else line_format
            return "{message}\n"

        return _format

    @staticmethod
    def create_binding(
        logger_name: str,
        output_config: OutputConfig,
        gate: LevelHolder,
    ) -> Optional[SinkBinding]:
        """Resolve one output configuration through its writer plugin."""
        writer_name = output_config.writer
        plugin = get_plugin(PluginType.LOG_WRITER, writer_name)
        if plugin is None:
            print(f"cannot find writer plugin {PluginType.LOG_WRITER} {writer_name}")
            return None

        if isinstance(plugin, LoggerWriterPlugin):
            return plugin.create_log_sink(logger_name, output_config, gate)

        sink_kwargs = None
        if hasattr(plugin, "create_writer"):
            writer = plugin.create_writer(output_config)
            sink_kwargs = build_bridged_sink_kwargs(writer)

        if sink_kwargs is None:
            print(f"writer plugin {writer_name} provides no Loguru sink")
            return None
        return LoggerWriterPlugin.create_binding(logger_name, output_config, gate, sink_kwargs)

    @classmethod
    def target_key(cls, sink) -> str:
        """Return the sharing key of a sink target."""
        if isinstance(sink, (str, os.PathLike)):
            return "file:" + os.path.realpath(os.fspath(sink))  # cSpell:ignore fspath
        cls._target_key_seq += 1
        return "sink:%d" % cls._target_key_seq

    @staticmethod
    def rotation_signature(sink_kwargs: Dict):
        """Return settings a shared sink can only have one of."""
        return tuple(sink_kwargs.get(key) for key in ("rotation", "retention", "compression", "encoding"))

    @staticmethod
    def sink_is_alive(sink_id: Optional[int]) -> bool:
        """Return whether a sink was not removed behind this module's back."""
        handlers = getattr(getattr(_LOGURU_LOGGER, "_core", None), "handlers", None)
        if handlers is None:  # pragma: no cover - loguru internals moved
            return True
        return sink_id in handlers

    @classmethod
    def attach_binding(cls, binding: SinkBinding) -> PhysicalSink:
        """Attach a binding to its target, creating the sink if needed."""
        sink_kwargs = binding.options
        target_key = cls.target_key(sink_kwargs["sink"])
        physical = cls._sinks_by_target.get(target_key)
        if physical is not None and not cls.sink_is_alive(physical.sink_id):
            del cls._sinks_by_target[target_key]
            physical = None

        signature = cls.rotation_signature(sink_kwargs)
        if physical is not None:
            if physical.rotation_signature != signature:
                print(f"logger {binding.logger_name} reuses {target_key} with "
                      "different rotation settings; the ones of the first output "
                      "are kept")
            physical.bindings.append(binding)
            return physical

        physical = PhysicalSink(signature)
        physical.bindings.append(binding)
        sink_kwargs["filter"] = cls.build_filter(physical.bindings)
        sink_kwargs["format"] = cls.build_sink_format(physical.bindings)
        physical.sink_id = _LOGURU_LOGGER.add(**sink_kwargs)
        cls._sinks_by_target[target_key] = physical
        return physical

    @classmethod
    def remove_logger_sinks(cls, name: str) -> None:
        """Detach a logger, removing physical sinks left unused."""
        cls._bindings_by_logger.pop(name, None)
        cls._sink_ids_by_logger.pop(name, None)
        for target_key, physical in list(cls._sinks_by_target.items()):
            physical.bindings[:] = [binding for binding in physical.bindings if binding.logger_name != name]
            if physical.bindings:
                continue
            del cls._sinks_by_target[target_key]
            try:
                _LOGURU_LOGGER.remove(physical.sink_id)
            except (ValueError, Exception):  # pylint: disable=broad-except
                pass

    @classmethod
    def reset_state(cls) -> None:
        """Remove all managed sinks and clear registration bookkeeping."""
        for physical in list(cls._sinks_by_target.values()):
            try:
                _LOGURU_LOGGER.remove(physical.sink_id)
            except (ValueError, Exception):  # pylint: disable=broad-except
                pass
        cls._sinks_by_target.clear()
        cls._bindings_by_logger.clear()
        cls._sink_ids_by_logger.clear()

    @classmethod
    def register_sinks(
        cls,
        name: str,
        output_config_list: List[OutputConfig],
        gate: LevelHolder,
    ) -> int:
        """Register the sinks of a logger and return its minimum level."""
        cls.ensure_default_sink_removed()
        cls.remove_logger_sinks(name)

        bindings: List[SinkBinding] = []
        sink_ids: List[int] = []
        min_std_level = LEVEL_NAME_TO_LOGGING_LEVEL["fatal"]
        for output_config in output_config_list:
            level_name = (output_config.level or "debug").lower()
            min_std_level = min(
                min_std_level,
                LEVEL_NAME_TO_LOGGING_LEVEL.get(level_name, logging.DEBUG),
            )
            binding = cls.create_binding(name, output_config, gate)
            if binding is None:
                continue
            physical = cls.attach_binding(binding)
            bindings.append(binding)
            if physical.sink_id not in sink_ids:
                sink_ids.append(physical.sink_id)

        cls._bindings_by_logger[name] = bindings
        cls._sink_ids_by_logger[name] = sink_ids
        return min_std_level

    @classmethod
    def bindings_for(cls, name: str) -> List[SinkBinding]:
        """Return the current output bindings of a logger."""
        return cls._bindings_by_logger.get(name, [])

    @classmethod
    def capture_all_outputs(cls, name: str) -> None:
        """Enable third-party capture for every output of a logger."""
        for binding in cls.bindings_for(name):
            binding.capture_third_party = True
            binding.third_party_level = binding.level_holder.level_no

    @staticmethod
    def complete() -> None:
        """Drain all enqueued records before normal interpreter shutdown."""
        if _LOGURU_AVAILABLE:
            _LOGURU_LOGGER.complete()


def remove_logger_sinks(name: str) -> None:
    """Detach a trpc logger from its sinks."""
    _LoguruSinkManager.remove_logger_sinks(name)


def reset_state() -> None:
    """Remove all trpc-managed sinks and reset bookkeeping."""
    _LoguruSinkManager.reset_state()


def flush_logs() -> None:
    """Write out every record still queued for an ``enqueue=True`` sink.

    For a forked child about to exit: ``multiprocessing`` ends it with
    ``os._exit``, which skips the ``atexit`` hook that would otherwise do this.
    """
    _LoguruSinkManager.complete()


class LoguruLogger(BaseLogger):
    """A ``BaseLogger`` implementation backed by loguru.

    One instance corresponds to one trpc logger name (e.g. ``default``,
    ``worker``); its records are routed to the sinks registered for that name.
    ``_gate`` plays the role of ``logging.Logger``'s level and each binding
    holds the level of its own output, like a ``logging.Handler`` does.
    """

    def __init__(self, output_config_list: List[OutputConfig], name: str):
        """Initialize the loguru logger.

        Args:
            output_config_list: List of output configurations (console/file).
            name: The trpc logger name this instance represents.
        """
        super().__init__()
        self._bound_fields: Dict = {}
        self._name = name
        # ``filename`` is read by some framework bookkeeping; keep it meaningful.
        # The last file output wins, like it does in ``LoggingLogger``.
        self.filename = name
        for output_config in output_config_list:
            if (output_config.writer == "file" and output_config.writer_config is not None):
                self.filename = resolve_file_path(output_config.writer_config)

        level_name = "debug"
        for output_config in output_config_list:
            if output_config.level:
                candidate = output_config.level.lower()
                if LEVEL_NAME_TO_LOGGING_LEVEL.get(candidate, logging.DEBUG) < LEVEL_NAME_TO_LOGGING_LEVEL.get(
                        level_name, logging.DEBUG):
                    level_name = candidate
        self._gate = LevelHolder(LEVEL_NAME_TO_LOGGING_LEVEL.get(level_name, logging.DEBUG))

        if _LOGURU_AVAILABLE:
            self.min_level = _LoguruSinkManager.register_sinks(name, output_config_list, self._gate)
        else:  # pragma: no cover - exercised only without loguru installed
            self.min_level = logging.DEBUG

    def _log(self, level_name: str, format_str: str, args, kwargs) -> None:
        """Route a log record into loguru with trpc metadata bound to ``extra``.

        Every field the caller passes in ``extra`` travels along and ends up on
        the rebuilt LogRecord, like the stdlib backend does.  Sinks run with
        ``enqueue=True``, so those values are pickled on their way to the owner
        queue: a value that cannot be pickled sends the record into loguru's
        ``catch`` branch instead of the log file.
        """
        if not _LOGURU_AVAILABLE:
            return
        if LEVEL_NAME_TO_LOGGING_LEVEL[level_name] < self._gate.level_no:
            return
        extra = {**self._bound_fields, **(kwargs.get("extra") or {})}
        trpc_file = extra.get("filename")
        trpc_line = extra.get("lineno")
        if not trpc_file or not trpc_line:
            caller = sys._getframe(2)  # cSpell:ignore _getframe
            trpc_file = trpc_file or os.path.basename(caller.f_code.co_filename)
            trpc_line = trpc_line or caller.f_lineno
        bound = _LOGURU_LOGGER.bind(
            **_LogRecordHelper.bound_fields(self._name, self.filename, extra, trpc_file, trpc_line))
        bound.opt(depth=1, exception=kwargs.get("exc_info")).log(
            LEVEL_NAME_TO_LOGURU_LEVEL_NAME[level_name],
            _LogRecordHelper.format_message(format_str, args),
        )

    def trace(self, format_str: str, *args, **kwargs):
        """Log a trace level message."""
        self._log("trace", format_str, args, kwargs)

    def debug(self, format_str: str, *args, **kwargs):
        """Log a debug level message."""
        self._log("debug", format_str, args, kwargs)

    def info(self, format_str: str, *args, **kwargs):
        """Log an info level message."""
        self._log("info", format_str, args, kwargs)

    def warning(self, format_str: str, *args, **kwargs):
        """Log a warning level message."""
        self._log("warning", format_str, args, kwargs)

    def error(self, format_str: str, *args, **kwargs):
        """Log an error level message."""
        self._log("error", format_str, args, kwargs)

    def fatal(self, format_str: str, *args, **kwargs):
        """Log a fatal level message."""
        self._log("fatal", format_str, args, kwargs)

    def _bindings(self) -> List[SinkBinding]:
        """Return the outputs this logger is currently attached to."""
        return _LoguruSinkManager.bindings_for(self._name)

    def _file_bindings(self) -> List[SinkBinding]:
        """Return the bindings writing the file this logger is named after.

        This is the output the admin level commands act on, mirroring the writer
        the stdlib backend selects with ``writer.filename == logger.filename``.
        """
        target = os.path.realpath(self.filename)
        return [b for b in self._bindings() if b.filename and b.filename == target]

    def set_level(self, level: LogLevelType):
        """Set the logger-wide minimum level, leaving each output's own level.

        Like ``logging.Logger.setLevel()``, this only moves the gate every
        record passes first: an output configured above the new level keeps
        filtering the records it was already filtering.

        Args:
            level: LogLevelType enum value to set as minimum level.
        """
        level_name = ENUM_TO_LEVEL_NAME.get(level, "debug")
        self._gate.level_no = LEVEL_NAME_TO_LOGGING_LEVEL[level_name]
        self.min_level = LEVEL_NAME_TO_LOGGING_LEVEL[level_name]

    def set_min_level(self, level: int) -> None:
        """Set the level of this logger's file output (admin entry point).

        Mirrors ``trpc_sdk.admin.set_log_level`` on the stdlib backend: the output
        writing ``self.filename`` takes the requested level, the other outputs
        keep the level they were configured with, and the logger-wide gate drops
        to the lowest of them so that it hides none of them.  A logger with no
        file output has no such primary target, so every output follows.

        Args:
            level: A stdlib logging level number.
        """
        level_no = level
        bindings = self._bindings()
        for binding in self._file_bindings() or bindings:
            binding.level_holder.level_no = level_no

        if bindings:
            level_no = min(binding.level_holder.level_no for binding in bindings)
        self._gate.level_no = level_no
        self.min_level = level_no

    def get_min_level(self) -> int:
        """Return the level of this logger's file output as a stdlib number."""
        file_bindings = self._file_bindings()
        if not file_bindings:
            return self.min_level
        return min(b.level_holder.level_no for b in file_bindings)

    def with_fields(self, **kwargs) -> BaseLogger:
        """Return a shallow logger copy with additional structured fields.

        Args:
            **kwargs: Structured fields bound to subsequent records.

        Returns:
            A logger carrying the combined fields.
        """
        logger = copy.copy(self)
        logger._bound_fields = {**self._bound_fields, **kwargs}
        return logger


class InterceptHandler(logging.Handler):
    """Route standard ``logging`` records into loguru.

    Installing it on the root logger makes stdlib logs and third-party library
    logs flow into loguru's sinks, tagged with their source library.  Records
    are marked as third-party rather than addressed to a trpc logger: which
    outputs receive them is decided by each output's ``capture_third_party``.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Forward one stdlib record to loguru, preserving its call site."""
        if not _LOGURU_AVAILABLE:
            return
        # Get corresponding loguru level if it exists.
        try:
            level = _LOGURU_LOGGER.level(record.levelname).name  # cSpell:ignore levelname
        except (ValueError, Exception):  # pylint: disable=broad-except
            level = record.levelno  # cSpell:ignore levelno

        # Walk out of the logging machinery so that the reported location is the
        # third-party call site rather than logging/__init__.py.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        bound = _LOGURU_LOGGER.bind(
            **{
                THIRD_PARTY_KEY: True,
                "trpc_file": record.filename,
                "trpc_line": record.lineno,
                RECORD_NAME_KEY: record.name,
                CTX_FIELDS_IN_EXTRA_KEY: {
                    "lib": record.name
                },
                CTX_SUFFIX_KEY: _LogRecordHelper.context_suffix({"lib": record.name}),
                EXTRA_FIELDS_KEY: _LogRecordHelper.own_fields(record),
            })
        bound.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def install_third_party_capture(level: int = logging.NOTSET) -> None:
    """Install ``InterceptHandler`` on the root logger, idempotent and thread-safe.

    Args:
        level: Level to set on the root logger.
    """
    if not _LOGURU_AVAILABLE:
        return

    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, InterceptHandler):
            root.setLevel(level)
            return

    # add InterceptHandler to the root logger, it will be used to route stdlib and third-party library logs into loguru
    root.addHandler(InterceptHandler())
    root.setLevel(level)


def capture_all_outputs(name: str) -> None:
    """Make every output of a trpc logger receive third-party logs.

    This is the fallback of the ``TRPC_LOG_INTERCEPT`` override, which asks for
    the capture without saying where it should land.

    Args:
        name: The trpc logger name whose outputs take the third-party records.
    """
    _LoguruSinkManager.capture_all_outputs(name)


@register_plugin(PluginType.LOG, DEFAULT_LOG_BACKEND_NAME)
class LoguruFactoryPlugin(BasePlugin):
    """The framework's runtime log factory plugin.

    Imports remain local to avoid circular imports during bootstrap.
    """

    def __init__(self):
        """Initialize the Loguru factory plugin."""
        super().__init__()
        self._type = PluginType.LOG
        self._name = DEFAULT_LOG_BACKEND_NAME

    def available(self) -> bool:
        """Return True if the loguru dependency is importable."""
        return _LOGURU_AVAILABLE

    def create_logger(self, name: str, output_config_list: List[OutputConfig]) -> BaseLogger:
        """Create a loguru-backed logger for the given trpc logger name.

        Args:
            name: The trpc logger name, used to route records to its own sinks.
            output_config_list: List of output configurations.

        Returns:
            A LoguruLogger instance.
        """
        return LoguruLogger(output_config_list, name)

    def install_third_party_capture(self, root_level: int) -> None:
        """Route stdlib and third-party library logs into the marked sinks.

        Args:
            root_level: Level to set on the root logger.
        """
        install_third_party_capture(root_level)

    def capture_all_outputs(self, name: str) -> None:
        """Make every output of a trpc logger receive third-party logs.

        Args:
            name: The trpc logger name whose outputs take the third-party records.
        """
        _LoguruSinkManager.capture_all_outputs(name)


def create_loguru_logger(name: str, output_config_list: List[OutputConfig]) -> LoguruLogger:
    """Factory: build a ``LoguruLogger`` for the given trpc logger name.
    
    Args:
        name: The trpc logger name.
        output_config_list: List of output configurations.

    Returns:
        A LoguruLogger instance.
    """
    return LoguruLogger(output_config_list, name)


atexit.register(_LoguruSinkManager.complete)
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_BootstrapLoguruLogger._reset_write_lock)
