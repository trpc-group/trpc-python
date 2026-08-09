# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Loguru-based logging backend for the TRPC framework.

This module implements the ``loguru`` log backend plugin registered in
``trpc_sdk.log.logger`` as ``PluginType.LOG_BACKEND`` / ``"loguru"``. Business code
keeps calling ``from trpc_sdk.log import logger`` and needs no change
"""
import atexit
import inspect
import json
import logging
import os
import re
import sys
from typing import Dict,List,Optional

from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import get_plugin
from trpc_sdk.abc import BaseLogger
from trpc_sdk.abc import LogLevelType
from trpc_sdk.abc import OutputConfig

from ._format import CTX_FIELDS_IN_EXTRA_KEY
from ._format import get_logger_formatter

try:
    from loguru import logger as _loguru_logger

    _LOGURU_AVAILABLE = True
except ImportError:
    _loguru_logger = None
    _LOGURU_AVAILABLE = False


# Map trpc level names to loguru level names.  ``trace`` renders as DEBUG, like
# the stdlib backend, which maps it to logging.DEBUG for lack of a TRACE level.
_LEVEL_MAP = {
    "trace": "DEBUG",
    "debug": "DEBUG",
    "info": "INFO",
    "warning": "WARNING",
    "error": "ERROR",
    "fatal": "CRITICAL",
}

# Map trpc level names to level numbers.  loguru and stdlib share the same
# scale here, so a level read by admin can be written back unchanged.
_LEVEL_NO = {
    "trace": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.CRITICAL,
}

# Map LogLevelType enum to trpc level names (for set_level()).
_ENUM_TO_NAME = {
    LogLevelType.TRACE: "trace",
    LogLevelType.DEBUG: "debug",
    LogLevelType.INFO: "info",
    LogLevelType.WARNING: "warning",
    LogLevelType.ERROR: "error",
    LogLevelType.FATAL: "fatal",
}

# Default trpc logger name
_DEFAULT_LOG_NAME = "default"

# Default layout, identical to BaseWriter's.
_DEFAULT_FMT = "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(process)d][%(message)s]"
_DEFAULT_TIME_FMT = "%Y-%m-%d %H:%M:%S"

# Physical loguru sinks, keyed by output target.  File targets are keyed by
# their resolved path so that every logger writing that file shares one sink
# (one queue, one writer thread, one rotation owner); console and bridged
# targets get a unique key and are never shared.
_sinks_by_target: Dict[str, "_PhysicalSink"] = {}
_target_key_seq = 0

# Bookkeeping so that re-registering a logger detaches its previous bindings.
_bindings_by_logger: Dict[str, List["_SinkBinding"]] = {}
_sink_ids_by_logger: Dict[str, List[int]] = {}
_default_sink_removed = False

# Rendered-line keys must be unique per output (each one has its own formatter).
_render_key_seq = 0

# Remembered so that a forked child can rebuild the intercept handler lock.
_intercept_level: Optional[int] = None

# Extra field used by the native-format fast path.  It contains either an
# already-rendered ``[context-json]`` suffix or an empty string.
_CTX_SUFFIX_KEY = "trpc_ctx_suffix"

# Extra field marking a record intercepted from stdlib logging.  It belongs to
# no trpc logger, so the sink filters route it by ``capture_third_party``.
_THIRD_PARTY_KEY = "trpc_third_party"

# Extra field carrying the caller's own ``extra`` fields.  The stdlib backend
# copies them onto the LogRecord (see ``logging_logger.makeRecord``), so a
# custom formatter using ``%(request_id)s`` must find them here too.
_EXTRA_FIELDS_KEY = "trpc_extra"

# Extra field carrying the name the rebuilt LogRecord must report.  The stdlib
# backend names its logger after the output file, so ``%(name)s`` renders that
# and not the module of the call site.
_RECORD_NAME_KEY = "trpc_record_name"

# Fields of ``extra`` which are already carried by a dedicated key.
_CONSUMED_EXTRA_KEYS = frozenset({"filename", "lineno", CTX_FIELDS_IN_EXTRA_KEY})

# Attribute names any LogRecord owns, used to tell a third-party record's own
# fields from the ones logging puts there.
_LOG_RECORD_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"message", "asctime"}

# logging.Formatter percent-style fields which have exact loguru equivalents.
_PERCENT_FIELD_RE = re.compile(
    r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)"
    r"(?P<flags>[-+ #0]*)(?P<width>\d*)(?P<precision>\.\d+)?(?P<type>[diouxXeEfFgGcrs])"
)

# Map supported logging.Formatter fields to equivalent native loguru fields.
_NATIVE_FIELDS = {
    ("asctime", "", "", None, "s"): None,  # filled with the configured time format
    ("levelname", "", "", None, "s"): "{level}",
    ("filename", "", "", None, "s"): "{extra[trpc_file]}",
    ("lineno", "", "", None, "s"): "{extra[trpc_line]}",
    ("lineno", "", "", None, "d"): "{extra[trpc_line]}",
    ("process", "", "", None, "d"): "{process}",
    ("message", "", "", None, "s"): "{message}",
}

# Translate the supported Python strftime subset to loguru time tokens.
_TIME_TOKENS = {
    "%Y": "YYYY",
    "%m": "MM",
    "%d": "DD",
    "%H": "HH",
    "%M": "mm",
    "%S": "ss",
}


def loguru_available() -> bool:
    """Return True if the optional loguru dependency is importable."""
    return _LOGURU_AVAILABLE


class BootstrapLoguruLogger(BaseLogger):
    """Small import-time logger used before the YAML sinks are registered.

    It deliberately uses loguru's built-in stderr sink. The first configured
    ``LoguruLogger`` removes that sink and replaces it with the managed sinks.
    """

    def __init__(self):
        super().__init__([])
        self.filename = _DEFAULT_LOG_NAME
        self.min_level = logging.DEBUG

    def _log(self, level_name: str, format_str: str, args, kwargs) -> None:
        if not _LOGURU_AVAILABLE or _LEVEL_NO[level_name] < self.min_level:
            return
        _loguru_logger.opt(depth=2, exception=kwargs.get("exc_info")).log(
            _LEVEL_MAP[level_name], _format_msg(format_str, args)
        )

    def trace(self, format_str: str, *args, **kwargs):
        self._log("trace", format_str, args, kwargs)

    def debug(self, format_str: str, *args, **kwargs):
        self._log("debug", format_str, args, kwargs)

    def info(self, format_str: str, *args, **kwargs):
        self._log("info", format_str, args, kwargs)

    def warning(self, format_str: str, *args, **kwargs):
        self._log("warning", format_str, args, kwargs)

    def error(self, format_str: str, *args, **kwargs):
        self._log("error", format_str, args, kwargs)

    def fatal(self, format_str: str, *args, **kwargs):
        self._log("fatal", format_str, args, kwargs)

    def set_level(self, level: LogLevelType):
        level_name = _ENUM_TO_NAME.get(level, "debug")
        self.min_level = _LEVEL_NO[level_name]

    def set_min_level(self, level: int) -> None:
        self.min_level = level

    def get_min_level(self) -> int:
        return self.min_level


def new_bootstrap_logger() -> BootstrapLoguruLogger:
    """Create the logger used until configured sinks are available."""
    if not _LOGURU_AVAILABLE:
        raise RuntimeError("loguru is required by the trpc logging runtime")
    return BootstrapLoguruLogger()


def _log_error(message: str) -> None:
    """Report a backend error through the trpc facade (imported lazily)."""
    from ._logger import error  # local import: avoids a circular import at module load

    error(message)


def _ensure_default_sink_removed() -> None:
    """Remove loguru's built-in stderr sink (id 0) exactly once.

    trpc manages its own sinks, so the default handler would otherwise produce
    duplicate console output.  Only id 0 is removed: sinks the application added
    itself are none of this module's business, and loguru never reuses id 0 for
    them because handler ids only ever increase.
    """
    global _default_sink_removed
    if _default_sink_removed or not _LOGURU_AVAILABLE:
        return
    try:
        _loguru_logger.remove(0)
    except (ValueError, Exception):  # pylint: disable=broad-except
        pass  # already removed by the application
    _default_sink_removed = True


class _LevelHolder:
    """A mutable level number read by the sink filters at emit time.

    Holding the level here (instead of relying on the level passed to
    ``logger.add()``) lets ``set_level()`` take effect without re-registering
    sinks, which would create a second owner queue and writer thread.
    """

    __slots__ = ("level_no",)

    def __init__(self, level_no: int):
        self.level_no = level_no


class _SinkBinding:
    """One trpc output of one trpc logger, attached to a physical sink.

    ``gate`` is the logger-wide holder (the equivalent of ``logging.Logger``'s
    level) and ``level_holder`` the output's own threshold (the equivalent of
    ``logging.Handler``'s level); a record must pass both.

    ``capture_third_party`` says whether the output also receives the records
    ``InterceptHandler`` picks up from stdlib logging; those are gated by
    ``third_party_level`` instead, and never by the logger-wide ``gate``, since
    they do not belong to any trpc logger.
    """

    __slots__ = ("logger_name", "gate", "level_holder", "line_format", "filename",
                 "capture_third_party", "third_party_level")

    def __init__(self, logger_name: str, gate: _LevelHolder, level_holder: _LevelHolder,
                 line_format, filename: Optional[str], capture_third_party: bool = False,
                 third_party_level: int = logging.NOTSET):
        self.logger_name = logger_name
        self.gate = gate
        self.level_holder = level_holder
        self.line_format = line_format
        self.filename = filename
        self.capture_third_party = capture_third_party
        self.third_party_level = third_party_level


class _PhysicalSink:
    """One ``loguru.logger.add()`` registration and the bindings routed to it."""

    __slots__ = ("sink_id", "bindings", "rotation_signature")

    def __init__(self, rotation_signature):
        self.sink_id: Optional[int] = None
        self.bindings: List[_SinkBinding] = []
        self.rotation_signature = rotation_signature


def _std_level_from_loguru(level_no: int) -> int:
    """Convert a loguru level number to the closest stdlib level number.

    trpc's own levels share stdlib's scale, but a record intercepted from
    ``logging`` may carry a level loguru knows and stdlib does not, such as
    loguru's own TRACE at 5.
    """
    return logging.DEBUG if level_no <= logging.DEBUG else level_no


def _resolve_layout(output_config: OutputConfig):
    """Return the (fmt, time_fmt) pair configured for an output."""
    fmt = _DEFAULT_FMT
    time_fmt = _DEFAULT_TIME_FMT
    format_config = output_config.format_config
    if format_config:
        if format_config.fmt:
            fmt = format_config.fmt
        if format_config.time_fmt:
            time_fmt = format_config.time_fmt
    return fmt, time_fmt


def _compile_loguru_time_format(time_fmt: str) -> Optional[str]:
    """Translate the python standard strftime subset into loguru time tokens.

    Returning ``None`` for an unknown directive keeps custom formats on the
    compatibility path instead of silently changing their output.
    """
    result = []
    index = 0
    while index < len(time_fmt):
        if time_fmt[index] != "%":
            result.append(time_fmt[index])
            index += 1
            continue
        token = time_fmt[index:index + 2]
        replacement = _TIME_TOKENS.get(token)
        if replacement is None:
            return None
        result.append(replacement)
        index += 2
    return "".join(result)


def _escape_loguru_literal(text: str) -> str:
    """Escape braces in literal layout text for loguru's format parser."""
    return text.replace("{", "{{").replace("}", "}}")


def _compile_loguru_format(fmt: str, time_fmt: str) -> Optional[str]:
    """Compile a supported stdlib percent layout into a native loguru layout.

    The conversion happens once while the sink is registered.  Unsupported
    fields or formatting modifiers return ``None`` and use the existing
    ``logging.Formatter`` compatibility path.
    """
    native_time_fmt = _compile_loguru_time_format(time_fmt)
    if native_time_fmt is None:
        return None

    result = []
    index = 0
    while index < len(fmt):
        if fmt.startswith("%%", index):
            result.append("%")
            index += 2
            continue
        if fmt[index] != "%":
            literal_start = index
            while index < len(fmt) and fmt[index] != "%":
                index += 1
            result.append(_escape_loguru_literal(fmt[literal_start:index]))
            continue

        match = _PERCENT_FIELD_RE.match(fmt, index)
        if match is None:
            return None
        key = (
            match.group("name"),
            match.group("flags"),
            match.group("width"),
            match.group("precision"),
            match.group("type"),
        )
        if key == ("msecs", "0", "3", None, "d"):
            replacement = "{time:SSS}"
        else:
            replacement = _NATIVE_FIELDS.get(key)
            if key == ("asctime", "", "", None, "s"):
                replacement = "{time:%s}" % native_time_fmt
        if replacement is None:
            return None
        result.append(replacement)
        index = match.end()

    # logging.Formatter appends exception text after the configured layout, and
    # TRPCFormatterWithCtx appends context fields after that.
    # loguru appends a newline to static string formats automatically.
    return "".join(result) + "{exception}{extra[%s]}" % _CTX_SUFFIX_KEY


def _context_suffix(fields: Dict) -> str:
    """Render context fields once per record for the native format fast path."""
    return "[%s]" % json.dumps(fields) if fields else ""


def to_std_record(raw_record) -> logging.LogRecord:
    """Convert a loguru record into a stdlib LogRecord.

    Used both to render lines with trpc's ``logging.Formatter`` based formatters
    and to feed bridged ``WRITER_COMPONENT`` handlers. Every field is taken from
    the loguru record captured at call time, so the caller location, pid and
    timestamp stay correct even when the sink runs in the master process.

    Args:
        raw_record: The ``dict`` carried by a loguru message.

    Returns:
        An equivalent logging.LogRecord.
    """
    extra = raw_record["extra"]
    exception = raw_record["exception"]
    exc_info = None
    if exception:
        exc_info = (exception.type, exception.value, exception.traceback)

    record = logging.LogRecord(
        name=extra.get(_RECORD_NAME_KEY) or raw_record["name"] or _DEFAULT_LOG_NAME,
        level=_std_level_from_loguru(raw_record["level"].no),
        pathname=raw_record["file"].path,
        lineno=extra.get("trpc_line") or raw_record["line"],
        msg=raw_record["message"],
        args=(),
        exc_info=exc_info,
        func=raw_record["function"],
    )
    record.levelname = raw_record["level"].name
    record.filename = extra.get("trpc_file") or raw_record["file"].name
    record.process = raw_record["process"].id
    record.processName = raw_record["process"].name
    record.thread = raw_record["thread"].id
    record.threadName = raw_record["thread"].name
    record.created = raw_record["time"].timestamp()
    record.msecs = (record.created - int(record.created)) * 1000
    record.__dict__[CTX_FIELDS_IN_EXTRA_KEY] = extra.get(CTX_FIELDS_IN_EXTRA_KEY) or {}
    # Last, like logging_logger.makeRecord() does: the caller's own fields win.
    record.__dict__.update(extra.get(_EXTRA_FIELDS_KEY) or {})
    return record


def _record_own_fields(record: logging.LogRecord) -> Dict:
    """Return the fields a stdlib record carries beyond LogRecord's own ones.

    These are what the emitting code passed as ``extra``; they must survive the
    trip through loguru or a formatter naming one of them would fail to render.

    Args:
        record: The stdlib record being forwarded.

    Returns:
        The record's custom fields.
    """
    return {
        key: value for key, value in record.__dict__.items()
        if key not in _LOG_RECORD_RESERVED and key not in _CONSUMED_EXTRA_KEYS
    }


def _next_render_key() -> str:
    """Return a process-unique key used to pass a rendered line to loguru."""
    global _render_key_seq
    _render_key_seq += 1
    return "trpc_rendered_%d" % _render_key_seq


def _make_format(output_config: OutputConfig):
    """Build the native format or a callback delegating to a trpc formatter.

    Supported ``json`` text layouts are compiled once into a native loguru
    template.  Other layouts use a callback which renders the line with the
    formatter registered in ``logger_format`` and stores the result in
    ``record['extra']``; the returned template performs a single substitution,
    so braces inside the message or context fields are never re-parsed.

    Args:
        output_config: Output configuration providing formatter name and layout.

    Returns:
        A loguru template string, or a callable returning one.
    """
    fmt, time_fmt = _resolve_layout(output_config)
    # The empty formatter name resolves to ``json`` in logger_format.  Only
    # these built-in text layouts have native equivalents; noop/full_json and
    # custom formatter classes retain the compatibility callback below.
    if output_config.formatter in ("", "json"):
        native_format = _compile_loguru_format(fmt, time_fmt)
        if native_format is not None:
            return native_format

    formatter_cls = get_logger_formatter(output_config.formatter)
    trpc_formatter = formatter_cls(fmt=fmt, datefmt=time_fmt)
    key = _next_render_key()
    template = "{extra[%s]}\n" % key

    def _formatter(record) -> str:
        record["extra"][key] = trpc_formatter.format(to_std_record(record))
        return template

    return _formatter


class WriterHandlerSink:
    """Bridge a ``WRITER_COMPONENT`` plugin's stdlib handler into a loguru sink.

    loguru can take a ``logging.Handler`` directly, but its built-in adapter
    rebuilds the record in the sink-owning process, which loses the caller
    location, reports the owner pid and nests trpc's ``extra`` one level deep.
    Converting the record ourselves keeps a custom writer plugin seeing exactly
    what it sees on the stdlib backend.
    """

    def __init__(self, handler: logging.Handler):
        """Initialize the sink.

        Args:
            handler: The handler produced by the writer plugin.
        """
        self._handler = handler

    def __call__(self, message) -> None:
        """Emit one loguru message through the wrapped handler."""
        self._handler.handle(to_std_record(message.record))

    def stop(self) -> None:
        """Close the wrapped handler when the sink is removed."""
        self._handler.close()


def _common_sink_kwargs(output_config: OutputConfig) -> Dict:
    """Return the sink arguments shared by every trpc-managed loguru sink."""
    return {
        # The static level stays at the lowest value: the real threshold is
        # enforced by the filter so that set_level() needs no sink rebuild.
        "level": "TRACE",
        "format": _make_format(output_config),
        "enqueue": True,
        "backtrace": False,
        "diagnose": False,
        "catch": True,
    }


def build_console_sink_kwargs(output_config: OutputConfig) -> Dict:
    """Build ``loguru.logger.add()`` arguments for the built-in console writer.

    Args:
        output_config: Output configuration for the console writer.

    Returns:
        Keyword arguments for a native loguru sink.
    """
    sink_kwargs = _common_sink_kwargs(output_config)
    sink_kwargs["sink"] = sys.stderr
    sink_kwargs["colorize"] = False
    return sink_kwargs


def _resolve_file_path(writer_config) -> str:
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


def build_file_sink_kwargs(output_config: OutputConfig) -> Dict:
    """Build ``loguru.logger.add()`` arguments for the built-in file writer.

    Maps trpc's ``writer_config`` onto loguru's own rotation, retention and
    compression so that the single-owner enqueue machinery stays in use.

    Args:
        output_config: Output configuration for the file writer.

    Returns:
        Keyword arguments for a native loguru sink.
    """
    writer_config = output_config.writer_config
    path = _resolve_file_path(writer_config)

    sink_kwargs = _common_sink_kwargs(output_config)
    sink_kwargs["sink"] = path
    sink_kwargs["rotation"] = writer_config.max_size * 1000000 if writer_config.max_size else None
    sink_kwargs["retention"] = writer_config.max_backups if writer_config.max_backups else None
    sink_kwargs["compression"] = "gz" if writer_config.compress else None
    sink_kwargs["encoding"] = "utf-8"
    return sink_kwargs


def _build_bridged_sink_kwargs(writer, output_config: OutputConfig) -> Optional[Dict]:
    """Build sink arguments that reuse a writer plugin's stdlib handler."""
    handler = getattr(writer, "handler", None)
    if handler is None:
        return None

    return {
        "sink": WriterHandlerSink(handler),
        # The handler keeps its own formatter, loguru only carries the message.
        "format": "{message}",
        "level": "TRACE",
        "enqueue": True,
        "backtrace": False,
        "diagnose": False,
        "catch": True,
    }


def _build_filter(bindings: List[_SinkBinding]):
    """Build a sink filter accepting the records its bindings ask for.

    Framework records carry ``extra['trpc_logger']`` set by ``LoguruLogger`` and
    reach the bindings of that logger.  Records intercepted from stdlib logging
    carry ``extra['trpc_third_party']`` instead and reach only the bindings of
    the outputs configured with ``capture_third_party``.  Anything else is
    dropped, so the destination of a record is never implicit.

    ``bindings`` is the live list owned by the sink, so attaching or detaching a
    logger takes effect without re-registering the sink.
    """

    def _filter(record) -> bool:
        extra = record["extra"]
        level_no = record["level"].no
        if extra.get(_THIRD_PARTY_KEY):
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


def _build_sink_format(bindings: List[_SinkBinding]):
    """Build the sink format rendering each record with its own binding layout.

    A shared sink carries records of several loggers, each configured with its
    own formatter, so the layout is picked per record.  loguru does not append a
    newline to the output of a format callable, hence every binding layout
    already ends with one.

    An intercepted third-party record belongs to no trpc logger; it is rendered
    with the layout of the output that asked for it, so that it looks like every
    other line of that file.
    """

    def _format(record) -> str:
        extra = record["extra"]
        third_party = extra.get(_THIRD_PARTY_KEY)
        name = extra.get("trpc_logger")
        for binding in bindings:
            matches = binding.capture_third_party if third_party else binding.logger_name == name
            if matches:
                line_format = binding.line_format
                return line_format(record) if callable(line_format) else line_format
        return "{message}\n"

    return _format


def _sink_kwargs_for_output(output_config: OutputConfig) -> Optional[Dict]:
    """Resolve one output config into loguru sink arguments via its writer plugin."""
    writer_name = output_config.writer
    plugin = get_plugin(PluginType.WRITER_COMPONENT, writer_name)
    if plugin is None:
        _log_error(f"cannot find writer plugin {PluginType.WRITER_COMPONENT} {writer_name}")
        return None

    if hasattr(plugin, "setup_loguru_sink"):
        return plugin.setup_loguru_sink(output_config)

    sink_kwargs = _build_bridged_sink_kwargs(plugin.setup(output_config), output_config)
    if sink_kwargs is None:
        _log_error(f"writer plugin {writer_name} provides no handler to bridge into loguru")
    return sink_kwargs


def _target_key(sink) -> str:
    """Return the sharing key of a sink target.

    Only file paths are shared: two loggers writing the same file must go
    through the same sink, otherwise each one owns a queue, a writer thread and
    a rotation schedule for that file.  Streams and bridged handlers get a key
    of their own.
    """
    if isinstance(sink, (str, os.PathLike)):
        return "file:" + os.path.realpath(os.fspath(sink))

    global _target_key_seq
    _target_key_seq += 1
    return "sink:%d" % _target_key_seq


def _rotation_signature(sink_kwargs: Dict):
    """Return the file-management settings a shared sink can only have one of."""
    return tuple(sink_kwargs.get(key) for key in ("rotation", "retention", "compression", "encoding"))


def _sink_is_alive(sink_id: Optional[int]) -> bool:
    """Return True unless the sink was removed behind this module's back."""
    handlers = getattr(getattr(_loguru_logger, "_core", None), "handlers", None)
    if handlers is None:  # pragma: no cover - loguru internals moved
        return True
    return sink_id in handlers


def _attach_binding(sink_kwargs: Dict, binding: _SinkBinding) -> _PhysicalSink:
    """Attach a binding to the sink of its target, creating the sink if needed."""
    target_key = _target_key(sink_kwargs["sink"])
    physical = _sinks_by_target.get(target_key)
    if physical is not None and not _sink_is_alive(physical.sink_id):
        del _sinks_by_target[target_key]
        physical = None

    signature = _rotation_signature(sink_kwargs)
    if physical is not None:
        if physical.rotation_signature != signature:
            _log_error(f"logger {binding.logger_name} reuses {target_key} with different rotation "
                       f"settings; the ones of the first output are kept")
        physical.bindings.append(binding)
        return physical

    physical = _PhysicalSink(signature)
    physical.bindings.append(binding)
    sink_kwargs["filter"] = _build_filter(physical.bindings)
    sink_kwargs["format"] = _build_sink_format(physical.bindings)
    physical.sink_id = _loguru_logger.add(**sink_kwargs)
    _sinks_by_target[target_key] = physical
    return physical


def remove_logger_sinks(name: str) -> None:
    """Detach a trpc logger from its sinks, removing the ones left unused.

    A sink is only removed once no logger writes to it any more: another logger
    may share it, and removing it would take its queue and writer thread down.

    Args:
        name: The trpc logger name.
    """
    _bindings_by_logger.pop(name, None)
    _sink_ids_by_logger.pop(name, None)

    for target_key, physical in list(_sinks_by_target.items()):
        physical.bindings[:] = [b for b in physical.bindings if b.logger_name != name]
        if physical.bindings:
            continue
        del _sinks_by_target[target_key]
        try:
            _loguru_logger.remove(physical.sink_id)
        except (ValueError, Exception):  # pylint: disable=broad-except
            pass


def reset_state() -> None:
    """Remove every trpc-managed sink and forget the bookkeeping.

    Only useful to tests and benchmarks, which build loggers repeatedly in one
    interpreter and must not inherit sinks from the previous case.
    """
    for physical in list(_sinks_by_target.values()):
        try:
            _loguru_logger.remove(physical.sink_id)
        except (ValueError, Exception):  # pylint: disable=broad-except
            pass
    _sinks_by_target.clear()
    _bindings_by_logger.clear()
    _sink_ids_by_logger.clear()


def _binding_format(sink_kwargs: Dict):
    """Take the layout of one output out of its sink arguments.

    loguru appends a newline to a static string format but not to the output of
    a format callable, and every layout ends up behind the sink's routing
    callback, so string layouts get their newline here.
    """
    line_format = sink_kwargs.pop("format", "{message}")
    if isinstance(line_format, str) and not line_format.endswith("\n"):
        line_format += "\n"
    return line_format


def _output_filename(output_config: OutputConfig) -> Optional[str]:
    """Return the resolved file an output writes to, or None if it is not a file.

    The path is normalised like ``_target_key()`` does, so that two outputs are
    recognised as the same file whichever way it is spelled, and two files of
    the same name in different directories stay distinct.
    """
    if output_config.writer != "file" or output_config.writer_config is None:
        return None
    path = _resolve_file_path(output_config.writer_config)
    return os.path.realpath(path) if path else None


def _register_sinks(name: str, output_config_list: List[OutputConfig], gate: _LevelHolder) -> int:
    """(Re)register the loguru sinks of a trpc logger name.

    Args:
        name: The trpc logger name.
        output_config_list: List of output configurations.
        gate: The logger-wide level holder shared with the sink filters.

    Returns:
        The minimum stdlib level number across the configured outputs.
    """
    _ensure_default_sink_removed()
    remove_logger_sinks(name)

    bindings: List[_SinkBinding] = []
    sink_ids: List[int] = []
    min_std_level = _LEVEL_NO["fatal"]

    for output_config in output_config_list:
        level_name = (output_config.level or "debug").lower()
        min_std_level = min(min_std_level, _LEVEL_NO.get(level_name, logging.DEBUG))

        sink_kwargs = _sink_kwargs_for_output(output_config)
        if sink_kwargs is None:
            continue

        binding = _SinkBinding(
            logger_name=name,
            gate=gate,
            level_holder=_LevelHolder(_LEVEL_NO.get(level_name, logging.DEBUG)),
            line_format=_binding_format(sink_kwargs),
            filename=_output_filename(output_config),
            capture_third_party=bool(output_config.capture_third_party),
            third_party_level=_LEVEL_NO.get(output_config.effective_third_party_level(), logging.DEBUG),
        )
        physical = _attach_binding(sink_kwargs, binding)
        bindings.append(binding)
        if physical.sink_id not in sink_ids:
            sink_ids.append(physical.sink_id)

    _bindings_by_logger[name] = bindings
    _sink_ids_by_logger[name] = sink_ids
    return min_std_level


def _format_msg(format_str: str, args) -> str:
    """Apply ``%``-style formatting like stdlib logging, without loguru re-parsing.

    The result is passed to loguru as a plain message with no extra args, so
    loguru will not attempt ``{}``-style substitution on it.
    """
    if args:
        try:
            return format_str % args
        except Exception:  # pylint: disable=broad-except
            return format_str
    return format_str


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
        super().__init__([])
        self._name = name
        # ``filename`` is read by some framework bookkeeping; keep it meaningful.
        # The last file output wins, like it does in ``LoggingLogger``.
        self.filename = name
        for output_config in output_config_list:
            if output_config.writer == "file" and output_config.writer_config is not None:
                self.filename = _resolve_file_path(output_config.writer_config)

        level_name = "debug"
        for output_config in output_config_list:
            if output_config.level:
                candidate = output_config.level.lower()
                if _LEVEL_NO.get(candidate, logging.DEBUG) < _LEVEL_NO.get(level_name, logging.DEBUG):
                    level_name = candidate
        self._gate = _LevelHolder(_LEVEL_NO.get(level_name, logging.DEBUG))

        if _LOGURU_AVAILABLE:
            self.min_level = _register_sinks(name, output_config_list, self._gate)
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
        if _LEVEL_NO[level_name] < self._gate.level_no:
            return
        extra = kwargs.get("extra") or {}
        trpc_file = extra.get("filename")
        trpc_line = extra.get("lineno")
        if not trpc_file or not trpc_line:
            caller = sys._getframe(2)
            trpc_file = trpc_file or os.path.basename(caller.f_code.co_filename)
            trpc_line = trpc_line or caller.f_lineno
        ctx_fields = extra.get(CTX_FIELDS_IN_EXTRA_KEY) or {}
        bound = _loguru_logger.bind(**{
            "trpc_logger": self._name,
            "trpc_file": trpc_file,
            "trpc_line": trpc_line,
            _RECORD_NAME_KEY: self.filename,
            CTX_FIELDS_IN_EXTRA_KEY: ctx_fields,
            _CTX_SUFFIX_KEY: _context_suffix(ctx_fields),
            _EXTRA_FIELDS_KEY: {k: v for k, v in extra.items() if k not in _CONSUMED_EXTRA_KEYS},
        })
        bound.opt(depth=1, exception=kwargs.get("exc_info")).log(
            _LEVEL_MAP[level_name], _format_msg(format_str, args)
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

    def _bindings(self) -> List[_SinkBinding]:
        """Return the outputs this logger is currently attached to."""
        return _bindings_by_logger.get(self._name, [])

    def _file_bindings(self) -> List[_SinkBinding]:
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
        level_name = _ENUM_TO_NAME.get(level, "debug")
        self._gate.level_no = _LEVEL_NO[level_name]
        self.min_level = _LEVEL_NO[level_name]

    def set_min_level(self, level: int) -> None:
        """Set the level of this logger's file output (admin entry point).

        Mirrors ``trpc.admin.setLoglevel`` on the stdlib backend: the output
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
            level = _loguru_logger.level(record.levelname).name
        except (ValueError, Exception):  # pylint: disable=broad-except
            level = record.levelno

        # Walk out of the logging machinery so that the reported location is the
        # third-party call site rather than logging/__init__.py.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        bound = _loguru_logger.bind(**{
            _THIRD_PARTY_KEY: True,
            "trpc_file": record.filename,
            "trpc_line": record.lineno,
            _RECORD_NAME_KEY: record.name,
            CTX_FIELDS_IN_EXTRA_KEY: {"lib": record.name},
            _CTX_SUFFIX_KEY: _context_suffix({"lib": record.name}),
            _EXTRA_FIELDS_KEY: _record_own_fields(record),
        })
        bound.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def install_intercept(level: int = logging.NOTSET) -> None:
    """Install ``InterceptHandler`` on the root logger, idempotently.

    Args:
        level: Level to set on the root logger.
    """
    if not _LOGURU_AVAILABLE:
        return

    global _intercept_level
    _intercept_level = level

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
    for binding in _bindings_by_logger.get(name, []):
        binding.capture_third_party = True
        binding.third_party_level = binding.level_holder.level_no


def install_third_party_capture(root_level: int) -> None:
    """Route stdlib and third-party library logs into the marked sinks.

    Args:
        root_level: Level to set on the root logger, the most permissive of the
            levels the marked outputs ask for.
    """
    install_intercept(root_level)


def after_fork_in_child() -> None:
    """Reset locks in a forked child without touching the inherited sinks.

    loguru keeps a single owner queue: the child only enqueues, so rebuilding
    sinks here would create a second writer. Handler locks still need to be
    recreated, exactly like the stdlib backend does.
    """
    from ._logger import recreate_logging_locks  # local import: avoids a circular import

    recreate_logging_locks()


def new_loguru_logger(name: str, output_config_list: List[OutputConfig]) -> LoguruLogger:
    """Factory: build a ``LoguruLogger`` for the given trpc logger name."""
    return LoguruLogger(output_config_list, name)


def complete() -> None:
    """Drain all enqueued records before normal interpreter shutdown."""
    if _LOGURU_AVAILABLE:
        _loguru_logger.complete()


atexit.register(complete)
