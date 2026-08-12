# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Writer plugins and sink adapters for the Loguru backend."""

import logging
import os
import re
import sys
from abc import abstractmethod
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseWriter
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._constants import CTX_SUFFIX_KEY
from ._constants import LEVEL_NAME_TO_LOGGING_LEVEL
from ._format import get_logger_formatter
from ._types import LevelHolder
from ._types import SinkBinding
from ._utils import resolve_file_path
from ._utils import to_std_record

# cSpell:ignore levelname
# cSpell:ignore trpc

# Default layout, identical to BaseWriter's.
_DEFAULT_FMT = ("[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(process)d][%(message)s]")
_DEFAULT_TIME_FMT = "%Y-%m-%d %H:%M:%S"
# Translate the supported Python strftime subset to loguru time tokens.
_TIME_TOKENS = {
    "%Y": "YYYY",
    "%m": "MM",
    "%d": "DD",
    "%H": "HH",
    "%M": "mm",
    "%S": "ss",
}

# logging.Formatter percent-style fields which have exact loguru equivalents.
_PERCENT_FIELD_RE = re.compile(r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)"
                               r"(?P<flags>[-+ #0]*)(?P<width>\d*)(?P<precision>\.\d+)?(?P<type>[diouxXeEfFgGcrs])")
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


class LoggerWriterPlugin(BaseWriter):
    """Base class for all logger writer plugins."""

    # Rendered-line keys must be unique per output (each one has its own formatter).
    _render_key_seq: int = 0

    def create_log_sink(
        self,
        logger_name: str,
        output_config: OutputConfig,
        gate: LevelHolder,
    ) -> Optional[SinkBinding]:
        """Create a logical binding for one logger output.

        Args:
            logger_name: Name of the tRPC logger that owns the output.
            output_config: The output configuration.
            gate: The gate level holder.

        Returns:
            A sink binding, or ``None`` when the writer cannot create a sink.
        """
        sink_kwargs = self._get_sink_kwargs(output_config)
        if sink_kwargs is None:
            return None
        return self._create_binding(logger_name, output_config, gate, sink_kwargs)

    @classmethod
    def _create_binding(
        cls,
        logger_name: str,
        output_config: OutputConfig,
        gate: LevelHolder,
        sink_kwargs: Dict,
    ) -> SinkBinding:
        """Create a logical binding from prepared sink arguments.

        Args:
            logger_name: Name of the tRPC logger that owns the output.
            output_config: The output configuration.
            gate: The logger-wide level holder.
            sink_kwargs: Arguments passed to ``loguru.logger.add``.

        Returns:
            A logical sink binding.
        """
        level_name = (output_config.level or "debug").lower()
        return SinkBinding(
            logger_name=logger_name,
            gate=gate,
            level_holder=LevelHolder(LEVEL_NAME_TO_LOGGING_LEVEL.get(level_name, logging.DEBUG)),
            line_format=cls._binding_format(sink_kwargs),
            filename=cls._output_filename(output_config),
            capture_third_party=bool(output_config.capture_third_party),
            third_party_level=LEVEL_NAME_TO_LOGGING_LEVEL.get(output_config.effective_third_party_level(),
                                                              logging.DEBUG),
            options=sink_kwargs,
        )

    @classmethod
    def _resolve_layout(cls, output_config: OutputConfig):
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

    @staticmethod
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

    @staticmethod
    def _escape_loguru_literal(text: str) -> str:
        """Escape braces in literal layout text for loguru's format parser."""
        return text.replace("{", "{{").replace("}", "}}")

    @classmethod
    def _compile_loguru_format(cls, fmt: str, time_fmt: str) -> Optional[str]:
        """Compile a supported stdlib percent layout into a native loguru layout.

        The conversion happens once while the sink is registered.  Unsupported
        fields or formatting modifiers return ``None`` and use the existing
        ``logging.Formatter`` compatibility path.
        """
        native_time_fmt = cls._compile_loguru_time_format(time_fmt)
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
                result.append(cls._escape_loguru_literal(fmt[literal_start:index]))
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
            if key == ("msecs", "0", "3", None, "d"):  # cSpell:ignore msecs
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
        # TrpcFormatterWithCtx appends context fields after that.
        # loguru appends a newline to static string formats automatically.
        return "".join(result) + "{exception}{extra[%s]}" % CTX_SUFFIX_KEY

    @classmethod
    def _common_sink_kwargs(cls, output_config: OutputConfig) -> Dict:
        """Return the sink arguments shared by every trpc-managed loguru sink."""
        return {
            # The static level stays at the lowest value: the real threshold is
            # enforced by the filter so that set_level() needs no sink rebuild.
            "level": "TRACE",
            "format": cls._make_format(output_config),
            "enqueue": True,
            "backtrace": False,
            "diagnose": False,
            "catch": True,
        }

    @classmethod
    def _next_render_key(cls) -> str:
        """Return a process-unique key used to pass a rendered line to loguru."""
        cls._render_key_seq += 1
        return "trpc_rendered_%d" % cls._render_key_seq

    @classmethod
    def _make_format(cls, output_config: OutputConfig):
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
        fmt, time_fmt = cls._resolve_layout(output_config)
        # The empty formatter name resolves to ``json`` in logger_format.  Only
        # these built-in text layouts have native equivalents; noop/full_json and
        # custom formatter classes retain the compatibility callback below.
        if output_config.formatter in ("", "json"):
            native_format = cls._compile_loguru_format(fmt, time_fmt)
            if native_format is not None:
                return native_format

        formatter_cls = get_logger_formatter(output_config.formatter)
        trpc_formatter = formatter_cls(fmt=fmt, datefmt=time_fmt)
        key = cls._next_render_key()
        template: str = "{extra[%s]}\n" % key

        def _formatter(record) -> str:
            """Render a Loguru record through the configured formatter."""
            record["extra"][key] = trpc_formatter.format(to_std_record(record))
            return template

        return _formatter

    @staticmethod
    def _binding_format(sink_kwargs: Dict) -> str:
        """Take the layout of one output out of its sink arguments.

        loguru appends a newline to a static string format but not to the output of
        a format callable, and every layout ends up behind the sink's routing
        callback, so string layouts get their newline here.
        """
        line_format = sink_kwargs.pop("format", "{message}")
        if isinstance(line_format, str) and not line_format.endswith("\n"):
            line_format += "\n"
        return line_format

    @staticmethod
    def _output_filename(output_config: OutputConfig) -> Optional[str]:
        """Return the resolved file an output writes to, or None if it is not a file.

        The path is normalized like ``_target_key()`` does, so that two outputs are
        recognized as the same file whichever way it is spelled, and two files of
        the same name in different directories stay distinct.
        """
        if output_config.writer != "file" or output_config.writer_config is None:
            return None
        path = resolve_file_path(output_config.writer_config)
        return os.path.realpath(path) if path else None

    @abstractmethod
    def _get_sink_kwargs(self, output_config: OutputConfig) -> Dict:
        """Get the sink kwargs for the writer plugin."""
        ...


@register_plugin(PluginType.LOG_WRITER, "console")
class ConsoleWriter(LoggerWriterPlugin):
    """Console writer plugin for logging to stdout/stderr."""

    def _get_sink_kwargs(self, output_config: OutputConfig) -> Dict:
        """Create a log sink for console output.

        Args:
            output_config: Output configuration for the console writer.

        Returns:
            Keyword arguments for a log sink.
        """
        self._output_config = output_config
        sink_kwargs = self._common_sink_kwargs(output_config)
        sink_kwargs["sink"] = sys.stderr
        sink_kwargs["colorize"] = False
        return sink_kwargs


@register_plugin(PluginType.LOG_WRITER, "file")
class FileWriter(LoggerWriterPlugin):
    """File writer plugin for logging to rotating files."""

    def _get_sink_kwargs(self, output_config: OutputConfig) -> Dict:
        """Create a log sink for rotating file output.

        Args:
            output_config: Output configuration for the file writer.

        Returns:
            Keyword arguments for a log sink, keeping loguru's own
            rotation/retention/compression and single-owner enqueue machinery.
        """
        self._output_config = output_config
        writer_config = output_config.writer_config
        path = resolve_file_path(writer_config)

        sink_kwargs = self._common_sink_kwargs(output_config)
        sink_kwargs["sink"] = path
        sink_kwargs["rotation"] = (writer_config.max_size * 1000000 if writer_config.max_size else None)
        sink_kwargs["retention"] = (writer_config.max_backups if writer_config.max_backups else None)
        sink_kwargs["compression"] = "gz" if writer_config.compress else None
        sink_kwargs["encoding"] = "utf-8"
        return sink_kwargs


class WriterHandlerSink:
    """Bridge a ``LOG_WRITER`` plugin's stdlib handler into a Loguru sink.

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


def build_bridged_sink_kwargs(writer: BaseWriter) -> Optional[Dict]:
    """Build sink arguments that reuse a writer plugin's stdlib handler.

    Args:
        writer: The writer plugin.

    Returns:
        Sink arguments, or ``None`` when the writer has no handler.
    """
    handler: Optional[logging.Handler] = writer.handler
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
