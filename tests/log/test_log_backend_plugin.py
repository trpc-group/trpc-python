# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test the log backend plugin layer.

This module covers:
- the baseline guarantees for the default (standard logging) backend
- log backend selection via config / environment variable, including fallback
- the loguru backend's use of WRITER_COMPONENT plugins (native + bridged)
- output equality between both backends for every registered formatter
- runtime level changes through the admin entry points
- third-party (stdlib logging) capture into loguru sinks
"""

import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile

import pytest

from trpc_sdk.abc.log import BaseWriter
from trpc_sdk.abc.log import FormatConfig
from trpc_sdk.abc.log import LogLevel
from trpc_sdk.abc.log import OutputConfig
from trpc_sdk.abc.log import WriterConfig
from trpc_sdk.admin import getLoglevel
from trpc_sdk.admin import setLoglevel
from trpc_sdk.config import config
from trpc_sdk.log import logger as logger_mod
from trpc_sdk.log._format import CTX_FIELDS_IN_EXTRA_KEY
from trpc_sdk.log._logger import ConsoleWriter
from trpc_sdk.log._logger import LoggingLogger
from trpc_sdk.plugin import DefaultPluginManager
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import register_plugin

try:
    from loguru import logger as loguru_logger

    LOGURU_INSTALLED = True
except ImportError:  # pragma: no cover - only without the optional dependency
    loguru_logger = None
    LOGURU_INSTALLED = False

needs_loguru = pytest.mark.skipif(not LOGURU_INSTALLED, reason="loguru is an optional dependency")

# Timestamp prefix of the default layout, masked before comparing two backends.
_TIME_RE = re.compile(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\]")


@pytest.fixture(autouse=True)
def restore_log_globals():
    """Keep every test from leaking backend / logger registry state."""
    saved_default = logger_mod._DEFAULT_LOGGER
    saved_registry = dict(logger_mod._GLOBAL_LOGGER)
    saved_backend = logger_mod._ACTIVE_BACKEND_NAME
    saved_options = logger_mod._ACTIVE_BACKEND_OPTIONS
    saved_root_handlers = list(logging.getLogger().handlers)
    saved_root_level = logging.getLogger().level
    # Another test module may have initialised the framework with the loguru
    # backend and left its intercept handler on root; the assertions below
    # describe what this file installs, so start from a clean root.
    logging.getLogger().handlers = [h for h in saved_root_handlers if not _is_intercept_handler(h)]

    yield

    logger_mod._DEFAULT_LOGGER = saved_default
    logger_mod._GLOBAL_LOGGER.clear()
    logger_mod._GLOBAL_LOGGER.update(saved_registry)
    logger_mod._ACTIVE_BACKEND_NAME = saved_backend
    logger_mod._ACTIVE_BACKEND_OPTIONS = saved_options
    logging.getLogger().handlers = saved_root_handlers
    logging.getLogger().setLevel(saved_root_level)
    if LOGURU_INSTALLED:
        from trpc_sdk.log._loguru_logger import reset_state

        # reset_state() drops the shared-sink bookkeeping, remove() also takes
        # down sinks a test registered on its own.
        reset_state()
        loguru_logger.remove()


def _is_intercept_handler(handler):
    """Return True if the handler routes stdlib logging into loguru."""
    if not LOGURU_INSTALLED:
        return False
    from trpc_sdk.log._loguru_logger import InterceptHandler

    return isinstance(handler, InterceptHandler)


def _intercept_handlers():
    """Return the InterceptHandler instances currently installed on root.

    Root itself is not expected to be empty: pytest installs its own handlers.
    """
    return [h for h in logging.getLogger().handlers if _is_intercept_handler(h)]


def _console_output_config(formatter="json", level="debug", fmt=None):
    """Build a console OutputConfig, optionally with a custom layout."""
    format_config = None
    if fmt is not None:
        format_config = FormatConfig(fmt=fmt, time_fmt="%Y-%m-%d %H:%M:%S")
    return OutputConfig(writer="console", level=level, formatter=formatter, format_config=format_config)


def _capture_output_config(level="debug", fmt=None, third_party_level=""):
    """Build a console OutputConfig which also receives third-party logs."""
    format_config = None
    if fmt is not None:
        format_config = FormatConfig(fmt=fmt, time_fmt="%Y-%m-%d %H:%M:%S")
    return OutputConfig(writer="console", level=level, formatter="json",
                        format_config=format_config,
                        capture_third_party=True, third_party_level=third_party_level)


def _file_output_config(path, level="debug", formatter="json", fmt="%(message)s", max_size=0):
    """Build a file OutputConfig writing ``path`` with a one-field layout."""
    return OutputConfig(writer="file", level=level, formatter=formatter,
                        writer_config=WriterConfig(filename=path, max_size=max_size),
                        format_config=FormatConfig(fmt=fmt, time_fmt="%Y-%m-%d %H:%M:%S"))


def _read_lines(path):
    """Return the non-empty lines written to a log file."""
    with open(path, encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def _loguru_module():
    """Return the loguru backend module, for tests inspecting its bookkeeping."""
    return sys.modules["trpc_sdk.log._loguru_logger"]


def _stdlib_render(output_config, message, extra, level="info"):
    """Render one line through the stdlib backend and return it."""
    writer = ConsoleWriter(output_config)
    stream = io.StringIO()
    writer.handler.setStream(stream)
    stdlib_logger = LoggingLogger([writer])
    getattr(stdlib_logger, level)(message, extra=dict(extra))
    return stream.getvalue().strip()


def _loguru_render(monkeypatch, output_config, message, extra, name="fmtcheck", level="info"):
    """Render one line through the loguru backend and return it."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    # build_console_sink_kwargs captures sys.stderr when the sink is created.
    monkeypatch.setattr(sys, "stderr", stream)
    loguru_backed = new_loguru_logger(name, [output_config])
    getattr(loguru_backed, level)(message, extra=dict(extra))
    loguru_logger.complete()
    return stream.getvalue().strip()


def _stdlib_file_render(output_config, message, extra):
    """Render one line into a file through the stdlib backend and return it."""
    from trpc_sdk.log._logger import FileWriter

    writer = FileWriter(output_config)
    LoggingLogger([writer]).info(message, extra=dict(extra))
    writer.handler.close()
    return _read_lines(writer.filename)


def _loguru_file_render(output_config, message, extra, name="filecheck"):
    """Render one line into a file through the loguru backend and return it."""
    from trpc_sdk.log._loguru_logger import _resolve_file_path
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    new_loguru_logger(name, [output_config]).info(message, extra=dict(extra))
    loguru_logger.complete()
    return _read_lines(_resolve_file_path(output_config.writer_config))


def test_loguru_native_format_compiles_default_layout():
    """The common TRPC text layout has an exact native loguru equivalent."""
    from trpc_sdk.log._loguru_logger import _compile_loguru_format

    compiled = _compile_loguru_format(
        "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(process)d][%(message)s]",
        "%Y-%m-%d %H:%M:%S",
    )

    assert compiled == (
        "[{time:YYYY-MM-DD HH:mm:ss}][{level}]"
        "[{extra[trpc_file]}:{extra[trpc_line]}][{process}][{message}]"
        "{exception}{extra[trpc_ctx_suffix]}"
    )


def test_loguru_native_format_compiles_milliseconds_and_literal_braces():
    """Milliseconds are native time tokens and literal braces stay literal."""
    from trpc_sdk.log._loguru_logger import _compile_loguru_format

    compiled = _compile_loguru_format(
        "{trpc}[%(asctime)s.%(msecs)03d][%(message)s]",
        "%Y-%m-%d %H:%M:%S",
    )

    assert compiled.startswith("{{trpc}}[{time:YYYY-MM-DD HH:mm:ss}.{time:SSS}][{message}]")


@pytest.mark.parametrize(
    ("fmt", "time_fmt"),
    [
        ("[%(name)s][%(message)s]", "%Y-%m-%d %H:%M:%S"),
        ("[%(asctime)s][%(message)s]", "%Y-%j"),
        ("[%(lineno)04d][%(message)s]", "%Y-%m-%d"),
    ],
)
def test_loguru_native_format_rejects_unsupported_layout(fmt, time_fmt):
    """Unknown fields and modifiers fall back instead of changing output."""
    from trpc_sdk.log._loguru_logger import _compile_loguru_format

    assert _compile_loguru_format(fmt, time_fmt) is None


def test_loguru_make_format_selects_native_and_compatibility_paths():
    """Only the built-in text formatter uses the native format fast path."""
    from trpc_sdk.log._loguru_logger import _make_format

    assert isinstance(_make_format(_console_output_config(formatter="json")), str)
    assert callable(_make_format(_console_output_config(formatter="full_json")))


class MemoryWriter(BaseWriter):
    """A writer plugin payload holding a handler that records LogRecords."""

    def __init__(self, output_config):
        super().__init__(output_config)
        self.records = []
        self.handler = _MemoryHandler(self.records)
        self.handler.name = "Memory"


class _MemoryHandler(logging.Handler):
    """Collect the records it is given, without formatting them."""

    def __init__(self, sink_list):
        super().__init__()
        self._sink_list = sink_list

    def emit(self, record):
        self._sink_list.append(record)


@pytest.fixture
def memory_writer_plugin():
    """Register a custom WRITER_COMPONENT plugin with no loguru sink hook."""
    created = []

    @register_plugin(PluginType.WRITER_COMPONENT, "memory_test")
    class MemoryWriterPlugin:
        """Custom writer plugin exposing only the stdlib contract."""

        def setup(self, output_config):
            writer = MemoryWriter(output_config)
            created.append(writer)
            return writer

    yield created

    DefaultPluginManager.plugin_container[PluginType.WRITER_COMPONENT].pop("memory_test", None)
    DefaultPluginManager.plugin_instance.get(PluginType.WRITER_COMPONENT, {}).pop("memory_test", None)


def test_default_backend_writers_are_exported():
    """The import-time default logger and its writers stay public.

    Checked in a subprocess: importing any module that initialises the framework
    (tests/test_server.py does it at import time) legitimately replaces
    the default logger, so the invariant only holds on a fresh interpreter.
    """
    code = ("from trpc_sdk.log import logger as m;"
            "print(m.DEFAULT_FILE_WRITER is not None,"
            " m.DEFAULT_CONSOLE_WRITER is not None,"
            " m._DEFAULT_LOGGER.writers == [m.DEFAULT_CONSOLE_WRITER, m.DEFAULT_FILE_WRITER])")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.split() == ["True", "True", "True"]


@pytest.mark.parametrize("backend_env", [None, "loguru"])
def test_import_does_not_touch_root_logger(backend_env):
    """Importing trpc_sdk.log must not add root handlers nor change its level."""
    env = dict(os.environ)
    env.pop("TRPC_LOG_BACKEND", None)
    if backend_env:
        env["TRPC_LOG_BACKEND"] = backend_env

    code = ("import logging, trpc_sdk.log;"
            "root = logging.getLogger();"
            "print(len(root.handlers), root.level)")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, check=True)
    assert result.stdout.split() == ["0", str(logging.WARNING)]


def test_logging_backend_is_registered_and_default(monkeypatch):
    """Without config or env the logging backend is selected.

    The config is reset explicitly: other test modules load the example yaml,
    which selects loguru, into the process-wide config and never restore it.
    """
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {})

    assert logger_mod.resolve_backend_name() == logger_mod._DEFAULT_LOG_BACKEND_NAME
    backend = logger_mod.get_log_backend(logger_mod._DEFAULT_LOG_BACKEND_NAME)
    assert isinstance(backend, logger_mod.LoggingBackendPlugin)


def test_backend_name_from_config(monkeypatch):
    """plugins.log_backend.<name> selects the backend."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    assert logger_mod.resolve_backend_name() == "loguru"


def test_backend_name_env_overrides_config(monkeypatch):
    """TRPC_LOG_BACKEND wins over the config section."""
    monkeypatch.setenv("TRPC_LOG_BACKEND", "logging")
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    assert logger_mod.resolve_backend_name() == "logging"


def test_unknown_backend_falls_back_to_logging(monkeypatch):
    """An unknown backend name reports an error and falls back."""
    errors = []
    monkeypatch.setenv("TRPC_LOG_BACKEND", "nope")
    monkeypatch.setattr(logger_mod, "error", errors.append)

    name, backend, _ = logger_mod._select_backend()

    assert name == logger_mod._DEFAULT_LOG_BACKEND_NAME
    assert isinstance(backend, logger_mod.LoggingBackendPlugin)
    assert any("nope" in message for message in errors)


def test_unavailable_backend_falls_back_to_logging(monkeypatch):
    """A backend whose dependency is missing reports an error and falls back."""
    errors = []
    monkeypatch.setenv("TRPC_LOG_BACKEND", "loguru")
    monkeypatch.setattr(logger_mod, "error", errors.append)
    monkeypatch.setattr(logger_mod.LoguruBackendPlugin, "available", lambda self: False)

    name, backend, _ = logger_mod._select_backend()

    assert name == logger_mod._DEFAULT_LOG_BACKEND_NAME
    assert isinstance(backend, logger_mod.LoggingBackendPlugin)
    assert any("not available" in message for message in errors)


def test_register_logger_uses_logging_backend_by_default(monkeypatch):
    """The default path still builds LoggingLogger instances."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {})

    logger_mod._register_logger({"default": [{"writer": "console", "level": "info"}]})

    assert isinstance(logger_mod._DEFAULT_LOGGER, LoggingLogger)
    assert logger_mod._ACTIVE_BACKEND_NAME == logger_mod._DEFAULT_LOG_BACKEND_NAME
    assert not _intercept_handlers()


@needs_loguru
def test_register_logger_uses_loguru_backend_from_config(monkeypatch):
    """Config-selected loguru backend builds LoguruLogger instances."""
    from trpc_sdk.log._loguru_logger import LoguruLogger

    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})

    logger_mod._register_logger({
        "default": [{"writer": "console", "level": "info"}],
        "worker": [{"writer": "console", "level": "warning"}],
    })

    assert isinstance(logger_mod._DEFAULT_LOGGER, LoguruLogger)
    assert isinstance(logger_mod._GLOBAL_LOGGER["worker"], LoguruLogger)
    # no output asks for third-party logs -> root logger untouched
    assert not _intercept_handlers()


@needs_loguru
@pytest.mark.parametrize("formatter", ["json", "noop", "full_json"])
def test_formatter_output_matches_stdlib_backend(monkeypatch, formatter):
    """Both backends render a record identically for every formatter."""
    output_config = _console_output_config(formatter=formatter)
    extra = {"filename": "biz.py", "lineno": 42, CTX_FIELDS_IN_EXTRA_KEY: {"uid": 7, "brace": "{oops}"}}

    stdlib_line = _stdlib_render(output_config, "hello world", extra)
    loguru_line = _loguru_render(monkeypatch, _console_output_config(formatter=formatter),
                                 "hello world", extra)

    if formatter == "full_json":
        stdlib_fields = json.loads(stdlib_line)
        loguru_fields = json.loads(loguru_line)
        stdlib_fields.pop("Timestamp")
        loguru_fields.pop("Timestamp")
        assert loguru_fields == stdlib_fields
    else:
        assert _TIME_RE.sub("[TIME]", loguru_line) == _TIME_RE.sub("[TIME]", stdlib_line)


@needs_loguru
def test_custom_format_config_is_honoured(monkeypatch):
    """format_config.fmt applies on the loguru backend too."""
    fmt = "%(levelname)s|%(filename)s:%(lineno)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}

    line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "custom", extra)

    assert line == "INFO|biz.py:7|custom"


@needs_loguru
def test_custom_extra_fields_reach_the_formatter(monkeypatch):
    """A layout naming a caller-supplied field renders it on both backends."""
    fmt = "%(request_id)s|%(uin)d|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7, "request_id": "req-42", "uin": 1000}

    stdlib_line = _stdlib_render(_console_output_config(fmt=fmt), "custom", extra)
    loguru_line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "custom", extra)

    assert loguru_line == stdlib_line == "req-42|1000|custom"


@needs_loguru
def test_logger_name_matches_stdlib_backend(tmp_path):
    """%(name)s reports the output file, like the stdlib backend does."""
    fmt = "%(name)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}
    stdlib_path = str(tmp_path / "stdlib.log")
    loguru_path = str(tmp_path / "loguru.log")

    stdlib_lines = _stdlib_file_render(_file_output_config(stdlib_path, fmt=fmt), "named", extra)
    loguru_lines = _loguru_file_render(_file_output_config(loguru_path, fmt=fmt), "named", extra)

    assert stdlib_lines == ["%s|named" % stdlib_path]
    assert loguru_lines == ["%s|named" % loguru_path]


@needs_loguru
def test_trace_renders_as_debug_like_stdlib(monkeypatch):
    """trace has no loguru TRACE of its own: both backends report DEBUG."""
    fmt = "%(levelname)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}

    stdlib_line = _stdlib_render(_console_output_config(fmt=fmt), "traced", extra, level="trace")
    loguru_line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "traced", extra,
                                 level="trace")

    assert loguru_line == stdlib_line == "DEBUG|traced"


@needs_loguru
def test_trace_level_survives_an_admin_round_trip(monkeypatch):
    """Reading the level of a trace output and writing it back keeps it open."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    loguru_backed = new_loguru_logger("traced", [_console_output_config(level="trace")])

    setLoglevel(loguru_backed, getLoglevel(loguru_backed))
    loguru_backed.trace("still here", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert "still here" in stream.getvalue()


@needs_loguru
def test_set_level_trace_opens_the_gate_to_debug(monkeypatch):
    """LevelTrace is the stdlib DEBUG gate, not a level of its own."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    monkeypatch.setattr(sys, "stderr", io.StringIO())
    loguru_backed = new_loguru_logger("gatetrace", [_console_output_config(level="debug")])

    loguru_backed.set_level(LogLevel.LevelTrace)

    assert loguru_backed.min_level == logging.DEBUG


@needs_loguru
def test_custom_writer_plugin_is_bridged(memory_writer_plugin):
    """A writer plugin without a loguru hook still receives correct records."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    loguru_backed = new_loguru_logger("bridge", [OutputConfig(writer="memory_test", level="debug")])
    loguru_backed.info("bridged %s", "message",
                       extra={"filename": "biz.py", "lineno": 11, CTX_FIELDS_IN_EXTRA_KEY: {"uid": 3}})
    loguru_logger.complete()

    records = memory_writer_plugin[0].records
    assert len(records) == 1
    record = records[0]
    assert record.getMessage() == "bridged message"
    assert record.filename == "biz.py"
    assert record.lineno == 11
    assert record.process == os.getpid()
    assert record.levelname == "INFO"
    assert record.__dict__[CTX_FIELDS_IN_EXTRA_KEY] == {"uid": 3}


@needs_loguru
def test_bridged_writer_receives_custom_extra_fields(memory_writer_plugin):
    """A bridged writer sees the caller's own fields, like on the stdlib backend."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    loguru_backed = new_loguru_logger("bridgeextra", [OutputConfig(writer="memory_test", level="debug")])
    loguru_backed.info("with fields", extra={"filename": "biz.py", "lineno": 11, "request_id": "req-7"})
    loguru_logger.complete()

    record = memory_writer_plugin[0].records[0]
    assert record.request_id == "req-7"


@needs_loguru
def test_unknown_writer_reports_error_instead_of_stderr(monkeypatch):
    """An unregistered writer name is an error, not a silent console fallback."""
    from trpc_sdk.log import loguru_logger as loguru_backend
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    errors = []
    monkeypatch.setattr(loguru_backend, "_log_error", errors.append)

    new_loguru_logger("badwriter", [OutputConfig(writer="does_not_exist", level="info")])

    assert any("does_not_exist" in message for message in errors)
    assert loguru_backend._sink_ids_by_logger["badwriter"] == []


@needs_loguru
def test_dynamic_level_change(monkeypatch):
    """set_level takes effect without re-registering sinks."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    loguru_backed = new_loguru_logger("levels", [_console_output_config(level="debug")])
    sink_ids = list(sys.modules["trpc_sdk.log._loguru_logger"]._sink_ids_by_logger["levels"])

    loguru_backed.set_level(LogLevel.LevelError)
    loguru_backed.info("dropped", extra={"filename": "biz.py", "lineno": 1})
    loguru_backed.error("kept", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    output = stream.getvalue()
    assert "dropped" not in output
    assert "kept" in output
    # no new sink was created
    assert sys.modules["trpc_sdk.log._loguru_logger"]._sink_ids_by_logger["levels"] == sink_ids


@needs_loguru
def test_admin_loglevel_on_loguru_backend(monkeypatch):
    """admin get/setLoglevel work on the loguru backend."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    monkeypatch.setattr(sys, "stderr", io.StringIO())
    loguru_backed = new_loguru_logger("adminlevel", [_console_output_config(level="info")])

    assert getLoglevel(loguru_backed) == logging.INFO
    setLoglevel(loguru_backed, logging.ERROR)
    assert getLoglevel(loguru_backed) == logging.ERROR


@needs_loguru
def test_each_sink_keeps_its_own_level(monkeypatch, tmp_path):
    """A record only reaches the outputs whose own level accepts it."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "levels.log")
    loguru_backed = new_loguru_logger("persink", [_console_output_config(level="debug"),
                                                  _file_output_config(path, level="error")])

    loguru_backed.debug("only-console", extra={"filename": "biz.py", "lineno": 1})
    loguru_backed.error("both", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    console = stream.getvalue()
    assert [line.split("[")[-1].rstrip("]") for line in console.splitlines()] == ["only-console", "both"]
    assert _read_lines(path) == ["both"]


@needs_loguru
def test_set_level_moves_the_logger_gate_only(monkeypatch, tmp_path):
    """set_level tightens every output at once without erasing their levels."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "gate.log")
    loguru_backed = new_loguru_logger("gate", [_console_output_config(level="debug"),
                                               _file_output_config(path, level="info")])

    loguru_backed.set_level(LogLevel.LevelWarning)
    loguru_backed.info("dropped", extra={"filename": "biz.py", "lineno": 1})
    loguru_backed.warning("kept", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    assert "dropped" not in stream.getvalue()
    assert _read_lines(path) == ["kept"]

    # Reopening the gate restores each output's configured level: the console
    # takes DEBUG again, the file output still filters it at info.
    loguru_backed.set_level(LogLevel.LevelDebug)
    loguru_backed.debug("back", extra={"filename": "biz.py", "lineno": 3})
    loguru_logger.complete()

    assert "back" in stream.getvalue()
    assert _read_lines(path) == ["kept"]


@needs_loguru
def test_admin_loglevel_targets_the_file_output(monkeypatch, tmp_path):
    """setLoglevel moves the file output only, like on the stdlib backend."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "admin.log")
    loguru_backed = new_loguru_logger("adminfile", [_console_output_config(level="info"),
                                                    _file_output_config(path, level="error")])

    assert getLoglevel(loguru_backed) == logging.ERROR
    assert loguru_backed.min_level == logging.INFO

    setLoglevel(loguru_backed, logging.DEBUG)
    loguru_backed.debug("debugged", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert getLoglevel(loguru_backed) == logging.DEBUG
    assert loguru_backed.min_level == logging.DEBUG
    assert _read_lines(path) == ["debugged"]
    assert "debugged" not in stream.getvalue()


@needs_loguru
def test_admin_loglevel_tells_same_named_files_apart(tmp_path):
    """Two outputs whose files share a basename are distinct admin targets."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    other = OutputConfig(writer="file", level="error", formatter="json",
                         writer_config=WriterConfig(filename="app.log", log_path=str(first_dir)),
                         format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    # The logger is named after its last file output, so this one is the target.
    target = OutputConfig(writer="file", level="error", formatter="json",
                          writer_config=WriterConfig(filename="app.log", log_path=str(second_dir)),
                          format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    loguru_backed = new_loguru_logger("samename", [other, target])

    setLoglevel(loguru_backed, logging.DEBUG)
    loguru_backed.debug("debugged", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert _read_lines(str(second_dir / "app.log")) == ["debugged"]
    assert _read_lines(str(first_dir / "app.log")) == []


@needs_loguru
def test_same_file_shares_one_sink(tmp_path):
    """Two loggers on one file write through a single sink, each with its layout."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    path = str(tmp_path / "shared.log")
    logger_a = new_loguru_logger("share_a", [_file_output_config(path, fmt="A|%(message)s")])
    # Same file spelled differently: the target is keyed by its resolved path.
    logger_b = new_loguru_logger("share_b", [_file_output_config(os.path.join(str(tmp_path), ".", "shared.log"),
                                                                 fmt="B|%(message)s")])

    module = _loguru_module()
    assert module._sink_ids_by_logger["share_a"] == module._sink_ids_by_logger["share_b"]
    assert len(loguru_logger._core.handlers) == 1

    logger_a.info("one", extra={"filename": "biz.py", "lineno": 1})
    logger_b.info("two", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    assert _read_lines(path) == ["A|one", "B|two"]


@needs_loguru
def test_shared_sink_outlives_one_of_its_loggers(monkeypatch, tmp_path):
    """A shared sink is only removed once no logger writes to it any more."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    path = str(tmp_path / "refcount.log")
    new_loguru_logger("ref_a", [_file_output_config(path)])
    new_loguru_logger("ref_b", [_file_output_config(path)])
    module = _loguru_module()
    sink_id = module._sink_ids_by_logger["ref_a"][0]

    logger_a = new_loguru_logger("ref_a", [_file_output_config(path)])

    assert module._sink_ids_by_logger["ref_a"] == [sink_id]
    assert sink_id in loguru_logger._core.handlers
    logger_a.info("still here", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()
    assert _read_lines(path) == ["still here"]

    monkeypatch.setattr(sys, "stderr", io.StringIO())
    new_loguru_logger("ref_a", [_console_output_config()])
    new_loguru_logger("ref_b", [_console_output_config()])

    assert sink_id not in loguru_logger._core.handlers


@needs_loguru
def test_conflicting_rotation_on_shared_file_reports_error(monkeypatch, tmp_path):
    """Sharing a file means sharing its rotation settings, which is reported."""
    from trpc_sdk.log import loguru_logger as loguru_backend

    errors = []
    monkeypatch.setattr(loguru_backend, "_log_error", errors.append)
    path = str(tmp_path / "rotation.log")
    loguru_backend.new_loguru_logger("rot_a", [_file_output_config(path, max_size=10)])
    loguru_backend.new_loguru_logger("rot_b", [_file_output_config(path, max_size=20)])

    assert any("rotation" in message for message in errors)


@needs_loguru
def test_initialisation_keeps_application_sinks(monkeypatch):
    """Only loguru's default sink is removed, not the ones the app registered."""
    from trpc_sdk.log import loguru_logger as loguru_backend

    stream = io.StringIO()
    app_sink = loguru_logger.add(stream, format="{message}")
    monkeypatch.setattr(loguru_backend, "_default_sink_removed", False)
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    loguru_backend.new_loguru_logger("keepsinks", [_console_output_config()])
    loguru_logger.info("application line")
    loguru_logger.complete()

    assert app_sink in loguru_logger._core.handlers
    assert "application line" in stream.getvalue()


def test_admin_loglevel_on_logging_backend():
    """admin get/setLoglevel keep working on the default backend."""
    writer = ConsoleWriter(_console_output_config(level="info"))
    writer.filename = "adminstdlib"
    stdlib_logger = LoggingLogger([writer])
    stdlib_logger.filename = "adminstdlib"

    setLoglevel(stdlib_logger, logging.ERROR)

    assert getLoglevel(stdlib_logger) == logging.ERROR
    assert stdlib_logger.logger.level == logging.ERROR


@needs_loguru
def test_third_party_capture(monkeypatch):
    """Third-party stdlib logs reach the sinks with a real call site and lib tag."""
    from trpc_sdk.log._loguru_logger import install_third_party_capture
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    new_loguru_logger("default", [_capture_output_config(level="debug")])
    install_third_party_capture(logging.INFO)

    third_party = logging.getLogger("urllib3.connectionpool")
    third_party.info("Starting new HTTPS connection")
    expected_lineno = sys._getframe().f_lineno - 1
    loguru_logger.complete()

    line = stream.getvalue().strip()
    assert "Starting new HTTPS connection" in line
    assert "[test_log_backend_plugin.py:%d]" % expected_lineno in line
    assert '{"lib": "urllib3.connectionpool"}' in line


@needs_loguru
def test_third_party_capture_keeps_custom_extra_fields(monkeypatch):
    """Fields a library passes in extra survive the trip through loguru."""
    from trpc_sdk.log._loguru_logger import install_third_party_capture
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    new_loguru_logger("default", [_capture_output_config(fmt="%(request_id)s|%(message)s")])
    install_third_party_capture(logging.INFO)

    logging.getLogger("some.library").info("with fields", extra={"request_id": "req-9"})
    loguru_logger.complete()

    assert stream.getvalue().strip() == 'req-9|with fields[{"lib": "some.library"}]'


@needs_loguru
def test_third_party_capture_only_reaches_marked_outputs(monkeypatch, tmp_path):
    """Marking the file output keeps third-party logs out of the console."""
    from trpc_sdk.log._loguru_logger import install_third_party_capture
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "capture.log")
    file_output = _file_output_config(path)
    file_output.capture_third_party = True
    loguru_backed = new_loguru_logger("default", [_console_output_config(level="debug"), file_output])
    install_third_party_capture(logging.INFO)

    logging.getLogger("some.library").info("third party line")
    loguru_backed.info("framework line", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert _read_lines(path) == ['third party line[{"lib": "some.library"}]', "framework line"]
    assert "third party line" not in stream.getvalue()
    assert "framework line" in stream.getvalue()


@needs_loguru
def test_third_party_level_is_independent_of_the_output_level(monkeypatch):
    """An output can take framework debug logs but only third-party warnings."""
    from trpc_sdk.log._loguru_logger import install_third_party_capture
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    loguru_backed = new_loguru_logger(
        "default", [_capture_output_config(level="debug", third_party_level="warning")])
    install_third_party_capture(logging.DEBUG)

    library = logging.getLogger("some.library")
    library.info("filtered out")
    library.warning("kept")
    loguru_backed.debug("framework debug", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    output = stream.getvalue()
    assert "filtered out" not in output
    assert "kept" in output
    assert "framework debug" in output


@needs_loguru
def test_third_party_capture_needs_a_marked_output(monkeypatch):
    """Selecting the loguru backend alone must not touch the root logger."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    root_level = logging.getLogger().level

    logger_mod._register_logger({"default": [{"writer": "console", "level": "info"}]})

    assert not _intercept_handlers()
    assert logging.getLogger().level == root_level


@needs_loguru
def test_third_party_capture_from_yaml_marks_the_output(monkeypatch):
    """capture_third_party in the log config installs the intercept handler."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)

    logger_mod._register_logger({
        "default": [{"writer": "console", "level": "info", "capture_third_party": True,
                     "third_party_level": "warning"}],
    })

    assert len(_intercept_handlers()) == 1
    assert logging.getLogger().level == logging.WARNING

    logging.getLogger("some.library").warning("captured")
    loguru_logger.complete()
    assert "captured" in stream.getvalue()


@needs_loguru
def test_third_party_capture_forced_by_env_falls_back_to_default_logger(monkeypatch):
    """TRPC_LOG_INTERCEPT captures into every output of the default logger."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setenv("TRPC_LOG_INTERCEPT", "1")
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)

    logger_mod._register_logger({"default": [{"writer": "console", "level": "info"}]})

    assert len(_intercept_handlers()) == 1

    logging.getLogger("some.library").info("forced in")
    loguru_logger.complete()
    assert "forced in" in stream.getvalue()


@needs_loguru
def test_third_party_capture_disabled_by_env(monkeypatch):
    """TRPC_LOG_INTERCEPT=0 wins over a marked output."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setenv("TRPC_LOG_INTERCEPT", "0")
    monkeypatch.setattr(config, "global_plugins_config", {"log_backend": {"loguru": {}}})
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    logger_mod._register_logger({
        "default": [{"writer": "console", "level": "info", "capture_third_party": True}],
    })

    assert not _intercept_handlers()


def test_third_party_capture_is_skipped_on_the_logging_backend(monkeypatch):
    """The stdlib backend has no such entry point; root is left alone."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(config, "global_plugins_config", {})
    root_handlers = list(logging.getLogger().handlers)
    root_level = logging.getLogger().level

    logger_mod._register_logger({
        "default": [{"writer": "console", "level": "info", "capture_third_party": True}],
    })

    assert logging.getLogger().handlers == root_handlers
    assert logging.getLogger().level == root_level


@needs_loguru
def test_install_intercept_keeps_existing_handlers(monkeypatch):
    """install_intercept must not drop handlers installed by other code."""
    from trpc_sdk.log._loguru_logger import InterceptHandler
    from trpc_sdk.log._loguru_logger import install_intercept

    existing = logging.NullHandler()
    logging.getLogger().addHandler(existing)

    install_intercept(logging.INFO)
    install_intercept(logging.INFO)  # idempotent

    handlers = logging.getLogger().handlers
    assert existing in handlers
    assert len([h for h in handlers if isinstance(h, InterceptHandler)]) == 1


@needs_loguru
def test_multiprocess_same_file_integrity():
    """Forked children enqueue into the master's single owner queue."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    procs, count = 2, 200
    out_dir = tempfile.mkdtemp(prefix="trpc_log_backend_")
    path = os.path.join(out_dir, "shared.log")
    writer_config = WriterConfig(filename=path)
    output_config = OutputConfig(writer="file", level="info", formatter="json",
                                 writer_config=writer_config,
                                 format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    loguru_backed = new_loguru_logger("mp", [output_config])

    children = []
    for _ in range(procs):
        pid = os.fork()
        if pid == 0:  # child: only enqueue, the master thread is the sole writer
            try:
                for seq in range(count):
                    loguru_backed.info("MARK|%d|%d|END", os.getpid(), seq,
                                       extra={"filename": "child.py", "lineno": seq})
            finally:
                os._exit(0)
        children.append(pid)

    for pid in children:
        os.waitpid(pid, 0)
    loguru_logger.complete()

    with open(path, encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    assert len(lines) == procs * count
    assert all(re.fullmatch(r"MARK\|\d+\|\d+\|END", line) for line in lines)
    assert len(set(lines)) == procs * count


@needs_loguru
def test_multiprocess_shared_file_across_loggers(tmp_path):
    """Two loggers on one file still leave a single owner queue to inherit."""
    from trpc_sdk.log._loguru_logger import new_loguru_logger

    procs, count = 2, 100
    path = str(tmp_path / "shared_mp.log")
    logger_a = new_loguru_logger("mp_a", [_file_output_config(path, level="info", fmt="A|%(message)s")])
    logger_b = new_loguru_logger("mp_b", [_file_output_config(path, level="info", fmt="B|%(message)s")])

    children = []
    for _ in range(procs):
        pid = os.fork()
        if pid == 0:  # child: only enqueue, the master thread is the sole writer
            try:
                for seq in range(count):
                    logger_a.info("MARK|%d|%d|END", os.getpid(), seq,
                                  extra={"filename": "child.py", "lineno": seq})
                    logger_b.info("MARK|%d|%d|END", os.getpid(), seq,
                                  extra={"filename": "child.py", "lineno": seq})
            finally:
                os._exit(0)
        children.append(pid)

    for pid in children:
        os.waitpid(pid, 0)
    loguru_logger.complete()

    lines = _read_lines(path)
    assert len(lines) == procs * count * 2
    assert all(re.fullmatch(r"[AB]\|MARK\|\d+\|\d+\|END", line) for line in lines)
    assert len(set(lines)) == procs * count * 2
    assert sum(1 for line in lines if line.startswith("A|")) == procs * count
