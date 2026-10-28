# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test the log backend plugin layer.

Loguru is the only runtime backend. These tests cover sink registration,
formatter output, writer plugins, admin level changes, and third-party capture.
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

from trpc_sdk.abc import FormatConfig
from trpc_sdk.abc import LogLevelType
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import WriterConfig
from trpc_sdk.admin import get_log_level
from trpc_sdk.admin import set_log_level
from trpc_sdk.log import LoguruFactoryPlugin
from trpc_sdk.log import LoguruLogger
from trpc_sdk.log import create_loguru_logger
from trpc_sdk.log import get_logger_formatter
from trpc_sdk.log import logger as logger_mod
from trpc_sdk.log._constants import CTX_FIELDS_IN_EXTRA_KEY
from trpc_sdk.log._loguru import InterceptHandler
from trpc_sdk.log._loguru import _LoguruSinkManager
from trpc_sdk.log._loguru import install_third_party_capture
from trpc_sdk.log._loguru import reset_state
from trpc_sdk.log._writer import LoggerWriterPlugin
from trpc_sdk.log._writer import WriterHandlerSink
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER
from trpc_sdk.plugin import register_plugin

try:
    from loguru import logger as loguru_logger

    LOGURU_INSTALLED = True
except ImportError:  # pragma: no cover - only without the optional dependency
    loguru_logger = None
    LOGURU_INSTALLED = False

needs_loguru = pytest.mark.skipif(not LOGURU_INSTALLED, reason="loguru is an optional dependency")

# Timestamp prefix of the default layout, masked before comparing two renders.
_TIME_RE = re.compile(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\]")
_LEVEL_NO = {
    "trace": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.CRITICAL,
}


@pytest.fixture(autouse=True)
def restore_log_globals():
    """Keep every test from leaking backend / logger registry state."""
    manager = logger_mod._LoggerManager
    saved_default = manager._default_logger
    saved_registry = dict(manager._loggers)
    saved_backend = manager._active_backend_name
    saved_initialized = manager._already_initialized
    saved_root_handlers = list(logging.getLogger().handlers)
    saved_root_level = logging.getLogger().level
    saved_default_removed = _LoguruSinkManager._default_sink_removed
    logging.getLogger().handlers = [h for h in saved_root_handlers if not _is_intercept_handler(h)]

    yield

    manager._default_logger = saved_default
    manager._loggers.clear()
    manager._loggers.update(saved_registry)
    manager._active_backend_name = saved_backend
    manager._already_initialized = saved_initialized
    logging.getLogger().handlers = saved_root_handlers
    logging.getLogger().setLevel(saved_root_level)
    _LoguruSinkManager._default_sink_removed = saved_default_removed
    if LOGURU_INSTALLED:
        reset_state()
        loguru_logger.remove()


def _is_intercept_handler(handler):
    """Return True if the handler routes stdlib logging into loguru."""
    if not LOGURU_INSTALLED:
        return False
    return isinstance(handler, InterceptHandler)


def _intercept_handlers():
    """Return the InterceptHandler instances currently installed on root."""
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
    return OutputConfig(writer="console",
                        level=level,
                        formatter="json",
                        format_config=format_config,
                        capture_third_party=True,
                        third_party_level=third_party_level)


def _file_output_config(path, level="debug", formatter="json", fmt="%(message)s", max_size=0):
    """Build a file OutputConfig writing ``path`` with a one-field layout."""
    return OutputConfig(writer="file",
                        level=level,
                        formatter=formatter,
                        writer_config=WriterConfig(filename=path, max_size=max_size),
                        format_config=FormatConfig(fmt=fmt, time_fmt="%Y-%m-%d %H:%M:%S"))


def _read_lines(path):
    """Return the non-empty lines written to a log file."""
    with open(path, encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def _reference_line(output_config, message, extra, level="info", name=""):
    """Render one line with the registered logging formatter."""
    fmt, time_fmt = LoggerWriterPlugin._resolve_layout(output_config)
    formatter = get_logger_formatter(output_config.formatter or "json")(fmt=fmt, datefmt=time_fmt)
    record = logging.LogRecord(
        name=name,
        level=_LEVEL_NO[level],
        pathname=extra.get("filename", ""),
        lineno=int(extra.get("lineno") or 0),
        msg=message,
        args=None,
        exc_info=None,
    )
    record.filename = extra.get("filename", record.filename)
    record.process = os.getpid()
    for key, value in extra.items():
        setattr(record, key, value)
    return formatter.format(record)


def _loguru_render(monkeypatch, output_config, message, extra, name="fmtcheck", level="info"):
    """Render one line through the loguru backend and return it."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    backed = create_loguru_logger(name, [output_config])
    getattr(backed, level)(message, extra=dict(extra))
    loguru_logger.complete()
    return stream.getvalue().rstrip("\n")


def _loguru_file_render(output_config, message, extra, name="filecheck"):
    """Render one line into a file through the loguru backend and return it."""
    create_loguru_logger(name, [output_config]).info(message, extra=dict(extra))
    loguru_logger.complete()
    from trpc_sdk.log._utils import resolve_file_path

    return _read_lines(resolve_file_path(output_config.writer_config))


def test_loguru_native_format_compiles_default_layout():
    """The common TRPC text layout has an exact native loguru equivalent."""
    compiled = LoggerWriterPlugin._compile_loguru_format(
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
    compiled = LoggerWriterPlugin._compile_loguru_format(
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
    assert LoggerWriterPlugin._compile_loguru_format(fmt, time_fmt) is None


def test_loguru_make_format_selects_native_and_compatibility_paths():
    """Only the built-in text formatter uses the native format fast path."""
    assert isinstance(LoggerWriterPlugin._make_format(_console_output_config(formatter="json")), str)
    assert callable(LoggerWriterPlugin._make_format(_console_output_config(formatter="full_json")))


class _MemoryHandler(logging.Handler):
    """Collect the records it is given, without formatting them."""

    def __init__(self, sink_list):
        super().__init__()
        self._sink_list = sink_list

    def emit(self, record):
        self._sink_list.append(record)


@pytest.fixture
def memory_writer_plugin():
    """Register a custom LOG_WRITER plugin that bridges a stdlib handler."""
    created = []

    @register_plugin(PluginType.LOG_WRITER, "memory_test")
    class MemoryWriterPlugin(LoggerWriterPlugin):
        """Writer plugin whose sink records the stdlib LogRecord it receives."""

        def __init__(self):
            super().__init__()
            self.records = []
            self._handler = _MemoryHandler(self.records)
            self._handler.name = "Memory"
            created.append(self)

        def _get_sink_kwargs(self, output_config):
            return {
                "sink": WriterHandlerSink(self._handler),
                "format": "{message}",
                "level": "TRACE",
                "enqueue": False,
                "backtrace": False,
                "diagnose": False,
                "catch": True,
            }

    yield created

    DEFAULT_PLUGIN_MANAGER._plugin_specs.get(PluginType.LOG_WRITER, {}).pop("memory_test", None)
    DEFAULT_PLUGIN_MANAGER._plugin_instances.get(PluginType.LOG_WRITER, {}).pop("memory_test", None)


def test_default_backend_writers_are_exported():
    """A fresh interpreter exposes the default logger and the built-in writers."""
    code = ("from trpc_sdk.abc import BaseLogger, PluginType;"
            "from trpc_sdk.log import get_logger;"
            "from trpc_sdk.plugin import get_plugin;"
            "print(isinstance(get_logger('default'), BaseLogger),"
            " get_plugin(PluginType.LOG_WRITER, 'console') is not None,"
            " get_plugin(PluginType.LOG_WRITER, 'file') is not None)")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
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
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["0", str(logging.WARNING)]


def test_loguru_backend_is_registered_and_default(monkeypatch):
    """Without config or env the loguru factory is the runtime backend."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)

    assert logger_mod._LoggerManager._resolve_backend_name() == "loguru"
    backend = logger_mod.get_log_factory("loguru")
    assert isinstance(backend, LoguruFactoryPlugin)
    assert backend.available()


def test_legacy_backend_env_still_selects_loguru(monkeypatch):
    """A retired backend name warns and still resolves to loguru."""
    monkeypatch.setenv("TRPC_LOG_BACKEND", "logging")
    with pytest.warns(DeprecationWarning):
        assert logger_mod._LoggerManager._resolve_backend_name() == "loguru"


def test_unknown_backend_env_still_selects_loguru(monkeypatch):
    """An unknown backend name warns and still resolves to loguru."""
    monkeypatch.setenv("TRPC_LOG_BACKEND", "nope")
    with pytest.warns(DeprecationWarning):
        assert logger_mod._LoggerManager._resolve_backend_name() == "loguru"


def test_unavailable_loguru_factory_raises(monkeypatch):
    """The runtime refuses to start when the loguru factory is unavailable."""
    monkeypatch.setattr(LoguruFactoryPlugin, "available", lambda self: False)
    with pytest.raises(RuntimeError, match="mandatory log factory"):
        logger_mod._LoggerManager._select_log_factory()


def test_register_logger_uses_loguru(monkeypatch):
    """Registering loggers always builds LoguruLogger instances."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)

    logger_mod._LoggerManager._register_logger({
        "default": [{"writer": "console", "level": "info"}],
        "worker": [{"writer": "console", "level": "warning"}],
    })

    assert isinstance(logger_mod._LoggerManager._default_logger, LoguruLogger)
    assert isinstance(logger_mod._LoggerManager._loggers["worker"], LoguruLogger)
    assert logger_mod._LoggerManager._active_backend_name == "loguru"
    assert not _intercept_handlers()


@needs_loguru
@pytest.mark.parametrize("formatter", ["json", "noop", "full_json"])
def test_formatter_output_matches_registered_formatter(monkeypatch, formatter):
    """Loguru output matches the formatter registered for that name."""
    output_config = _console_output_config(formatter=formatter)
    extra = {"filename": "biz.py", "lineno": 42, CTX_FIELDS_IN_EXTRA_KEY: {"uid": 7, "brace": "{oops}"}}

    reference = _reference_line(output_config, "hello world", extra)
    rendered = _loguru_render(monkeypatch, output_config, "hello world", extra)

    if formatter == "full_json":
        reference_fields = json.loads(reference)
        rendered_fields = json.loads(rendered)
        reference_fields.pop("Timestamp", None)
        rendered_fields.pop("Timestamp", None)
        assert rendered_fields == reference_fields
    else:
        assert _TIME_RE.sub("[TIME]", rendered) == _TIME_RE.sub("[TIME]", reference)


@needs_loguru
def test_custom_format_config_is_honoured(monkeypatch):
    """format_config.fmt applies on the loguru backend."""
    fmt = "%(levelname)s|%(filename)s:%(lineno)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}

    line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "custom", extra)

    assert line == "INFO|biz.py:7|custom"


@needs_loguru
def test_custom_extra_fields_reach_the_formatter(monkeypatch):
    """A layout naming a caller-supplied field renders it."""
    fmt = "%(request_id)s|%(uin)d|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7, "request_id": "req-42", "uin": 1000}

    line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "custom", extra)

    assert line == "req-42|1000|custom"


@needs_loguru
def test_logger_name_is_the_output_file(tmp_path):
    """%(name)s reports the output file path."""
    fmt = "%(name)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}
    path = str(tmp_path / "loguru.log")

    lines = _loguru_file_render(_file_output_config(path, fmt=fmt), "named", extra)

    assert lines == ["%s|named" % path]


@needs_loguru
def test_trace_renders_as_debug(monkeypatch):
    """trace has no loguru TRACE of its own and renders as DEBUG."""
    fmt = "%(levelname)s|%(message)s"
    extra = {"filename": "biz.py", "lineno": 7}

    line = _loguru_render(monkeypatch, _console_output_config(fmt=fmt), "traced", extra, level="trace")

    assert line == "DEBUG|traced"


@needs_loguru
def test_trace_level_survives_an_admin_round_trip(monkeypatch):
    """Reading the level of a trace output and writing it back keeps it open."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    backed = create_loguru_logger("traced", [_console_output_config(level="trace")])

    set_log_level(backed, get_log_level(backed))
    backed.trace("still here", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert "still here" in stream.getvalue()


@needs_loguru
def test_set_level_trace_opens_the_gate_to_debug(monkeypatch):
    """Level TRACE is the stdlib DEBUG gate."""
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    backed = create_loguru_logger("gatetrace", [_console_output_config(level="debug")])

    backed.set_level(LogLevelType.TRACE)

    assert backed.min_level == logging.DEBUG


@needs_loguru
def test_custom_writer_plugin_is_bridged(memory_writer_plugin):
    """A writer plugin without a native sink still receives correct records."""
    backed = create_loguru_logger("bridge", [OutputConfig(writer="memory_test", level="debug")])
    backed.info("bridged %s", "message",
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
    """A bridged writer sees the caller's own fields."""
    backed = create_loguru_logger("bridgeextra", [OutputConfig(writer="memory_test", level="debug")])
    backed.info("with fields", extra={"filename": "biz.py", "lineno": 11, "request_id": "req-7"})
    loguru_logger.complete()

    record = memory_writer_plugin[0].records[0]
    assert record.request_id == "req-7"


@needs_loguru
def test_unknown_writer_reports_error_instead_of_stderr(capsys):
    """An unregistered writer name is an error, not a silent console fallback."""
    create_loguru_logger("badwriter", [OutputConfig(writer="does_not_exist", level="info")])

    captured = capsys.readouterr()
    assert "does_not_exist" in captured.out
    assert _LoguruSinkManager._sink_ids_by_logger["badwriter"] == []


@needs_loguru
def test_dynamic_level_change(monkeypatch):
    """set_level takes effect without re-registering sinks."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    backed = create_loguru_logger("levels", [_console_output_config(level="debug")])
    sink_ids = list(_LoguruSinkManager._sink_ids_by_logger["levels"])

    backed.set_level(LogLevelType.ERROR)
    backed.info("dropped", extra={"filename": "biz.py", "lineno": 1})
    backed.error("kept", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    output = stream.getvalue()
    assert "dropped" not in output
    assert "kept" in output
    assert _LoguruSinkManager._sink_ids_by_logger["levels"] == sink_ids


@needs_loguru
def test_admin_loglevel_on_loguru_backend(monkeypatch):
    """admin get_log_level and set_log_level work on the loguru backend."""
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    backed = create_loguru_logger("adminlevel", [_console_output_config(level="info")])

    assert get_log_level(backed) == logging.INFO
    set_log_level(backed, logging.ERROR)
    assert get_log_level(backed) == logging.ERROR


@needs_loguru
def test_each_sink_keeps_its_own_level(monkeypatch, tmp_path):
    """A record only reaches the outputs whose own level accepts it."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "levels.log")
    backed = create_loguru_logger("persink", [
        _console_output_config(level="debug"),
        _file_output_config(path, level="error"),
    ])

    backed.debug("only-console", extra={"filename": "biz.py", "lineno": 1})
    backed.error("both", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    console = stream.getvalue()
    assert [line.split("[")[-1].rstrip("]") for line in console.splitlines()] == ["only-console", "both"]
    assert _read_lines(path) == ["both"]


@needs_loguru
def test_set_level_moves_the_logger_gate_only(monkeypatch, tmp_path):
    """set_level tightens every output at once without erasing their levels."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "gate.log")
    backed = create_loguru_logger("gate", [
        _console_output_config(level="debug"),
        _file_output_config(path, level="info"),
    ])

    backed.set_level(LogLevelType.WARNING)
    backed.info("dropped", extra={"filename": "biz.py", "lineno": 1})
    backed.warning("kept", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    assert "dropped" not in stream.getvalue()
    assert _read_lines(path) == ["kept"]

    backed.set_level(LogLevelType.DEBUG)
    backed.debug("back", extra={"filename": "biz.py", "lineno": 3})
    loguru_logger.complete()

    assert "back" in stream.getvalue()
    assert _read_lines(path) == ["kept"]


@needs_loguru
def test_admin_loglevel_targets_the_file_output(monkeypatch, tmp_path):
    """set_log_level moves the file output only."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "admin.log")
    backed = create_loguru_logger("adminfile", [
        _console_output_config(level="info"),
        _file_output_config(path, level="error"),
    ])

    assert get_log_level(backed) == logging.ERROR
    assert backed.min_level == logging.INFO

    set_log_level(backed, logging.DEBUG)
    backed.debug("debugged", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert get_log_level(backed) == logging.DEBUG
    assert backed.min_level == logging.DEBUG
    assert _read_lines(path) == ["debugged"]
    assert "debugged" not in stream.getvalue()


@needs_loguru
def test_admin_loglevel_tells_same_named_files_apart(tmp_path):
    """Two outputs whose files share a basename are distinct admin targets."""
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    other = OutputConfig(writer="file",
                         level="error",
                         formatter="json",
                         writer_config=WriterConfig(filename="app.log", log_path=str(first_dir)),
                         format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    target = OutputConfig(writer="file",
                          level="error",
                          formatter="json",
                          writer_config=WriterConfig(filename="app.log", log_path=str(second_dir)),
                          format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    backed = create_loguru_logger("samename", [other, target])

    set_log_level(backed, logging.DEBUG)
    backed.debug("debugged", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert _read_lines(str(second_dir / "app.log")) == ["debugged"]
    assert _read_lines(str(first_dir / "app.log")) == []


@needs_loguru
def test_same_file_shares_one_sink(tmp_path):
    """Two loggers on one file write through a single sink, each with its layout."""
    path = str(tmp_path / "shared.log")
    create_loguru_logger("share_a", [_file_output_config(path, fmt="A|%(message)s")])
    create_loguru_logger("share_b",
                         [_file_output_config(os.path.join(str(tmp_path), ".", "shared.log"), fmt="B|%(message)s")])

    assert _LoguruSinkManager._sink_ids_by_logger["share_a"] == _LoguruSinkManager._sink_ids_by_logger["share_b"]
    assert len(loguru_logger._core.handlers) == 1

    logger_a = create_loguru_logger("share_a", [_file_output_config(path, fmt="A|%(message)s")])
    logger_b = create_loguru_logger("share_b", [_file_output_config(path, fmt="B|%(message)s")])
    logger_a.info("one", extra={"filename": "biz.py", "lineno": 1})
    logger_b.info("two", extra={"filename": "biz.py", "lineno": 2})
    loguru_logger.complete()

    assert _read_lines(path) == ["A|one", "B|two"]


@needs_loguru
def test_shared_sink_outlives_one_of_its_loggers(monkeypatch, tmp_path):
    """A shared sink is only removed once no logger writes to it any more."""
    path = str(tmp_path / "refcount.log")
    create_loguru_logger("ref_a", [_file_output_config(path)])
    create_loguru_logger("ref_b", [_file_output_config(path)])
    sink_id = _LoguruSinkManager._sink_ids_by_logger["ref_a"][0]

    logger_a = create_loguru_logger("ref_a", [_file_output_config(path)])

    assert _LoguruSinkManager._sink_ids_by_logger["ref_a"] == [sink_id]
    assert sink_id in loguru_logger._core.handlers
    logger_a.info("still here", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()
    assert _read_lines(path) == ["still here"]

    monkeypatch.setattr(sys, "stderr", io.StringIO())
    create_loguru_logger("ref_a", [_console_output_config()])
    create_loguru_logger("ref_b", [_console_output_config()])

    assert sink_id not in loguru_logger._core.handlers


@needs_loguru
def test_conflicting_rotation_on_shared_file_reports_error(capsys, tmp_path):
    """Sharing a file means sharing its rotation settings, which is reported."""
    path = str(tmp_path / "rotation.log")
    create_loguru_logger("rot_a", [_file_output_config(path, max_size=10)])
    create_loguru_logger("rot_b", [_file_output_config(path, max_size=20)])

    assert "rotation" in capsys.readouterr().out


@needs_loguru
def test_initialisation_keeps_application_sinks(monkeypatch):
    """Only loguru's default sink is removed, not the ones the app registered."""
    stream = io.StringIO()
    app_sink = loguru_logger.add(stream, format="{message}")
    monkeypatch.setattr(_LoguruSinkManager, "_default_sink_removed", False)
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    create_loguru_logger("keepsinks", [_console_output_config()])
    loguru_logger.info("application line")
    loguru_logger.complete()

    assert app_sink in loguru_logger._core.handlers
    assert "application line" in stream.getvalue()


@needs_loguru
def test_third_party_capture(monkeypatch):
    """Third-party stdlib logs reach the sinks with a real call site and lib tag."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    create_loguru_logger("default", [_capture_output_config(level="debug")])
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
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    create_loguru_logger("default", [_capture_output_config(fmt="%(request_id)s|%(message)s")])
    install_third_party_capture(logging.INFO)

    logging.getLogger("some.library").info("with fields", extra={"request_id": "req-9"})
    loguru_logger.complete()

    assert stream.getvalue().strip() == 'req-9|with fields[{"lib": "some.library"}]'


@needs_loguru
def test_third_party_capture_only_reaches_marked_outputs(monkeypatch, tmp_path):
    """Marking the file output keeps third-party logs out of the console."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    path = str(tmp_path / "capture.log")
    file_output = _file_output_config(path)
    file_output.capture_third_party = True
    backed = create_loguru_logger("default", [_console_output_config(level="debug"), file_output])
    install_third_party_capture(logging.INFO)

    logging.getLogger("some.library").info("third party line")
    backed.info("framework line", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    assert _read_lines(path) == ['third party line[{"lib": "some.library"}]', "framework line"]
    assert "third party line" not in stream.getvalue()
    assert "framework line" in stream.getvalue()


@needs_loguru
def test_third_party_level_is_independent_of_the_output_level(monkeypatch):
    """An output can take framework debug logs but only third-party warnings."""
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    backed = create_loguru_logger("default",
                                  [_capture_output_config(level="debug", third_party_level="warning")])
    install_third_party_capture(logging.DEBUG)

    library = logging.getLogger("some.library")
    library.info("filtered out")
    library.warning("kept")
    backed.debug("framework debug", extra={"filename": "biz.py", "lineno": 1})
    loguru_logger.complete()

    output = stream.getvalue()
    assert "filtered out" not in output
    assert "kept" in output
    assert "framework debug" in output


@needs_loguru
def test_third_party_capture_needs_a_marked_output(monkeypatch):
    """Selecting loguru alone must not touch the root logger."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    root_level = logging.getLogger().level

    logger_mod._LoggerManager._register_logger({"default": [{"writer": "console", "level": "info"}]})

    assert not _intercept_handlers()
    assert logging.getLogger().level == root_level


@needs_loguru
def test_third_party_capture_from_config_marks_the_output(monkeypatch):
    """capture_third_party in the log config installs the intercept handler."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)

    logger_mod._LoggerManager._register_logger({
        "default": [{
            "writer": "console",
            "level": "info",
            "capture_third_party": True,
            "third_party_level": "warning",
        }],
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
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)

    logger_mod._LoggerManager._register_logger({"default": [{"writer": "console", "level": "info"}]})

    assert len(_intercept_handlers()) == 1

    logging.getLogger("some.library").info("forced in")
    loguru_logger.complete()
    assert "forced in" in stream.getvalue()


@needs_loguru
def test_third_party_capture_disabled_by_env(monkeypatch):
    """TRPC_LOG_INTERCEPT=0 wins over a marked output."""
    monkeypatch.delenv("TRPC_LOG_BACKEND", raising=False)
    monkeypatch.setenv("TRPC_LOG_INTERCEPT", "0")
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    logger_mod._LoggerManager._register_logger({
        "default": [{"writer": "console", "level": "info", "capture_third_party": True}],
    })

    assert not _intercept_handlers()


def test_legacy_logging_backend_env_still_captures(monkeypatch):
    """TRPC_LOG_BACKEND=logging is ignored; capture still follows the loguru path."""
    monkeypatch.setenv("TRPC_LOG_BACKEND", "logging")
    monkeypatch.delenv("TRPC_LOG_INTERCEPT", raising=False)
    monkeypatch.setattr(sys, "stderr", io.StringIO())

    with pytest.warns(DeprecationWarning):
        logger_mod._LoggerManager._register_logger({
            "default": [{"writer": "console", "level": "info", "capture_third_party": True}],
        })

    assert _intercept_handlers()


@needs_loguru
def test_install_third_party_capture_keeps_existing_handlers():
    """install_third_party_capture must not drop handlers installed by other code."""
    existing = logging.NullHandler()
    logging.getLogger().addHandler(existing)

    install_third_party_capture(logging.INFO)
    install_third_party_capture(logging.INFO)

    handlers = logging.getLogger().handlers
    assert existing in handlers
    assert len([h for h in handlers if isinstance(h, InterceptHandler)]) == 1


@needs_loguru
def test_multiprocess_same_file_integrity():
    """Forked children enqueue into the master's single owner queue."""
    procs, count = 2, 200
    out_dir = tempfile.mkdtemp(prefix="trpc_log_backend_")
    path = os.path.join(out_dir, "shared.log")
    writer_config = WriterConfig(filename=path)
    output_config = OutputConfig(writer="file",
                                 level="info",
                                 formatter="json",
                                 writer_config=writer_config,
                                 format_config=FormatConfig(fmt="%(message)s", time_fmt="%Y-%m-%d %H:%M:%S"))
    backed = create_loguru_logger("mp", [output_config])

    children = []
    for _ in range(procs):
        pid = os.fork()
        if pid == 0:
            try:
                for seq in range(count):
                    backed.info("MARK|%d|%d|END", os.getpid(), seq, extra={"filename": "child.py", "lineno": seq})
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
    procs, count = 2, 100
    path = str(tmp_path / "shared_mp.log")
    logger_a = create_loguru_logger("mp_a", [_file_output_config(path, level="info", fmt="A|%(message)s")])
    logger_b = create_loguru_logger("mp_b", [_file_output_config(path, level="info", fmt="B|%(message)s")])

    children = []
    for _ in range(procs):
        pid = os.fork()
        if pid == 0:
            try:
                for seq in range(count):
                    logger_a.info("MARK|%d|%d|END", os.getpid(), seq, extra={"filename": "child.py", "lineno": seq})
                    logger_b.info("MARK|%d|%d|END", os.getpid(), seq, extra={"filename": "child.py", "lineno": seq})
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
