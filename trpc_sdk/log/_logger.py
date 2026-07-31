# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""TRPC log module.

This module provides logging functionality for the TRPC framework, including:
- Logger registration and management
- Default logger configuration
- Context-aware logging
- Plugin-based writer components (console, file)
- Fork-safe logging support
"""
import json
import logging
import os
import sys
import warnings
from typing import Dict, List, Optional

from trpc_sdk import context
from trpc_sdk import fork_hook
from trpc_sdk.config import config
from trpc_sdk.config import TRPCConfig
from trpc_sdk.plugin import PluginInitPos
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin
from trpc_sdk.abc import BaseLogger
from trpc_sdk.abc import LogLevelType
from trpc_sdk.abc import OutputConfig

from ._loguru_logger import new_bootstrap_logger
from ._loguru_logger import new_loguru_logger
from ._format import CTX_FIELDS_IN_EXTRA_KEY


# Name of the logger the module-level debug()/info()/... helpers write to.
DEFAULT_LOG_NAME = "default"

DEFAULT_WRITER_CONFIG = None
DEFAULT_FILE_WRITER = None
DEFAULT_CONSOLE_WRITER = None

# Logger registry, reached through register() / get_logger().
_GLOBAL_LOGGER: Dict[str, BaseLogger] = {}
_DEFAULT_LOGGER = new_bootstrap_logger()

# Flag to track logger initialization
_LOGGER_ALREADY_INIT = False

# Loguru is the only runtime backend.
_DEFAULT_LOG_BACKEND_NAME = "loguru"

# Key of the log backend section in the global plugins config.
_LOG_BACKEND_CONFIG_KEY = "log_backend"

# Backend selected by _register_logger(). Used to dispatch the fork hook.
_ACTIVE_BACKEND_NAME: str = _DEFAULT_LOG_BACKEND_NAME
_ACTIVE_BACKEND_OPTIONS: Dict = {}

# Public backend-neutral level maps formerly exported by ``_logging_logger``.
LEVELS = {
    "trace": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.CRITICAL,
}
LEVEL_TO_LOGGING_LEVEL = {
    LogLevelType.TRACE: logging.DEBUG,
    LogLevelType.DEBUG: logging.DEBUG,
    LogLevelType.INFO: logging.INFO,
    LogLevelType.WARNING: logging.WARNING,
    LogLevelType.ERROR: logging.ERROR,
    LogLevelType.FATAL: logging.CRITICAL,
}


def register(name: str, logger: BaseLogger) -> None:
    """Register a logger with the given name.

    Args:
        name: Logger name for registration.
        logger: Logger instance to register.
    """
    global _GLOBAL_LOGGER
    _GLOBAL_LOGGER[name] = logger


def set_default_logger(logger: BaseLogger) -> None:
    """Set the default logger for the framework.

    Args:
        logger: Logger instance to set as default.
    """
    global _DEFAULT_LOGGER
    _DEFAULT_LOGGER = logger
    trpc_sdk.plugin.set_logger(logger)
    register(DEFAULT_LOG_NAME, logger)


set_default_logger(_DEFAULT_LOGGER)


def get_logger(name: str) -> BaseLogger:
    """Get logger by name.

    If the logger is not found, returns the default logger and logs a warning.

    Args:
        name: Logger name to retrieve.

    Returns:
        Logger instance associated with the name, or default logger if not found.
    """
    global _GLOBAL_LOGGER

    if name not in _GLOBAL_LOGGER:
        global _DEFAULT_LOGGER
        log_instance = _DEFAULT_LOGGER
        warning(f"Cannot find log: {name}")
    else:
        log_instance = _GLOBAL_LOGGER[name]

    return log_instance


def new_logging_logger(plugin_config: List) -> BaseLogger:
    """Deprecated compatibility factory returning a Loguru-backed logger.

    Args:
        plugin_config: List of output configuration dictionaries.

    Returns:
        A LoguruLogger instance.
    """
    warnings.warn(
        "new_logging_logger() is deprecated; Loguru is now the only runtime backend",
        DeprecationWarning,
        stacklevel=2,
    )
    return new_loguru_logger("legacy", parse_output_configs(plugin_config))


def new_logger(output_config_list: List[OutputConfig]) -> BaseLogger:
    """Deprecated compatibility factory returning a Loguru-backed logger.

    Args:
        output_config_list: List of output configurations for different writers.

    Returns:
        A LoguruLogger instance.
    """
    warnings.warn(
        "new_logger() is deprecated; use new_loguru_logger()",
        DeprecationWarning,
        stacklevel=2,
    )
    return new_loguru_logger("legacy", output_config_list)


def recreate_logging_locks() -> None:
    """Recreate the lock of every registered logging handler.

    Called after fork so that a lock held by another thread at fork time cannot
    deadlock the child process.
    """
    logger_keys = list(logging.Logger.manager.loggerDict.keys())
    logger_keys.insert(0, '')

    for logger_name in logger_keys:
        logger = logging.getLogger(logger_name)
        for handler in logger.handlers:
            handler.createLock()


def logging_after_fork() -> None:
    """Reset logging locks after fork in child process.

    This function is called after fork to recreate locks in the child process,
    ensuring thread-safe logging in the forked process. The actual work is
    delegated to the active log backend plugin, so a backend can replace the
    stdlib queue machinery with its own (loguru inherits the owner queue and
    must not rebuild the listeners).
    """
    from ._loguru_logger import after_fork_in_child

    after_fork_in_child()


# Register fork hook for logging. Reset lock in child process.
fork_hook.register_at_fork(before=None, after_in_parent=None, after_in_child=logging_after_fork)


def parse_output_configs(plugin_config: List) -> List[OutputConfig]:
    """Convert a raw plugin config list into OutputConfig objects.

    Args:
        plugin_config: List of output configuration dictionaries.

    Returns:
        List of OutputConfig instances.
    """
    output_config_list = [OutputConfig() for _ in range(len(plugin_config))]
    TRPCConfig.unmarshal_spec(plugin_config, output_config_list)
    return output_config_list


def _backend_config() -> Dict:
    """Return the ``plugins.log_backend`` section of the global config."""
    return config.global_plugins_config.get(_LOG_BACKEND_CONFIG_KEY) or {}


def resolve_backend_name() -> str:
    """Resolve the single runtime backend.

    Legacy selectors are accepted for one transition release. Any value other
    than ``loguru`` is ignored with a deprecation warning.

    Returns:
        Name of the log backend plugin.
    """
    env_backend = os.environ.get("TRPC_LOG_BACKEND", "").strip().lower()
    configured = env_backend or next(iter(_backend_config()), "")
    if configured and configured != _DEFAULT_LOG_BACKEND_NAME:
        warnings.warn(
            f"log backend '{configured}' is no longer available; using loguru",
            DeprecationWarning,
            stacklevel=2,
        )
    return _DEFAULT_LOG_BACKEND_NAME


def get_log_backend(name: str = None):
    """Get a log backend plugin instance by name.

    Args:
        name: Backend plugin name. Defaults to the backend selected at startup.

    Returns:
        The backend plugin instance, or None if it is not registered.
    """
    return get_plugin(PluginType.LOG_BACKEND, name or _ACTIVE_BACKEND_NAME)


def _select_backend():
    """Return the mandatory Loguru backend.

    Returns:
        Tuple of (backend name, backend plugin instance, backend options dict).
    """
    name = resolve_backend_name()
    backend = get_log_backend(name)
    if backend is None or (hasattr(backend, "available") and not backend.available()):
        raise RuntimeError("the mandatory loguru backend is not available")
    return name, backend, _backend_config().get(name) or {}


def _third_party_capture_forced() -> Optional[bool]:
    """Return the value TRPC_LOG_INTERCEPT forces, or None if it is not set.

    The environment variable overrides the yaml configuration either way, so
    that a running service can be switched without editing its config.
    """
    env_switch = os.environ.get("TRPC_LOG_INTERCEPT", "").strip().lower()
    if not env_switch:
        return None
    return env_switch not in ("0", "false", "no")


def _third_party_root_level(output_config_list: List[OutputConfig]) -> Optional[int]:
    """Return the root level the outputs asking for third-party logs need.

    The most permissive of their levels: root only has to let the records
    through, each output filters them at its own threshold afterwards.

    Args:
        output_config_list: Every output of every registered logger.

    Returns:
        A stdlib level number, or None if no output asks for third-party logs.
    """
    levels = [LEVELS.get(output_config.effective_third_party_level(), logging.DEBUG)
              for output_config in output_config_list if output_config.capture_third_party]
    return min(levels) if levels else None


def _setup_third_party_capture(backend, output_configs_by_logger: Dict[str, List[OutputConfig]]) -> None:
    """Ask the backend to route third-party logs into the marked outputs.

    Backends which cannot do it simply do not expose the entry point, in which
    case the capture is silently unavailable and root is left untouched.

    Args:
        backend: The active log backend plugin.
        output_configs_by_logger: The parsed outputs of every registered logger.
    """
    if not hasattr(backend, "install_third_party_capture"):
        return

    all_outputs = [c for configs in output_configs_by_logger.values() for c in configs]
    root_level = _third_party_root_level(all_outputs)
    forced = _third_party_capture_forced()

    if forced is False or (forced is None and root_level is None):
        return

    if root_level is None:
        # Forced on without a target: fall back to the default logger's outputs.
        backend.capture_all_outputs(DEFAULT_LOG_NAME)
        root_level = logging.NOTSET

    backend.install_third_party_capture(root_level)


def _register_logger(log_plugin_config_dict: Dict) -> None:
    """Register loggers from plugin configuration dictionary.

    Args:
        log_plugin_config_dict: Dictionary mapping logger names to their configurations.
    """
    global _ACTIVE_BACKEND_NAME, _ACTIVE_BACKEND_OPTIONS
    _ACTIVE_BACKEND_NAME, backend, _ACTIVE_BACKEND_OPTIONS = _select_backend()

    output_configs_by_logger = {}
    for name, log_plugin_config in log_plugin_config_dict.items():
        output_configs_by_logger[name] = parse_output_configs(log_plugin_config)
        logger = backend.setup(name, output_configs_by_logger[name])

        if name == DEFAULT_LOG_NAME:
            set_default_logger(logger)
        else:
            register(name, logger)

    _setup_third_party_capture(backend, output_configs_by_logger)


@register_plugin(plugin_type=PluginType.LOG_COMPONENT, plugin_name="default", init_pos=PluginInitPos.MASTER)
class DefaultLoggerPlugin:
    """Default logger plugin for TRPC framework.

    This plugin initializes loggers from the global configuration.
    It ensures loggers are only initialized once.
    """

    def __init__(self):
        """Initialize the default logger plugin."""
        global _LOGGER_ALREADY_INIT
        if _LOGGER_ALREADY_INIT:
            return

        log_plugin_config_dict = config.global_plugins_config.get("log")

        if log_plugin_config_dict:
            _register_logger(log_plugin_config_dict)

        _LOGGER_ALREADY_INIT = True


@register_plugin(PluginType.LOG_BACKEND, _DEFAULT_LOG_BACKEND_NAME)
class LoguruBackendPlugin:
    """The framework's runtime log backend.

    Imports remain local to avoid circular imports during bootstrap.
    """

    def available(self) -> bool:
        """Return True if the loguru dependency is importable."""
        from ._loguru_logger import loguru_available  # local import: optional dependency

        return loguru_available()

    def setup(self, name: str, output_config_list: List[OutputConfig]) -> BaseLogger:
        """Create a loguru-backed logger for the given trpc logger name.

        Args:
            name: The trpc logger name, used to route records to its own sinks.
            output_config_list: List of output configurations.

        Returns:
            A LoguruLogger instance.
        """
        from ._loguru_logger import new_loguru_logger  # local import: optional dependency

        return new_loguru_logger(name, output_config_list)

    def after_fork_in_child(self) -> None:
        """Reset locks in the child, keeping the inherited owner queue intact."""
        from ._loguru_logger import after_fork_in_child  # local import: optional dependency

        after_fork_in_child()

    def install_third_party_capture(self, root_level: int) -> None:
        """Route stdlib and third-party library logs into the marked sinks.

        Args:
            root_level: Level to set on the root logger.
        """
        from ._loguru_logger import install_third_party_capture  # local import: optional dependency

        install_third_party_capture(root_level)

    def capture_all_outputs(self, name: str) -> None:
        """Make every output of a trpc logger receive third-party logs.

        Args:
            name: The trpc logger name whose outputs take the third-party records.
        """
        from ._loguru_logger import capture_all_outputs  # local import: optional dependency

        capture_all_outputs(name)


@register_plugin(PluginType.WRITER_COMPONENT, "console")
class ConsoleWriterPluginFactory:
    """Console writer plugin factory.

    Creates console writer instances for logging to stdout/stderr.
    """
    def setup_loguru_sink(self, output_config: OutputConfig) -> Dict:
        """Build ``loguru.logger.add()`` arguments for console output.

        Args:
            output_config: Output configuration for the console writer.

        Returns:
            Keyword arguments for a native loguru sink.
        """
        from ._loguru_logger import build_console_sink_kwargs  # local import: optional dependency

        return build_console_sink_kwargs(output_config)


@register_plugin(PluginType.WRITER_COMPONENT, "file")
class FileWriterPlugin:
    """File writer plugin factory.

    Creates file writer instances for logging to rotating files.
    """
    def setup_loguru_sink(self, output_config: OutputConfig) -> Dict:
        """Build ``loguru.logger.add()`` arguments for rotating file output.

        Args:
            output_config: Output configuration for the file writer.

        Returns:
            Keyword arguments for a native loguru sink, keeping loguru's own
            rotation/retention/compression and single-owner enqueue machinery.
        """
        from ._loguru_logger import build_file_sink_kwargs

        return build_file_sink_kwargs(output_config)


def set_log_name(log_name: str) -> BaseLogger:
    """Get logger by name (alias for get_logger).

    Args:
        log_name: Logger name to retrieve.

    Returns:
        Logger instance associated with the name.
    """
    return get_logger(log_name)


_STACK_LEVEL: int = 2  # default stack level


def set_stack_level(level: int) -> None:
    """Set the stack level for logging.

    Args:
        level: Stack level to set.
    """
    global _STACK_LEVEL
    _STACK_LEVEL = level


def ensure_filepath_and_lineno(**kwargs) -> dict:
    """Ensure filepath and lineno are present in kwargs['extra'].

    Automatically extracts caller's filename and line number if not provided.

    Args:
        **kwargs: Keyword arguments that may contain 'extra' dict.

    Returns:
        Updated kwargs with 'extra' containing 'filename' and 'lineno'.
    """
    global _STACK_LEVEL
    if 'extra' not in kwargs:
        kwargs['extra'] = {}
    if 'filename' not in kwargs['extra']:
        kwargs['extra']['filename'] = os.path.split(sys._getframe(_STACK_LEVEL).f_code.co_filename)[1]
    if 'lineno' not in kwargs['extra']:
        kwargs['extra']['lineno'] = sys._getframe(_STACK_LEVEL).f_lineno
    return kwargs


def debug(format_str: str, *args, **kwargs) -> None:
    """Log a debug level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        set_log_name(DEFAULT_LOG_NAME).debug(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        error(f"log err! format_str: {format_str}; args: {json.dumps(args)}; err: {err!r}")


def info(format_str: str, *args, **kwargs) -> None:
    """Log an info level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        set_log_name(DEFAULT_LOG_NAME).info(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        error(f"log err! format_str: {format_str}; args: {json.dumps(args)}; err: {err!r}")


def warning(format_str: str, *args, **kwargs) -> None:
    """Log a warning level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        set_log_name(DEFAULT_LOG_NAME).warning(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        error(f"log err! format_str: {format_str}; args: {json.dumps(args)}; err: {err!r}")


def error(format_str: str, *args, **kwargs) -> None:
    """Log an error level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        set_log_name(DEFAULT_LOG_NAME).error(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        logging.error("log err! format_str: %s; args: %s; err: %s", format_str, json.dumps(args), repr(err))


def fatal(format_str: str, *args, **kwargs) -> None:
    """Log a fatal level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        set_log_name(DEFAULT_LOG_NAME).fatal(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        logging.fatal("log err! format_str: %s; args: %s; err: %s", format_str, json.dumps(args), repr(err))


def with_context_fields(ctx: context.Context, fields: dict) -> None:
    """Add custom fields to the context for logging.

    Args:
        ctx: TRPC context object.
        fields: Dictionary of field names and values to add to context.

    Raises:
        Exception: If fields is not a dictionary.
    """
    if not isinstance(fields, dict):
        raise TypeError("with_context_fields param fields must be type of dict")
    ctx_fields = ctx.get_meta_data(context.META_DATA_KEY_LOG_FIELDS, {})
    for key, val in fields.items():
        ctx_fields[key] = val
    ctx.with_meta_data(context.META_DATA_KEY_LOG_FIELDS, ctx_fields)


def get_ctx_fields(ctx: context.Context) -> dict:
    """Get custom log fields from the context.

    Args:
        ctx: TRPC context object.

    Returns:
        Dictionary of custom log fields stored in the context.
    """
    return ctx.get_meta_data(context.META_DATA_KEY_LOG_FIELDS, dict())


def debug_context(ctx: context.Context, format_str: str, *args, **kwargs) -> None:
    """Log a debug level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    if hasattr(set_log_name(DEFAULT_LOG_NAME), "debug_context"):
        set_log_name(DEFAULT_LOG_NAME).debug_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    debug(format_str, *args, **kwargs)


def info_context(ctx: context.Context, format_str: str, *args, **kwargs) -> None:
    """Log an info level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    if hasattr(set_log_name(DEFAULT_LOG_NAME), "info_context"):
        set_log_name(DEFAULT_LOG_NAME).info_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    info(format_str, *args, **kwargs)


def warning_context(ctx: context.Context, format_str: str, *args, **kwargs) -> None:
    """Log a warning level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    if hasattr(set_log_name(DEFAULT_LOG_NAME), "warning_context"):
        set_log_name(DEFAULT_LOG_NAME).warning_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    warning(format_str, *args, **kwargs)


def error_context(ctx: context.Context, format_str: str, *args, **kwargs) -> None:
    """Log an error level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    if hasattr(set_log_name(DEFAULT_LOG_NAME), "error_context"):
        set_log_name(DEFAULT_LOG_NAME).error_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    error(format_str, *args, **kwargs)


def fatal_context(ctx: context.Context, format_str: str, *args, **kwargs) -> None:
    """Log a fatal level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    if hasattr(set_log_name(DEFAULT_LOG_NAME), "fatal_context"):
        set_log_name(DEFAULT_LOG_NAME).fatal_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    fatal(format_str, *args, **kwargs)
