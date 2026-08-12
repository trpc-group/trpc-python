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
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseLogger
from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import OutputConfig
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin
from trpc_sdk.utils import register_at_fork

from ._config import get_log_backend_config
from ._config import get_log_config
from ._config import parse_output_configs
from ._constants import CTX_FIELDS_IN_EXTRA_KEY
from ._constants import DEFAULT_LOG_BACKEND_NAME
from ._constants import DEFAULT_LOG_NAME
from ._constants import LEVEL_NAME_TO_LOGGING_LEVEL
from ._constants import META_DATA_KEY_LOG_FIELDS
from ._constants import TRPC_LOG_BACKEND
from ._loguru import LoguruFactoryPlugin
from ._loguru import create_bootstrap_logger
from ._loguru import create_loguru_logger

# cSpell: ignore trpc


class _LoggerManager:
    """Own mutable logger state and private initialization operations."""

    _loggers: Dict[str, BaseLogger] = {}
    _default_logger = create_bootstrap_logger()
    _already_initialized = False
    _active_backend_name = DEFAULT_LOG_BACKEND_NAME
    _stack_level = 2

    @staticmethod
    def _resolve_backend_name() -> str:
        """Resolve the single runtime backend.

        Legacy selectors are accepted for one transition release. Any value other
        than ``loguru`` is ignored with a deprecation warning.

        Returns:
            Name of the log backend plugin.
        """
        env_backend = os.environ.get(TRPC_LOG_BACKEND, "").strip().lower()
        configured = env_backend or next(iter(get_log_backend_config()), "")
        if configured and configured != DEFAULT_LOG_BACKEND_NAME:
            warnings.warn(
                f"log backend '{configured}' is no longer available; using loguru",
                DeprecationWarning,
                stacklevel=2,
            )
        return DEFAULT_LOG_BACKEND_NAME

    @classmethod
    def _select_log_factory(cls) -> LoguruFactoryPlugin:
        """Return the mandatory Loguru backend."""
        name = cls._resolve_backend_name()
        log_factory = get_log_factory(name)
        is_available = (log_factory is not None and isinstance(log_factory, LoguruFactoryPlugin)
                        and log_factory.available())
        if not is_available:
            raise RuntimeError("the mandatory log factory is not available")
        return log_factory

    @staticmethod
    def _third_party_capture_forced() -> Optional[bool]:
        """Return the value forced by ``TRPC_LOG_INTERCEPT``, if set."""
        env_switch = os.environ.get("TRPC_LOG_INTERCEPT", "").strip().lower()
        if not env_switch:
            return None
        return env_switch not in ("0", "false", "no")

    @staticmethod
    def _third_party_root_level(output_config_list: List[OutputConfig]) -> Optional[int]:
        """Return the minimum root level needed for third-party capture."""
        levels = [
            LEVEL_NAME_TO_LOGGING_LEVEL.get(output_config.effective_third_party_level(), logging.DEBUG)
            for output_config in output_config_list if output_config.capture_third_party
        ]
        return min(levels) if levels else None

    @classmethod
    def _setup_third_party_capture(
        cls,
        backend,
        output_configs_by_logger: Dict[str, List[OutputConfig]],
    ) -> None:
        """Ask the backend to route third-party logs into marked outputs."""
        if not hasattr(backend, "install_third_party_capture"):
            return

        all_outputs = [config_item for configs in output_configs_by_logger.values() for config_item in configs]
        root_level = cls._third_party_root_level(all_outputs)
        forced = cls._third_party_capture_forced()
        if forced is False or (forced is None and root_level is None):
            return

        if root_level is None:
            backend.capture_all_outputs(DEFAULT_LOG_NAME)
            root_level = logging.NOTSET
        backend.install_third_party_capture(root_level)

    @classmethod
    def _register_logger(cls, log_plugin_config_dict: Dict) -> None:
        """Register loggers from a plugin configuration dictionary."""
        log_factory = cls._select_log_factory()
        cls._active_backend_name = log_factory.name
        output_configs_by_logger = {}
        for name, log_plugin_config in log_plugin_config_dict.items():
            output_configs_by_logger[name] = parse_output_configs(log_plugin_config)
            logger = log_factory.create_logger(name, output_configs_by_logger[name])
            if name == DEFAULT_LOG_NAME:
                cls._default_logger = logger
            cls._loggers[name] = logger

        cls._setup_third_party_capture(log_factory, output_configs_by_logger)

    @staticmethod
    def _recreate_logging_locks() -> None:
        """Recreate every registered logging handler lock after a fork."""
        logger_keys = list(logging.Logger.manager.loggerDict.keys())
        logger_keys.insert(0, "")
        for logger_name in logger_keys:
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers:
                handler.createLock()


def register_logger(name: str, logger: BaseLogger) -> None:
    """Register a logger with the given name.

    Args:
        name: Logger name for registration.
        logger: Logger instance to register.
    """
    _LoggerManager._loggers[name] = logger


def set_default_logger(logger: BaseLogger) -> None:
    """Set the default logger for the framework.

    Args:
        logger: Logger instance to set as default.
    """
    _LoggerManager._default_logger = logger
    register_logger(DEFAULT_LOG_NAME, logger)


set_default_logger(_LoggerManager._default_logger)


def get_logger(name: str) -> BaseLogger:
    """Get logger by name.

    If the logger is not found, returns the default logger and logs a warning.

    Args:
        name: Logger name to retrieve.

    Returns:
        Logger instance associated with the name, or default logger if not found.
    """
    if name not in _LoggerManager._loggers:
        log_instance = _LoggerManager._default_logger
        warning(f"Cannot find log: {name}")
    else:
        log_instance = _LoggerManager._loggers[name]

    return log_instance


def create_logging_logger(plugin_config: List[Dict]) -> BaseLogger:
    """Deprecated compatibility factory returning a Loguru-backed logger.

    Args:
        plugin_config: List of output configuration dictionaries.

    Returns:
        A LoguruLogger instance.
    """
    warnings.warn(
        "create_logging_logger() is deprecated; Loguru is now the only runtime backend",
        DeprecationWarning,
        stacklevel=2,
    )
    return create_loguru_logger("legacy", parse_output_configs(plugin_config))


def get_log_factory(name: Optional[str] = None):
    """Get a log factory plugin instance by name.

    Args:
        name: Factory plugin name. Defaults to the factory selected at startup.

    Returns:
        The factory plugin instance, or None if it is not registered.
    """
    return get_plugin(PluginType.LOG, name or _LoggerManager._active_backend_name)


@register_plugin(plugin_type=PluginType.LOG, plugin_name=DEFAULT_LOG_NAME)
class DefaultLoggerInitPlugin(BasePlugin):
    """Default logger initialization plugin for TRPC framework.

    This plugin initializes loggers from the global configuration.
    It ensures loggers are only initialized once.
    """

    def __init__(self):
        """Initialize the default logger initialization plugin."""
        super().__init__()
        self._type = PluginType.LOG
        self._name = DEFAULT_LOG_NAME

    def init(self) -> None:
        """Initialize the default logger initialization plugin."""
        if _LoggerManager._already_initialized:
            return

        log_plugin_config_dict = get_log_config()

        if log_plugin_config_dict:
            self._register_logger(log_plugin_config_dict)
        _LoggerManager._already_initialized = True

    @staticmethod
    def _register_logger(log_plugin_config_dict: Dict) -> None:
        """Compatibility entry point for logger plugin initialization."""
        _LoggerManager._register_logger(log_plugin_config_dict)


def set_stack_level(level: int) -> None:
    """Set the stack level for logging.

    Args:
        level: Stack level to set.
    """
    _LoggerManager._stack_level = level


def ensure_filepath_and_lineno(**kwargs) -> dict:
    """Ensure filepath and lineno are present in kwargs['extra'].

    Automatically extracts caller's filename and line number if not provided.

    Args:
        **kwargs: Keyword arguments that may contain 'extra' dict.

    Returns:
        Updated kwargs with 'extra' containing 'filename' and 'lineno'.
    """
    if "extra" not in kwargs:
        kwargs["extra"] = {}
    if "filename" not in kwargs["extra"]:
        kwargs["extra"]["filename"] = os.path.split(
            sys._getframe(_LoggerManager._stack_level).f_code.co_filename  # cSpell: ignore _getframe
        )[1]
    if "lineno" not in kwargs["extra"]:
        kwargs["extra"]["lineno"] = sys._getframe(_LoggerManager._stack_level).f_lineno
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
        get_logger(DEFAULT_LOG_NAME).debug(format_str, *args, **kwargs)
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
        get_logger(DEFAULT_LOG_NAME).info(format_str, *args, **kwargs)
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
        get_logger(DEFAULT_LOG_NAME).warning(format_str, *args, **kwargs)
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
        get_logger(DEFAULT_LOG_NAME).error(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        logging.error(
            "log err! format_str: %s; args: %s; err: %s",
            format_str,
            json.dumps(args),
            repr(err),
        )


def fatal(format_str: str, *args, **kwargs) -> None:
    """Log a fatal level message.

    Args:
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    try:
        kwargs = ensure_filepath_and_lineno(**kwargs)
        get_logger(DEFAULT_LOG_NAME).fatal(format_str, *args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        logging.fatal(
            "log err! format_str: %s; args: %s; err: %s",
            format_str,
            json.dumps(args),
            repr(err),
        )


def with_context_fields(ctx: TrpcContext, fields: dict) -> None:
    """Add custom fields to the context for logging.

    Args:
        ctx: TRPC context object.
        fields: Dictionary of field names and values to add to context.

    Raises:
        TypeError: If fields is not a dictionary.
    """
    if not isinstance(fields, dict):
        raise TypeError("with_context_fields param fields must be type of dict")
    ctx_fields = ctx.get_meta_data(META_DATA_KEY_LOG_FIELDS, {})
    for key, val in fields.items():
        ctx_fields[key] = val
    ctx.with_meta_data(META_DATA_KEY_LOG_FIELDS, ctx_fields)


def get_ctx_fields(ctx: TrpcContext) -> dict:
    """Get custom log fields from the context.

    Args:
        ctx: TRPC context object.

    Returns:
        Dictionary of custom log fields stored in the context.
    """
    return ctx.get_meta_data(META_DATA_KEY_LOG_FIELDS, dict())


def debug_context(ctx: TrpcContext, format_str: str, *args, **kwargs) -> None:
    """Log a debug level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    default_logger = get_logger(DEFAULT_LOG_NAME)
    if hasattr(default_logger, "debug_context"):
        default_logger.debug_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    debug(format_str, *args, **kwargs)


def info_context(ctx: TrpcContext, format_str: str, *args, **kwargs) -> None:
    """Log an info level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    default_logger = get_logger(DEFAULT_LOG_NAME)
    if hasattr(default_logger, "info_context"):
        default_logger.info_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    info(format_str, *args, **kwargs)


def warning_context(ctx: TrpcContext, format_str: str, *args, **kwargs) -> None:
    """Log a warning level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    default_logger = get_logger(DEFAULT_LOG_NAME)
    if hasattr(default_logger, "warning_context"):
        default_logger.warning_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    warning(format_str, *args, **kwargs)


def error_context(ctx: TrpcContext, format_str: str, *args, **kwargs) -> None:
    """Log an error level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    default_logger = get_logger(DEFAULT_LOG_NAME)
    if hasattr(default_logger, "error_context"):
        default_logger.error_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    error(format_str, *args, **kwargs)


def fatal_context(ctx: TrpcContext, format_str: str, *args, **kwargs) -> None:
    """Log a fatal level message with context fields.

    Args:
        ctx: TRPC context object containing custom log fields.
        format_str: Format string for the log message.
        *args: Arguments for format string.
        **kwargs: Additional keyword arguments including 'extra'.
    """
    kwargs = ensure_filepath_and_lineno(**kwargs)
    default_logger = get_logger(DEFAULT_LOG_NAME)
    if hasattr(default_logger, "fatal_context"):
        default_logger.fatal_context(ctx, format_str, *args, **kwargs)
        return
    kwargs["extra"][CTX_FIELDS_IN_EXTRA_KEY] = get_ctx_fields(ctx)
    fatal(format_str, *args, **kwargs)


def _recreate_logging_locks() -> None:
    """Recreate standard-library logging locks after a process fork."""
    _LoggerManager._recreate_logging_locks()


# Register fork hook for logging. Reset lock in child process.
register_at_fork(before=None, after_in_parent=None, after_in_child=_recreate_logging_locks)
