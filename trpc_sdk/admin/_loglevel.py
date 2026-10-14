# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Log level accessors used by the admin ``loglevel`` commands."""

from __future__ import annotations


def getLoglevel(logger) -> int:
    """Return the level of a logger's file output.

    Delegates to the Loguru runtime backend. When the logger has file
    outputs, the lowest of their levels is returned; otherwise the
    logger-wide minimum level is returned.

    Args:
        logger: A tRPC Loguru-backed logger.

    Returns:
        A stdlib logging level number.
    """
    return logger.get_min_level()


def setLoglevel(logger, level: int) -> None:
    """Set the level of a logger's file output.

    Delegates to the Loguru runtime backend. Only the outputs writing the
    logger's own file take the new level; other outputs keep theirs, and
    the logger-wide gate drops to the lowest level so none is hidden. A
    logger without file outputs applies the level to every output.

    Args:
        logger: A tRPC Loguru-backed logger.
        level: A stdlib logging level number.
    """
    logger.set_min_level(level)
