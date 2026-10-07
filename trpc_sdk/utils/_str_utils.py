# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""String helpers used by config parsing.

:func:`str2bool` is the distutils-style truth parser consumed by
YAML / env flag readers. It returns the integers ``1`` and ``0``, not
``True`` / ``False``.
"""

from __future__ import annotations


def str2bool(val: str) -> bool:
    """Convert a truthy string to ``1`` or ``0``.

    The annotation is ``bool`` for historical callers; the values
    actually returned are the integers ``1`` and ``0``.

    Args:
        val: Case-insensitive token. Leading and trailing whitespace
            is stripped before matching.

    Returns:
        ``1`` for ``y``, ``yes``, ``t``, ``true``, ``on``, ``1``.
        ``0`` for ``n``, ``no``, ``f``, ``false``, ``off``, ``0``.

    Raises:
        ValueError: If ``val`` is not one of the tokens above.
    """
    val = val.lower()
    val = val.strip()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    if val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    raise ValueError("invalid truth value %r" % (val, ))
