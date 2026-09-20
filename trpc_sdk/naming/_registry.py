# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service-registry lookup and process-wide heartbeat gating.

:func:`get_registry` is the naming counterpart of :func:`get_selector`:
``get_plugin(PluginType.NAMING_REGISTRY, name)``. :data:`defaultRegistry`
is the no-op default that :class:`~trpc_sdk.server._option.ServiceOption`
starts with when YAML does not name a registry plugin.

:func:`pause_heartbeat` / :func:`resume_heartbeat` are a nested counter,
not a boolean. :func:`trpc_sdk.server._task.build_task_infos` increments
once per task that set ``pause_heartbeat``, still in the parent, so the
registry client stops advertising before any child is forked.
:func:`resume_heartbeat` runs in each such child after its initializer;
:func:`should_heartbeat` is true only when the counter is back at zero.

``_counter`` is a :class:`multiprocessing.Value` allocated at import so
the integer lives in shared memory across fork. A plain ``int`` would
be copied per child and the parent would never see resumes.
"""

from __future__ import annotations

from multiprocessing import Value
from typing import Optional

from trpc_sdk.abc import BaseServiceRegistry
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin

# No-op default used by ServiceOption until set_registry() loads a plugin.
defaultRegistry: Optional[BaseServiceRegistry] = None

# Nested pause count. Name is module-private; Value is process-shared.
_counter = Value('i', 0)


def get_registry(name: str) -> Optional[BaseServiceRegistry]:
    """Return the instantiated registry plugin registered as ``name``.

    Instantiates the plugin on first lookup if it is registered but not
    yet in the instance map.

    Args:
        name: Name passed to ``@register_plugin`` for
            ``PluginType.NAMING_REGISTRY``.

    Returns:
        The registry instance, or ``None`` if no plugin is registered
        under that name.
    """
    return get_plugin(PluginType.NAMING_REGISTRY, name)


def pause_heartbeat() -> None:
    """Increment the process-wide registry-heartbeat pause count.

    Heartbeats stay off until every matching :func:`resume_heartbeat`
    has run. Safe to call more than once; each call needs its own resume.
    """
    global _counter
    with _counter.get_lock():
        _counter.value += 1


def resume_heartbeat() -> None:
    """Decrement the pause count, stopping at zero.

    A resume without a matching pause is a no-op so a child that was
    not asked to pause cannot drive the counter negative.
    """
    global _counter
    with _counter.get_lock():
        if _counter.value > 0:
            _counter.value -= 1


def should_heartbeat() -> bool:
    """Return whether registry heartbeats are currently allowed.

    Returns:
        ``True`` when no pause is outstanding, otherwise ``False``.
    """
    global _counter
    with _counter.get_lock():
        return _counter.value == 0
