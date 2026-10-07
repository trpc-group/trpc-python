# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Per-process tracemalloc snapshots dumped to files on demand.

Workers construct :class:`MemoryInspecter` only when
``server.memory_inspection.enabled`` is set, start tracing in
``_before_event_loop`` so loop and plugin setup is included, and bind
:meth:`MemoryInspecter.dump_to_file` to SIGUSR2. Each dump writes a
lineno-grouped allocation snapshot under the configured filename prefix.
"""

from __future__ import annotations

import os
import tracemalloc
from typing import List
from typing import Optional

from trpc_sdk.log import logger


class MemoryInspector:
    """Process-local tracemalloc inspector used by worker processes.

    Tracing is not started in ``__init__``; the caller must invoke
    :meth:`start_tracing` after fork and before the allocations it wants
    to see. :meth:`take_stats` keeps the previous snapshot so later dumps
    are deltas, not absolute totals.
    """

    # Grouping key passed to ``Snapshot.statistics`` / ``compare_to``.
    # ``lineno`` collapses frames that share the same file and line.
    _DEFAULT_KEY_TYPE = "lineno"

    def __init__(self, filename_prefix: str = "tracemalloc", line_limit: int = 50) -> None:
        """Create an inspector; does not start tracing.

        Args:
            filename_prefix: Prefix of each dump file. The full name is
                ``<prefix>_<pid>_<index>.txt`` in the process cwd.
            line_limit: Maximum statistic lines written per dump. ``-1``
                is documented as unlimited in server config.
        """
        # Dump filename prefix; pid and a monotonic index are appended.
        self.filename_prefix = filename_prefix
        # Slice end applied to the statistic list before writing.
        self.line_limit = line_limit
        # Last snapshot taken by :meth:`take_stats`; ``None`` until the first call.
        self._pre_snapshot: Optional[tracemalloc.Snapshot] = None
        # Monotonic dump index used in the generated filename.
        self._saved_index = 0

    def start_tracing(self) -> None:
        """Start tracing allocations in this process.

        Must run after fork. Workers call this from
        ``_before_event_loop`` so later loop and plugin allocations are
        included. Raises ``RuntimeError`` if tracing is already on.
        """
        tracemalloc.start()

    def take_stats(self) -> List[tracemalloc.Statistic]:
        """Return allocation stats since the previous call, then store this snapshot.

        The first call has no baseline, so it returns the current
        absolute statistics. Every later call compares against
        ``_pre_snapshot`` and then replaces it. Grouping is always
        ``lineno``.

        Returns:
            Statistic rows grouped by file and line, newest snapshot
            compared to the previous one when a baseline exists.
        """
        snapshot = tracemalloc.take_snapshot()
        if self._pre_snapshot:
            diff = snapshot.compare_to(self._pre_snapshot, self._DEFAULT_KEY_TYPE)
        else:
            diff = snapshot.statistics(self._DEFAULT_KEY_TYPE)
        self._pre_snapshot = snapshot
        return diff

    def _next_dump_filename(self, prefix: str) -> str:
        """Build the next dump path and increment ``_saved_index``.

        The file is created in the process current working directory;
        this method only returns the name.

        Args:
            prefix: Filename prefix, normally ``self.filename_prefix``.

        Returns:
            ``<prefix>_<pid>_<index>.txt``.
        """
        full_name = f"{prefix}_{os.getpid()}_{self._saved_index}.txt"
        self._saved_index += 1
        return full_name

    def dump_to_file(self) -> None:
        """Take a snapshot and write the top statistic lines to a dump file.

        Advances the :meth:`take_stats` baseline as a side effect, so
        consecutive SIGUSR2 dumps are incremental. Failures are logged
        and swallowed so a signal handler cannot crash the worker.
        """
        try:
            top_stats = self.take_stats()
            filename = self._next_dump_filename(self.filename_prefix)
            with open(filename, "w") as fin:
                for each in top_stats[:self.line_limit]:
                    fin.write(str(each))
                    fin.write("\n")
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"print memory alloc info to file failed, error: {exc}")
