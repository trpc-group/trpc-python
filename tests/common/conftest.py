# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Fixtures that build fake ``/proc/self/cgroup`` and ``/proc/self/mountinfo`` trees."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

import pytest

FakeProc = Callable[..., Tuple[str, str]]


@pytest.fixture
def fake_proc(tmp_path: Path) -> FakeProc:
    """Return a builder writing fake proc files under ``tmp_path``.

    The builder takes ``cgroup`` and ``mountinfo`` contents plus an optional
    ``files`` mapping of ``tmp_path``-relative paths to cgroup parameter file
    contents, and returns ``(proc_cgroup, proc_mountinfo)`` paths. Use
    ``{root}`` inside ``mountinfo`` to reference ``tmp_path``.
    """

    def build(cgroup: str, mountinfo: str, files: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
        proc_cgroup = tmp_path / "proc_cgroup"
        proc_mountinfo = tmp_path / "proc_mountinfo"
        proc_cgroup.write_text(cgroup + "\n")
        proc_mountinfo.write_text(mountinfo.format(root=tmp_path) + "\n")
        for rel_path, content in (files or {}).items():
            path = tmp_path / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content + "\n")
        return str(proc_cgroup), str(proc_mountinfo)

    return build
