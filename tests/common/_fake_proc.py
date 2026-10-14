# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Build fake ``/proc/self/cgroup`` and ``/proc/self/mountinfo`` trees."""

from __future__ import annotations

from pathlib import Path
from typing import Dict
from typing import Optional
from typing import Tuple


def v1_mount_line(mount_point: Path, root: str = "/", super_options: str = "rw,cpu,cpuacct") -> str:
    return f"30 25 0:26 {root} {mount_point} rw,nosuid shared:10 - cgroup cgroup {super_options}"


def v2_mount_line(mount_point: Path, root: str = "/") -> str:
    return f"31 25 0:27 {root} {mount_point} rw,nosuid shared:11 - cgroup2 cgroup2 rw,nsdelegate"


def make_proc(
    tmp_path: Path,
    cgroup_lines: str,
    mount_lines: str,
    files: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """Write fake proc files and cgroup parameter files under ``tmp_path``.

    Args:
        tmp_path: Scratch directory.
        cgroup_lines: Content of the fake ``/proc/self/cgroup``.
        mount_lines: Content of the fake ``/proc/self/mountinfo``.
        files: Relative path -> content for cgroup parameter files.

    Returns:
        ``(proc_cgroup, proc_mountinfo)`` paths.
    """
    proc_cgroup = tmp_path / "cgroup"
    proc_mountinfo = tmp_path / "mountinfo"
    proc_cgroup.write_text(cgroup_lines + "\n")
    proc_mountinfo.write_text(mount_lines + "\n")
    for rel_path, content in (files or {}).items():
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content + "\n")
    return str(proc_cgroup), str(proc_mountinfo)
