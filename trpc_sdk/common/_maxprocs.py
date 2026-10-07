# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Container-aware CPU count.

On Linux the CPU quota is read from the cgroup the current process belongs
to: cgroup v1 ``cpu.cfs_quota_us`` / ``cpu.cfs_period_us`` first, then
cgroup v2 ``cpu.max``. Every other platform, and any Linux host without a
CPU quota, falls back to :func:`os.cpu_count`.
"""

from __future__ import annotations

import math
import os
import sys

from typing import Dict
from typing import List
from typing import Optional

_PROC_SELF_CGROUP = "/proc/self/cgroup"
_PROC_SELF_MOUNTINFO = "/proc/self/mountinfo"

_CGROUP_V1_FS_TYPE = "cgroup"
_CGROUP_V2_FS_TYPE = "cgroup2"
_CPU_SUBSYS = "cpu"
# cgroup v2 hierarchy is listed in /proc/self/cgroup as "0::<path>".
_CGROUP_V2_SUBSYS = ""

_CFS_QUOTA_US = "cpu.cfs_quota_us"
_CFS_PERIOD_US = "cpu.cfs_period_us"
_CPU_MAX = "cpu.max"
_CPU_MAX_UNLIMITED = "max"

# Fields before the optional-fields section in a mountinfo line, and the
# fields after its "-" separator (fs type, mount source, super options).
_MOUNTINFO_FIRST_HALF = 7
_MOUNTINFO_SECOND_HALF = 3

_MIN_PROCS = 1


def _read_first_line(path: str) -> str:
    with open(path, mode="r") as file:
        return file.readline().strip()


def _read_int(path: str) -> int:
    """Read an integer file, returning -1 when it is missing or malformed."""
    try:
        return int(_read_first_line(path))
    except (OSError, ValueError):
        return -1


def _parse_cgroup_subsystems(proc_cgroup: str) -> Dict[str, str]:
    """Map each subsystem in ``/proc/self/cgroup`` to its cgroup path.

    Lines look like ``4:cpu,cpuacct:/docker/abc`` (v1) or ``0::/user.slice``
    (v2, which yields the empty-string subsystem key).
    """
    subsystems: Dict[str, str] = {}
    with open(proc_cgroup, mode="r") as file:
        for line in file:
            fields = line.strip().split(":")
            if len(fields) != 3:
                continue
            try:
                int(fields[0])
            except ValueError:
                continue
            for subsys in fields[1].split(","):
                subsystems[subsys] = fields[2]
    return subsystems


class _MountPoint:
    """The parts of a ``/proc/self/mountinfo`` line needed to locate cgroups."""

    def __init__(self, root: str, mount_point: str, fs_type: str, super_options: List[str]) -> None:
        self.root = root
        self.mount_point = mount_point
        self.fs_type = fs_type
        self.super_options = super_options

    def translate(self, abs_path: str) -> str:
        """Map a cgroup path to the directory under this mount, or ``""``.

        Returns ``""`` when ``abs_path`` lies outside the mount root, e.g. a
        host cgroup path seen from inside a container with its own root.
        """
        rel_path = os.path.relpath(abs_path, self.root)
        if rel_path == ".." or rel_path.startswith("../"):
            return ""
        return os.path.join(self.mount_point, rel_path)


def _parse_mountinfo_line(line: str) -> Optional[_MountPoint]:
    fields = line.split(" ")
    if len(fields) < _MOUNTINFO_FIRST_HALF + _MOUNTINFO_SECOND_HALF:
        return None
    try:
        int(fields[0])
        int(fields[1])
    except ValueError:
        return None
    for idx in range(_MOUNTINFO_FIRST_HALF - 1, len(fields)):
        if fields[idx] != "-":
            continue
        fs_type_idx = idx + 1
        if len(fields) != fs_type_idx + _MOUNTINFO_SECOND_HALF:
            return None
        return _MountPoint(
            root=fields[3],
            mount_point=fields[4],
            fs_type=fields[fs_type_idx],
            super_options=fields[fs_type_idx + 2].split(","),
        )
    return None


def _find_cgroup_dirs(proc_cgroup: str, proc_mountinfo: str) -> Dict[str, str]:
    """Resolve the cgroup directory of each mounted subsystem.

    Keys are v1 subsystem names (``cpu``, ``cpuacct``, ...) and
    ``_CGROUP_V2_SUBSYS`` for the unified v2 hierarchy.
    """
    subsystems = _parse_cgroup_subsystems(proc_cgroup)
    cgroup_dirs: Dict[str, str] = {}
    with open(proc_mountinfo, mode="r") as file:
        for line in file:
            mount = _parse_mountinfo_line(line.strip())
            if mount is None:
                continue
            if mount.fs_type == _CGROUP_V1_FS_TYPE:
                for opt in mount.super_options:
                    if opt not in subsystems:
                        continue
                    cgroup_dir = mount.translate(subsystems[opt])
                    if cgroup_dir:
                        cgroup_dirs[opt] = cgroup_dir
            elif mount.fs_type == _CGROUP_V2_FS_TYPE and _CGROUP_V2_SUBSYS in subsystems:
                cgroup_dir = mount.translate(subsystems[_CGROUP_V2_SUBSYS])
                if cgroup_dir:
                    cgroup_dirs[_CGROUP_V2_SUBSYS] = cgroup_dir
    return cgroup_dirs


def _cgroup_v1_quota(cgroup_dir: str) -> Optional[float]:
    quota_us = _read_int(os.path.join(cgroup_dir, _CFS_QUOTA_US))
    period_us = _read_int(os.path.join(cgroup_dir, _CFS_PERIOD_US))
    if quota_us == -1 or period_us == -1 or period_us == 0:
        return None
    return float(quota_us) / float(period_us)


def _cgroup_v2_quota(cgroup_dir: str) -> Optional[float]:
    try:
        fields = _read_first_line(os.path.join(cgroup_dir, _CPU_MAX)).split()
    except OSError:
        return None
    if len(fields) != 2 or fields[0] == _CPU_MAX_UNLIMITED:
        return None
    try:
        quota_us = int(fields[0])
        period_us = int(fields[1])
    except ValueError:
        return None
    if period_us <= 0:
        return None
    return float(quota_us) / float(period_us)


def _cpu_quota(
    proc_cgroup: str = _PROC_SELF_CGROUP,
    proc_mountinfo: str = _PROC_SELF_MOUNTINFO,
) -> Optional[float]:
    """Return the cgroup CPU quota in cores, or ``None`` when undefined.

    A mounted v1 ``cpu`` controller takes precedence over v2, matching how a
    hybrid host enforces the quota. Any unreadable or malformed ``/proc``
    file is treated as "no quota".

    Args:
        proc_cgroup: Path of the process cgroup membership file.
        proc_mountinfo: Path of the process mountinfo file.
    """
    try:
        cgroup_dirs = _find_cgroup_dirs(proc_cgroup, proc_mountinfo)
    except OSError:
        return None
    if _CPU_SUBSYS in cgroup_dirs:
        return _cgroup_v1_quota(cgroup_dirs[_CPU_SUBSYS])
    if _CGROUP_V2_SUBSYS in cgroup_dirs:
        return _cgroup_v2_quota(cgroup_dirs[_CGROUP_V2_SUBSYS])
    return None


def _quota_to_procs(quota: float) -> int:
    return max(_MIN_PROCS, math.floor(quota))


def get_max_procs() -> int:
    """Return the number of CPUs this process may use.

    Uses the cgroup CPU quota on Linux (rounded down, at least 1) and falls
    back to :func:`os.cpu_count` elsewhere or when no quota is set. Returns 0
    if the CPU count cannot be determined either.
    """
    if sys.platform.startswith("linux"):
        quota = _cpu_quota()
        if quota is not None:
            return _quota_to_procs(quota)
    return max(os.cpu_count() or 0, 0)
