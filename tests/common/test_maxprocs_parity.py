# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Parity of :func:`trpc_sdk.common.get_max_procs` with ``automaxprocs==2.0.0``.

Both implementations read the same fake ``/proc`` trees. ``automaxprocs`` only
understands cgroup v1, so v2 cases assert the intended divergence instead.
"""

from __future__ import annotations

import math
import os
from typing import Optional

import pytest

from trpc_sdk.common import _maxprocs
from trpc_sdk.common import get_max_procs

from _cgroup_cases import HYBRID_CASE
from _cgroup_cases import V1_CASES
from _cgroup_cases import V2_CASES
from _cgroup_cases import CgroupCase

automaxprocs = pytest.importorskip("automaxprocs")
from automaxprocs.cgroups import new_cgroups  # noqa: E402


def _reference_quota(proc_cgroup: str, proc_mountinfo: str) -> Optional[float]:
    """Quota as ``automaxprocs`` computes it; ``None`` for CPUQuotaUndefined.

    ``automaxprocs.maxprocs.get_cpu_quota_to_max_procs`` turns any exception
    (missing file, zero period) into "undefined", so this does the same.
    """
    try:
        quota, defined = new_cgroups(proc_mountinfo, proc_cgroup).cpu_quota()
    except Exception:
        return None
    return quota if defined else None


def _reference_procs(quota: Optional[float]) -> Optional[int]:
    """Mirror ``automaxprocs.runtime.cpu_quota_to_max_procs(min_value=1)``."""
    if quota is None:
        return None
    return max(1, int(math.floor(quota)))


def _own_procs(quota: Optional[float]) -> Optional[int]:
    return None if quota is None else _maxprocs._quota_to_procs(quota)


def test_host_matches_reference() -> None:
    expected = automaxprocs.get_max_procs()
    if expected == 0 and (os.cpu_count() or 0) > 0:
        pytest.skip("automaxprocs could not run its CPU-count probe (e.g. sysctl blocked by a sandbox)")
    assert get_max_procs() == expected


@pytest.mark.parametrize("case", V1_CASES)
def test_cgroup_v1_matches_reference(fake_proc, case: CgroupCase) -> None:
    proc_cgroup, proc_mountinfo = fake_proc(case.cgroup, case.mountinfo, case.files)
    reference = _reference_quota(proc_cgroup, proc_mountinfo)
    own = _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo)
    assert own == reference == case.expected_quota
    assert _own_procs(own) == _reference_procs(reference)


def test_missing_proc_files_match_reference(tmp_path) -> None:
    proc_cgroup, proc_mountinfo = str(tmp_path / "no_cgroup"), str(tmp_path / "no_mountinfo")
    assert _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo) is None
    assert _reference_quota(proc_cgroup, proc_mountinfo) is None


def test_hybrid_host_matches_reference(fake_proc) -> None:
    proc_cgroup, proc_mountinfo = fake_proc(HYBRID_CASE.cgroup, HYBRID_CASE.mountinfo, HYBRID_CASE.files)
    assert _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo) == _reference_quota(proc_cgroup, proc_mountinfo)


@pytest.mark.parametrize("case", V2_CASES)
def test_cgroup_v2_is_intentional_divergence(fake_proc, case: CgroupCase) -> None:
    proc_cgroup, proc_mountinfo = fake_proc(case.cgroup, case.mountinfo, case.files)
    assert _reference_quota(proc_cgroup, proc_mountinfo) is None
    assert _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo) == case.expected_quota
