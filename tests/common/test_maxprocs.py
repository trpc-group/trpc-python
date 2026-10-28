# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for :func:`trpc_sdk.common.get_max_procs`."""

from __future__ import annotations

import functools

import pytest

from trpc_sdk.common import _maxprocs
from trpc_sdk.common import get_max_procs
from trpc_sdk.config import ProcessModel

from _cgroup_cases import HYBRID_CASE
from _cgroup_cases import V1_CASES
from _cgroup_cases import V2_CASES
from _cgroup_cases import CgroupCase


@pytest.mark.parametrize("case", V1_CASES + V2_CASES)
def test_cpu_quota(fake_proc, case: CgroupCase) -> None:
    proc_cgroup, proc_mountinfo = fake_proc(case.cgroup, case.mountinfo, case.files)
    assert _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo) == case.expected_quota


def test_cpu_quota_prefers_v1_on_hybrid_host(fake_proc) -> None:
    proc_cgroup, proc_mountinfo = fake_proc(HYBRID_CASE.cgroup, HYBRID_CASE.mountinfo, HYBRID_CASE.files)
    assert _maxprocs._cpu_quota(proc_cgroup, proc_mountinfo) == HYBRID_CASE.expected_quota


def test_cpu_quota_missing_proc_files(tmp_path) -> None:
    assert _maxprocs._cpu_quota(str(tmp_path / "no_cgroup"), str(tmp_path / "no_mountinfo")) is None


@pytest.mark.parametrize(
    "quota, expected",
    [(2.0, 2), (2.9, 2), (1.5, 1), (0.5, 1), (0.0, 1)],
)
def test_get_max_procs_uses_quota_on_linux(monkeypatch, quota: float, expected: int) -> None:
    monkeypatch.setattr(_maxprocs.sys, "platform", "linux")
    monkeypatch.setattr(_maxprocs, "_cpu_quota", lambda: quota)
    monkeypatch.setattr(_maxprocs.os, "cpu_count", lambda: 64)
    assert get_max_procs() == expected


def test_get_max_procs_falls_back_to_cpu_count_without_quota(monkeypatch) -> None:
    monkeypatch.setattr(_maxprocs.sys, "platform", "linux")
    monkeypatch.setattr(_maxprocs, "_cpu_quota", lambda: None)
    monkeypatch.setattr(_maxprocs.os, "cpu_count", lambda: 7)
    assert get_max_procs() == 7


def test_get_max_procs_skips_cgroup_off_linux(monkeypatch) -> None:
    def fail() -> None:
        raise AssertionError("cgroup must not be probed off Linux")

    monkeypatch.setattr(_maxprocs.sys, "platform", "darwin")
    monkeypatch.setattr(_maxprocs, "_cpu_quota", fail)
    monkeypatch.setattr(_maxprocs.os, "cpu_count", lambda: 5)
    assert get_max_procs() == 5


def test_get_max_procs_unknown_cpu_count(monkeypatch) -> None:
    monkeypatch.setattr(_maxprocs.sys, "platform", "darwin")
    monkeypatch.setattr(_maxprocs.os, "cpu_count", lambda: None)
    assert get_max_procs() == 0


def test_get_max_procs_reads_fake_proc_end_to_end(monkeypatch, fake_proc) -> None:
    case = V2_CASES[1].values[0]
    proc_cgroup, proc_mountinfo = fake_proc(case.cgroup, case.mountinfo, case.files)
    monkeypatch.setattr(_maxprocs.sys, "platform", "linux")
    monkeypatch.setattr(_maxprocs, "_cpu_quota", functools.partial(_maxprocs._cpu_quota, proc_cgroup, proc_mountinfo))
    assert get_max_procs() == 2


def test_process_model_leaves_handle_num_unset() -> None:
    # The CPU count is read by the option resolver at startup, not by the YAML schema.
    assert ProcessModel().handle_num == 0
