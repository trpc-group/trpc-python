# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Fake cgroup layouts shared by the max-procs unit and parity tests.

Each case is ``(cgroup, mountinfo, files, expected_quota)``; see the
``fake_proc`` fixture in ``conftest.py`` for how they are written to disk.
"""

from __future__ import annotations

from typing import Dict
from typing import NamedTuple
from typing import Optional

import pytest


class CgroupCase(NamedTuple):
    cgroup: str
    mountinfo: str
    files: Dict[str, str]
    expected_quota: Optional[float]


_V1_CGROUP = "4:cpu,cpuacct:/docker/abc\n3:memory:/docker/abc"
_V1_MOUNT = "30 25 0:26 / {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct"
_V1_DIR = "cpu/docker/abc"


def _v1_files(quota: str, period: str = "100000", cgroup_dir: str = _V1_DIR) -> Dict[str, str]:
    return {f"{cgroup_dir}/cpu.cfs_quota_us": quota, f"{cgroup_dir}/cpu.cfs_period_us": period}


V1_CASES = [
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("200000"), 2.0), id="v1-quota-2"),
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("150000"), 1.5), id="v1-quota-1.5"),
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("50000"), 0.5), id="v1-quota-below-1"),
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("-1"), None), id="v1-unlimited"),
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("200000", "0"), None), id="v1-period-zero"),
    pytest.param(
        CgroupCase(_V1_CGROUP, _V1_MOUNT, {f"{_V1_DIR}/cpu.cfs_quota_us": "200000"}, None),
        id="v1-period-missing",
    ),
    pytest.param(CgroupCase(_V1_CGROUP, _V1_MOUNT, _v1_files("abc"), None), id="v1-quota-malformed"),
    pytest.param(
        CgroupCase(
            "4:cpu:/docker/abc\n5:cpuacct:/docker/abc",
            "30 25 0:26 / {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu",
            _v1_files("300000"),
            3.0,
        ),
        id="v1-separate-cpu-mount",
    ),
    pytest.param(
        CgroupCase(
            "4:cpu,cpuacct:/docker/abc/nested",
            "30 25 0:26 /docker {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct",
            _v1_files("400000", cgroup_dir="cpu/abc/nested"),
            4.0,
        ),
        id="v1-nested-mount-root",
    ),
    pytest.param(
        CgroupCase(
            _V1_CGROUP,
            "30 25 0:26 /other {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct",
            _v1_files("200000"),
            None,
        ),
        id="v1-cgroup-outside-mount-root",
    ),
    pytest.param(
        CgroupCase(
            _V1_CGROUP,
            "30 25 0:26 / {root}/cpu rw,nosuid shared:10 cgroup cgroup rw,cpu,cpuacct",
            _v1_files("200000"),
            None,
        ),
        id="v1-mountinfo-missing-separator",
    ),
    pytest.param(
        CgroupCase(
            _V1_CGROUP,
            "30 25 0:26 / {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct extra",
            _v1_files("200000"),
            None,
        ),
        id="v1-mountinfo-extra-field",
    ),
    pytest.param(
        CgroupCase(
            _V1_CGROUP,
            "x 25 0:26 / {root}/cpu rw,nosuid shared:10 - cgroup cgroup rw,cpu,cpuacct",
            _v1_files("200000"),
            None,
        ),
        id="v1-mountinfo-bad-mount-id",
    ),
    pytest.param(
        CgroupCase("3:memory:/docker/abc", _V1_MOUNT, _v1_files("200000"), None),
        id="v1-no-cpu-subsystem",
    ),
    pytest.param(
        CgroupCase(
            _V1_CGROUP,
            "30 25 0:26 / {root}/cpu rw,nosuid shared:10 - tmpfs tmpfs rw,cpu,cpuacct",
            _v1_files("200000"),
            None,
        ),
        id="v1-not-cgroup-fs",
    ),
]

_V2_CGROUP = "0::/user.slice/app"
_V2_MOUNT = "31 25 0:27 / {root}/unified rw,nosuid shared:11 - cgroup2 cgroup2 rw,nsdelegate"
_V2_CPU_MAX = "unified/user.slice/app/cpu.max"

V2_CASES = [
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {_V2_CPU_MAX: "150000 100000"}, 1.5), id="v2-quota-1.5"),
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {_V2_CPU_MAX: "250000 100000"}, 2.5), id="v2-quota-2.5"),
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {_V2_CPU_MAX: "max 100000"}, None), id="v2-unlimited"),
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {_V2_CPU_MAX: "150000 0"}, None), id="v2-period-zero"),
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {_V2_CPU_MAX: "garbage"}, None), id="v2-malformed"),
    pytest.param(CgroupCase(_V2_CGROUP, _V2_MOUNT, {}, None), id="v2-cpu-max-missing"),
]

HYBRID_CASE = CgroupCase(
    f"{_V1_CGROUP}\n{_V2_CGROUP}",
    f"{_V1_MOUNT}\n{_V2_MOUNT}",
    {**_v1_files("200000"), _V2_CPU_MAX: "500000 100000"},
    2.0,
)
