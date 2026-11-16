# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Guard the hatch-cython exclude list in pyproject.toml against rot.

The exclude patterns are matched with ``re.match()`` against paths such as
``trpc_sdk/transport/_cython/_ipc_codec.pyx``. Stale patterns inherited from
the old package layout matched nothing for a long time while claiming that
production modules must not be compiled. These tests pin two invariants:

* every exclude pattern still matches at least one real file, so dead
  patterns are removed instead of silently accumulating;
* no ``.pyx`` file is ever excluded, so a "fixed" pattern cannot drop a
  production Cython extension (e.g. the inner-IPC codec) from the wheel.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPO_ROOT / "trpc_sdk"
SOURCE_SUFFIXES = (".py", ".pyx", ".pxd")


def _load_exclude_patterns() -> List[str]:
    """Return the hatch-cython exclude patterns declared in pyproject.toml.

    ``tomllib`` only exists on Python 3.11+, while the package still supports
    3.8, so older interpreters fall back to extracting the quoted strings of
    the ``exclude`` list directly from the TOML text.

    Returns:
        The exclude patterns in declaration order.
    """
    pyproject_path = REPO_ROOT / "pyproject.toml"
    try:
        import tomllib
    except ModuleNotFoundError:
        text = pyproject_path.read_text(encoding="utf-8")
        section = text.split("[tool.hatch.build.hooks.cython.options.files]", 1)[1]
        block = section.split("exclude = [", 1)[1].split("]", 1)[0]
        # TOML basic strings escape backslashes ("\\." in the file is the
        # two-character regex "\."), so undo that escaping after extraction.
        return [pattern.replace("\\\\", "\\") for pattern in re.findall(r'"([^"]+)"', block)]
    with pyproject_path.open("rb") as file:
        config = tomllib.load(file)
    files_options = config["tool"]["hatch"]["build"]["hooks"]["cython"]["options"]["files"]
    return list(files_options.get("exclude", []))


def _source_paths() -> List[str]:
    """Return package source paths in the form hatch-cython matches against."""
    return sorted(
        path.relative_to(REPO_ROOT).as_posix()
        for path in PACKAGE_ROOT.rglob("*")
        if path.is_file() and path.suffix in SOURCE_SUFFIXES
    )


def _excluded_paths(patterns: List[str], paths: List[str]) -> List[str]:
    """Return the subset of ``paths`` matched by any exclude pattern."""
    return [path for path in paths if any(re.match(pattern, path) for pattern in patterns)]


def test_every_exclude_pattern_matches_a_real_file():
    patterns = _load_exclude_patterns()
    paths = _source_paths()
    dead = [pattern for pattern in patterns if not any(re.match(pattern, path) for path in paths)]
    assert not dead, f"exclude patterns match no file and must be removed: {dead}"


def test_no_pyx_file_is_excluded():
    excluded = _excluded_paths(_load_exclude_patterns(), _source_paths())
    excluded_pyx = [path for path in excluded if path.endswith(".pyx")]
    assert not excluded_pyx, f"production Cython sources must be compiled, excluded: {excluded_pyx}"


def test_ipc_codec_is_compiled():
    excluded = _excluded_paths(_load_exclude_patterns(), _source_paths())
    assert "trpc_sdk/transport/_cython/_ipc_codec.pyx" not in excluded
