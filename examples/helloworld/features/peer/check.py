# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Check ``server.mode: peer`` on this machine.

    python features/peer/check.py

Run from anywhere. This process starts ``server.py`` and exits non-zero
when a check fails. macOS does not balance ``SO_REUSEPORT``, so replies
are not required to hit every worker there; every worker must still own
a listen socket, and every other check still runs.

Restarting one worker, preloading before the fork, and refusing a second
instance are design sections 9.4 and 15. This script checks the current
contract instead: a dead worker exits the whole tree, and a second
instance binds.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _install_helloworld_path() -> None:
    """Put the helloworld root and its stub on ``sys.path``."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "trpc_main.py").is_file() and (parent / "stub").is_dir():
            root = str(parent)
            stub = str(parent / "stub")
            if stub not in sys.path:
                sys.path.insert(0, stub)
            if root not in sys.path:
                sys.path.insert(0, root)
            return
    raise SystemExit("cannot find examples/helloworld (trpc_main.py + stub)")


_install_helloworld_path()

from features.peer._check import main  # noqa: E402

if __name__ == "__main__":
    main()
