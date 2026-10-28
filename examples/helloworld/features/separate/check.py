# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Check ``server.mode: separate`` on this machine.

    python features/separate/check.py

Run from anywhere. This process starts ``server.py`` and exits non-zero
when a scheduling, socket or shutdown check fails. macOS does not balance
``SO_REUSEPORT``, so UDP, HTTP and the user protocol are not required to
hit both rpc tasks here; every other check still is.

This is not ``features/thread/check.py``. That one checks
``handle_thread_num`` inside a single ``mode: merge`` process.
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

from features.separate._check import main  # noqa: E402

if __name__ == "__main__":
    main()
