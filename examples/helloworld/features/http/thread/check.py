# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Check merge and separate over HTTP.

    python features/http/thread/check.py --conf features/http/thread/conf/merge.yaml
    python features/http/thread/check.py --conf features/http/thread/conf/separate.yaml

Run from anywhere. This process starts ``server.py`` and exits non-zero
when a thread count, thread id or listen-socket check fails.
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

from features.thread._check import main  # noqa: E402

if __name__ == "__main__":
    main(Path(__file__).resolve().parent / "conf" / "merge.yaml")
