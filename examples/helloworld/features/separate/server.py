# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server for ``server.mode: separate``.

The I/O process accepts tRPC-over-TCP and forwards frames to ``worker_num``
children. UDP, HTTP and the user protocol are bound by those children.
This is not ``features/thread``: that example's ``conf/separate.yaml`` is
``mode: merge`` with ``handle_thread_num`` (threads in one process).

    python features/separate/server.py --conf features/separate/conf/trpc_python.yaml

``check.py`` starts this itself. ``SCHEDULER=pin0`` pins unary TCP to rpc
task 0. ``SLOW_HOOK``, ``FAIL_HOOK`` and ``SLOW_TASK`` change startup.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List
from typing import Optional


def _install_helloworld_path() -> None:
    """Put the helloworld root and its stub on ``sys.path``.

    This file is started by path, so ``features`` is not importable until
    the directory that contains ``trpc_main.py`` is inserted.
    """
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

from trpc_sdk.server import create_server  # noqa: E402

from features.separate._servicer import SeparateGreeter  # noqa: E402
from features.separate._servicer import install_hooks  # noqa: E402
from features.separate._servicer import install_scheduler  # noqa: E402
from features.separate._servicer import install_signals  # noqa: E402
from features.separate._servicer import register_streams  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DEFAULT_CONF = Path(__file__).resolve().parent / "conf" / "trpc_python.yaml"


def main(argv: Optional[List[str]] = None) -> None:
    """Load ``--conf``, register the greeter, and serve until SIGTERM.

    YAML ``server.io_wait_count: 1`` waits for the Task initializer's
    ``inc_io_wait_count`` before any listen socket is bound, so naming
    registration (which follows the bind) cannot run before that initializer.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Multi-process separate-mode server")
    parser.add_argument("--conf", default=str(_DEFAULT_CONF), help="path to trpc_python.yaml")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    conf = str(Path(args.conf).resolve())

    # Hooks must be registered before serve(); signals and the scheduler must
    # be in place before the fork, because the I/O process is this one.
    install_hooks()
    install_signals()
    install_scheduler()

    server = create_server(conf)
    rpc.register_GreeterServicer_server(server, SeparateGreeter(), fix_rpc_name=True)
    register_streams(server)
    server.serve()


if __name__ == "__main__":
    main()
