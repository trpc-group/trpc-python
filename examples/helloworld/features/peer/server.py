# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server for ``server.mode: peer``.

The process that runs this file is worker-0. ``serve()`` forks the other
workers before any event loop exists. Each worker then binds every
endpoint itself.

    python features/peer/server.py --conf features/peer/conf/trpc_python.yaml

``check.py`` starts this itself. Environment variables, all optional:

* ``FAIL_HOOK=<index>`` raises in that worker's ``on_start``
* ``SLOW_HOOK=<index>`` sleeps in that worker's ``on_start``
* ``TASK=1`` registers a Task, which peer rejects before the fork
* ``SCHEDULER=1`` registers a dispatcher scheduler, which peer ignores
* ``IO_WAIT=1`` sets ``ServerOption.io_wait_count=1``, which peer ignores
* ``SYNC=1`` registers one plain ``def`` handler, which is only warned about

Not covered here, and not implemented by this version of peer: restarting
a single worker (design section 9.4), preloading before the fork, Task as
a supported feature, and refusing a second instance (design section 15).
"""

from __future__ import annotations

import argparse
import os
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

from features.peer._servicer import PeerGreeter  # noqa: E402
from features.peer._servicer import install_hooks  # noqa: E402
from features.peer._servicer import install_scheduler  # noqa: E402
from features.peer._servicer import install_signals  # noqa: E402
from features.peer._servicer import install_sync  # noqa: E402
from features.peer._servicer import install_task  # noqa: E402
from features.peer._servicer import register_streams  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DEFAULT_CONF = Path(__file__).resolve().parent / "conf" / "trpc_python.yaml"


def main(argv: Optional[List[str]] = None) -> None:
    """Load ``--conf``, register the greeter, and serve until SIGTERM.

    Hooks, signals, and the optional Task or scheduler are installed
    before ``serve()``. The fork copies that memory into every worker;
    nothing here may start a thread or an event loop first.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Peer-mode server")
    parser.add_argument("--conf", default=str(_DEFAULT_CONF), help="path to trpc_python.yaml")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    conf = str(Path(args.conf).resolve())

    install_hooks()
    install_signals()
    install_scheduler()
    install_task()

    server = create_server(conf)
    rpc.register_GreeterServicer_server(server, PeerGreeter(), fix_rpc_name=True)
    register_streams(server)
    install_sync(server)
    if os.environ.get("IO_WAIT") == "1":
        server.opt.io_wait_count = 1
    server.serve()


if __name__ == "__main__":
    main()
