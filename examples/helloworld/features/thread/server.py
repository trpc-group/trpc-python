# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""TCP and UDP server for one single-process thread model.

The model comes from the YAML: ``server.handle_thread_num > 0`` splits I/O
threads from handle threads. This process is the only worker (``mode:
merge``). ``conf/separate.yaml`` is that thread split, not ``server.mode:
separate`` (see ``features/separate``).

    python features/thread/server.py --conf features/thread/conf/merge.yaml
    python features/thread/server.py --conf features/thread/conf/separate.yaml

``check.py`` starts this itself. Run it directly only to poke at the ports.

``features.user_protocol`` is imported before ``create_server`` so a
``protocol: user`` service is served by that starter. The runtime starts
it once, on serving thread 0. Configs without that protocol never call it.
"""

from __future__ import annotations

import argparse
import sys
import threading
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

import features.user_protocol  # noqa: E402,F401
from features.thread._servicer import MAIN_IDENT  # noqa: E402
from features.thread._servicer import ThreadGreeter  # noqa: E402
from features.thread._servicer import install_hooks  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DEFAULT_CONF = Path(__file__).resolve().parent / "conf" / "merge.yaml"


def main(argv: Optional[List[str]] = None) -> None:
    """Load ``--conf``, register the greeter, and serve until SIGTERM.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Single-process TCP/UDP thread-model server")
    parser.add_argument("--conf", default=str(_DEFAULT_CONF), help="path to trpc_python.yaml")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    conf = str(Path(args.conf).resolve())

    # Hooks must be registered before serve(); later ones never run.
    install_hooks()
    # Printed from this thread so the checker can reject it as a handler id.
    print(f"MAIN_IDENT {MAIN_IDENT} {threading.current_thread().name}", flush=True)

    server = create_server(conf)
    rpc.register_GreeterServicer_server(server, ThreadGreeter(), fix_rpc_name=True)
    server.serve()


if __name__ == "__main__":
    main()
