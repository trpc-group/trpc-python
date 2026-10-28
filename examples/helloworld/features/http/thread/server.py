# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP server for one single-process thread model.

Started as a script so it does not import ``features.http``. That package
registers a different HTTP demo, and loading it here would put both route
sets in one process. The ``@route`` handler and the HTTP-RPC greeter both
reply with the serving thread that ran them.

    python features/http/thread/server.py --conf features/http/thread/conf/merge.yaml
    python features/http/thread/server.py --conf features/http/thread/conf/separate.yaml
"""

from __future__ import annotations

import argparse
import sys
import threading
from pathlib import Path
from typing import List
from typing import Optional

from aiohttp.web_request import Request


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

from trpc_sdk.abc import TrpcContext  # noqa: E402
from trpc_sdk.http import route  # noqa: E402
from trpc_sdk.server import create_server  # noqa: E402

from features.thread._servicer import MAIN_IDENT  # noqa: E402
from features.thread._servicer import ThreadGreeter  # noqa: E402
from features.thread._servicer import install_hooks  # noqa: E402
from features.thread._servicer import thread_reply  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DEFAULT_CONF = Path(__file__).resolve().parent / "conf" / "merge.yaml"


@route("/thread_info")
async def thread_info(ctx: TrpcContext, request: Request) -> bytes:
    """Return the serving thread that is running this route.

    HTTP connections are handed to a serving thread as a whole, so every
    request on one connection reports the same thread.

    Args:
        ctx: Request context. Unused; the identity comes from the thread.
        request: aiohttp request. Unused.

    Returns:
        UTF-8 ``info:<index>:<count>:<ident>:<on_main>:<thread_name>``.
    """
    del ctx, request
    return thread_reply().encode()


def main(argv: Optional[List[str]] = None) -> None:
    """Load ``--conf``, register the route and the greeter, and serve.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Single-process HTTP thread-model server")
    parser.add_argument("--conf", default=str(_DEFAULT_CONF), help="path to trpc_python.yaml")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    conf = str(Path(args.conf).resolve())

    install_hooks()
    print(f"MAIN_IDENT {MAIN_IDENT} {threading.current_thread().name}", flush=True)

    server = create_server(conf)
    rpc.register_GreeterServicer_server(server, ThreadGreeter(), fix_rpc_name=True)
    server.serve()


if __name__ == "__main__":
    main()
