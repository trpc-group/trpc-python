# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server that exposes the admin HTTP API and a custom ``whoami`` command.

``whoami`` is registered on both sides of the admin plugin before
``serve()`` forks:

* The master side adds ``GET /cmds/whoami`` and collects a pid from every
  worker that has a WS connection.
* The worker side answers that request with this process's pid.

Which processes answer depends on ``server.mode`` in ``--conf``:

* ``merge`` — the root process only, one pid
* ``peer`` — every worker, ``worker_num`` pids
* ``separate`` — the I/O root process and every rpc task

Task processes (``ProcessType.TASK``) never connect, so they never appear.

    python features/admin/server.py --conf features/admin/conf/merge.yaml

``check.py`` starts this itself.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
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

from trpc_sdk.admin import on_master_start  # noqa: E402
from trpc_sdk.admin import on_worker_start  # noqa: E402
from trpc_sdk.server import create_server  # noqa: E402

from greeter import GreeterServicer  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DEFAULT_CONF = Path(__file__).resolve().parent / "conf" / "merge.yaml"

_WHOAMI_TIMEOUT_S = 5.0


@on_master_start
def _register_whoami(routes, configer) -> None:
    """Add ``GET /cmds/whoami`` and the handler for worker replies.

    The route keeps its own ``request_id`` futures, separate from the
    admin server's rpcz channel, and asks every connected worker at once.

    Args:
        routes: The admin server's aiohttp route table.
        configer: The admin server's command registry.
    """
    from aiohttp import web  # pylint: disable=import-outside-toplevel

    pending = {}

    @configer.reg_cmd('whoami')
    async def _on_reply(ws_sock, data: str) -> None:
        payload = json.loads(data)
        future = pending.pop(payload.get('request_id'), None)
        if future is not None and not future.done():
            future.set_result(payload['pid'])

    async def _ask(ws_sock) -> int:
        """Ask one worker for its pid and wait for the reply.

        Args:
            ws_sock: The worker's WS connection.

        Returns:
            The pid the worker reports.
        """
        request_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        pending[request_id] = future
        await ws_sock.send_str('whoami ' + json.dumps({'request_id': request_id}))
        try:
            return await asyncio.wait_for(future, timeout=_WHOAMI_TIMEOUT_S)
        finally:
            pending.pop(request_id, None)

    @routes.get('/cmds/whoami')
    async def whoami(request) -> web.Response:
        """list the pid of every worker connected to admin"""
        clients = configer._healthy_clients()  # pylint: disable=protected-access
        pids = list(await asyncio.gather(*[_ask(ws_sock) for ws_sock in clients]))
        body = json.dumps({'pids': pids, 'errorcode': 0, 'message': ''}, indent=2)
        return web.Response(text=body, content_type='application/json')


@on_worker_start
def _answer_whoami(configer) -> None:
    """Answer a ``whoami`` request with this process's pid.

    Args:
        configer: The worker's command registry.
    """

    @configer.reg_cmd('whoami')
    async def _whoami(ws_sock, data: str) -> None:
        payload = json.loads(data) if data else {}
        reply = json.dumps({'request_id': payload.get('request_id'), 'pid': os.getpid()})
        await ws_sock.send_str(f'whoami {reply}')


def main(argv: Optional[List[str]] = None) -> None:
    """Load ``--conf``, register the greeter, and serve until SIGTERM.

    The ``whoami`` decorators above run at import, which is before
    ``serve()`` forks, so every worker inherits the registration.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="Admin example server")
    parser.add_argument("--conf", default=str(_DEFAULT_CONF), help="path to trpc_python.yaml")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    server = create_server(str(Path(args.conf).resolve()))
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()


if __name__ == "__main__":
    main()
