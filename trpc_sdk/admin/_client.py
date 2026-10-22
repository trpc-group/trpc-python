# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker-side admin WS client.

Each worker process dials the admin HTTP server's WS endpoint and serves the
commands it pushes: config updates (such as log levels) and rpcz span
collection. The connection is retried forever, so a worker that starts before
the admin server simply connects on a later attempt.
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback

import aiohttp
from aiohttp import WSMsgType

from trpc_sdk.log import DEFAULT_LOG_NAME
from trpc_sdk.log import get_logger
from trpc_sdk.log import logger
from trpc_sdk.rpcz import pop_all_spans
from trpc_sdk.version import __version__
from ._constants import WS_PATH
from ._constants import WS_RECONNECT_INTERVAL_S
from ._constants import WS_USER_AGENT_PREFIX
from ._loglevel import set_log_level


class Configer:
    """Registry of WS commands and config setters on the worker side.

    Attributes:
        handlers: Command name to ``async handler(ws_sock, data)``.
        setters: Config key to ``async setter(ws_sock, value)``, dispatched by
            the ``config`` command.
    """

    def __init__(self):
        self.handlers = {}
        self.setters = {}

    def reg_cmd(self, cmd):
        """Register a handler for a WS command.

        Args:
            cmd: Command name, the first word of a WS text message.

        Returns:
            A decorator that stores ``func`` and returns it unchanged.
        """

        def deco(func):
            self.handlers[cmd] = func
            return func

        return deco

    async def handle(self, ws_sock, data):
        """Dispatch a ``'<cmd> <data>'`` WS text message to its handler.

        Args:
            ws_sock: The WS connection the message arrived on.
            data: Raw message text.
        """
        cmd, _, data = data.partition(' ')
        data = data.strip()
        await self.handlers[cmd](ws_sock, data)

    def reg_cfg(self, name):
        """Register a setter for a config key pushed by the admin server.

        Args:
            name: Config key inside the ``config`` command payload.

        Returns:
            A decorator that stores ``func`` and returns it unchanged.
        """

        def deco(func):
            self.setters[name] = func
            return func

        return deco

    async def config(self, ws_sock, name, value):
        """Apply one config value through its registered setter.

        Args:
            ws_sock: The WS connection the value arrived on.
            name: Config key.
            value: Decoded value for that key.
        """
        await self.setters[name](ws_sock, value)


configer = Configer()


@configer.reg_cmd('config')
async def handle_config(ws_sock, data):
    """Apply every key of a JSON ``config`` payload through its setter."""
    data = json.loads(data)
    for key, value in data.items():
        await configer.config(ws_sock, key, value)


@configer.reg_cmd('rpcz')
async def handle_rpcz(ws_sock, data):
    """Drain this process's rpcz spans and send them back to the admin server.

    The reply echoes ``request_id`` and ``child_index`` so the server can
    resolve its pending request and tag the spans with this worker's index.
    """
    payload = json.loads(data) if data else {}
    request_id = payload.get('request_id')
    child_index = payload.get('child_index')

    spans = pop_all_spans()
    resp = {
        'request_id': request_id,
        'child_index': child_index,
        'spans': [span.to_dict() for span in spans],
    }
    logger.debug('handle_rpcz: sending rpcz response, request_id=%s, child_index=%s, count=%s', request_id, child_index,
                 len(spans))
    await ws_sock.send_str(f'rpcz {json.dumps(resp)}')


@configer.reg_cfg('loglevel')
async def handle_loglevel(ws_sock, cfg):
    """Apply ``{logger_name: level}`` pushed by the admin server.

    Changing the default logger also moves the stdlib root logger, so
    third-party logs follow the new level.
    """
    logger.debug('set loglevel, cfg:%s', cfg)
    for name, level in cfg.items():
        logger_ins = get_logger(name)
        set_log_level(logger_ins, level)
        if name == DEFAULT_LOG_NAME:
            logging.getLogger().setLevel(logger_ins.min_level)


async def websocket(session, url):
    """Serve WS commands on one connection until the server closes it.

    Args:
        session: The shared ``aiohttp.ClientSession``.
        url: Admin server WS URL.
    """
    async with session.ws_connect(url) as ws_sock:
        async for msg in ws_sock:
            if msg.type == WSMsgType.TEXT:
                try:
                    await configer.handle(ws_sock, msg.data)
                except Exception:  # pylint: disable=broad-except
                    traceback.print_exc()


async def run(host, port):
    """Keep a WS connection to the admin server, reconnecting on failure.

    Args:
        host: Admin server host.
        port: Admin server port.
    """
    session = aiohttp.ClientSession(headers={'User-Agent': f'{WS_USER_AGENT_PREFIX}{__version__}'})
    url = f'http://{host}:{port}{WS_PATH}'
    while True:
        try:
            await websocket(session, url)
        except (SystemExit, KeyboardInterrupt):
            break
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(WS_RECONNECT_INTERVAL_S)
