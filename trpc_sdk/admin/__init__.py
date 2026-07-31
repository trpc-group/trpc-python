# -*- coding: utf-8 -*-
__version__ = '0.0.1'

import asyncio
import logging
import threading

from trpc_sdk import config
from trpc_sdk import plugin
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import PluginInitPos
from trpc_sdk.utils.loop_lib import asynclib


def getLoglevel(logger):
    """Return the level exposed by the Loguru runtime backend."""
    return logger.get_min_level()
    # for writer in logger.writers:
    #     if hasattr(writer, 'filename') and writer.filename == logger.filename:
    #         return writer.level


def setLoglevel(logger, level):
    """Set the level exposed by the Loguru runtime backend."""
    logger.set_min_level(level)
    # min_level = level
    # for writer in logger.writers:
    #     if hasattr(writer, 'filename') and writer.filename == logger.filename:
    #         writer.level = level
    #         writer.handler.setLevel(level)
    #     elif writer.level is not None and (min_level is None or writer.level < min_level):
    #         min_level = writer.level
    # if min_level is None:
    #     min_level = logging.DEBUG
    # logger.min_level = min_level
    # logger.logger.setLevel(min_level)


on_master_starts = []
on_worker_starts = []


def on_master_start(func):
    on_master_starts.append(func)
    return func


def on_worker_start(func):
    on_worker_starts.append(func)
    return func


@plugin.register_plugin(PluginType.ADMIN, 'admin', init_pos=PluginInitPos.MASTER)
class AdminPlugin(plugin.BasePlugin):

    def get_config(self, master):
        cfg = config.global_config_obj.server.admin
        if not cfg:
            return '', 0
        ip, port = cfg.ip, cfg.port
        if not master and ip == '0.0.0.0':
            ip = '127.0.0.1'
        return ip, port

    def on_master_start(self):
        ip, port = self.get_config(True)
        if not ip or not port:
            return

        from . import server
        for func in on_master_starts:
            func(server.routes, server.configer)

        def run():
            asyncio.set_event_loop(asynclib.new_event_loop())
            server.run(ip, port, print=None, access_log=None, handle_signals=False)

        threading.Thread(target=run, daemon=True).start()

    def on_worker_start(self):
        ip, port = self.get_config(False)
        if not ip or not port:
            return

        from . import client
        for func in on_worker_starts:
            func(client.configer)

        def run():
            loop = asynclib.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(client.run(ip, port))

        threading.Thread(target=run, daemon=True).start()
