# -*- coding: utf-8 -*-
"""
Init for trpc
"""

import os
import socket
import platform

from trpc.log import *
from trpc.trpc import *
from trpc import admin as _
from trpc import transport
from trpc.utils.loop_lib import use_uvloop

transport.trpc_use_uvloop = use_uvloop

if os.environ.get('TRPC_C_DISPATCHER', '0') == '1':
    import trpc.server.server_engine as trpc_server_engine
    trpc_server_engine.USE_C_DISPATCHER = True

if os.environ.get('TRPC_USE_TRPCIO', '0') == '1':
    import trpc.server.worker as trpc_server_worker
    trpc_server_worker.USE_TRPCIO = True

USE_C_TASK = False
if os.environ.get('TRPC_C_TASK', '0') == '1':
    USE_C_TASK = True

# set socket.SO_REUSEPORT (anaconda compatibility)
if "SO_REUSEPORT" not in socket.__dict__:
    if "linux" in platform.platform().lower():
        socket.SO_REUSEPORT = 15
    elif "darwin" in platform.platform().lower():
        socket.SO_REUSEPORT = 512

__version__ = '0.10.2'
