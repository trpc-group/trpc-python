# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Bind-info file writer used by :func:`~trpc_sdk.server.create_server`."""

from __future__ import annotations

import os

from trpc_sdk.config import GlobalConfig
from trpc_sdk.config import ServerService


def save_bind_info(global_config: GlobalConfig, service_config: ServerService) -> None:
    """Append this service's listen address to the bind-info file.

    No-op unless YAML ``global.save_bind_info.enable`` is true. The file is
    opened in append mode so several services in one process each add a
    line; :func:`create_server` writes after it has filled ``port``, so an
    ephemeral port chosen by :func:`find_free_port` is what gets recorded.

    Args:
        global_config: The ``global:`` section holding ``save_bind_info``.
        service_config: The ``server.service[]`` entry whose ``name``,
            ``ip`` and ``port`` are written.
    """
    bind_info = global_config.save_bind_info
    if bind_info is None or not bind_info.enable:
        return

    file_name = bind_info.file_name
    dirname = os.path.dirname(file_name)
    if dirname == '':
        dirname = './'
    os.makedirs(dirname, exist_ok=True)
    with open(file_name, 'a+') as f:
        f.write("%s %s %s\n" % (service_config.name, service_config.ip, service_config.port))
