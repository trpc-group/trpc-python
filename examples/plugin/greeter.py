# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Greeter that reads the user-defined plugin on SayHello."""

from __future__ import annotations

import os
import sys

from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.plugin import get_plugin

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../helloworld/stub")))  # noqa

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

from plugin import BusinessPlugin


class GreeterServicer(rpc.GreeterServicer):
    """SayHello goes through ``BusinessPlugin``. SayHi does not."""

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply with the token stored by the business plugin.

        The plugin module is imported before ``create_server``, and
        ``init_worker_plugins`` has already called ``init``.

        Args:
            ctx: Request context. Unused.
            request: Incoming hello request.

        Returns:
            Reply whose ``msg`` is ``hello: {msg} token={token}``.
        """
        business: BusinessPlugin = get_plugin(PluginType.USER_DEFINED, "business")
        logger.info("SayHello req: %s", request.msg)
        rsp = pb.HelloReply()
        rsp.msg = business.apply(request.msg)
        return rsp

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply without the business plugin.

        Args:
            ctx: Request context. Unused.
            request: Incoming hello request.

        Returns:
            Reply whose ``msg`` is ``hi: `` plus the request message.
        """
        logger.info("SayHi req: %s", request.msg)
        rsp = pb.HelloReply()
        rsp.msg = "hi: " + request.msg
        return rsp
