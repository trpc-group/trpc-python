# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""This business logic"""

import os
import sys

from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc


class GreeterServicer(rpc.GreeterServicer):
    """Provides methods that implement functionality of greeter servicer."""

    def __init__(self):
        pass

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Business method"""
        logger.info(f"test log in SayHello req: {request.msg}")

        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Business method"""
        logger.info("test log in SayHi")

        rsp = pb.HelloReply()
        rsp.msg = "hi: " + request.msg
        return rsp
