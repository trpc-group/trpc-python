# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Business logic."""

import os
import sys
import asyncio
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa
if __name__ != '__main__':  # noqa
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

from trpc_py_stream_helloworld import pb
from trpc_py_stream_helloworld import rpc
from trpc_py_stream_helloworld import rpc_stream


class GreeterServicer(rpc.GreeterServicer):
    """Provides methods that implement functionality of Greeter servicer."""

    def __init__(self):
        pass

    # pylint: disable=invalid-name
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Copy ``request.name`` into the reply message."""
        del ctx
        logger.info("SayHello request: %s", request.name)
        rsp = pb.HelloReply()
        rsp.message = request.name
        return rsp

    # pylint: disable=invalid-name
    async def SayHelloReq(self, stream: rpc_stream.GreeterServerStream.SayHelloReq):
        """Abstract client stream method needed to implementation"""
        while True:
            try:
                req = await stream.recv()
                logger.info("request: %s", req.name)
            except EOFError as err:
                logger.debug(f"recv done {err}")
                rsp = pb.HelloReply()
                rsp.message = "SayHelloReq done"
                await stream.send_and_close(rsp)
                return
            except Exception as err:  # pylint: disable=broad-except
                logger.error(f"recv err: {err}", exc_info=True)

    # pylint: disable=invalid-name
    async def SayHelloRsp(self, request: pb.HelloRequest, stream: rpc_stream.GreeterServerStream.SayHelloRsp):
        """Abstract stream server method needed to implementation"""
        logger.info("request: %s", request.name)
        for i in range(0, 3):
            rsp = pb.HelloReply()
            rsp.message = "rsp: " + str(i)
            logger.info(f"rsp: {rsp.message}")
            await stream.send(rsp)
            await asyncio.sleep(1)

    # pylint: disable=invalid-name
    async def SayHelloAll(self, stream: rpc_stream.GreeterServerStream.SayHelloAll):
        """Abstract client stream method needed to implementation"""
        while True:
            try:
                req = await stream.recv()
                logger.info(f"request: {req.name}")
            except EOFError as err:
                logger.debug(f"recv done error {err}")
                return
            except Exception as err:  # pylint: disable=broad-except
                logger.error(f"recv err: {err}", exc_info=True)
            rsp = pb.HelloReply()
            rsp.message = "rsp data"
            await stream.send(rsp)
