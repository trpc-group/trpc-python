# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""This business logic"""

import os
import sys
import time
from typing import List

from trpc_sdk.log import logger
from trpc_sdk.abc import TaskInfo
from trpc_sdk.abc import TrpcContext
from trpc_sdk.plugin import register_task
from trpc_sdk.task import send_to_task
# from trpc.codec import SerializationType
# from trpc.codec import get_request_attachment
# from trpc.codec import set_response_attachment

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc
# from trpc.client import Options

PY_TASK_NAME = "py_task"

# Set by trpc_main.py after register_py_task(). Other hosts of
# GreeterServicer (features/admin) fork no task, so SayHello must not send.
task_enabled = False


def _py_task_initializer() -> None:
    """Runs once in the task child after fork, before it reads requests."""
    logger.info("initializer %s done, pid %s", PY_TASK_NAME, os.getpid())


def _py_task_handler(reqs: List[str]) -> List[str]:
    """Echo one batch; runs in the task child, not on the serving loop.

    Args:
        reqs: Request bodies sent by send_to_task, at most batch_size items.

    Returns:
        One response per request, same order.
    """
    return list(reqs)


def register_py_task() -> None:
    """Register py_task. Call before create_server so serve() forks its child."""
    register_task(PY_TASK_NAME,
                  TaskInfo(name=PY_TASK_NAME, handler=_py_task_handler, initializer=_py_task_initializer))


def run_in_thread(timeout: int):
    """Test run in thread"""
    time.sleep(timeout)
    return "test_thread-" + str(timeout)


def get_metadata(ctx: TrpcContext):
    """Get metadata"""
    for k, v in ctx.meta_data.items():
        logger.info(f"{k}: {v}")
    msg = ctx.get_message()
    logger.info(f"server_meta_data: {msg.server_meta_data}")
    logger.info(f"client_meta_data: {msg.client_meta_data}")


def get_addr(ctx: TrpcContext):
    msg = ctx.get_message()
    logger.info(f"msg local_ip: {msg.local_ip}")
    logger.info(f"msg remote_addr: {msg.remote_addr}")
    logger.info(f"ctx remote_addr: {ctx.remote_ip}:{ctx.remote_port}")
    logger.info(f"ctx local_ip: {ctx.local_ip}:{ctx.local_port}")


class GreeterServicer(rpc.GreeterServicer):
    """Provides methods that implement functionality of greeter servicer."""

    def __init__(self):
        pass

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Business method"""
        logger.info(f"test log in SayHello req: {request.msg}")
        logger.with_context_fields(ctx, {"trace_id": "12345", "aa": "bb", "t1": {"a": 1}})
        logger.error_context(ctx, "test log")
        # get_metadata(ctx)
        # get_addr(ctx)

        if task_enabled:
            task_rsp = await send_to_task(ctx, PY_TASK_NAME, request.msg)
            logger.info("%s echo: %r", PY_TASK_NAME, task_rsp)

        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Business method"""
        logger.info("test log in SayHi")

        # proxy = rpc.GreeterClientProxyImpl()
        # hi_req = pb.HelloRequest()
        # hi_req.msg = request.msg
        # options = Options().set_target('ip://127.0.0.1:8000').set_serialization_type(SerializationType.PB).set_network('tcp').set_protocol('trpc').set_namespace('Development').set_timeout(3000)
        # hi_rsp = await proxy.asyncSayHello(ctx, hi_req, options)
        rsp = pb.HelloReply()
        # rsp.msg = "hi: " + hi_rsp.msg
        rsp.msg = "hi: " + request.msg
        return rsp
