# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Test all."""

import os
import sys
import asyncio

from trpc_sdk.abc import TrpcContext
# from trpc.codec import set_request_attachment
# from trpc.codec import get_response_attachment
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.serialization import SerializationType
# from trpc.plugin import plugin as trpc_plugin

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa
sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa


# DO NOT remove the plugin below!
# import trpc_metrics_runtime as _

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc
from trpc_sdk.client import Options

# HTTP client stays off unless this import is enabled.
# from features.http import test_http_client, get_addr

async_test = []
sync_test = []


# pylint: disable=invalid-name
def test_sync_Greeter_SayHello():
    """Test Greeter of SayHello"""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = 'hello'
    options = Options().set_target('ip://127.0.0.1:8000').set_protocol('trpc').set_serialization_type(SerializationType.PB).set_network('tcp')
    ctx = TrpcContext()
    try:
        ret = proxy.SayHello(ctx, req, options)
        print(f"\n  test_sync_Greeter_SayHello recv result is {ret}\n")
        assert ret is not None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error(exc)
        assert 0


sync_test.append(test_sync_Greeter_SayHello)


# pylint: disable=invalid-name
async def test_Greeter_SayHello():
    """Async test Greeter of SayHello"""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = 'hello'
    options = Options().set_target('ip://127.0.0.1:8000').set_protocol('trpc').set_serialization_type(SerializationType.PB).set_network('tcp').set_namespace('Development').set_timeout(3000)
    ctx = TrpcContext()
    # set_request_attachment(ctx, "request attachment")
    try:
        ret = await proxy.asyncSayHello(ctx, req, options)
        # get_addr(ctx)
        # rsp_attachment = get_response_attachment(ctx, is_str=True)
        print(f"\n  test_Greeter_SayHello recv result is {ret}\n")
        assert ret is not None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error(exc)
        assert 0


async_test.append(test_Greeter_SayHello())


#pylint: disable=invalid-name
def test_sync_Greeter_SayHi():
    """Test Greeter of SayHi"""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    options = Options().set_target('ip://127.0.0.1:8000').set_protocol('trpc').set_serialization_type(SerializationType.PB).set_network('tcp')
    ctx = TrpcContext()
    try:
        ret = proxy.SayHi(ctx, req, options)
        # get_addr(ctx)
        print(f"\n  test_sync_Greeter_SayHi recv result is {ret}\n")
        assert ret is not None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error(exc)
        assert 0


sync_test.append(test_sync_Greeter_SayHi)


#pylint: disable=invalid-name
async def test_Greeter_SayHi():
    """Async test Greeter of SayHi"""
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    options = Options().set_target('ip://127.0.0.1:8000').set_protocol('trpc').set_serialization_type(SerializationType.PB).set_network('tcp')
    ctx = TrpcContext()
    try:
        ret = await proxy.asyncSayHi(ctx, req, options)
        # get_addr(ctx)
        print(f"\n  test_Greeter_SayHi recv result is {ret}\n")
        assert ret is not None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error(exc)
        assert 0


async_test.append(test_Greeter_SayHi())


async def test_rpc():
    """Async test http"""
    print(f"\n=== test_rpc start=== \n")
    await asyncio.gather(*async_test)
    print(f"\n=== test_rpc end=== \n")


async def run_all():
    """Async test http"""
    await test_rpc()
    # await test_http_client()


def main():
    # Load a config file when one exists; the server still starts without one.
    # Precedence of service settings:
    # options set in code win
    # then the config file
    # then the defaults
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))
    TrpcConfig.load(config_path, 'utf-8')
    # trpc_plugin.setup()
    # for test_func in sync_test:
    #     test_func()
    asyncio.run(run_all())


if __name__ == "__main__":
    # Load a config file when one exists; the server still starts without one.
    # Precedence of service settings:
    # options set in code win
    # then the config file
    # then the defaults
    main()
