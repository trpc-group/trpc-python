# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import trpc_sdk

from examples.helloworld.greeter import GreeterServicer
from examples.helloworld.stub.trpc_trpc_python_group_helloworld import rpc


def serve(conf_path):
    ignore_services = set("trpc.trpc_python_group.helloworld.Greeter3")
    svr = trpc_sdk.new(conf_path, ignore_services)
    rpc.register_GreeterServicer_server(svr, GreeterServicer(), True)


def test_main():
    try:
        file_path = './tests/test_config_data/test_main_conf.yaml'
        serve(file_path)
    except Exception as err:
        print(f"err is {err}")
