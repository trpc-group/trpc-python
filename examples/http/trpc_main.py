# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""This is server module"""

import os
import sys
import argparse
from typing import List

from trpc_sdk.log import logger
from trpc_sdk.server import create_server

# add the stub to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "rpc/stub")))  # noqa
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "rpc")))  # noqa
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "normal")))  # noqa
if __name__ != '__main__':  # noqa
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

# import sdk and plugins

# pylint: disable=unused-import

# DO NOT remove the plugin below!
# import trpc_metrics_runtime as _

import normal as _
# Import the generated RPC stub.
from trpc_trpc_python_group_helloworld import rpc
# Import the service implementation.
from greeter import GreeterServicer

# argparse parser
parser = argparse.ArgumentParser(description='TRPC Python Framework Server')

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf/trpc_python.yaml"))

# define config file argument
parser.add_argument('--conf',
                    '-c',
                    default=DEFAULT_CONF_PATH,
                    help='start server with conf, for example: `python3 trpc_main.py  --conf=trpc_python.yaml`')


def parse_args(argv: List[str]):
    """Parse command-line arguments.

    :param argv: command-line arguments
    """
    try:
        args = parser.parse_args(argv[1:])  # Skip the script name
        if os.path.exists(args.conf):
            print(f"conf_path: {args.conf}")
            return args.conf
        print(f"conf_path: {args.conf} not exists")
        if os.path.exists(DEFAULT_CONF_PATH):
            print(f"use default conf path: {DEFAULT_CONF_PATH}")
            return DEFAULT_CONF_PATH
        print(f"default conf path: {DEFAULT_CONF_PATH} not exists, please check the path")
        sys.exit(1)
    except SystemExit:
        # argparse will call sys.exit() when --help is used or invalid args
        sys.exit(1)


def serve(conf_path: str):
    """Start the server.

    :param conf_path: path to the config file
    """

    # Create the server.
    server = create_server(conf_path)

    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)

    server.serve()


def main():
    """Program entry point."""

    # Parse command-line arguments.
    conf_path = parse_args(sys.argv)
    logger.info("load config %s", conf_path)

    # Start the server.
    serve(conf_path)


if __name__ == '__main__':
    sys.exit(main())
