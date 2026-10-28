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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa
if __name__ != '__main__':  # noqa
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # noqa

# import sdk and plugins

# pylint: disable=unused-import

# DO NOT remove the plugin below!
# import trpc_metrics_runtime as _

# Import the generated RPC stub.
from trpc_trpc_python_group_helloworld import rpc
# Import the service implementation.
import greeter
from greeter import GreeterServicer

# Registers protocol "user" before Server.serve(). A config without that
# protocol never calls the starter. This does not install thread hooks.
import features.user_protocol as _  # pylint: disable=unused-import

# HTTP routes stay off unless this import is enabled.
# import features.http as _

# argparse parser
parser = argparse.ArgumentParser(description='TRPC Python Framework Server')

DEFAULT_CONF_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "trpc_python.yaml"))

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

    # Build server options.
    # server_option = create_server_opts()

    # Tasks must be registered before serve() forks; task_num in the YAML
    # sets how many py_task children start.
    greeter.register_py_task()
    greeter.task_enabled = True

    # Create the server.
    server = create_server(conf_path)
    # print("backend:", logger._active_backend_name)
    # print("logger class:", type(logger.DefaultLogger))
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)

    # When server.io_wait_count is N, the I/O process does not listen until
    # inc_io_wait_count has run N times.
    # Use this only when a child should not serve during startup, for example
    # while a task process is loading a model.
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
