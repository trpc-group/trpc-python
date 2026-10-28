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

# ``python -m examples.hellostream.trpc_main`` sets ``__name__`` to
# ``__main__`` but does not put this directory on ``sys.path``. Running the
# file directly does. Always insert both so ``greeter`` and the stub import.
_HERE = os.path.abspath(os.path.dirname(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
_STUB = os.path.join(_HERE, "stub")
if _STUB not in sys.path:
    sys.path.insert(0, _STUB)

# import sdk and plugins

# pylint: disable=unused-import

# DO NOT remove the plugin below!
# import trpc_metrics_runtime as _

from trpc_py_stream_helloworld import rpc

from greeter import GreeterServicer

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


def _build_servicer():
    """Return the servicer registered before the server forks.

    ``HELLOSTREAM_STRESS=1`` selects the stress handlers. The instance is
    created in the parent and inherited across the fork; children do not
    build another one.
    """
    if os.environ.get("HELLOSTREAM_STRESS") == "1":
        from stress_greeter import StressGreeter
        return StressGreeter()
    return GreeterServicer()


def serve(conf_path: str):
    """Start the server.

    :param conf_path: path to the config file
    """

    server = create_server(conf_path)
    # UnixGreeter's name differs from the stub. Unary paths are rewritten
    # per service; stream paths stay on the generated Greeter name, which
    # every service registers as-is.
    rpc.register_GreeterServicer_server(server, _build_servicer(), fix_rpc_name=True)
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
