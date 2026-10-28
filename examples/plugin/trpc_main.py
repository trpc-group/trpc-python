# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server for the user-defined plugin example.

Same entry shape as helloworld: parse ``--conf``, ``create_server``,
register the Greeter, then ``serve``. Importing ``plugin`` before
``create_server`` is what registers ``PluginType.USER_DEFINED`` /
``business``. The instance is built later, in the worker.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List

from trpc_sdk.log import logger
from trpc_sdk.server import create_server

_HERE = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(_HERE, "../helloworld/stub")))
if __name__ != "__main__":
    sys.path.append(_HERE)

# Registers BusinessPlugin. The decorator does not build the instance.
import plugin as _  # pylint: disable=unused-import

from trpc_trpc_python_group_helloworld import rpc
from greeter import GreeterServicer

parser = argparse.ArgumentParser(description="tRPC Python user-defined plugin example")

DEFAULT_CONF_PATH = os.path.join(_HERE, "trpc_python.yaml")

parser.add_argument("--conf",
                    "-c",
                    default=DEFAULT_CONF_PATH,
                    help="start server with conf, for example: `python3 trpc_main.py --conf=trpc_python.yaml`")


def parse_args(argv: List[str]) -> str:
    """Parse command-line arguments.

    Args:
        argv: Command-line arguments, including the program name.

    Returns:
        Config path to load.
    """
    try:
        args = parser.parse_args(argv[1:])
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
        sys.exit(1)


def serve(conf_path: str) -> None:
    """Start the server.

    Args:
        conf_path: Path to the framework YAML.
    """
    server = create_server(conf_path)
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()


def main() -> None:
    """Program entry point."""
    conf_path = parse_args(sys.argv)
    logger.info("load config %s", conf_path)
    serve(conf_path)


if __name__ == "__main__":
    sys.exit(main())
