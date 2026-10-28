# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import argparse
import os
import sys
from typing import List

import asyncio

import aiohttp.web as web
from aiohttp.web_request import Request
from aiohttp.web_response import StreamResponse

from trpc_sdk.abc import TrpcContext
from trpc_sdk.http import route
from trpc_sdk.log import logger
from trpc_sdk.server import create_server


@route("/sse")
async def sse(ctx: TrpcContext, request: Request) -> StreamResponse:
    """must set response like this"""
    rsp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )

    await rsp.prepare(request)

    async def send_event(msg: str):
        await rsp.write(f"data: {msg}\n\n".encode("utf-8"))

    try:
        for i in range(0, 3):
            # Replace this with your own logic, for example a database read.
            message = f"Hello, this is a sse {i} message from the server!"
            await send_event(message)
            await asyncio.sleep(1)  # Send one message per second.
    except asyncio.CancelledError:
        pass
    finally:
        await rsp.write_eof()

    return rsp


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
    svr = create_server(conf_path)
    svr.serve()


def main():
    """Program entry point."""
    # Parse command-line arguments.
    conf_path = parse_args(sys.argv)
    logger.info("load config %s", conf_path)

    # Start the server.
    serve(conf_path)


if __name__ == "__main__":
    sys.exit(main())
