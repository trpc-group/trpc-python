# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Http test module"""

import os
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from trpc_sdk.abc import TrpcContext as Context
from trpc_sdk.log import logger
from trpc_sdk.http import route
from trpc_sdk.http import USER_ROUTE_KEY
from trpc_sdk.http import get_http_response
from trpc_sdk.http import register_static
from trpc_sdk.http import set_http_rsp_header
from trpc_sdk.utils import is_worker_process
from trpc_sdk.utils import is_task_process
from trpc_sdk.utils import is_timer_process
from trpc_sdk.utils import set_current_process_var
from trpc_sdk.utils import get_current_process_var

print("=========== register http ================== \n")


# No service_name: every HTTP service in the config binds this handler.
@route("/normal_http")
async def normal_http(ctx: Context, request: Request) -> Response:
    """Test normal http"""
    logger.debug(f"=== normal_http, http_req header: {request.headers} ===")
    msg = ctx.get_message()
    register_service_name = "trpc.trpc_python_group.helloworld.Greeter1"
    msg_info = f"\n\n test_route, register service_name: {register_service_name}"
    msg_info = f"{msg_info}, callee_service_name: {msg.callee_service_name}\n\n"
    logger.debug(f"=== normal_http, msg_info: {msg_info} ===")
    return web.json_response({"data": "json"})


# Only the service named trpc.trpc_python_group.helloworld.Greeter1 runs this handler.
@route("/bind_service_name", service_name="trpc.trpc_python_group.helloworld.Greeter1")
async def bind_service_name(ctx: Context, request: Request) -> Response:
    """Test normal http"""
    data = get_current_process_var("_test_var")
    if not data:
        set_current_process_var("_test_var", "test")
    else:
        print(f"get_current_process_var: {data}")
    print(f"[http] test is_worker_process: {is_worker_process()} expect True")
    print(f"[http] test  is_task_process: {is_task_process()} expect False")
    print(f"[http] test  is_timer_process: {is_timer_process()} expect False")
    rsp = get_http_response(ctx)
    route_des = ctx.get_meta_data(USER_ROUTE_KEY)
    rsp.headers["url_path"] = request.path
    msg = ctx.get_message()
    register_service_name = "trpc.trpc_python_group.helloworld.Greeter1"
    msg_info = f"\n\n test_route, register service_name: {register_service_name}"
    msg_info = f"{msg_info}, route service name: {route_des.service_name}"
    msg_info = f"{msg_info}, callee_service_name: {msg.callee_service_name}\n\n"
    print(f"{msg_info}")
    return web.json_response({"data": "json"})


# Example of setting HTTP response headers.
@route("/route_with_rsp_header")
async def route_with_rsp_header(ctx: Context, request: Request) -> bytes:
    """Test normal http"""
    rsp = get_http_response(ctx)
    data = get_current_process_var("_test_var")
    print(f"get_current_process_var: {data}")
    print(f"[http] test2 is_worker_process: {is_worker_process()} expect True")
    print(f"[http] test2 is_task_process: {is_task_process()} expect False")
    print(f"[http] test2 is_timer_process: {is_timer_process()} expect False")

    route_des = ctx.get_meta_data(USER_ROUTE_KEY)
    msg = ctx.get_message()
    register_service_name = ""
    msg_info = f"\n\n xxx test_route2, register service_name: {register_service_name}"
    msg_info = f"{msg_info}, route service name: {route_des.service_name}"
    msg_info = f"{msg_info}, callee_service_name: {msg.callee_service_name}\n\n"
    print(f"{msg_info}")
    rsp.headers["url_path"] = request.path
    set_http_rsp_header(ctx, header={"test": "data"})
    return b"user-defined test"


current_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
parent_path = os.path.abspath(os.path.join(current_path, "../../../../"))
docs_path = os.path.abspath(os.path.join(parent_path, "docs"))

# Example of serving static HTTP files.
register_static("/static", docs_path)
