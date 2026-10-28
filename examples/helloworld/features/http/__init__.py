# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Http test module"""

from ._server import normal_http
from ._server import bind_service_name
from ._server import route_with_rsp_header
from ._server import route_with_rsp_header
from ._client import HTTP_TEST
from ._client import test_http_client
from ._client import get_addr

__all__ = [
    "normal_http",
    "bind_service_name",
    "route_with_rsp_header",
    "HTTP_TEST",
    "test_http_client",
    "get_addr",
]
