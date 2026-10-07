# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP protocol constants shared across the package.

Scheme names, aiohttp kwargs keys, and context-meta keys live here so
codec, transport and client code share one spelling. Header-name
strings live on :class:`~trpc_sdk.http.HttpTrpcHeader`; only the lookup keys
themselves are here.
"""

from __future__ import annotations

# Wire scheme names used as plugin keys and HttpClientReqHeader.scheme.
HTTP = "http"
HTTPS = "https"

# Deprecated: the aiohttp Request / Response now live on the message as
# server_raw_req / server_raw_rsp and are no longer written to context meta.
# Use get_http_request / get_http_response instead.
HTTP_REQ_KEY = "request"
HTTP_RSP_KEY = "response"

# Keys inside the kwargs dict stored under HTTP_ARGS_META_KEY.
HTTP_ARG_JSON = "json"
HTTP_ARG_SSL = "ssl"
HTTP_ARG_STREAM_KEY = "stream"

# Context meta key for extra aiohttp ClientSession.request kwargs.
HTTP_ARGS_META_KEY = "http_args_meta_key"

# Whether trans_info values in HTTP headers are base64-encoded.
TRANS_INFO_BASE64_KEY = "is_base64"

# Context meta key for a user-registered @route HttpRoute.
USER_ROUTE_KEY = "user_route"

# Context meta key for the listen address of this HTTP service.
LOCAL_ADDR = "local_addr"

# Context meta keys for extra response headers / accepted HTTP status codes.
HTTP_RSP_HEADER_KEY = "__trpc_rsp_http_header"
HTTP_RSP_OK_CODE_KEY = "__trpc_rsp_http_ok_code"

# Name of the process-wide reusable aiohttp ClientSession.
DEFAULT_SESSION_NAME = "default"

# Characters ``str.splitlines`` treats as line boundaries; used by the SSE
# decoder to tell whether a chunk ends on a complete line.
SSE_NEWLINE_CHARS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"
