# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python exception definitions."""

from ._trpc_exception import TrpcErrorCode
from ._trpc_exception import TrpcErrorType
from ._trpc_exception import TrpcException
from ._trpc_exception import UnknownError
from ._trpc_exception import create_business_error
from ._trpc_exception import create_frame_error
from ._server_exceptions import ServerEncodeError
from ._server_exceptions import ServerFuncNotFoundError
from ._server_exceptions import ServerSystemError
from ._server_exceptions import ServerTimeoutError
from ._exceptions import TaskHandleError
from ._client_exceptions import ClientConnectionError
from ._client_exceptions import ClientDecodeError
from ._client_exceptions import ClientEncodeError
from ._client_exceptions import ClientInvokeError
from ._client_exceptions import ClientPoolClosedError
from ._client_exceptions import ClientPoolConnClosedError
from ._client_exceptions import ClientPoolLimitError
from ._client_exceptions import ClientRouteError
from ._client_exceptions import ClientTimeoutError

__all__ = [
    "TrpcErrorType",
    "TrpcErrorCode",
    "TrpcException",
    "UnknownError",
    "create_frame_error",
    "create_business_error",
    "ServerEncodeError",
    "ServerFuncNotFoundError",
    "ServerTimeoutError",
    "ServerSystemError",
    "TaskHandleError",
    "ClientTimeoutError",
    "ClientConnectionError",
    "ClientEncodeError",
    "ClientDecodeError",
    "ClientRouteError",
    "ClientInvokeError",
    "ClientPoolLimitError",
    "ClientPoolClosedError",
    "ClientPoolConnClosedError"
]
