# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""TRPC Framework Exception Definitions
This file contains all exception classes and error codes used in the TRPC framework
"""

from __future__ import annotations

from ._trpc_exception import TrpcErrorCode
from ._trpc_exception import TrpcErrorType
from ._trpc_exception import TrpcException


class ServerEncodeError(TrpcException):
    """Exception raised when server fails to encode response data"""

    def __init__(self, msg: str):
        """
        Initialize the encode error
        
        Args:
            msg: Description of the encode failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_ENCODE_ERR, msg)

class ServerFuncNotFoundError(TrpcException):
    """Exception raised when requested function/method is not found in service"""

    def __init__(self, msg: str):
        """
        Initialize the function not found error
        
        Args:
            msg: Description of the missing function, typically includes function name
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_NOFUNC_ERR, msg)


class ServerTimeoutError(TrpcException):
    """Exception raised when server processing exceeds time limit"""

    def __init__(self, msg: str):
        """
        Initialize the server timeout error
        
        Args:
            msg: Description of the timeout condition
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_TIMEOUT_ERR, msg)


class ServerSystemError(TrpcException):
    """Exception raised for internal server system errors"""

    def __init__(self, msg: str):
        """
        Initialize the system error
        
        Args:
            msg: Description of the system failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_SYSTEM_ERR, msg)


