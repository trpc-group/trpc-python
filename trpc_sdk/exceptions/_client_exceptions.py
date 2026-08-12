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





class ClientTimeoutError(TrpcException):
    """Exception raised when client request times out"""

    def __init__(self, msg: str):
        """
        Initialize the timeout error
        
        Args:
            msg: Description of the timeout
        """
        self.timeout_type = None
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_INVOKE_TIMEOUT_ERR, msg)

    def set_timeout_type(self, timeout_type: str):
        """
        Set the specific type of timeout
        
        Args:
            timeout_type: Detailed classification of timeout reason
        """
        self.timeout_type = timeout_type

    def __str__(self) -> str:
        """Return formatted string representation of the timeout error"""
        return '%s. timeout reason: %s' % (self.msg, self.timeout_type)


class ClientConnectionError(TrpcException):
    """Exception raised when client fails to establish connection"""

    def __init__(self, msg: str):
        """
        Initialize the connection error
        
        Args:
            msg: Description of the connection failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_CONNECT_ERR, msg)


class ClientEncodeError(TrpcException):
    """Exception raised when client fails to encode request data"""

    def __init__(self, msg: str):
        """
        Initialize the encode error
        
        Args:
            msg: Description of the encode failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_ENCODE_ERR, msg)


class ClientDecodeError(TrpcException):
    """Exception raised when client fails to decode response data"""

    def __init__(self, msg: str):
        """
        Initialize the decode error
        
        Args:
            msg: Description of the decode failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_DECODE_ERR, msg)


class ClientRouteError(TrpcException):
    """Exception raised when client encounters routing issues"""

    def __init__(self, msg: str):
        """
        Initialize the routing error
        
        Args:
            msg: Description of the routing failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_ROUTER_ERR, msg)


class ClientInvokeError(TrpcException):
    """Exception raised for unknown invocation errors"""

    def __init__(self, msg: str):
        """
        Initialize the invocation error
        
        Args:
            msg: Description of the invocation failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR, msg)


class ClientPoolLimitError(TrpcException):
    """Exception raised when connection pool exceeds its limit"""

    def __init__(self, msg: str):
        """
        Initialize the pool limit error
        
        Args:
            msg: Description of the pool limit exceeded
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_VALIDATE_ERR, msg)


class ClientPoolClosedError(TrpcException):
    """Exception raised when trying to use a closed connection pool"""

    def __init__(self, msg: str):
        """
        Initialize the pool closed error
        
        Args:
            msg: Description of the closed pool access attempt
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_POOL_CLOSED, msg)


class ClientPoolConnClosedError(TrpcException):
    """Exception raised when trying to use a closed connection"""

    def __init__(self, msg):
        """
        Initialize the connection closed error
        
        Args:
            msg: Description of the closed connection access attempt
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_CLIENT_POOL_CONN_CLOSED, msg)
