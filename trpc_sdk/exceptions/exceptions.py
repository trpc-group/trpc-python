# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com
#
"""TRPC Framework Exception Definitions
This file contains all exception classes and error codes used in the TRPC framework
"""


class ErrorType:
    """Classification of error types in TRPC framework"""
    Framework = 1  # Errors related to framework operation
    Business = 2  # Errors related to business logic
    CalleeFramework = 3  # Errors from callee framework


class ErrorCode:
    """Standard error codes used in TRPC framework"""
    TRPC_INVOKE_SUCCESS = 0

    TRPC_SERVER_DECODE_ERR = 1
    TRPC_SERVER_ENCODE_ERR = 2
    TRPC_SERVER_NOSERVICE_ERR = 11
    TRPC_SERVER_NOFUNC_ERR = 12
    TRPC_SERVER_TIMEOUT_ERR = 21
    TRPC_SERVER_OVERLOAD_ERR = 22
    TRPC_SERVER_SYSTEM_ERR = 31
    TRPC_SERVER_AUTH_FAIL = 41
    TRPC_SERVER_VALIDATE_FAIL = 51

    TRPC_CLIENT_INVOKE_TIMEOUT_ERR = 101
    TRPC_CLIENT_CONNECT_ERR = 111
    TRPC_CLIENT_ENCODE_ERR = 121
    TRPC_CLIENT_DECODE_ERR = 122
    TRPC_CLIENT_ROUTER_ERR = 131
    TRPC_CLIENT_NET_ERR = 141
    TRPC_CLIENT_POOL_LIMIT = 151  # 连接数量超过限制错误
    TRPC_CLIENT_POOL_CLOSED = 152  # 连接池关闭错误
    TRPC_CLIENT_POOL_CONN_CLOSED = 153  # 连接关闭
    TRPC_CLIENT_CANCELED = 161

    TRPC_TASK_HANDLE_ERR = 200
    TRPC_CLIENT_STREAM_QUEUE_FULL = 201
    TRPC_PLUGIN_INIT_ERR = 202  # 插件初始化错误

    TRPC_INVOKE_UNKNOWN_ERR = 999


class TRPCBaseError(Exception):
    """
    Base exception class for all TRPC framework errors
    
    Attributes:
        type (int): Error type classification (from ErrorType)
        code (int): Specific error code (from ErrorCode)
        msg (str): Human-readable error message
    """

    def __init__(self, exception_type: int, code: int, msg: str):
        """
        Initialize the TRPC base exception
        
        Args:
            exception_type: Error type from ErrorType enum
            code: Error code from ErrorCode enum
            msg: Error message (will be converted to str if not already)
        """
        self.type = exception_type
        self.code = code
        self.msg = msg
        if isinstance(msg, bytes):
            self.msg = msg.decode('utf-8')
        elif not isinstance(msg, str):
            self.msg = str(msg)


class ServerDecodeError(TRPCBaseError):
    """Exception raised when server fails to decode incoming request data"""

    def __init__(self, msg):
        """
        Initialize the decode error
        
        Args:
            msg: Description of the decode failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_DECODE_ERR, msg)


class ServerEncodeError(TRPCBaseError):
    """Exception raised when server fails to encode response data"""

    def __init__(self, msg):
        """
        Initialize the encode error
        
        Args:
            msg: Description of the encode failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_ENCODE_ERR, msg)


class ServiceNotFoundError(TRPCBaseError):
    """Exception raised when requested service is not available"""

    def __init__(self, msg):
        """
        Initialize the service not found error
        
        Args:
            msg: Description of the missing service
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_NOSERVICE_ERR, msg)


class FuncNotFoundError(TRPCBaseError):
    """Exception raised when requested function/method is not found in service"""

    def __init__(self, msg):
        """
        Initialize the function not found error
        
        Args:
            msg: Description of the missing function, typically includes function name
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_NOFUNC_ERR, msg)


class ServerTimeoutError(TRPCBaseError):
    """Exception raised when server processing exceeds time limit"""

    def __init__(self, msg):
        """
        Initialize the server timeout error
        
        Args:
            msg: Description of the timeout condition
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_TIMEOUT_ERR, msg)


class ServerOverloadError(TRPCBaseError):
    """Exception raised when server is overloaded and cannot handle more requests"""

    def __init__(self, msg):
        """
        Initialize the server overload error
        
        Args:
            msg: Description of the overload condition
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_OVERLOAD_ERR, msg)


class ServerSystemError(TRPCBaseError):
    """Exception raised for internal server system errors"""

    def __init__(self, msg):
        """
        Initialize the system error
        
        Args:
            msg: Description of the system failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_SYSTEM_ERR, msg)


class ServerAuthFailError(TRPCBaseError):
    """Exception raised when authentication fails"""

    def __init__(self, msg):
        """
        Initialize the authentication error
        
        Args:
            msg: Description of the authentication failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_AUTH_FAIL, msg)


class ServerValidateFailError(TRPCBaseError):
    """Exception raised when request validation fails"""

    def __init__(self, msg):
        """
        Initialize the validation error
        
        Args:
            msg: Description of the validation failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_SERVER_VALIDATE_FAIL, msg)


class TaskHandleError(TRPCBaseError):
    """Exception raised when task processing encounters an error"""

    def __init__(self, msg):
        """
        Initialize the task handling error
        
        Args:
            msg: Description of the task processing failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_TASK_HANDLE_ERR, msg)


class ClientTimeoutError(TRPCBaseError):
    """Exception raised when client request times out"""

    def __init__(self, msg):
        """
        Initialize the timeout error
        
        Args:
            msg: Description of the timeout
        """
        self.timeout_type = None
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_INVOKE_TIMEOUT_ERR, msg)

    def set_timeout_type(self, timeout_type):
        """
        Set the specific type of timeout
        
        Args:
            timeout_type: Detailed classification of timeout reason
        """
        self.timeout_type = timeout_type

    def __str__(self):
        """Return formatted string representation of the timeout error"""
        return '%s. timeout reason: %s' % (self.msg, self.timeout_type)


class ClientConnectionError(TRPCBaseError):
    """Exception raised when client fails to establish connection"""

    def __init__(self, msg):
        """
        Initialize the connection error
        
        Args:
            msg: Description of the connection failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_CONNECT_ERR, msg)


class ClientEncodeError(TRPCBaseError):
    """Exception raised when client fails to encode request data"""

    def __init__(self, msg):
        """
        Initialize the encode error
        
        Args:
            msg: Description of the encode failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_ENCODE_ERR, msg)


class ClientDecodeError(TRPCBaseError):
    """Exception raised when client fails to decode response data"""

    def __init__(self, msg):
        """
        Initialize the decode error
        
        Args:
            msg: Description of the decode failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_DECODE_ERR, msg)


class ClientRouteError(TRPCBaseError):
    """Exception raised when client encounters routing issues"""

    def __init__(self, msg):
        """
        Initialize the routing error
        
        Args:
            msg: Description of the routing failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_ROUTER_ERR, msg)


class ClientNetError(TRPCBaseError):
    """Exception raised for general network-related errors"""

    def __init__(self, msg):
        """
        Initialize the network error
        
        Args:
            msg: Description of the network issue
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_NET_ERR, msg)


class ClientInvokeError(TRPCBaseError):
    """Exception raised for unknown invocation errors"""

    def __init__(self, msg):
        """
        Initialize the invocation error
        
        Args:
            msg: Description of the invocation failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_INVOKE_UNKNOWN_ERR, msg)


class ClientPoolLimitError(TRPCBaseError):
    """Exception raised when connection pool exceeds its limit"""

    def __init__(self, msg):
        """
        Initialize the pool limit error
        
        Args:
            msg: Description of the pool limit exceeded
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_POOL_LIMIT, msg)


class ClientPoolClosedError(TRPCBaseError):
    """Exception raised when trying to use a closed connection pool"""

    def __init__(self, msg):
        """
        Initialize the pool closed error
        
        Args:
            msg: Description of the closed pool access attempt
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_POOL_CLOSED, msg)


class ClientPoolConnClosedError(TRPCBaseError):
    """Exception raised when trying to use a closed connection"""

    def __init__(self, msg):
        """
        Initialize the connection closed error
        
        Args:
            msg: Description of the closed connection access attempt
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_POOL_CONN_CLOSED, msg)


class ClientCanceledError(TRPCBaseError):
    """Exception raised when operation is canceled"""

    def __init__(self, msg):
        """
        Initialize the canceled operation error
        
        Args:
            msg: Description of the canceled operation
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_CANCELED, msg)


class ClientStreamQueueFullError(TRPCBaseError):
    """Exception raised when stream queue is full"""

    def __init__(self, msg):
        """
        Initialize the stream queue full error
        
        Args:
            msg: Description of the queue full condition
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_CLIENT_STREAM_QUEUE_FULL, msg)


class PluginInitError(TRPCBaseError):
    """Exception raised when plugin initialization fails
    
    This is a critical error that will cause the service to exit
    """

    def __init__(self, msg: str):
        """
        Initialize the plugin initialization error
        
        Args:
            msg: Description of the initialization failure
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_PLUGIN_INIT_ERR, msg)


class UnknownError(TRPCBaseError):
    """Exception raised for unclassified/unrecognized errors"""

    def __init__(self, msg):
        """
        Initialize the unknown error
        
        Args:
            msg: Description of the unknown error condition
        """
        super().__init__(ErrorType.Framework, ErrorCode.TRPC_INVOKE_UNKNOWN_ERR, msg)


def NewFrameError(code: int, msg: str):
    """
    Helper function to create a framework-level error
    
    Args:
        code: Error code from ErrorCode
        msg: Error message
        
    Returns:
        TRPCBaseError: Configured exception instance
    """
    return TRPCBaseError(ErrorType.Framework, code, msg)


def NewBusinessError(code: int, msg: str):
    """
    Helper function to create a business-level error
    
    Args:
        code: Error code from ErrorCode
        msg: Error message
        
    Returns:
        TRPCBaseError: Configured exception instance
    """
    return TRPCBaseError(ErrorType.Business, code, msg)
