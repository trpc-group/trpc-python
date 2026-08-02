# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com
#
"""Test cases for TRPC framework exceptions"""

import pytest

from trpc_sdk.exceptions import ClientCanceledError
from trpc_sdk.exceptions import ClientConnectionError
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientNetError
from trpc_sdk.exceptions import ClientPoolClosedError
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.exceptions import ClientPoolLimitError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ClientStreamQueueFullError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ErrorCode
from trpc_sdk.exceptions import ErrorType
from trpc_sdk.exceptions import FuncNotFoundError
from trpc_sdk.exceptions import NewBusinessError
from trpc_sdk.exceptions import NewFrameError
from trpc_sdk.exceptions import PluginInitError
from trpc_sdk.exceptions import ServerAuthFailError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import ServerEncodeError
from trpc_sdk.exceptions import ServerOverloadError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.exceptions import ServerTimeoutError
from trpc_sdk.exceptions import ServerValidateFailError
from trpc_sdk.exceptions import ServiceNotFoundError
from trpc_sdk.exceptions import TRPCBaseError
from trpc_sdk.exceptions import UnknownError


def test_error_type_values():
    """Test ErrorType enum values"""
    assert ErrorType.Framework == 1
    assert ErrorType.Business == 2
    assert ErrorType.CalleeFramework == 3


def test_error_code_values():
    """Test key ErrorCode values"""
    assert ErrorCode.TRPC_INVOKE_SUCCESS == 0
    assert ErrorCode.TRPC_SERVER_DECODE_ERR == 1
    assert ErrorCode.TRPC_CLIENT_INVOKE_TIMEOUT_ERR == 101
    assert ErrorCode.TRPC_INVOKE_UNKNOWN_ERR == 999


def test_trpc_base_error_initialization():
    """Test TRPCBaseError initialization"""
    error = TRPCBaseError(ErrorType.Framework, ErrorCode.TRPC_SERVER_DECODE_ERR, "test error")
    assert error.type == ErrorType.Framework
    assert error.code == ErrorCode.TRPC_SERVER_DECODE_ERR
    assert error.msg == "test error"
    assert str(error) == "(1, 1, 'test error')"


def test_trpc_base_error_msg_conversion():
    """Test TRPCBaseError message type conversion"""
    # Test bytes to str conversion
    error = TRPCBaseError(1, 1, b"bytes message")
    assert error.msg == "bytes message"

    # Test non-str conversion
    error = TRPCBaseError(1, 1, 123)
    assert error.msg == "123"


@pytest.mark.parametrize("exception_class,error_code", [
    (ServerDecodeError, ErrorCode.TRPC_SERVER_DECODE_ERR),
    (ServerEncodeError, ErrorCode.TRPC_SERVER_ENCODE_ERR),
    (ServiceNotFoundError, ErrorCode.TRPC_SERVER_NOSERVICE_ERR),
    (FuncNotFoundError, ErrorCode.TRPC_SERVER_NOFUNC_ERR),
    (ServerTimeoutError, ErrorCode.TRPC_SERVER_TIMEOUT_ERR),
    (ServerOverloadError, ErrorCode.TRPC_SERVER_OVERLOAD_ERR),
    (ServerSystemError, ErrorCode.TRPC_SERVER_SYSTEM_ERR),
    (ServerAuthFailError, ErrorCode.TRPC_SERVER_AUTH_FAIL),
    (ServerValidateFailError, ErrorCode.TRPC_SERVER_VALIDATE_FAIL),
])
def test_server_exceptions(exception_class, error_code):
    """Test server-side exception classes"""
    msg = "test message"
    exc = exception_class(msg)

    assert isinstance(exc, TRPCBaseError)
    assert exc.type == ErrorType.Framework
    assert exc.code == error_code
    assert exc.msg == msg


def test_client_timeout_error():
    """Test ClientTimeoutError specific features"""
    exc = ClientTimeoutError("timeout occurred")
    assert exc.timeout_type is None

    exc.set_timeout_type("connection")
    assert exc.timeout_type == "connection"
    assert "timeout reason: connection" in str(exc)


@pytest.mark.parametrize("exception_class,error_code", [
    (ClientConnectionError, ErrorCode.TRPC_CLIENT_CONNECT_ERR),
    (ClientEncodeError, ErrorCode.TRPC_CLIENT_ENCODE_ERR),
    (ClientDecodeError, ErrorCode.TRPC_CLIENT_DECODE_ERR),
    (ClientRouteError, ErrorCode.TRPC_CLIENT_ROUTER_ERR),
    (ClientNetError, ErrorCode.TRPC_CLIENT_NET_ERR),
    (ClientPoolLimitError, ErrorCode.TRPC_CLIENT_POOL_LIMIT),
    (ClientPoolClosedError, ErrorCode.TRPC_CLIENT_POOL_CLOSED),
    (ClientPoolConnClosedError, ErrorCode.TRPC_CLIENT_POOL_CONN_CLOSED),
    (ClientCanceledError, ErrorCode.TRPC_CLIENT_CANCELED),
    (ClientStreamQueueFullError, ErrorCode.TRPC_CLIENT_STREAM_QUEUE_FULL),
])
def test_client_exceptions(exception_class, error_code):
    """Test client-side exception classes"""
    msg = "client error"
    exc = exception_class(msg)

    assert isinstance(exc, TRPCBaseError)
    assert exc.type == ErrorType.Framework
    assert exc.code == error_code
    assert exc.msg == msg


def test_plugin_init_error():
    """Test PluginInitError specific features"""
    exc = PluginInitError("plugin failed")
    assert exc.code == ErrorCode.TRPC_PLUGIN_INIT_ERR
    assert isinstance(exc, TRPCBaseError)


def test_unknown_error():
    """Test UnknownError features"""
    exc = UnknownError("unknown issue")
    assert exc.code == ErrorCode.TRPC_INVOKE_UNKNOWN_ERR
    assert isinstance(exc, TRPCBaseError)


def test_new_frame_error():
    """Test NewFrameError helper function"""
    exc = NewFrameError(ErrorCode.TRPC_SERVER_DECODE_ERR, "frame error")
    assert exc.type == ErrorType.Framework
    assert exc.code == ErrorCode.TRPC_SERVER_DECODE_ERR
    assert exc.msg == "frame error"


def test_new_business_error():
    """Test NewBusinessError helper function"""
    exc = NewBusinessError(1001, "business error")
    assert exc.type == ErrorType.Business
    assert exc.code == 1001
    assert exc.msg == "business error"
