# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test cases for TRPC framework exceptions"""

import pytest

from trpc_sdk.exceptions import ClientConnectionError
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientPoolClosedError
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.exceptions import ClientPoolLimitError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import ServerEncodeError
from trpc_sdk.exceptions import ServerFuncNotFoundError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.exceptions import ServerTimeoutError
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.exceptions import create_business_error
from trpc_sdk.exceptions import create_frame_error
from trpc_sdk.plugin import PluginInitError


def test_error_type_values():
    """Test TrpcErrorType enum values"""
    assert TrpcErrorType.FRAMEWORK == 1
    assert TrpcErrorType.BUSINESS == 2
    assert TrpcErrorType.CALLEE_FRAMEWORK == 3


def test_error_code_values():
    """Test key TrpcErrorCode values"""
    assert TrpcErrorCode.TRPC_INVOKE_SUCCESS == 0
    assert TrpcErrorCode.TRPC_SERVER_DECODE_ERR == 1
    assert TrpcErrorCode.TRPC_CLIENT_INVOKE_TIMEOUT_ERR == 101
    assert TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR == 999


def test_trpc_exception_initialization():
    """Test TrpcException initialization"""
    error = TrpcException(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_SERVER_DECODE_ERR, "test error")
    assert error.type == TrpcErrorType.FRAMEWORK
    assert error.code == TrpcErrorCode.TRPC_SERVER_DECODE_ERR
    assert error.msg == "test error"
    assert str(error) == str((error.type, error.code, error.msg))


def test_trpc_exception_msg_conversion():
    """Test TrpcException message type conversion"""
    error = TrpcException(1, 1, b"bytes message")
    assert error.msg == "bytes message"

    error = TrpcException(1, 1, 123)
    assert error.msg == "123"


@pytest.mark.parametrize("exception_class,error_code", [
    (ServerDecodeError, TrpcErrorCode.TRPC_SERVER_DECODE_ERR),
    (ServerEncodeError, TrpcErrorCode.TRPC_SERVER_ENCODE_ERR),
    (ServerFuncNotFoundError, TrpcErrorCode.TRPC_SERVER_NOFUNC_ERR),
    (ServerTimeoutError, TrpcErrorCode.TRPC_SERVER_TIMEOUT_ERR),
    (ServerSystemError, TrpcErrorCode.TRPC_SERVER_SYSTEM_ERR),
])
def test_server_exceptions(exception_class, error_code):
    """Test server-side exception classes"""
    msg = "test message"
    exc = exception_class(msg)

    assert isinstance(exc, TrpcException)
    assert exc.type == TrpcErrorType.FRAMEWORK
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
    (ClientConnectionError, TrpcErrorCode.TRPC_CLIENT_CONNECT_ERR),
    (ClientEncodeError, TrpcErrorCode.TRPC_CLIENT_ENCODE_ERR),
    (ClientDecodeError, TrpcErrorCode.TRPC_CLIENT_DECODE_ERR),
    (ClientRouteError, TrpcErrorCode.TRPC_CLIENT_ROUTER_ERR),
    (ClientInvokeError, TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR),
    (ClientPoolLimitError, TrpcErrorCode.TRPC_CLIENT_VALIDATE_ERR),
    (ClientPoolClosedError, TrpcErrorCode.TRPC_CLIENT_POOL_CLOSED),
    (ClientPoolConnClosedError, TrpcErrorCode.TRPC_CLIENT_POOL_CONN_CLOSED),
])
def test_client_exceptions(exception_class, error_code):
    """Test client-side exception classes"""
    msg = "client error"
    exc = exception_class(msg)

    assert isinstance(exc, TrpcException)
    assert exc.type == TrpcErrorType.FRAMEWORK
    assert exc.code == error_code
    assert exc.msg == msg


@pytest.mark.parametrize("code", [
    TrpcErrorCode.TRPC_SERVER_NOSERVICE_ERR,
    TrpcErrorCode.TRPC_SERVER_AUTH_ERR,
    TrpcErrorCode.TRPC_SERVER_VALIDATE_ERR,
    TrpcErrorCode.TRPC_CLIENT_CANCELED_ERR,
    TrpcErrorCode.TRPC_CLIENT_NETWORK_ERR,
])
def test_frame_error_for_codes_without_dedicated_classes(code):
    """Codes that no longer have their own exception class still build a frame error."""
    exc = create_frame_error(code, "frame error")
    assert isinstance(exc, TrpcException)
    assert exc.type == TrpcErrorType.FRAMEWORK
    assert exc.code == code
    assert exc.msg == "frame error"


def test_plugin_init_error():
    """Test PluginInitError specific features"""
    exc = PluginInitError("plugin failed")
    assert exc.code == TrpcErrorCode.TRPC_PLUGIN_INIT_ERR
    assert isinstance(exc, TrpcException)


def test_unknown_error():
    """Test UnknownError features"""
    exc = UnknownError("unknown issue")
    assert exc.code == TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR
    assert isinstance(exc, TrpcException)


def test_create_frame_error():
    """Test create_frame_error helper function"""
    exc = create_frame_error(TrpcErrorCode.TRPC_SERVER_DECODE_ERR, "frame error")
    assert exc.type == TrpcErrorType.FRAMEWORK
    assert exc.code == TrpcErrorCode.TRPC_SERVER_DECODE_ERR
    assert exc.msg == "frame error"


def test_create_business_error():
    """Test create_business_error helper function"""
    exc = create_business_error(1001, "business error")
    assert exc.type == TrpcErrorType.BUSINESS
    assert exc.code == 1001
    assert exc.msg == "business error"
