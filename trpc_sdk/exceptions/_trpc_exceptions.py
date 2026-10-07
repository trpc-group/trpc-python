# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC exception types and error codes."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING
from typing import Any
from typing import Union

from .._inner import trpc_pb2


class TrpcErrorType(IntEnum):
    """Classify the origin of a tRPC error."""

    FRAMEWORK = 1
    BUSINESS = 2
    CALLEE_FRAMEWORK = 3


def _build_exception_code() -> type[IntEnum]:
    """Build ``TrpcErrorCode`` from ``TrpcRetCode`` plus Python extensions."""
    members = {
        name: int(value) for name, value in trpc_pb2.TrpcRetCode.items()
    }

    # Python-only extensions. Values stay above the protocol reserved range so
    # they never collide with newly added TrpcRetCode members.
    members.update(
        {
            "TRPC_CLIENT_POOL_CLOSED": 152,
            "TRPC_CLIENT_POOL_CONN_CLOSED": 153,
            "TRPC_TASK_HANDLE_ERR": 200,
            "TRPC_PLUGIN_INIT_ERR": 202,
            "TRPC_CONFIG_ERR": 1001,
            "TRPC_CONFIG_KEY_ERR": 1002,
            "TRPC_CONFIG_DECODE_ERR": 1003,
            "TRPC_CONFIG_DATASOURCE_ERR": 1004,
            "TRPC_CONFIG_DATASOURCE_NOT_EXIST_ERR": 1005,
        }
    )

    exception_code = IntEnum("TrpcErrorCode", members)
    exception_code.__doc__ = (
        "Protocol and Python-framework error codes.\n\n"
        "Protocol members are imported from ``trpc.TrpcRetCode`` at runtime, so "
        "new protobuf retcodes become available without code changes. "
        "Python-only extensions use values starting at 1001. Legacy name aliases "
        "are intentionally omitted here and should be added by dependent packages."
    )
    return exception_code


if TYPE_CHECKING:
    # Functional ``IntEnum(...)`` assigns a runtime variable, which type checkers
    # reject in annotations. Provide a static IntEnum type for analysis only.
    class TrpcErrorCode(IntEnum):
        """Protocol and Python-framework error codes."""

else:
    TrpcErrorCode = _build_exception_code()


class TrpcException(Exception):
    """Base exception for all tRPC framework errors.

    Attributes:
        type: Error type classification from ``TrpcErrorType``.
        code: Specific error code from ``TrpcErrorCode``.
        msg: Human-readable error message.
    """

    def __init__(
        self,
        exception_type: TrpcErrorType,
        code: Union[TrpcErrorCode, int],
        msg: Any,
    ):
        """Initialize the tRPC base exception.

        Args:
            exception_type: Error type from ``TrpcErrorType``.
            code: Error code from ``TrpcErrorCode`` or a raw integer code.
            msg: Error message; non-string values are converted to ``str``.
        """
        self.type = exception_type
        self.code = code
        self.msg = msg
        if isinstance(msg, bytes):
            self.msg = msg.decode("utf-8")
        elif not isinstance(msg, str):
            self.msg = str(msg)


class UnknownError(TrpcException):
    """Exception raised for unclassified/unrecognized errors"""

    def __init__(self, msg: str):
        """
        Initialize the unknown error
        
        Args:
            msg: Description of the unknown error condition
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR, msg)


def create_frame_error(code: Union[TrpcErrorCode, int], msg: str) -> TrpcException:
    """Create a framework-scoped error.

    Args:
        code: Error code value.
        msg: Human-readable error message.

    Returns:
        Constructed ``TrpcException`` instance.
    """
    return TrpcException(TrpcErrorType.FRAMEWORK, code, msg)


def create_business_error(code: Union[TrpcErrorCode, int], msg: str) -> TrpcException:
    """Create a business-scoped error.

    Args:
        code: Error code value.
        msg: Human-readable error message.

    Returns:
        Constructed ``TrpcException`` instance.
    """
    return TrpcException(TrpcErrorType.BUSINESS, code, msg)
