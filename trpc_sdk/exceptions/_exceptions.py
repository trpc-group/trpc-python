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





class TaskHandleError(TrpcException):
    """Exception raised when task processing encounters an error"""

    def __init__(self, msg: str):
        """
        Initialize the task handling error
        
        Args:
            msg: Description of the task processing failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_TASK_HANDLE_ERR, msg)











