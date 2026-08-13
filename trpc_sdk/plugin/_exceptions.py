# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""TRPC Framework Exception Definitions
This file contains all exception classes and error codes used in the TRPC framework
"""

from __future__ import annotations

from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException


class PluginInitError(TrpcException):
    """Exception raised when plugin initialization fails
    
    This is a critical error that will cause the service to exit
    """

    def __init__(self, msg: str):
        """
        Initialize the plugin initialization error
        
        Args:
            msg: Description of the initialization failure
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_PLUGIN_INIT_ERR, msg)
