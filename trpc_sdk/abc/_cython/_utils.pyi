# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from typing import Tuple


def get_app_server_service(service_name: str) -> Tuple[str, str, str]: ...

def get_method_from_rpc_func(rpc_name: str) -> str: ...
