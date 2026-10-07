# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type declarations for the unary RPC filter chain."""

from typing import Any
from typing import Awaitable
from typing import Callable
from typing import List

from trpc_sdk.abc import TrpcContext  # pyright: ignore[reportMissingModuleSource]
from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterReturnType

FilterHandle = Callable[[TrpcContext, Any], Awaitable[Any]]


class FilterChain:
    """Maintain an onion-ordered list of unary filters."""

    def __init__(self, is_server: bool = True) -> None: ...

    def add_filter(self, filter_obj: BaseFilter) -> None: ...

    def add_filter_list(self, filters: List[BaseFilter]) -> None: ...

    def set_filter(self, filters: List[BaseFilter]) -> None: ...

    async def handle(
        self,
        ctx: TrpcContext,
        req_obj: Any,
        func: FilterHandle,
        wait_for_time: float = -1,
    ) -> FilterReturnType: ...


def get_server_filter(name: str) -> BaseFilter: ...


def get_client_filter(name: str) -> BaseFilter: ...
