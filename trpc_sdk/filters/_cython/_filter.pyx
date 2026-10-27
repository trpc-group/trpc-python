# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Onion-style filter chain for unary RPC handling."""

import asyncio
import concurrent.futures
from functools import partial
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Coroutine
from typing import List

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerTimeoutError
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.log import logger
from trpc_sdk.plugin import get_plugin
from trpc_sdk.telemetry import report_attr

# Business handler invoked at the center of the filter onion.
FilterHandle = Callable[[TrpcContext, Any], Awaitable[Any]]


cdef class FilterChain:
    """Maintain an onion-ordered list of unary filters.

    Filters wrap the business handler from the outside in. Each filter receives
    the next stage as a callback and may observe or modify the request and
    response before continuing the chain.
    """

    def __cinit__(self, bint is_server=True):
        """Initialize an empty filter chain.

        Args:
            is_server: ``True`` for server-side chains, ``False`` for clients.
        """
        self.filters = []
        self.is_server = is_server

    def add_filter(self, filter_obj: BaseFilter):
        """Append one filter to the chain.

        Args:
            filter_obj: Filter instance to append. Falsy values are ignored.
        """
        if filter_obj:
            self.filters.append(filter_obj)

    def add_filter_list(self, filters: List[BaseFilter]):
        """Append multiple filters to the chain.

        Args:
            filters: Filter instances to append. Falsy values are ignored.
        """
        for filter_obj in filters:
            if filter_obj:
                self.filters.append(filter_obj)

    def set_filter(self, filters: List[BaseFilter]):
        """Replace the entire filter list.

        Args:
            filters: New ordered list of filter instances.
        """
        self.filters = filters

    def remove_duplicates(self):
        """Drop repeated filter instances, keeping each one at its first position."""
        cdef list kept = []
        cdef set seen = set()
        cdef object filter_obj
        for filter_obj in self.filters:
            if id(filter_obj) not in seen:
                seen.add(id(filter_obj))
                kept.append(filter_obj)
        self.filters = kept

    async def _call_handle(
        self,
        coro: Coroutine[[TrpcContext, Any], Any],
        wait_for_time: float
    ):
        """Invoke the business handler and normalize failures.

        Args:
            coro: Coroutine to invoke.
            wait_for_time: Handler timeout in seconds, or ``-1`` for no timeout.

        Returns:
            Tuple of ``(response, error)``. Exactly one side is expected to be
            set by callers that inspect the result.
        """
        error, rsp = None, None
        info = "server" if self.is_server else "client"
        try:
            if wait_for_time == -1:
                rsp = await asyncio.wait_for(coro, None)
            else:
                rsp = await asyncio.wait_for(coro, wait_for_time)
        except TrpcException as err:
            error = err
            logger.warning(f"{info} trpc exception is {err}")
        except asyncio.TimeoutError:
            logger.warning(
                f"{info} catch busi timeout exception wait_for_time: "
                f"{wait_for_time:.3f} s"
            )
            report_attr("trpc_busi_timeout", 1)
            reason = f"{info} handle timeout"
            error = (
                ServerTimeoutError(reason)
                if self.is_server
                else ClientTimeoutError(reason)
            )
        except (concurrent.futures.CancelledError, asyncio.CancelledError) as err:
            logger.warning(
                f"{info} coroutine is cancelled {err}, "
                f"wait_for_time: {wait_for_time}"
            )
            report_attr("trpc_service_cor_cancel", 1)
            reason = f"{info} coroutine is cancelled"
            error = (
                ServerTimeoutError(reason)
                if self.is_server
                else ClientTimeoutError(reason)
            )
        except Exception as err:  # pylint: disable=broad-except
            logger.error(f"{info} catch unknown exception {err}", exc_info=True)
            error = UnknownError(repr(err))
        return rsp, error

    async def handle(
        self,
        ctx: TrpcContext,
        req_obj: Any,
        func: FilterHandle,
        wait_for_time: float = -1,
    ) -> FilterReturnType:
        """Run the onion filter chain around one business handler.

        Args:
            ctx: Request-scoped RPC context.
            req_obj: Application request object.
            func: Innermost business handler.
            wait_for_time: Handler timeout in seconds, or ``-1`` for no timeout.

        Returns:
            Tuple of ``(response, error)`` produced by the filter chain.
        """
        coro = func(ctx, req_obj)
        call_handle = partial(self._call_handle, coro, wait_for_time)
        for filter_obj in reversed(self.filters):
            call_handle = partial(filter_obj.run, ctx, req_obj, call_handle)
        return await call_handle()


def get_server_filter(name: str) -> BaseFilter:
    """Return one registered server filter plugin by name.

    Args:
        name: Registered server filter plugin name.

    Returns:
        Server filter plugin instance.
    """
    return get_plugin(PluginType.SERVER_FILTER_COMPONENT, name)


def get_client_filter(name: str) -> BaseFilter:
    """Return one registered client filter plugin by name.

    Args:
        name: Registered client filter plugin name.

    Returns:
        Client filter plugin instance.
    """
    return get_plugin(PluginType.CLIENT_FILTER_COMPONENT, name)
