#!python
#cython: language_level=3, boundscheck=False, wraparound=False

import asyncio
import concurrent
from functools import partial
from typing import Any, Callable, List, Awaitable

from trpc.log import logger
from trpc.metrics import report_attr
from trpc.exceptions import TRPCBaseError
from trpc.exceptions import ClientTimeoutError
from trpc.exceptions import ServerTimeoutError
from trpc.exceptions import UnknownError
from trpc.trpc_abc.context cimport Context
from trpc.plugin.plugin import PluginInitPos
from trpc.plugin.plugin import PluginType
from trpc.plugin.plugin import get_plugin
from trpc.plugin.plugin import register_plugin
from trpc.trpc_abc.filter import Filter
from trpc.trpc_abc.filter import FilterReturnType

# 业务运行逻辑函数类型
_BusiHandle = Callable[[Context, Any], Awaitable[Any]]

cdef class FilterChain:
    """FilterChain维护了server的洋葱式Filer列表"""

    def __cinit__(self, bint is_server = True):
        self.filters = []
        self.is_server = is_server

    def add_filter(self, fltr: Filter):
        if fltr:
            self.filters.append(fltr)

    def add_filter_list(self, filters: List[Filter]):
        for filter in filters:
            if filter:
                self.filters.append(filter)

    def set_filter(self, filters: List[Filter]):
        self.filters = filters

    async def _call_handle(self, Context ctx, req_obj: Any, func: _BusiHandle, float wait_for_time):
        # do business handle
        error, rsp = None, None
        info = "server" if self.is_server else "client"
        try:
            if wait_for_time == -1:
                rsp = await asyncio.wait_for(func(ctx, req_obj), None)
            else:
                rsp = await asyncio.wait_for(func(ctx, req_obj), wait_for_time)
        except TRPCBaseError as err:
            error = err
            logger.warning(f"{info} trpc exception is {err}")
        except asyncio.TimeoutError as err:
            logger.warning(f"{info} catch busi timeout exception wait_for_time: {wait_for_time:.3f} s")
            report_attr("trpc_busi_timeout", 1)
            reason = f"{info} handle timeout"
            error = ServerTimeoutError(reason) if self.is_server else ClientTimeoutError(reason)
        except (concurrent.futures.CancelledError, asyncio.CancelledError) as err:
            logger.warning(f"{info} coroutine is cancelled {err}, wait_for_time: {wait_for_time}")
            report_attr("trpc_service_cor_cancel", 1)
            reason = f"{info} coroutine is cancelled"
            error = ServerTimeoutError(reason) if self.is_server else ClientTimeoutError(reason)
        except Exception as err:  # pylint: disable=broad-except
            logger.error(f"{info} catch unknown exception {err}", exc_info=True)
            error = UnknownError(repr(err))
        return rsp, error

    async def handle(self, Context ctx, req_obj: Any, func: _BusiHandle, float wait_for_time = -1) -> FilterReturnType:
        """
        洋葱过滤器的入口
        :param ctx: 当前请求的上下文
        :param req_obj: 请求体
        :param func: 最内层业务函数
        :param wait_for_time 业务函数超时等待时间
        :return: 业务结果和错误码
        """
        call_handle = partial(self._call_handle, ctx, req_obj, func, wait_for_time)
        for filter in reversed(self.filters):
            call_handle = partial(filter.run, ctx, req_obj, call_handle)
        return await call_handle()


def get_server_filter(name: str) -> Filter:
    return get_plugin(PluginType.SERVER_FILTER_COMPONENT, name)


def get_client_filter(name: str) -> Filter:
    return get_plugin(PluginType.CLIENT_FILTER_COMPONENT, name)
