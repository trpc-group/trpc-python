# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Filter server module"""

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin

print("=========== register server filter ================== \n")


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "one")
class OneFilter(BaseFilter):
    """First server-side filter in the filter chain"""

    async def run(self, ctx: TrpcContext, req, handle) -> FilterReturnType:
        """
        Execute the filter logic
        
        Args:
            ctx: Request context
            req: Request object
            handle: Next handler in the chain
            
        Returns:
            Response from the next handler
        """
        message = ctx.get_client_message()

        logger.info(f"one server filter {message.callee_service_name}")
        return await handle()

    async def run_stream(self, ctx: TrpcContext, req, handle):
        """Streaming RPCs also go through ``run``; async-generator handlers are not used here."""
        raise NotImplementedError("OneFilter does not wrap async-generator handlers")


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "two")
class TwoFilter(BaseFilter):
    """Second server-side filter in the filter chain"""

    async def run(self, ctx: TrpcContext, req, handle) -> FilterReturnType:
        """
        Execute the filter logic
        
        Args:
            ctx: Request context
            req: Request object
            handle: Next handler in the chain
            
        Returns:
            Response from the next handler
        """
        logger.info("two server filter")
        return await handle()

    async def run_stream(self, ctx: TrpcContext, req, handle):
        """Streaming RPCs also go through ``run``; async-generator handlers are not used here."""
        raise NotImplementedError("TwoFilter does not wrap async-generator handlers")


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "default")
class DefaultServerFilter(BaseFilter):
    """Default server-side filter that logs request deadline information"""

    async def run(self, ctx: TrpcContext, req, handle) -> FilterReturnType:
        """
        Execute the default server filter logic with deadline logging
        
        Args:
            ctx: Request context containing deadline information
            req: Incoming request object
            handle: Next handler in the filter chain
            
        Returns:
            Response from the next handler in the chain
        """
        message = ctx.get_client_message()
        logger.info(
            f"default server filter; callee_service_name: {message.callee_service_name} ctx deadline: {ctx.deadline}")
        return await handle()

    async def run_stream(self, ctx: TrpcContext, req, handle):
        """Streaming RPCs also go through ``run``; async-generator handlers are not used here."""
        raise NotImplementedError("DefaultServerFilter does not wrap async-generator handlers")
