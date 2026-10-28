# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Filter client module"""

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin

print("=========== register client filter ================== \n")


@register_plugin(PluginType.CLIENT_FILTER_COMPONENT, "default")
class DefaultClientFilter(BaseFilter):
    """Default client-side filter with deadline logging"""

    async def run(self, ctx: TrpcContext, req, handle) -> FilterReturnType:
        """
        Execute the client filter logic with deadline logging
        
        Args:
            ctx: Request context (logs deadline)
            req: Request object
            handle: Next handler in the chain
            
        Returns:
            Response from the next handler
        """
        logger.info(f"default client filter; ctx deadline: {ctx.deadline}")
        return await handle()

    async def run_stream(self, ctx: TrpcContext, req, handle):
        """Streaming RPCs also go through ``run``; async-generator handlers are not used here."""
        raise NotImplementedError("DefaultClientFilter does not wrap async-generator handlers")
