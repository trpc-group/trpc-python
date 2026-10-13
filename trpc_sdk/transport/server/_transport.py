# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC server transport that registers listen endpoints on the engine.

This class does not accept connections itself and never has. It records one
service's listen address, checker, handler, timeout and transport options on
:class:`TrpcServerEngine`, index-aligned with every other service, and the
worker binds them later.

There used to be two registration paths here, one per process model. They are
gone: both merge and separate workers read the same nine tables, and the
choice they encoded — which children a master would fork — is no longer this
object's to make. ``server.mode`` selects a worker class instead, in
:mod:`trpc_sdk.server._modes`.
"""

from __future__ import annotations
import asyncio
from functools import partial
from typing import Type
from typing_extensions import override

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import PluginType
from trpc_sdk.log import logger
from trpc_sdk.utils import create_new_socket
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import submit_server_span_on_executor_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.plugin import register_plugin
from trpc_sdk.plugin import get_plugin_class

from ._connection import ConnectionOption
from ._connection import DefaultIoConnection
from ._constants import SERVER_CONN_KEY

@register_plugin(PluginType.SERVER_TRANSPORT, "trpc")
class TrpcServerTransport(BaseServerTransport):
    """Register one service listen address on the server engine.

    The engine, not this object, owns listen sockets. Binding happens later
    on the worker chosen by ``server.mode``.
    """

    @override
    def bind(self, opts: BaseServerTransportOption) -> None:
        """Bind the configured endpoint.
        
        Args:
            opts: Server transport options.
        """
        self.opts = opts
    
    @override
    def listen(self) -> None:
        """Listen for incoming connections.
        
        Args:
            opts: Server transport options.
        """
        sock = create_new_socket(self.opts.address, self.opts.socket_type, reuse_port=True)
        option = ConnectionOption(
            parser=self.opts.parser,
            msg_handler=self._msg_handler,
        )
        handle = partial(DefaultIoConnection, option=option)
        loop = asyncio.get_event_loop()
        ss = loop.create_server(handle, sock=sock, reuse_port=True, backlog=self.opts.backlog)
        loop.run_until_complete(ss)


    async def _msg_handler(self, conn: DefaultIoConnection, data: bytes) -> None:
        """Handle the message from the client.
        
        Args:
            conn: The protocol of the client.
            data: The data from the client.
        """
        context = TrpcContext()
        set_addr_to_ctx_from_transport(context, conn.aio_conn)
        context.timeout = self.opts.idle_timeout
        context.recv_time = now_ms()
        context.rpcz_data.server_post_sched_recv_msg_us = now_us()
        context.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(data))
        context.with_meta_data(SERVER_CONN_KEY, conn)
        try:
           rsp_buff =  await self.opts.rpc_handler(context, data)
           if not conn.aio_conn.is_closing():
            # In multi-listener mode, sched_send and io_send are in the same process.
                send_us = now_us()
                context.rpcz_data.server_pre_sched_send_msg_us = send_us
                context.rpcz_data.server_post_sched_send_msg_us = send_us
                context.rpcz_data.server_pre_io_send_msg_us = send_us
                # 开启 executor stream 模式时，stream 帧的响应由 TrpcServerStreamTransport.send
                # 异步直写回 TCP，这里 ret 为 None；跳过避免 conn.write(None) 的 TypeError
                if self.opts.is_stream and not rsp_buff:
                    return
                context.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(rsp_buff))
                conn.aio_conn.write(rsp_buff)
                context.rpcz_data.server_post_io_send_msg_us = now_us()
                submit_server_span_on_executor_post_io(context)
                logger.debug("[executor] rpcz submit on executor post-io completed")
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_executor_tcp_write_err", 1)
            logger.error(f"[executor] failed to send rsp to tcp client, err: {err}", exc_info=True)


def get_server_transport(plugin_name: str = "trpc") -> Type[BaseServerTransport]:
    """Get the server transport instance.
    
    Args:
        plugin_name: The name of the plugin.
    
    Returns:
        The server transport instance.
    """
    return get_plugin_class(PluginType.SERVER_TRANSPORT, plugin_name)()
