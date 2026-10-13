# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server connection module."""

from __future__ import annotations

import asyncio
import time

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import List
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.log import logger
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.telemetry import report_attr

MsgHandler = Callable[[asyncio.Protocol, List[bytes]], Awaitable[None]]
ConnectionIdleCleanHandler = Callable[[asyncio.Protocol], bool]
ConnectionInitHandler = Callable[[asyncio.Protocol], None]
ConnectionCloseHandler = Callable[[asyncio.Protocol], None]

@dataclass
class ConnectionOption:
    """Base class for server connections."""
    parser: BaseServerProtocolParser
    msg_handler: MsgHandler
    connection_idle_clean_handler: Optional[ConnectionIdleCleanHandler] = field(default=None)
    connection_init_handler: Optional[ConnectionInitHandler] = field(default=None)
    custom_data: Dict[str, Any] = field(default_factory=dict)
    connection_idle_timeout_ms: Optional[float] = field(default=None)
    connection_close_handler: Optional[ConnectionCloseHandler] = field(default=None)

class BaseConnection(asyncio.Protocol):
    """Base class for server connections."""

    def __init__(self, option: ConnectionOption):
        self.option = option
        self.aio_conn:Optional[asyncio.Transport] = None
        self.remote_ip:Optional[str] = None
        self._is_closed = False
        self._paused = False
        self.last_recv_time = 0
        self.check_task = None



class DefaultIoConnection(BaseConnection):
    """IO based connection."""

    def __init__(self, option: ConnectionOption):
        super().__init__(option)
        if not self.option.connection_init_handler:
            self.option.connection_init_handler = self._default_connection_init_handler

    def connection_made(self, transport: asyncio.Transport) -> None:
        """Called when a connection is made."""
        self.aio_conn = transport
        self.option.connection_init_handler(self)

    def data_received(self, data: bytes) -> None:
        """Called when data is received."""
        self.option.msg_handler([data])

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when a connection is lost."""
        self.option.connection_idle_clean_handler()

    async def conn_idle_check(self):
        """Conn idle check"""
        try:
            while True:
                await asyncio.sleep(self.option.connection_idle_timeout_ms / 1000)
                if self.option.connection_idle_clean_handler(self):
                    break
        except asyncio.exceptions.CancelledError:  # pylint: disable=broad-except
            report_attr("conn_idle_check_cancel_err", 1)
            logger.debug("[dispatcher] conn_idle_check CancelledError ")
        finally:
            logger.debug("[dispatcher] conn_idle_check exit")

    def connection_made(self, transport: asyncio.Transport):
        try:
            self.aio_conn = transport
            peer_name = self.aio_conn.get_extra_info("peername")
            self.remote_ip = peer_name[0] if peer_name is not None else None
            self.option.connection_init_handler(self)
            # if idle timeout is set, start a separate coroutine to check for idle connections
            if self.option.connection_idle_timeout_ms > 0:
                self.last_recv_time = time.time()
                self.check_task = asyncio.get_event_loop().create_task(self.conn_idle_check())
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_io_connection_init_err", 1)
            logger.error(f"[io_connection] failed to make connection with client, err: {err} ", exc_info=True)
            self.option.connection_close_handler(self)

    def data_received(self, data: bytes):
        """Handle data received from the client.
        Feed the data to the parser and read the complete data frames.
        param: data: the data received from the client
        return: None
        """
        if self.option.connection_idle_timeout_ms > 0:
            self.last_recv_time = time.time()
        self.option.parser.feed_data(data)
        while True:
            try:
                packages = self.option.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_io_connection_read_err", 1)
                logger.error(f"[io_connection] failed to decode in client conn, remote_ip: \
                             {self.remote_ip}, err: {err}", exc_info=True)
                self.option.connection_close_handler(self)
                return
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_io_connection_read_fatal_err", 1)
                logger.error(f"[io_connection] fatal err while decoding in client conn, remote_ip: \
                             {self.remote_ip}, err: {err}", exc_info=True)
                self.option.connection_close_handler(self)
                return

            try:
                # if no complete package is received, exit the callback
                if not packages:
                    return
                self.option.msg_handler(self, packages)
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_io_connection_write_err", 1)
                logger.error(f"[io_connection] failed to send data to worker, remote_ip: {self.remote_ip}, err: {err}", exc_info=True)
                self.option.connection_close_handler(self)

    def connection_lost(self, exc):
        """Connection lost """
        if exc:
            logger.warning(f"[io_connection] connection_lost, remote_ip: {self.remote_ip}, err: {exc}")
        if self.check_task:
            self.check_task.cancel()
        self.option.connection_close_handler(self)


    def pause_writing(self):
        if self._paused:
            return
        self._paused = True

    def resume_writing(self):
        if not self._paused:
            return
        self._paused = False

    def _default_connection_idle_clean_handler(self) -> bool:
        if self.aio_conn.is_closing():
            logger.debug(f"[io_connection] conn is closing, remote_ip: {self.remote_ip}")
            return True
        if (time.time() - self.last_recv_time) * 1000 > self.option.connection_idle_timeout_ms:
            report_attr("conn_idle_check_timeout_err", 1)
            logger.warning(f"[io_connection] conn idle timeout, remote_ip: {self.remote_ip}, \
                         idle_timeout: {self.option.connection_idle_timeout_ms}")
            self.option.connection_close_handler(self)
            return True
        return False
    
    def _default_connection_init_handler(self):
        logger.debug(f"[io_connection] connection init, remote_ip: {self.remote_ip}")
        self._is_closed = False
        self._paused = False
        if not self.option.connection_close_handler:
            self.option.connection_close_handler = self._default_connection_close_handler
        if not self.option.connection_idle_clean_handler:
            self.option.connection_idle_clean_handler = self._default_connection_idle_clean_handler
    
    def _default_connection_close_handler(self):
        logger.debug(f"[io_connection] connection close, remote_ip: {self.remote_ip}")
        self._is_closed = True
        if self.check_task:
            self.check_task.cancel()
        self.aio_conn.close()
        self.aio_conn = None
        self.remote_ip = None
