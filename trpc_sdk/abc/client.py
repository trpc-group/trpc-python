# -*- coding: utf-8 -*-
"""Abstract client module"""

from abc import ABCMeta, abstractmethod
from typing import Any, List

from trpc.context import Context

# pylint: disable=unnecessary-pass


class BaseClient(metaclass=ABCMeta):  # pylint: disable=too-few-public-methods
    """Client abstract base class"""

    @abstractmethod
    async def invoke(self, ctx: Context, req: Any, rsp_cls: Any, options: List) -> Any:
        """Abstract rpc invoke method
        :param ctx: Context
        :param req: any 
        :param rsp_cls: the class type of rsp
        :param options: list of setting functions.
        :return:
        """
        pass


class BaseClientStream(metaclass=ABCMeta):
    """Client stream abstract base class"""

    @abstractmethod
    async def recv_msg(self, msg: Any):
        """Abstract recv msg method of stream
        :param msg:
        :return:
        """
        pass

    @abstractmethod
    async def send_msg(self, msg: Any):
        """Abstract send msg method of stream
        :param msg:
        :return:
        """
        pass

    @abstractmethod
    async def close_send(self):
        """Abstract method of close sending stream
        :return:
        """
        pass

    @abstractmethod
    def context(self) -> Context:
        """Get context
        :return: Context
        """
        pass


class BaseStream(metaclass=ABCMeta):
    """Stream abstract base class"""

    @abstractmethod
    async def recv(self, ctx: Context) -> bytes:
        """Abstract recv method of stream
        :param ctx: Context
        :return bytes:
        """
        pass

    @abstractmethod
    async def send(self, ctx: Context, req: Any):
        """Abstract send method of stream
        :param ctx: Context
        :param req:
        :return:
        """
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """Abstract close method of stream
        :param ctx: Context
        :return:
        """
        pass

    @abstractmethod
    async def start(self, ctx: Context, options: List) -> Any:
        """Abstract start method of stream
        :param ctx: Context
        :param options:
        :return:
        """
        pass
