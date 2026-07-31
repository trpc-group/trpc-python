from abc import ABCMeta, abstractmethod
from typing import NoReturn

from trpc.trpc_abc.node import Node
from ..naming.option import SelectorOption


class BaseSelector(metaclass=ABCMeta):
    """
    路由选择器
    """

    @abstractmethod
    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        """
        选择服务节点
        :param service_name:服务名
        :param opt:
        :return:
        """
        pass

    @abstractmethod
    async def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """
        上报服务状态
        :param node:服务节点
        :param cost: 耗时
        :param success:调用结果
        :return:
        """
        pass
