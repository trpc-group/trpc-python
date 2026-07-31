# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from typing import NoReturn

from trpc.trpc_abc.node import Node


class BaseCircuitBreaker(metaclass=ABCMeta):
    """
    熔断服务基类
    """

    @abstractmethod
    def available(self, node: Node) -> bool:
        """
        判断节点是否可用
        :param node:服务节点
        :return:节点可用/节点不可用
        """
        pass

    @abstractmethod
    def report(self, node: Node, cost: int, success: int) -> NoReturn:
        """
        上报当前节点的状态
        :param node:服务节点
        :param cost:耗时
        :param success:调用结果
        :return:
        """
        pass
