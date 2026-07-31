# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from typing import List

from trpc.trpc_abc.node import Node


class BaseLoadBalance(metaclass=ABCMeta):
    """
    负载均衡服务基类
    """

    @abstractmethod
    def select(self, service_name: str, node_list: List[Node], namespace: str) -> Node:
        """
        选择负载均衡节点
        :param namespace: 命令空间
        :param service_name:服务名
        :param node_list:节点列表
        :return:服务节点
        """
        pass
