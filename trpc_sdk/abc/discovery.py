# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com

from abc import ABCMeta
from abc import abstractmethod
from typing import List, Any, Optional

from trpc.trpc_abc.node import Node


class BaseDiscovery(metaclass=ABCMeta):
    """
    服务发现基类
    """

    @abstractmethod
    def list(self, service_name: str, namespace: str) -> List[Node]:
        """
        获取服务节点列表
        :param namespace: 命名空间
        :param service_name:服务名
        :return list[Node]:服务节点列表
        """
        pass

    async def select_node(self, service_name: str, opt: Any) -> Optional[Node]:
        """
        选择服务节点
        :param service_name:服务名
        :param opt:选项
        :return Node:服务节点
        """
        return None
