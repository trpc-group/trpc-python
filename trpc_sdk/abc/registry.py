# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from typing import NoReturn
from typing import Tuple


class Registry(metaclass=ABCMeta):
    """
    服务注册基类
    """

    @abstractmethod
    def register(self, service: str, address: Tuple[str, int]) -> NoReturn:
        """
        注册服务
        :param address: ip:port
        :param service: 服务名
        :return:
        """
        pass

    @abstractmethod
    def deregister(self, service: str, address: Tuple[str, int]) -> NoReturn:
        """
        反注册服务
        :param address: ip:port
        :param service: 服务名
        :return:
        """
        pass
