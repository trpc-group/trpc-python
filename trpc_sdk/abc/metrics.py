# -*- coding: utf-8 -*-
"""Metric module"""

from abc import ABCMeta
from abc import abstractmethod
from enum import Enum
from enum import unique

from typing import List, Dict


class ActiveData:
    """Active data"""

    def __init__(self,
                 active_service: str = '',
                 active_interface: str = '',
                 passive_app: str = '',
                 passive_server: str = '',
                 passive_service: str = '',
                 passive_interface: str = '',
                 passive_ip: str = '',
                 passive_container: str = '',
                 passive_consetid: str = '',
                 ret_code: int = 0):
        # 主调信息
        self.active_service = active_service
        self.active_interface = active_interface

        # 被调信息
        self.passive_app = passive_app
        self.passive_server = passive_server
        self.passive_service = passive_service
        self.passive_interface = passive_interface
        self.passive_ip = passive_ip
        self.passive_container = passive_container
        self.passive_consetid = passive_consetid

        # 错误码
        self.ret_code = ret_code


class PassiveData:
    """Passive data"""

    def __init__(self,
                 active_app: str = '',
                 active_server: str = '',
                 active_service: str = '',
                 active_interface: str = '',
                 active_ip: str = '',
                 active_container: str = '',
                 active_consetid: str = '',
                 passive_service: str = '',
                 passive_interface: str = '',
                 ret_code: int = 0):
        # 主调信息
        self.active_app = active_app
        self.active_server = active_server
        self.active_service = active_service
        self.active_interface = active_interface
        self.active_ip = active_ip
        self.active_container = active_container
        self.active_consetid = active_consetid

        # 被调信息
        self.passive_service = passive_service
        self.passive_interface = passive_interface

        # 错误码
        self.ret_code = ret_code


class ReportStatus:
    """Report status"""
    SUCCESS = 0
    EXCEPTION = 1
    TIMEOUT = 2


class CallResult:
    """The result of call"""

    def __init__(self, status: ReportStatus = ReportStatus.SUCCESS, time: int = 0):
        # 状态
        self.status = status
        # 耗时 - ms
        self.time = time


# 统计策略
@unique
class StatPolicy(Enum):
    """stat policy"""
    SUM = 'SUM'
    AVG = 'AVG'
    MAX = 'MAX'
    MIN = 'MIN'
    SET = 'SET'
    HISTOGRAM = 'HISTOGRAM'


class StatValue:
    """Stat value"""

    def __init__(self, value: float, count: int, policy: StatPolicy):
        self.value = value
        self.count = count
        self.policy = policy


class CustomData:
    """custom data"""

    def __init__(self, name: str, dimensions: List[str], stat_values: List[StatValue], tags: Dict[str, str] = None):
        self.name = name
        self.dimensions = dimensions
        self.stat_values = stat_values
        self.tags = tags or {}


class BaseMetrics(metaclass=ABCMeta):
    """Base class of metrics"""

    @abstractmethod
    def report_active(self, active_data: ActiveData, call_result: CallResult) -> bool:
        """
        上报主调数据
        :param call_result: CallResult
        :param active_data: ActiveData
        :return:
        """
        return False

    @abstractmethod
    def report_passive(self, passive_data: PassiveData, call_result: CallResult) -> bool:
        """
        上报被调数据
        :param call_result: CallResult
        :param passive_data: PassiveData
        :return:
        """
        return False

    @abstractmethod
    def report_attr(self, key: str, value: float, policy: StatPolicy) -> bool:
        """
        上报属性数据
        :param policy:
        :param key:
        :param value:
        :return:
        """
        return False

    @abstractmethod
    def report_custom(self, data: CustomData) -> bool:
        """
        上报自定义数据
        :param data:
        :return:
        """
        return False
