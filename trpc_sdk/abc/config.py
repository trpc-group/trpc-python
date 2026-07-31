# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod
from asyncio import AbstractEventLoop
from typing import Callable


class BaseConfig(metaclass=ABCMeta):
    """
    配置基类
    """

    def __init__(self, data_source: str, decoder: str, ds_options: dict, de_options: dict) -> None:
        """
        配置类初始化函数
        :param data_source: 数据源
        :param decoder: 数据编码方式
        :param ds_options: 数据源配置信息
        :param de_options: 数据编码方式配置信息
        :return: 配置字典
        """
        self.data_source_name = data_source
        self.decoder_name = decoder
        self.ds_options = ds_options
        self.de_options = de_options
        self.config = dict()
        self.config_object = None

    @abstractmethod
    def load(self) -> dict:
        """
        获取配置信息
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, default: object) -> object:
        """
        获取特定的配置信息
        :param key: 配置信息存放的key
        :param default: 默认值
        :return: 对象
        """
        raise NotImplementedError

    @abstractmethod
    def watch(self, callback: Callable, interval: int, event_loop: AbstractEventLoop) -> None:
        """
        动态加载使用的方法
        :param callback: 配置变化的时候触发的回调函数
        :param interval: 间隔时间
        :param event_loop: 事件循环
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def unmarshal(self, object_dict: dict) -> None:
        """
        将指定部分的配置解码成对应的对象
        :param object_dict: 以dict保存配置的层级关系，其中最下层的value是需要赋值的对象。
        :return: None
        """
        raise NotImplementedError

    def set_config(self, config_dict: dict) -> None:
        """
        直接设置Config对象的配置信息。
        :param config_dict: 配置字典
        :return: None
        """
        self.config = config_dict


class BaseDataProvider(metaclass=ABCMeta):
    """
    数据源基类
    """

    @abstractmethod
    def load(self, file_path: str, options: dict) -> bytes:
        """
        从配置源导入配置数据
        :param file_path: 配置文件的路径
        :param options: 数据源配置信息
        :return: 配置数据
        """
        raise NotImplementedError

    @abstractmethod
    def watch(self, callback: Callable, interval: int, event_loop: AbstractEventLoop) -> None:
        """
        监控数据源变化
        :param callback: 回调函数
        :param interval: 间隔时间
        :param event_loop: 事件循环
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def set_options(self, options: dict) -> None:
        """
        设置数据源配置参数
        :param options: 配置参数
        :return: None
        """
        raise NotImplementedError


class BaseDecoder(metaclass=ABCMeta):
    """
    解码基类
    """

    @abstractmethod
    def decode(self, config_data: bytes, options: dict) -> dict:
        """Decode"""
        raise NotImplementedError

    @abstractmethod
    def set_options(self, options: dict) -> None:
        """
        设置数据源配置参数
        :param options: 配置参数
        :return: None
        """
        raise NotImplementedError
