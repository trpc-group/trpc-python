# -*- coding: utf-8 -*-
"""Abstract transport module"""
from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from socket import socket

# pylint:disable=import-error
# pylint:disable=no-name-in-module
from .context import Context
from .codec import Parser
from .codec import ClientParser
from .server import BaseService
from .server import BaseTrpcServerEngine

from ..utils import AddrType


class BaseServerTransportOption:
    """Server transport options"""

    def __init__(self,
                 service: BaseService = None,
                 address="",
                 port=8000,
                 checker: Parser = None,
                 handle=None,
                 network="tcp"):
        self.service = service
        self.address = address
        self.socket_type = ""
        self.port = port
        self.checker = checker
        self.handle = handle
        self.network = network
        self.service_name = ""
        self.worker_num = 2
        self.mode = ''
        self.idle_timeout = 0
        self.tls_key = ""
        self.tls_cert = ""
        self.target = ""

    def set_target(self, target: str):
        """set target"""
        self.target = target

    def set_service(self, service: BaseService):
        """Set service"""
        self.service = service

    def set_service_name(self, service_name: str = ""):
        """Set service name"""
        self.service_name = service_name

    def set_network(self, network: str = ""):
        """Set network, such as 'tcp'"""
        self.network = network

    def set_address(self, address_info: AddrType):
        """Set address """
        self.address = address_info[0]
        self.port = int(address_info[1])

    def set_socket_type(self, socket_type: str):
        """Set the type of socket"""
        self.socket_type = socket_type

    def set_checker(self, checker: Parser = None):
        """Set checker for bin data from conn"""
        self.checker = checker

    def set_idle_timeout(self, idle_timeout: float = 1.0):
        """Set idle timeout"""
        self.idle_timeout = idle_timeout

    def set_worker_num(self, worker_num: int = 2):
        """Set the num of worker process"""
        self.worker_num = worker_num

    def set_mode(self, mode=''):
        """Set mode of server executing, eg: Muti-listener"""
        self.mode = mode

    def set_tls_key(self, key_path: str):
        """Set tls key for https"""
        self.tls_key = key_path

    def set_tls_cert(self, cert_path: str):
        """Set cert path for https"""
        self.tls_cert = cert_path


class BaseServerTransport(ABC):
    """The base class of server transport"""

    @abstractmethod
    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine):
        """listen anf serve"""


# client class
class BaseClientTransportOptions:
    """Options for client transport"""

    def __init__(self) -> None:
        # IP:Port. 注意：到了transport层，已经过了名字服务解析，所以直接就是IP:Port
        self.address: AddrType = ('127.0.0.1', 8080)
        self.endpoint = '127.0.0.1:8080'  # 透传client options 的target
        self.socket_type = "net"  # net/unix
        self.network = 'tcp'  # tcp/udp
        self.short_conn = False
        self.checker: ClientParser = None
        self.codec = None
        self.local_addr = None
        self.sock: socket = None

    def set_address(self, address: AddrType):
        """
        设置IP:Port
        :param address:
        :return:
        """
        self.address = address

    def set_endpoint(self, endpoint: str):
        """
        设置endpoint
        :param endpoint:
        :return:
        """
        self.endpoint = endpoint

    def set_socket_type(self, socket_type: str):
        """
        设置socket类型
        :param socket_type:
        :return:
        """
        self.socket_type = socket_type

    def set_network(self, network: str):
        """
        设置网络通信类型
        :param network:
        :return:
        """
        self.network = network

    def set_short_conn(self, short_conn: bool):
        """
        设置短连接
        :param short_conn:
        :return:
        """
        self.short_conn = short_conn

    def set_checker(self, checker: ClientParser):
        """
        设置检包
        :param checker:
        :return:
        """
        self.checker = checker

    def set_codec(self, codec):
        """
        设置请求相关codec
        :param codec:
        :return:
        """
        self.codec = codec

    def set_local_address(self, address_info: AddrType):
        """Set address """
        self.local_addr = address_info

    def set_sock(self, sock: socket):
        """
        设置socket
        :param sock:
        :return:
        """
        self.sock = sock


class BaseClientTransport(metaclass=ABCMeta):
    """
    客户端通讯层基类
    """

    @abstractmethod
    async def round_trip(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        """
        发送请求
        :param ctx:
        :param req_data:
        :param options:
        :return:
        """


class BaseClientStreamTransport(metaclass=ABCMeta):
    """
    客户端通讯层基类
    """

    @abstractmethod
    async def round_trip(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        """
        发送请求
        :param ctx:
        :param req_data:
        :param options:
        :return:
        """

    @abstractmethod
    def send(self, ctx: Context, req_data: bytes, options: BaseClientTransportOptions):
        """
        发送流式消息
        :param ctx:
        :param req_data:
        :param options:
        :return:
        """

    @abstractmethod
    async def recv(self, ctx: Context, options: BaseClientTransportOptions) -> bytes:
        """
        接收流式消息
        :param ctx:
        :param options:
        :return:
        """

    @abstractmethod
    def close(self, ctx: Context):
        """
        关闭流
        :param ctx:
        :return:
        """

    @abstractmethod
    async def start(self, ctx: Context, options: BaseClientTransportOptions):
        """
        初始化一个数据流
        :param ctx:
        :param options:
        :return:
        """
