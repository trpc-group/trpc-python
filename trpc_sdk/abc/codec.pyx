from abc import ABCMeta
from abc import abstractmethod

from trpc.codec.message import Message

class Codec(metaclass=ABCMeta):
    """
    业务协议打解包基类
    """

    @abstractmethod
    def encode(self, message: Message, body: bytes) -> bytes:
        """
        把body打包成一个完整的二进制网络数据
        :param message:
        :param body: client: req_body / server: rsp_body
        :return: client: req_buffer / server: rsp_buffer
        """
        return bytes()

    @abstractmethod
    def decode(self, message: Message, buffer: bytes) -> bytes:
        """
        从完整的二进制网络数据包解析出body
        :param message:
        :param buffer: client: rsp_buffer / server: req_buffer
        :return: client: rsp_body / server: req_body
        """
        return bytes()


def checker(buf) -> int:
    return 10


class Frame:

    def __init__(self, buf):
        self.content = buf


cdef class Parser:
    def __init__(self):
        self.buf = bytearray()

    cpdef void feeds_data(self, bytes data):
        self.buf.extend(data)

    cpdef bytes read(self):
        pass

    cpdef void reset(self):
        self.buf = bytearray()


class ClientParser:

    async def read_frame(self, conn) -> bytes:
        pass
