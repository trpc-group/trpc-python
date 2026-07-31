#!python
#cython: language_level=3, boundscheck=False, wraparound=False

from trpc.trpc_abc.context cimport Context

cdef class Method:
    def __cinit__(self, str name, object req_cls, object rsp_cls, object impl_func):
        self.name = name
        self.impl_func = impl_func
        self.req_cls = req_cls
        self.rsp_cls = rsp_cls


cdef class StreamDesc:
    def __cinit__(self, str name, object impl_func, object server_stream):
        self.name = name
        self.impl_func = impl_func
        self.server_stream = server_stream


# ServiceDesc 服务描述service定义
cdef class ServiceDesc:
    def __cinit__(self, str service_name, list methods, list streams = None, object stream_handler = None):
        self.service_name = service_name
        self.methods = methods
        self.stream_handler = stream_handler
        self.streams = streams


cdef class BaseService:
    cpdef bytes handle(self, Context ctx, object reqbuf):
        pass

    cpdef void register(self, ServiceDesc serviceDesc, bint fix_rpc_name):
        pass

    cpdef void run(self):
        pass


cdef class BaseTrpcServerEngine:
    pass


cdef class BaseServerStream:
    cpdef object context(self):
        """
        context abstract method
        :return Context:
        """
        pass

    cpdef void send_msg(self, object msg):
        """
        send_msg abstract method
        :param msg:
        :return:
        """
        pass

    async def recv_msg(self, object msg):
        """
        recv_msg abstract method
        :param msg:
        :return:
        """
        pass