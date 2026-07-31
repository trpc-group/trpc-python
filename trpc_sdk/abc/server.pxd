#!python
#cython: language_level=3, boundscheck=False, wraparound=False

from trpc.trpc_abc.context cimport Context

cdef class Method:
    cdef:
        cdef public str name
        cdef public object impl_func
        cdef public object req_cls
        cdef public object rsp_cls


cdef class Streams:
    cdef:
        cdef public str name
        cdef public object impl_func
        cdef public object server_stream


cdef class StreamDesc:
    cdef:
        cdef public str name
        cdef public object impl_func
        cdef public object server_stream

# ServiceDesc 服务描述service定义
cdef class ServiceDesc:
    cdef:
        cdef public str service_name
        cdef public list methods
        cdef public list streams
        cdef public object stream_handler


cdef class BaseService:
    cpdef bytes handle(self, Context ctx, object reqbuf)
    cpdef void register(self, ServiceDesc serviceDesc, bint fix_rpc_name)
    cpdef void run(self)


cdef class BaseServerStream:
    cpdef object context(self)
    cpdef void send_msg(self, object msg)


cdef class BaseTrpcServerEngine:
    pass
