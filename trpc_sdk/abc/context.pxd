#!python
#cython: language_level=3, boundscheck=False, wraparound=False

from libc.stdint cimport uint16_t

from trpc.codec.message cimport Message

cdef class Context:
    cdef:
        cdef public long deadline
        cdef public int timeout
        cdef public Message msg
        cdef public object meta_data
        cdef public str remote_ip
        cdef public uint16_t remote_port
        cdef public str local_ip
        cdef public uint16_t local_port
        cdef public double recv_time
        cdef public long server_post_sched_recv_msg_us
        cdef public long server_pre_sched_send_msg_us
        cdef public long server_post_sched_send_msg_us
        cdef public long server_pre_io_send_msg_us
        cdef public long server_post_io_send_msg_us
        cdef public long client_pre_io_send_msg_us
        cdef public long client_post_io_send_msg_us
        cdef public long client_post_sched_send_msg_us
        cdef public long client_post_sched_recv_msg_us

    cpdef void with_meta_data(self, object key, object value)
    cpdef object get_client_message(self)
    cpdef void set_request_attachment(self, bytes attachment)
    cpdef bytes get_request_attachment(self)
    cpdef void set_response_attachment(self, bytes attachment)
    cpdef bytes get_response_attachment(self)
