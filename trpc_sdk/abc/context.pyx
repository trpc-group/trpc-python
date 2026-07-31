#!python
#cython: language_level=3, boundscheck=False, wraparound=False

cdef class Context:
    def __init__(self):
        self.deadline = 0
        self.timeout = 0
        self.msg = None
        self.meta_data = {}
        self.remote_ip = ""
        self.remote_port = 0
        self.recv_time = 0.0
        self.local_ip = ""
        self.local_port = 0
        self.server_post_sched_recv_msg_us = 0
        self.server_pre_sched_send_msg_us = 0
        self.server_post_sched_send_msg_us = 0
        self.server_pre_io_send_msg_us = 0
        self.server_post_io_send_msg_us = 0
        self.client_pre_io_send_msg_us = 0
        self.client_post_io_send_msg_us = 0
        self.client_post_sched_send_msg_us = 0
        self.client_post_sched_recv_msg_us = 0

    cpdef void with_meta_data(self, object key, object value):
        self.meta_data[key] = value

    def get_meta_data(self, key, default = None):
        return self.meta_data.get(key, default)

    def pop_meta_data(self, key, default = None):
        return self.meta_data.pop(key, default)

    cpdef object get_client_message(self):
        return self.msg

    cpdef void set_request_attachment(self, bytes attachment):
        if self.msg is None:
            raise ValueError("msg is not set, cannot set request attachment")
        self.msg.set_request_attachment(attachment)

    cpdef bytes get_request_attachment(self):
        if self.msg is None:
            raise ValueError("msg is not set, cannot get request attachment")
        return self.msg.request_attachment

    cpdef void set_response_attachment(self, bytes attachment):
        if self.msg is None:
            raise ValueError("msg is not set, cannot set response attachment")
        self.msg.set_response_attachment(attachment)

    cpdef bytes get_response_attachment(self):
        if self.msg is None:
            raise ValueError("msg is not set, cannot get response attachment")
        return self.msg.response_attachment

    def __copy__(self):
        ctx = Context()
        ctx.deadline = self.deadline
        ctx.timeout = self.timeout
        ctx.recv_time = self.recv_time
        ctx.remote_ip = self.remote_ip
        ctx.remote_port = self.remote_port
        ctx.local_ip = self.local_ip
        ctx.local_port = self.local_port
        ctx.server_post_sched_recv_msg_us = self.server_post_sched_recv_msg_us
        ctx.server_pre_sched_send_msg_us = self.server_pre_sched_send_msg_us
        ctx.server_post_sched_send_msg_us = self.server_post_sched_send_msg_us
        ctx.server_pre_io_send_msg_us = self.server_pre_io_send_msg_us
        ctx.server_post_io_send_msg_us = self.server_post_io_send_msg_us
        ctx.client_pre_io_send_msg_us = self.client_pre_io_send_msg_us
        ctx.client_post_io_send_msg_us = self.client_post_io_send_msg_us
        ctx.client_post_sched_send_msg_us = self.client_post_sched_send_msg_us
        ctx.client_post_sched_recv_msg_us = self.client_post_sched_recv_msg_us
        for key, value in self.meta_data.items():
            ctx.meta_data[key] = value

        return ctx
