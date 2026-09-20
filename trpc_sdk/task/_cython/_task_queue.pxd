# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

from libcpp.queue cimport queue
from cpython.ref cimport PyObject

from ._task_frame cimport TaskRequestHead
from ._task_frame cimport TaskResponseHead
from ._task_frame cimport ushort
from ._task_frame cimport uint
from ._task_frame cimport ulong

cdef extern from "<mutex>" namespace "std" nogil:
    cdef cppclass recursive_mutex:
        pass
    cdef cppclass lock_guard[T]:
        lock_guard(recursive_mutex mm)

# One queued request. obj is the decoded body and the queue owns a reference to
# it, taken when the frame was decoded and dropped by free_task_request. next
# chains a dequeued batch together; it is meaningless while queued.
ctypedef struct TaskRequest:
    TaskRequestHead* head
    int task_conn_index
    PyObject* obj
    TaskRequest* next

# One queued response. task_conn_index is copied off the request because the
# request is freed as soon as the response is queued, and the queue holds its
# own reference to obj for the same reason.
ctypedef struct TaskResponse:
    TaskResponseHead* head
    int task_conn_index
    PyObject* obj

# Outcome shipped in TaskResponseHead.ret_code. Mirrored on the Python side by
# trpc_sdk.task._types.TaskStatusCode, which the worker compares against.
ctypedef enum TaskStatusCode:
    SUCCESS,
    TIMEOUT,
    QUEUE_FULL,
    RSP_WRONG_TYPE,
    RSP_WRONG_LENGTH

# Occupancy of the bounded request queue. cap comes from task_init; once num
# reaches it, further requests are answered QUEUE_FULL instead of queueing,
# which is what bounds memory under a slow handler.
ctypedef struct TaskQueueMsg:
    uint cap
    uint num

cpdef void task_init(int capability)
cpdef void receive_pkg_from_worker(int task_conn_index, object package)
cpdef int consume_batch_req(object app)
cpdef int consume_batch_rsp()
