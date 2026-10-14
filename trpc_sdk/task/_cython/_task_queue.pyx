# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

"""Request / response queues of a task child process.

Inside a task child this module owns the two queues between the IPC main loop
and the ``TaskConsumer`` thread: the main loop fills ``REQ_QUEUE`` from
:func:`receive_pkg_from_worker` and drains ``RSP_QUEUE`` from
:func:`consume_batch_rsp`, while the consumer thread does the opposite through
:func:`consume_batch_req`. Bodies are encoded and decoded on the main loop so a
slow codec never stalls the handler thread.

The wire framing itself lives in :mod:`trpc_sdk.task._cython._task_frame`; this module
cimports its head structs and its two ``nogil`` helpers.
"""

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from cpython.ref cimport Py_DECREF
from cpython.ref cimport Py_INCREF

from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import TaskHandleError
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import now_ms
from ._task_frame cimport TASK_REQ_HEAD_LEN
from ._task_frame cimport TASK_RSP_HEAD_LEN
from ._task_frame cimport inner_decode_task_req_c
from ._task_frame cimport inner_encode_task_rsp_c
from .._default_codec import get_default_codec

cdef extern from * nogil:
    """
    #include <cassert>
    #include <mutex>

    class TrpcTaskLockGuard {
    public:
        TrpcTaskLockGuard() : mutex_(nullptr) {}

        ~TrpcTaskLockGuard() {
            if (mutex_ != nullptr) {
                mutex_->unlock();
            }
        }

        void acquire(std::recursive_mutex& mutex) {
            // A guard owns at most one lock for its entire lifetime.
            assert(mutex_ == nullptr);
            mutex.lock();
            mutex_ = &mutex;
        }

    private:
        std::recursive_mutex* mutex_;
        TrpcTaskLockGuard(const TrpcTaskLockGuard&) = delete;
        TrpcTaskLockGuard& operator=(const TrpcTaskLockGuard&) = delete;
    };
    """
    # Stack-allocated RAII guard: Cython cannot stack-allocate std::lock_guard
    # (no nullary constructor), and the destructor unlocks on every return path.
    cdef cppclass TrpcTaskLockGuard:
        TrpcTaskLockGuard()
        void acquire(recursive_mutex&)

# Both queues are crossed by two threads inside the task process: the
# TaskConsumer thread drains REQ_QUEUE and fills RSP_QUEUE, while the main
# loop reading the worker pipes fills REQ_QUEUE and drains RSP_QUEUE. Each
# queue has its own mutex so the two directions never block each other.
cdef recursive_mutex req_mtx
cdef recursive_mutex rsp_mtx
# Requests received from the workers. Bounded: once REQ_QUEUE_CAP (set by
# task_init) is reached further
# requests are answered with TaskStatusCode.QUEUE_FULL instead of queueing,
# which is what keeps a slow handler from growing memory without limit.
cdef queue[TaskRequest*] REQ_QUEUE
cdef uint32_t REQ_QUEUE_CAP = 1000
# Finished responses waiting to be written back to the worker pipes. Unbounded,
# because every entry corresponds to a request already removed from REQ_QUEUE.
cdef queue[TaskResponse*] RSP_QUEUE

cdef void free_task_request(TaskRequest* req):
    """Retire a queued request together with the body it owns.

    Nothing on the Python side holds the body while the request sits in the C
    queue, so the queue owns the reference taken when the frame was decoded
    and must drop it here.

    Args:
        req: Request to release. The pointer is dangling once this returns.
    """
    if req.obj != NULL:
        Py_DECREF(<object>req.obj)
    free(req.head)
    req.head = NULL
    free(req)
    req = NULL


cdef void free_task_response(TaskResponse* rsp):
    """Retire a queued response together with the body it owns.

    Counterpart of :func:`free_task_request` for the response side; the
    reference on the handler return value was taken in
    :func:`new_task_response_from_request`.

    Args:
        rsp: Response to release. The pointer is dangling once this returns.
    """
    if rsp.obj != NULL:
        Py_DECREF(<object>rsp.obj)
    free(rsp.head)
    rsp.head = NULL
    free(rsp)
    rsp = NULL


cdef short is_task_timeout(TaskRequest* req):
    """Report whether a queued request outlived its caller deadline.

    The wait is measured from the worker-side receive time carried in the
    frame head, so the deadline covers the queue wait and not just the
    handler run.

    Args:
        req: Queued request to check. A timeout of ``0`` means the caller set
            none.

    Returns:
        ``1`` when the deadline has passed, ``0`` otherwise.
    """
    cdef double wait_interval = now_ms() - req.head.recv_time
    if 0 < req.head.timeout <= wait_interval:
        return 1
    else:
        return 0


cdef int enque_req(TaskRequest* req) nogil:
    """Hand one request over to the consumer thread.

    Declared nogil, like the other queue primitives, so it stays callable
    from a ``with nogil`` block.

    Args:
        req: Request to queue. Ownership moves to the queue on success only.

    Returns:
        ``0`` on success, or ``-1`` when the queue is at capacity. The caller
        turns ``-1`` into ``TaskStatusCode.QUEUE_FULL`` rather than blocking.
    """
    cdef TrpcTaskLockGuard lck
    lck.acquire(req_mtx)
    if REQ_QUEUE.size() >= REQ_QUEUE_CAP:
        return -1

    REQ_QUEUE.push(req)
    return 0


cdef TaskRequest* deque_batch_req(int batch_size) nogil:
    """Pop up to one batch of requests and chain them together.

    The mutex is held for the whole batch, so the emptiness check and the pop
    cannot be split by another consumer; the hold time is bounded by
    ``batch_size`` pointer moves.

    Args:
        batch_size: Maximum number of requests to pop.

    Returns:
        Head of the list chained through ``TaskRequest.next``, or ``NULL``
        when nothing was available. The tail keeps whatever ``next`` it had,
        so terminating the list is left to the caller.
    """
    cdef TaskRequest* head = NULL
    cdef TaskRequest* curr = NULL
    cdef TaskRequest* req = NULL
    cdef TrpcTaskLockGuard lck
    lck.acquire(req_mtx)

    while batch_size > 0:
        if REQ_QUEUE.empty():
            break

        req = REQ_QUEUE.front()
        REQ_QUEUE.pop()

        if head == NULL:
            head = req
            curr = req
        else:
            curr.next = req
            curr = req

        batch_size -= 1

    return head


cdef int enque_rsp(TaskResponse* rsp) nogil:
    """Queue one finished response for the main loop to write back.

    Unlike :func:`enque_req` there is no capacity check: the response is
    already paid for by a request that left ``REQ_QUEUE``, so dropping it
    here would strand the caller.

    Args:
        rsp: Response to queue. Ownership moves to the queue.

    Returns:
        Always ``0``; the signature matches :func:`enque_req`.
    """
    cdef TrpcTaskLockGuard lck
    lck.acquire(rsp_mtx)
    RSP_QUEUE.push(rsp)
    return 0


cdef TaskResponse* deque_rsp() nogil:
    """Take the oldest response off the response queue.

    Returns:
        The dequeued response, whose ownership moves to the caller, or
        ``NULL`` when the queue is empty.
    """
    cdef TrpcTaskLockGuard lck
    lck.acquire(rsp_mtx)
    if RSP_QUEUE.empty():
        return NULL

    cdef TaskResponse* rsp = RSP_QUEUE.front()
    RSP_QUEUE.pop()
    return rsp


cpdef void task_init(int capability):
    """Size the bounded request queue of this task child.

    Called once before the consumer thread starts. Only the request queue is
    bounded, so this is the single tunable of the pair.

    Args:
        capability: Maximum number of queued requests.
    """
    global REQ_QUEUE_CAP
    REQ_QUEUE_CAP = capability


cdef TaskResponse* new_task_response_from_request(TaskRequest* req, TaskStatusCode code, PyObject* obj):
    """Build the response that answers one request.

    The return route is copied off the request, because the request is freed
    as soon as the response is queued. A reference is taken on ``obj`` for the
    same reason: the handler's result list is dropped while the response still
    sits in ``RSP_QUEUE``.

    Args:
        req: Request being answered; supplies the return route.
        code: Outcome written into the response head.
        obj: Handler return value, or ``NULL`` for a body-less answer.

    Returns:
        A malloc'd response owned by the caller.
    """
    cdef TaskResponse* rsp = <TaskResponse *> malloc(sizeof(TaskResponse))
    rsp.head = <TaskResponseHead *> malloc(sizeof(TaskResponseHead))
    rsp.head.magic = req.head.magic
    rsp.head.package_len = TASK_RSP_HEAD_LEN  # re-init while send_rsp_to_worker
    rsp.head.pkg_id = req.head.pkg_id
    rsp.head.ret_code = code
    rsp.task_conn_index = req.task_conn_index
    rsp.obj = obj
    if obj != NULL:
        Py_INCREF(<object>obj)
    return rsp


cdef void send_rsp_to_queue(TaskRequest* req, TaskStatusCode ret_code, PyObject* obj):
    """Answer one request and retire it.

    This is the only place a request is retired, so both the success and the
    error paths go through it.

    Args:
        req: Request being answered. Consumed; callers must not touch it
            afterwards.
        ret_code: Outcome written into the response head.
        obj: Handler return value, or ``NULL`` for a body-less answer.
    """
    cdef TaskResponse* rsp = new_task_response_from_request(req, ret_code, obj)
    enque_rsp(rsp)

    free_task_request(req)


cdef void send_rsp_to_worker(TaskResponse* rsp):
    """Encode one response and write it back to its worker pipe.

    The body is encoded late, on the main loop rather than in the consumer
    thread, so a slow codec never stalls the handler.

    Args:
        rsp: Response to write. Consumed; callers must not touch it
            afterwards.
    """
    if rsp.obj != NULL:
        data_bytes = get_default_codec().encode(<object>rsp.obj)
        rsp.head.body = data_bytes
        rsp.head.package_len = len(data_bytes) + TASK_RSP_HEAD_LEN

    cdef char* to_ptr = inner_encode_task_rsp_c(rsp.head)
    # Imported here to avoid a cycle: task protocols import receive_pkg_from_worker.
    # Must be the dict defined next to TaskWorkerConnection, which fills it in;
    # two dicts would KeyError here.
    from trpc_sdk.task._connection import TASK_CONN
    TASK_CONN[rsp.task_conn_index].write(bytes(to_ptr[:rsp.head.package_len]))

    free_task_response(rsp)
    free(to_ptr)
    to_ptr = NULL


cpdef void receive_pkg_from_worker(int task_conn_index, object package):
    """Decode one request frame and hand it to the consumer thread.

    Entry point of the task process IPC read path. It runs on the main loop,
    so it must not block; a request that does not fit in the queue is answered
    ``QUEUE_FULL`` immediately and never reaches the handler.

    Args:
        task_conn_index: Index of the worker pipe the frame arrived on; it is
            kept on the request so the reply takes the same route back.
        package: One complete frame, head included.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    cdef char* from_prt = package
    cdef TaskRequestHead* req_head = inner_decode_task_req_c(from_prt)

    if req_head == NULL:
        raise ServerDecodeError("invalid inner magic number in inner_decode_task_req")

    # The decoded body outlives this call inside the queue, so the queue takes
    # its own reference; free_task_request releases it.
    cdef TaskRequest* req = <TaskRequest *> malloc(sizeof(TaskRequest))
    req.head = req_head
    req.task_conn_index = task_conn_index
    if req_head.package_len > TASK_REQ_HEAD_LEN:
        obj = get_default_codec().decode(bytes(from_prt[TASK_REQ_HEAD_LEN:req_head.package_len]))
        req.obj = <PyObject*>obj
        Py_INCREF(obj)
    else:
        req.obj = NULL
    req.next = NULL

    cdef int ret = enque_req(req)
    if ret != 0:
        report_attr("trpc_task_put_queue_full", 1)
        logger.error("[task] failed to put queue due to Full")

        send_rsp_to_queue(req, TaskStatusCode.QUEUE_FULL, NULL)


cdef TaskRequest* _prepare_batch_requests(object app):
    """Dequeue one batch and drop the requests that already expired.

    Shared front half of the sync and async batch consumers. Expired requests
    are answered here and never reach the handler, so the caller only sees
    work still worth doing.

    Args:
        app: The ``TaskConsumer``; supplies ``batch_size``.

    Returns:
        Head of the chained list of live requests, or NULL when the queue was
        empty or every request in the batch had expired. Both cases mean the
        caller should back off; they are distinguished only in the metrics.
    """
    cdef int batch = app.batch_size

    cdef TaskRequest* req = NULL
    with nogil:
        req = deque_batch_req(batch)

    if req == NULL:
        report_attr("trpc_task_queue_empty", 1)
        return NULL

    cdef int total_num = 0
    cdef int timeout_num = 0

    # Split the batch in place. next is read into tmp before the request is
    # retired, because send_rsp_to_queue frees it.
    cdef TaskRequest* normal_req = NULL
    cdef TaskRequest* curr_normal_req = NULL
    cdef TaskRequest* tmp = NULL
    while req != NULL:
        total_num += 1

        tmp = req.next

        if is_task_timeout(req) == 1:
            timeout_num += 1
            send_rsp_to_queue(req, TaskStatusCode.TIMEOUT, NULL)
        else:
            if normal_req == NULL:
                normal_req = req
                curr_normal_req = req
            else:
                curr_normal_req.next = req
                curr_normal_req = req

        req = tmp

    if curr_normal_req != NULL:
        curr_normal_req.next = NULL

    if total_num == timeout_num:
        report_attr("trpc_task_timeout_all", 1)
        logger.error("[task] all tasks are timeout")
        return NULL

    return normal_req

cdef void _process_batch_responses(TaskRequest* normal_req, object normal_rsp_obj, int normal_req_num):
    """Answer every request in the batch and retire it.

    Shared back half of the sync and async batch consumers.
    Responses are matched to requests positionally, so a handler that returns
    something other than a same-length list cannot be paired up; every request
    in the batch is then failed with the corresponding status code. All three
    branches walk the chain exactly once, which is what frees the requests.

    Args:
        normal_req: Head of the chained list returned by
            :func:`_prepare_batch_requests`. Consumed.
        normal_rsp_obj: Whatever the user handler returned.
        normal_req_num: Length of the request chain.
    """
    cdef TaskRequest* normal_req_next = NULL
    cdef int index = 0

    if not isinstance(normal_rsp_obj, list):
        report_attr("trpc_task_handle_rsp_not_list", 1)
        logger.error("err in task handle: rsp should be list type")
        index = 0
        while index < normal_req_num:
            normal_req_next = normal_req.next
            send_rsp_to_queue(normal_req, TaskStatusCode.RSP_WRONG_TYPE, NULL)
            normal_req = normal_req_next
            index += 1
    elif len(normal_rsp_obj) != normal_req_num:
        report_attr("trpc_task_handle_rsp_length_no_equal", 1)
        logger.error("err in task handle: length of rsp and req are not equal")
        index = 0
        while index < normal_req_num:
            normal_req_next = normal_req.next
            send_rsp_to_queue(normal_req, TaskStatusCode.RSP_WRONG_LENGTH, NULL)
            normal_req = normal_req_next
            index += 1
    else:
        logger.debug("finish task handle: length is %s", len(normal_rsp_obj))
        index = 0
        while index < normal_req_num:
            normal_req_next = normal_req.next
            send_rsp_to_queue(normal_req, TaskStatusCode.SUCCESS, <PyObject*>(normal_rsp_obj[index]))
            normal_req = normal_req_next
            index += 1


cpdef int consume_batch_req(object app):
    """Run one batch through a synchronous handler.

    Called on the ``TaskConsumer`` thread.

    Args:
        app: The ``TaskConsumer``; supplies ``func`` and ``batch_size``.

    Returns:
        ``0`` when a batch was handled, ``-1`` when the queue was empty or the
        whole batch had expired. The caller backs off on ``-1``.
    """
    cdef TaskRequest* normal_req = _prepare_batch_requests(app)
    if normal_req == NULL:
        return -1

    normal_req_obj = []
    cdef int normal_req_num = 0
    curr_normal_req = normal_req
    while curr_normal_req != NULL:
        normal_req_obj.append(<object>curr_normal_req.obj)
        normal_req_num += 1
        curr_normal_req = curr_normal_req.next

    # A handler failure is not an error of the batch: it is turned into one
    # TaskHandleError per request so every caller gets an answer instead of
    # waiting out its timeout. The status code stays SUCCESS; the worker
    # recognises the error from the body type.
    cdef int index = 0
    try:
        normal_rsp_obj = app.func(normal_req_obj)
    except Exception as err:  # pylint: disable=broad-except
        report_attr("trpc_task_business_handle_exception", 1)
        logger.error("business logic in task failed: %s", repr(err), exc_info=True)

        normal_rsp_obj = []
        index = 0
        while index < normal_req_num:
            normal_rsp_obj.append(TaskHandleError("business logic in task failed: " + str(err)))
            index += 1

    _process_batch_responses(normal_req, normal_rsp_obj, normal_req_num)

    return 0


async def co_consume_batch_req(object app):
    """Run one batch through a coroutine handler.

    Coroutine variant of :func:`consume_batch_req`, used when the registered
    handler is a coroutine function. Same batching, same failure handling.

    Args:
        app: The ``TaskConsumer``; supplies ``func`` and ``batch_size``.

    Returns:
        ``0`` when a batch was handled, ``-1`` when there was nothing to do.
    """
    cdef TaskRequest* normal_req = _prepare_batch_requests(app)
    if normal_req == NULL:
        return -1

    normal_req_obj = []
    cdef int normal_req_num = 0
    curr_normal_req = normal_req
    while curr_normal_req != NULL:
        normal_req_obj.append(<object>curr_normal_req.obj)
        normal_req_num += 1
        curr_normal_req = curr_normal_req.next

    # Same failure handling as the synchronous variant: one TaskHandleError per
    # request rather than a dropped batch.
    cdef int index = 0
    try:
        normal_rsp_obj = await app.func(normal_req_obj)
    except Exception as err:  # pylint: disable=broad-except
        report_attr("trpc_task_business_handle_exception", 1)
        logger.error("business logic in task failed: %s", repr(err), exc_info=True)

        normal_rsp_obj = []
        index = 0
        while index < normal_req_num:
            normal_rsp_obj.append(TaskHandleError("business logic in task failed: " + str(err)))
            index += 1

    _process_batch_responses(normal_req, normal_rsp_obj, normal_req_num)

    return 0


cpdef int consume_batch_rsp():
    """Encode and write every queued response back to its worker pipe.

    Drains the whole queue in one go, on the task process main loop. Encoding
    happens here rather than in the consumer thread, so the handler thread is
    never blocked by a slow codec.

    Returns:
        Number of responses written; ``0`` means the queue was empty.
    """
    cdef int rsp_num = 0
    cdef TaskResponse* rsp = deque_rsp()
    while rsp != NULL:
        send_rsp_to_worker(rsp)
        rsp_num += 1

        rsp = deque_rsp()
    return rsp_num
