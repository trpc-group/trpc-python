# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

"""Wire framing of the task IPC protocol.

A worker and its task child exchange a fixed 26-byte request head and a fixed
14-byte response head, each followed by the body the installed task codec
produced. Fields are written in host byte order, since both processes are the
same build on the same machine.

Each direction comes in two layers. The ``*_c`` functions take and return head
structs and are declared ``nogil`` so the queue side can call them from a
``with nogil`` block; the ``cpdef`` wrappers add the codec call and hand back
plain ``bytes``. :mod:`trpc_sdk.task._cython._task_queue` cimports the former,
business-facing code uses the latter.
"""

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport memcpy

from trpc_sdk.exceptions import ServerDecodeError
from .._default_codec import get_default_codec

# magic number
cdef uint16_t INNER_MAGIC_VALUE = 1314


cdef char* inner_encode_task_req_c(TaskRequestHead* req) nogil:
    """Lay out the fixed 26-byte request head followed by the body.

    Fields are written in host byte order: worker and task are the same build
    on the same machine, so no conversion is done.

    Args:
        req: Head to serialize; ``package_len`` covers head plus body.

    Returns:
        A malloc'd buffer of ``req.package_len`` bytes, owned by the caller.
    """
    cdef char* to_ptr = <char *> malloc(req.package_len * sizeof(char))

    (<uint16_t *>(to_ptr))[0] = req.magic
    (<uint32_t *>(to_ptr+2))[0] = req.package_len
    (<uint32_t *>(to_ptr+6))[0] = req.pkg_id
    (<uint64_t *>(to_ptr+10))[0] = req.recv_time
    (<uint64_t *>(to_ptr+18))[0] = req.timeout
    if req.package_len - TASK_REQ_HEAD_LEN > 0:
        memcpy(to_ptr+TASK_REQ_HEAD_LEN, req.body, req.package_len - TASK_REQ_HEAD_LEN)

    return to_ptr

cpdef bytes inner_encode_task_req(uint32_t pkg_id, uint64_t recv_time, uint64_t timeout, object data):
    """Pack one Python-protocol request frame.

    ``req.body`` aliases the buffer of ``data_bytes``, which stays alive for
    the whole call because the local reference is held until the function
    returns.

    Args:
        pkg_id: Per-pipe request id the reply echoes back.
        recv_time: When the worker received the originating RPC.
        timeout: Deadline carried along so the task can drop work that
            expired while queued.
        data: Request body, encoded with the installed task codec.

    Returns:
        The complete frame, head included.
    """
    data_bytes = get_default_codec().encode(data)
    cdef uint32_t data_len = len(data_bytes)

    cdef TaskRequestHead* req = <TaskRequestHead *> malloc(sizeof(TaskRequestHead))
    req.magic = INNER_MAGIC_VALUE
    req.package_len = data_len + TASK_REQ_HEAD_LEN
    req.pkg_id = pkg_id
    req.recv_time = recv_time
    req.timeout = timeout
    req.body = data_bytes

    cdef char* to_ptr = inner_encode_task_req_c(req)
    results = bytes(to_ptr[:req.package_len])

    # Only the C scratch allocations are released; results and data_bytes are
    # Python objects and stay with the garbage collector.
    free(req)
    req = NULL
    free(to_ptr)
    to_ptr = NULL

    return results


cdef TaskRequestHead* inner_decode_task_req_c(char* from_prt) nogil:
    """Read the fixed head out of one Python-protocol request frame.

    ``body`` is deliberately left unset, because it would alias the input
    buffer.

    Args:
        from_prt: Start of the frame.

    Returns:
        A malloc'd head owned by the caller, or ``NULL`` on a magic mismatch.
        Failing this way rather than raising is what keeps the function
        callable without the GIL.
    """
    cdef uint16_t magic = (<uint16_t *>(from_prt))[0]
    if magic != INNER_MAGIC_VALUE:
        return NULL

    cdef TaskRequestHead* req = <TaskRequestHead *> malloc(sizeof(TaskRequestHead))

    req.magic = magic
    req.package_len = (<uint32_t *>(from_prt+2))[0]
    req.pkg_id = (<uint32_t *>(from_prt+6))[0]
    req.recv_time = (<uint64_t *>(from_prt+10))[0]
    req.timeout = (<uint64_t *>(from_prt+18))[0]
    # req.body = from_prt + TASK_REQ_HEAD_LEN

    return req


cpdef object inner_decode_task_req(bytes decode_data):
    """Unpack one Python-protocol request frame.

    Args:
        decode_data: One complete frame, head included.

    Returns:
        ``(pkg_id, recv_time, timeout, obj)``. ``obj`` is ``None`` for a
        head-only frame.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    cdef char* from_prt = decode_data

    cdef TaskRequestHead* req = inner_decode_task_req_c(from_prt)

    if req == NULL:
        raise ServerDecodeError("invalid inner magic number in inner_decode_task_req")

    pkg_id = req.pkg_id
    recv_time = req.recv_time
    timeout = req.timeout
    if req.package_len-TASK_REQ_HEAD_LEN > 0:
        data_bytes = decode_data[TASK_REQ_HEAD_LEN:req.package_len]
        obj = get_default_codec().decode(data_bytes)
    else:
        obj = None

    # Only the C scratch allocation is released; obj is a Python object and
    # stays with the garbage collector.
    free(req)
    req = NULL

    return (pkg_id, recv_time, timeout, obj)


cdef char* inner_encode_task_rsp_c(TaskResponseHead* rsp) nogil:
    """Lay out the fixed 14-byte response head followed by the body.

    Response counterpart of :func:`inner_encode_task_req_c`, with ``ret_code``
    where the request carries its timestamps.

    Args:
        rsp: Head to serialize; ``package_len`` covers head plus body.

    Returns:
        A malloc'd buffer of ``rsp.package_len`` bytes, owned by the caller.
    """
    cdef char* to_ptr = <char *> malloc(rsp.package_len * sizeof(char))

    (<uint16_t *>(to_ptr))[0] = rsp.magic
    (<uint32_t *>(to_ptr+2))[0] = rsp.package_len
    (<uint32_t *>(to_ptr+6))[0] = rsp.pkg_id
    (<uint32_t *>(to_ptr+10))[0] = rsp.ret_code
    if rsp.package_len - TASK_RSP_HEAD_LEN > 0:
        memcpy(to_ptr+TASK_RSP_HEAD_LEN, rsp.body, rsp.package_len - TASK_RSP_HEAD_LEN)

    return to_ptr


cpdef bytes inner_encode_task_rsp(uint32_t pkg_id, uint32_t ret_code, object data):
    """Pack one Python-protocol response frame.

    Same body-aliasing contract as :func:`inner_encode_task_req`.

    Args:
        pkg_id: Request id copied off the request being answered.
        ret_code: One of the ``TaskStatusCode`` values.
        data: Response body, encoded with the installed task codec.

    Returns:
        The complete frame, head included.
    """
    data_bytes = get_default_codec().encode(data)
    cdef uint32_t data_len = len(data_bytes)

    cdef TaskResponseHead* rsp = <TaskResponseHead *> malloc(sizeof(TaskResponseHead))
    rsp.magic = INNER_MAGIC_VALUE
    rsp.package_len = data_len + TASK_RSP_HEAD_LEN
    rsp.pkg_id = pkg_id
    rsp.ret_code = ret_code
    rsp.body = data_bytes

    cdef char* to_ptr = inner_encode_task_rsp_c(rsp)
    results = bytes(to_ptr[:rsp.package_len])

    # Only the C scratch allocations are released; results and data_bytes are
    # Python objects and stay with the garbage collector.
    free(rsp)
    rsp = NULL
    free(to_ptr)
    to_ptr = NULL

    return results


cdef TaskResponseHead* inner_decode_task_rsp_c(char* from_prt) nogil:
    """Read the fixed head out of one Python-protocol response frame.

    Args:
        from_prt: Start of the frame.

    Returns:
        A malloc'd head owned by the caller, or ``NULL`` on a magic mismatch,
        like :func:`inner_decode_task_req_c`.
    """
    cdef uint16_t magic = (<uint16_t *>(from_prt))[0]
    if magic != INNER_MAGIC_VALUE:
        return NULL

    cdef TaskResponseHead* rsp = <TaskResponseHead *> malloc(sizeof(TaskResponseHead))

    rsp.magic = magic
    rsp.package_len = (<uint32_t *>(from_prt+2))[0]
    rsp.pkg_id = (<uint32_t *>(from_prt+6))[0]
    rsp.ret_code = (<uint32_t *>(from_prt+10))[0]

    return rsp


cpdef object inner_decode_task_rsp(bytes decode_data):
    """Unpack one Python-protocol response frame.

    Args:
        decode_data: One complete frame, head included.

    Returns:
        ``(pkg_id, ret_code, obj)``. When the user handler raised, ``obj`` is
        the ``TaskHandleError`` the task encoded as the body, and ``ret_code``
        is still ``SUCCESS``.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    cdef char* from_prt = decode_data

    cdef TaskResponseHead* rsp = inner_decode_task_rsp_c(from_prt)

    if rsp == NULL:
        raise ServerDecodeError("invalid inner magic number in inner_decode_task_rsp")

    pkg_id = rsp.pkg_id
    ret_code = rsp.ret_code
    if rsp.package_len-TASK_RSP_HEAD_LEN > 0:
        data_bytes = decode_data[TASK_RSP_HEAD_LEN:rsp.package_len]
        obj = get_default_codec().decode(data_bytes)
    else:
        obj = None

    # Only the C scratch allocation is released; obj is a Python object and
    # stays with the garbage collector.
    free(rsp)
    rsp = NULL

    return (pkg_id, ret_code, obj)
