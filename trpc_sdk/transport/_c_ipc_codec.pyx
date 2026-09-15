# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

"""Production Cython inner-IPC codec for dispatcher / worker / task pipes.

Python dispatcher, worker, and task paths encode and decode req/rsp
frames here. :mod:`trpc_sdk.transport._ipc_codec` is the readable
``magic + length`` twin used by tests; it does not cover worker
broadcasts or the ``DataHeader`` path.

Stream-IPC frames are hand-packed (``+10`` / ``+28`` / ``+18``).
Datagram-IPC frames use ``DataHeader`` from ``driver.h``.
"""

from libc.string cimport memcpy
from libc.string cimport memmove
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from cpython.mem cimport PyMem_Realloc
from cython.operator cimport dereference

from trpc_sdk.exceptions import ServerDecodeError
from ._constants import DEFAULT_MEM_SIZE as _PY_DEFAULT_MEM_SIZE
from ._constants import FRAME_HEAD_LEN as _PY_FRAME_HEAD_LEN
from ._constants import INNER_MAGIC_VALUE as _PY_INNER_MAGIC_VALUE

cdef ushort _INNER_MAGIC_VALUE = _PY_INNER_MAGIC_VALUE
cdef uint _FRAME_HEAD_LEN = _PY_FRAME_HEAD_LEN
cdef int _DEFAULT_MEM_SIZE = _PY_DEFAULT_MEM_SIZE

# sizeof(DataHeader) is a C layout fact; it cannot live in _constants.py.
cdef uint _C_UDP_PACKAGE_HEADER_LEN = sizeof(DataHeader)
_UDP_PACKAGE_HEADER_LEN = _C_UDP_PACKAGE_HEADER_LEN
# GLOBAL_MAX_IPC_SIZE comes from driver.h via the pxd.
_GLOBAL_MAX_IPC_SIZE = GLOBAL_MAX_IPC_SIZE


cdef class InnerStreamParser:
    """Split a byte stream into complete inner-IPC frames.

    A frame is ``ushort`` magic plus ``uint32`` total length followed by
    the payload. ``read`` returns every complete package currently
    buffered; an incomplete tail stays in ``buf`` across ``feeds_data``.
    """

    def __cinit__(self):
        self.size = _DEFAULT_MEM_SIZE
        self.write_len = 0
        self.buf = <char *> PyMem_Malloc(self.size * sizeof(char))
        if not self.buf:
            raise MemoryError(f"resize failed. size: {self.size}")

    cpdef void reset(self):
        """Drop buffered bytes without freeing the allocation."""
        self.write_len = 0

    cpdef void feeds_data(self, bytes data):
        """Append ``data`` to the growable parse buffer.

        Args:
            data: Incremental stream bytes from the IPC pipe.
        """
        cdef int append_length = len(data)

        cdef int left_size = self.size - self.write_len
        if left_size < append_length:
            self._resize((self.write_len + append_length) * 2)

        cdef char* start_ptr = self.buf + self.write_len
        cdef const char* from_ptr = data
        memcpy(start_ptr, from_ptr, append_length)
        self.write_len += append_length

    cpdef list read(self):
        """Return every complete frame and compact leftover bytes.

        Returns:
            Complete packages including the inner header. Empty when the
            buffer has no full frame yet.

        Raises:
            ServerDecodeError: The leading magic is not ``INNER_MAGIC_VALUE``.
        """
        results = []

        cdef int scan_end = 0
        cdef ushort stx = 0
        cdef uint package_len = 0
        cdef int resize = 0

        while self.write_len >= scan_end + _FRAME_HEAD_LEN:

            stx = dereference(<ushort *>(self.buf + scan_end))
            if stx != _INNER_MAGIC_VALUE:
                raise ServerDecodeError("invalid inner magic number in InnerStreamParser")

            package_len = dereference(<uint *>(self.buf + scan_end + 2))

            if self.write_len < scan_end + package_len:
                break

            results.append(bytes(self.buf[scan_end:scan_end + package_len]))

            scan_end = scan_end + package_len

        if scan_end > 0:
            # Source and dest overlap; memcpy is undefined and has segfaulted.
            self.write_len = self.write_len - scan_end
            if self.write_len > 0:
                memmove(self.buf, self.buf + scan_end, self.write_len)
            resize = self.write_len if self.write_len > _DEFAULT_MEM_SIZE else _DEFAULT_MEM_SIZE
            self._resize(resize)

        return results

    cdef void _resize(self, size_t new_number) except *:
        mem = <char*> PyMem_Realloc(self.buf, new_number * sizeof(char))
        if not mem:
            raise MemoryError(f"resize failed. new_number: {new_number}")
        self.size = new_number
        self.buf = mem

    def __dealloc__(self):
        PyMem_Free(self.buf)


cdef class InnerDatagramParser:
    """Read ``DataHeader.flags`` from one C-dispatcher UDP datagram.

    One datagram is one frame; this checker does not reassemble chunks.
    """

    def __cinit__(self):
        self.size = 1000
        self.write_len = 0

    cpdef void reset(self):
        """Reset parser bookkeeping. The datagram path has no buffer."""
        self.write_len = 0

    cpdef int feeds_data(self, bytes data):
        """Return the UDP package flag from the datagram header.

        Does not buffer, does not check magic; the caller already has one
        complete datagram.

        Args:
            data: One complete UDP IPC datagram.

        Returns:
            A :class:`~trpc_sdk.transport._types.UDPPackageType` value.
        """
        cdef int append_length = len(data)
        cdef char* from_ptr = data
        cdef DataHeader *header = <DataHeader *>(from_ptr)
        return header.flags


cpdef bytes encode_worker_msg_req(uint from_worker, bytes data):
    """Encode one inter-worker broadcast frame.

    Layout after the payload is prefixed with ``+10`` bytes:
    magic (2) + total length (4) + ``from_worker`` (4).

    Args:
        from_worker: Sender worker id written after the inner header.
        data: Payload bytes, not including a header.
    """
    cdef uint data_len = len(data)
    cdef uint package_len = data_len + 10

    cdef char* to_ptr = <char *> PyMem_Malloc(package_len)

    (<ushort *>(to_ptr))[0] = _INNER_MAGIC_VALUE
    (<uint *>(to_ptr + 2))[0] = package_len
    (<uint *>(to_ptr + 6))[0] = from_worker

    cdef char* data_ptr = data
    memcpy(to_ptr + 10, data_ptr, data_len)

    results = bytes(to_ptr[:package_len])
    PyMem_Free(to_ptr)
    return results


cpdef decode_worker_msg_req(bytes data):
    """Decode one inter-worker broadcast frame.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(from_worker, payload)``.
    """
    cdef char* from_ptr = data

    cdef magic = dereference(<ushort *>(from_ptr))
    if magic != _INNER_MAGIC_VALUE:
        raise ServerDecodeError("invalid inner magic number in decode_worker_msg_req")

    cdef data_len = dereference(<uint *>(from_ptr + 2))
    cdef from_id = dereference(<uint *>(from_ptr + 6))

    return from_id, bytes(from_ptr[10:data_len])


cpdef bytes inner_encode_req(uint fd, uint port, uint session_id, ulong recv_time, bytes remote_ip, bytes data):
    """Encode one stream-IPC request from the Python dispatcher.

    The ``+28`` prefix is the historical hand-packed layout (magic, len,
    fd, port, session, recv_time, ip_len), not ``sizeof(DataHeader)``.

    Args:
        fd: Client connection fd forwarded to the worker.
        port: Listen port that accepted the client.
        session_id: Connection-lifetime id of the client TCP.
        recv_time: Receive timestamp as an integer microsecond count.
        remote_ip: Packed peer address bytes.
        data: Request body.
    """
    cdef uint ip_len = len(remote_ip)
    cdef uint data_len = len(data)
    cdef uint package_len = data_len + 28 + ip_len

    cdef char* to_ptr = <char *> PyMem_Malloc(package_len)

    (<ushort *>(to_ptr))[0] = _INNER_MAGIC_VALUE
    (<uint *>(to_ptr + 2))[0] = package_len
    (<uint *>(to_ptr + 6))[0] = fd
    (<uint *>(to_ptr + 10))[0] = port
    (<uint *>(to_ptr + 14))[0] = session_id
    (<ulong *>(to_ptr + 18))[0] = recv_time
    (<short *>(to_ptr + 26))[0] = ip_len

    cdef char* remote_ip_ptr = remote_ip
    memcpy(to_ptr + 28, remote_ip_ptr, ip_len)

    cdef char* data_ptr = data
    memcpy(to_ptr + 28 + ip_len, data_ptr, data_len)

    results = bytes(to_ptr[:package_len])
    PyMem_Free(to_ptr)
    return results


cpdef bytes inner_encode_data_header_req(uint fd, uint port, uint session_id, ulong recv_time, bytes remote_ip, bytes data):
    """Encode one datagram-IPC request using ``DataHeader``.

    Args:
        fd: Client connection fd forwarded to the worker.
        port: Unused in the packed header; kept for call-site symmetry.
        session_id: Connection-lifetime id of the client TCP.
        recv_time: Receive timestamp written into ``DataHeader.recv_time``.
        remote_ip: Unused in the packed header; kept for call-site symmetry.
        data: Request body placed after the ``DataHeader``.
    """
    # port and remote_ip are intentionally not packed; C dispatcher
    # recovers the peer from its connection table.
    cdef uint ip_len = len(remote_ip)
    cdef uint data_len = len(data)
    cdef uint package_len = data_len + _C_UDP_PACKAGE_HEADER_LEN

    cdef DataHeader* to_ptr = <DataHeader*> PyMem_Malloc(package_len)

    to_ptr.magic_num = _INNER_MAGIC_VALUE
    to_ptr.len = package_len
    to_ptr.fd = fd
    to_ptr.session_id = session_id
    to_ptr.recv_time = recv_time
    to_ptr.type = 1

    cdef char* data_ptr = data
    memcpy(<char*>to_ptr + _C_UDP_PACKAGE_HEADER_LEN, data_ptr, data_len)

    results = bytes((<char *>to_ptr)[:package_len])
    PyMem_Free(to_ptr)
    return results


cpdef inner_decode_req(bytes data):
    """Decode one stream-IPC request.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(fd, port, session_id, recv_time, remote_ip, body)``.
    """
    cdef char* from_ptr = data

    cdef magic = dereference(<ushort *>(from_ptr))
    if magic != _INNER_MAGIC_VALUE:
        raise ServerDecodeError("invalid inner magic number in inner_decode_req")

    cdef data_len = dereference(<uint *>(from_ptr + 2))
    cdef fd = dereference(<uint *>(from_ptr + 6))
    cdef port = dereference(<uint *>(from_ptr + 10))
    cdef session_id = dereference(<uint *>(from_ptr + 14))
    cdef recv_time = dereference(<ulong *>(from_ptr + 18))
    cdef ip_len = dereference(<short *>(from_ptr + 26))

    return fd, port, session_id, recv_time, bytes(from_ptr[28:28 + ip_len]), bytes(from_ptr[28 + ip_len:data_len])


cdef class PipeDataHeader:
    """Decoded ``DataHeader`` plus body for one C-dispatcher datagram.

    Fields are flattened for Python. ``body`` starts at
    ``sizeof(DataHeader)`` and runs to ``header.len``.
    """
    cdef DataHeader *header
    cdef public int fd
    cdef public long session_id
    cdef public double recv_time
    cdef public int port_id
    cdef public int worker_id
    cdef public int magic_num
    cdef public bytes body

    cdef _parse(self, char* data):
        self.header = <DataHeader *> data
        self.fd = self.header.fd
        self.session_id = self.header.session_id
        self.recv_time = self.header.recv_time
        self.port_id = self.header.port_id
        self.worker_id = self.header.worker_id
        self.magic_num = self.header.magic_num
        self.body = bytes(data[_C_UDP_PACKAGE_HEADER_LEN:_C_UDP_PACKAGE_HEADER_LEN + self.header.len])


cpdef inner_decode_data_header_req(bytes data):
    """Decode one datagram-IPC request into a :class:`PipeDataHeader`.

    Args:
        data: One complete UDP IPC datagram.
    """
    cdef char* from_ptr = data
    cdef PipeDataHeader header = PipeDataHeader()
    header._parse(from_ptr)

    cdef int magic = header.magic_num
    if magic != _INNER_MAGIC_VALUE:
        raise ServerDecodeError("invalid inner magic number in inner_decode_req")

    return header


cpdef bytes inner_encode_rsp(uint fd, uint session_id, uint timeout, bytes data):
    """Encode one stream-IPC response from a worker to the dispatcher.

    The ``+18`` prefix is magic (2) + len (4) + fd (4) + session (4) +
    timeout (4). The timeout slot is unused but kept for the historical
    frame layout.

    Args:
        fd: Client connection fd the dispatcher uses to look up the TCP.
        session_id: Connection-lifetime id that must match before write.
        timeout: Unused timeout slot kept for the historical frame layout.
        data: Response body.
    """
    cdef uint data_len = len(data)
    cdef uint package_len = data_len + 18

    cdef char* to_ptr = <char *> PyMem_Malloc(package_len)

    (<ushort *>(to_ptr))[0] = _INNER_MAGIC_VALUE
    (<uint *>(to_ptr + 2))[0] = package_len
    (<uint *>(to_ptr + 6))[0] = fd
    (<uint *>(to_ptr + 10))[0] = session_id
    (<uint *>(to_ptr + 14))[0] = timeout

    cdef char* data_ptr = data
    memcpy(to_ptr + 18, data_ptr, data_len)

    results = bytes(to_ptr[:package_len])
    PyMem_Free(to_ptr)
    return results


cpdef bytes inner_encode_data_header_rsp(uint fd, uint session_id, uint worker_id, uint flag, bytes data):
    """Encode one datagram-IPC response using ``DataHeader``.

    ``flags`` is CHUNK / CHUNK_END so the C dispatcher can reassemble a
    response that did not fit in one datagram.

    Args:
        fd: Client connection fd forwarded back to the C dispatcher.
        session_id: Session id used to reassemble CHUNK / CHUNK_END.
        worker_id: Worker id written into the header.
        flag: A :class:`~trpc_sdk.transport._types.UDPPackageType` value.
        data: One datagram body, possibly a slice of a larger response.
    """
    cdef uint data_len = len(data)
    cdef uint package_len = data_len + _C_UDP_PACKAGE_HEADER_LEN

    cdef DataHeader *header = <DataHeader*> PyMem_Malloc(package_len)

    header.magic_num = _INNER_MAGIC_VALUE
    header.len = package_len
    header.fd = fd
    header.worker_id = worker_id
    header.session_id = session_id
    header.type = 1
    header.flags = flag

    cdef char* data_ptr = data
    memcpy(<char*>header + _C_UDP_PACKAGE_HEADER_LEN, data_ptr, data_len)

    results = bytes((<char*>header)[:package_len])
    PyMem_Free(header)
    return results


cpdef inner_decode_rsp(bytes data):
    """Decode one stream-IPC response.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(fd, session_id, timeout, body)``.
    """
    cdef char* from_ptr = data

    cdef magic = dereference(<ushort *>(from_ptr))
    if magic != _INNER_MAGIC_VALUE:
        raise ServerDecodeError("invalid inner magic number in inner_decode_rsp")

    cdef data_len = dereference(<uint *>(from_ptr + 2))
    cdef fd = dereference(<uint *>(from_ptr + 6))
    cdef session_id = dereference(<uint *>(from_ptr + 10))
    cdef timeout = dereference(<uint *>(from_ptr + 14))

    return fd, session_id, timeout, bytes(from_ptr[18:data_len])
