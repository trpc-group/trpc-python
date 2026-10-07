# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

"""Length-only tRPC frame splitter for a TCP byte stream.

``TrpcServerProtocolChecker`` checks magic ``2352`` and ``package_len``.
It does not parse ``RequestProtocol``. The dispatcher uses it to cut
complete frames from a socket; protocol decode happens later in
:mod:`trpc_sdk.codec._cython._codec`.

A half-frame stays in ``buf`` across ``feed_data``. After ``read``
drains complete frames, the leftover is compacted to the front with
``memmove`` because source and dest overlap (``memcpy`` has
segfaulted). Grow uses ``(self.size + append_length) * 2``, i.e.
allocated capacity, not current write length.

Magic and frame-head length are read once at import from their Python
single sources; the module-level ``cdef`` keeps the hot scan on C
scalars.
"""

from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.string cimport memcpy
from libc.string cimport memmove
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from cpython.mem cimport PyMem_Realloc
from cython.operator cimport dereference

from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerDecodeError


cdef uint16_t TRPC_MAGIC_VALUE = 2352
cdef uint32_t FRAME_HEAD_LEN = 16


cdef class TrpcServerProtocolChecker(BaseServerProtocolChecker):
    """Split a TCP stream into complete tRPC frames.

    A frame is 16-byte head plus ``package_len - 16`` more bytes.
    ``read`` returns every complete package currently buffered.
    """

    def __cinit__(self):
        """Allocate a 1000-byte parse buffer; Python GC does not free it."""
        self.size = 1000
        self.length = 0
        self.buf = <char *> PyMem_Malloc(self.size * sizeof(char)) # gc
        if not self.buf:
            raise MemoryError()

    cpdef void reset(self) except *:
        """Drop buffered bytes without freeing the allocation."""
        self.length = 0

    cpdef void feed_data(self, bytes data) except *:
        """Append ``data`` to the growable parse buffer.

        Grows with ``(self.size + append_length) * 2`` when the
        remaining capacity is too small.

        Args:
            data: Incremental stream bytes from the client TCP.
        """
        cdef int append_length = len(data)

        if (self.size-self.length) < append_length:
            self.resize((self.size+append_length)*2)

        cdef char* start_ptr = self.buf + self.length
        cdef char* from_ptr = data
        memcpy(start_ptr, from_ptr, append_length)
        self.length += append_length

    cpdef list read(self):
        """Return every complete frame and compact leftover bytes.

        Each candidate head is checked for magic ``2352``,
        ``package_len >= 16``, and
        ``package_len <= TrpcConfig.server_config().max_rbuf_size``.
        An incomplete tail stays in ``buf``.

        Returns:
            Complete packages including the 16-byte head. Empty when
            the buffer has no full frame yet.

        Raises:
            ServerDecodeError: Magic mismatch, ``package_len`` smaller
                than the frame head, or ``package_len`` above
                ``max_rbuf_size``.
        """
        results = []

        cdef int scan_end = 0
        cdef uint16_t stx = 0
        cdef uint32_t package_len = 0

        while self.length >= scan_end+FRAME_HEAD_LEN:
            stx = ntohs(dereference(<uint16_t *>(self.buf+scan_end)))

            if stx != TRPC_MAGIC_VALUE:
                raise ServerDecodeError("framer head magic not match")

            package_len = ntohl(dereference(<uint32_t *>(self.buf+4+scan_end)))

            if package_len < FRAME_HEAD_LEN:
                raise ServerDecodeError("frame header total len invalid" + str(package_len))

            if package_len > TrpcConfig.server_config().max_rbuf_size:
                raise ServerDecodeError("frame header total len too large")

            if self.length < scan_end+package_len:
                break

            results.append(bytes(self.buf[scan_end:scan_end+package_len]))

            scan_end = scan_end + package_len

        if scan_end > 0:
            # Source and dest overlap; memcpy is undefined and has segfaulted.
            memmove(self.buf, self.buf + scan_end, self.length - scan_end)
            self.length = self.length - scan_end

        return results

    cpdef void resize(self, size_t new_number):
        """Reallocate the parse buffer to ``new_number`` bytes.

        Args:
            new_number: New capacity in bytes.

        Raises:
            MemoryError: ``PyMem_Realloc`` failed.
        """
        mem = <char*> PyMem_Realloc(self.buf, new_number * sizeof(char))
        if not mem:
            raise MemoryError()
        self.size = new_number
        self.buf = mem

    def __dealloc__(self):
        PyMem_Free(self.buf)
