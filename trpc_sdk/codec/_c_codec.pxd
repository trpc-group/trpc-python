# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Unary tRPC encode / decode. Stream frames are _c_codec_stream.pxd.
# CClientCodec.request_id is a per-instance wrap counter, not a
# globally unique id.

from ._message cimport CFrameHead
from ._message cimport Message


cdef class CClientCodec:
    cdef:
        cdef public long request_id  # Per-instance encode counter; wraps at UINT32_MAX.

    cpdef bytes encode(self, Message message, bytes body)
    cpdef bytes decode(self, Message message, bytes buffer)
    cpdef bytes _unpack(self, object rsp, Message message, bytes buffer)
    cpdef void update_msg_by_msg(self, Message origin_message, Message target_message)


cdef class CServerCodec:
    cpdef bytes encode(self, Message message, bytes body)
    cpdef bytes decode(self, Message message, bytes buffer)
    cpdef bytes _unpack(self, object req, Message message, bytes buffer)
