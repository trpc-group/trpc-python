# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Stream-frame encode / decode. Unary frames are _c_codec.pxd.
# Encode INIT / CLOSE / FEEDBACK ignore the body argument; the payload
# is message.stream_frame. Decode "body" is the complete 16-byte-headed
# frame. _common_pack writes head_len = 0.

from ._message cimport CFrameHead
from ._message cimport Message


cdef class CClientStreamCodec:
    # Encode: INIT / CLOSE / FEEDBACK serialize stream_frame; DATA uses body.
    cpdef bytes encode_init_frame(self, Message message, bytes body)
    cpdef bytes encode_data_frame(self, Message message, bytes body)
    cpdef bytes encode_close_frame(self, Message message, bytes body)
    cpdef bytes encode_feedback_frame(self, Message message, bytes body)

    # Decode: argument is the complete frame; only DATA returns a body.
    cpdef bytes decode_init_frame(self, Message message, bytes body)
    cpdef bytes decode_data_frame(self, Message message, bytes body)
    cpdef bytes decode_close_frame(self, Message message, bytes body)
    cpdef bytes decode_feedback_frame(self, Message message, bytes body)


cdef class CServerStreamCodec:
    # Encode: INIT / CLOSE / FEEDBACK serialize stream_frame; DATA uses body.
    cpdef bytes encode_init_frame(self, Message message, bytes body)
    cpdef bytes encode_data_frame(self, Message message, bytes body)
    cpdef bytes encode_close_frame(self, Message message, bytes body)
    cpdef bytes encode_feedback_frame(self, Message message, bytes body)

    # Decode: argument is the complete frame; only DATA returns a body.
    cpdef bytes decode_init_frame(self, Message message, bytes body)
    cpdef bytes decode_data_frame(self, Message message, bytes body)
    cpdef bytes decode_close_frame(self, Message message, bytes body)
    cpdef bytes decode_feedback_frame(self, Message message, bytes body)
