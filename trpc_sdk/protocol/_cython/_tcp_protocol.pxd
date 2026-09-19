# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# asyncio Protocol for one accepted client TCP connection on the merge
# / executor listen path. public object fields are the Python-visible
# surface; the remaining cdef fields stay C-only.


cdef class TCPConnection:
    cdef:
        public object app              # Service / app bound at listen time.
        public object handle           # Unary handler, typically Service.handle.
        public object aio_connection   # asyncio.Transport; None until connection_made.
        public object checker          # Per-connection frame splitter from checker.create().
        int idle_timeout               # Milliseconds copied onto ctx.timeout.
        bint closed                    # True after close(); close is idempotent.
        bint paused                    # Write-buffer high-water flag from asyncio.
        bint enable_stream_executor_mode  # Cached from server config at construct.

    # Asyncio Callbacks (Network Flow)
    cpdef void connection_made(self, transport)
    cpdef void data_received(self, bytes data)
    cpdef void connection_lost(self, exc)
    cpdef void pause_writing(self)
    cpdef void resume_writing(self)

    cpdef void close(self)
