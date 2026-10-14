# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# asyncio Protocol for one accepted client TCP connection on the merge
# / executor listen path. public / readonly fields are the Python-visible
# surface; the remaining cdef fields stay C-only.


cpdef int next_session_id()


cdef class TCPConnection:
    cdef:
        public object handle           # Service entry, typically ServiceAdapter.handle.
        public object aio_connection   # asyncio.Transport; None until connection_made.
        public object parser           # Per-connection frame splitter from parser.create().
        public object shared_pool      # SharedHandlerPool (io_handle_type: separate), else None.
        public object writer           # ConnWriter: from connection_made under shared_pool, else from the first stream frame.
        readonly int session_id        # Process-unique, non-zero; assigned in connection_made.
        readonly bint has_streams      # Set by the first stream frame; close() then aborts the streams.
        readonly object stream_dispatcher  # StreamDispatcher of the service; None until the first stream frame.
        readonly object stream_loops   # Handle-thread indexes streams were pinned to (shared_pool only), or None.
        int idle_timeout               # Milliseconds copied onto ctx.timeout.
        bint closed                    # True after close(); close is idempotent.
        bint paused                    # Write-buffer high-water flag from asyncio.

    # Asyncio Callbacks (Network Flow)
    cpdef void connection_made(self, transport)
    cpdef void data_received(self, bytes data)
    cpdef void connection_lost(self, exc)
    cpdef void pause_writing(self)
    cpdef void resume_writing(self)

    cpdef void close(self)

    # Stream frames
    cdef object _stream_writer(self)
    cdef void _dispatch_stream(self, bytes package, object ctx) except *
    cdef void _abort_streams(self)
