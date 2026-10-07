# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Header-only time helpers for Cython hot paths. Every function is
# ``cdef inline`` and there is no matching .pyx, so ``cimport`` expands them
# at compile time without importing ``trpc_sdk.utils`` at runtime; this keeps
# them usable from ``trpc_sdk.abc`` without an import cycle. Unit conversions
# live here so a precision change only touches one place.

from posix.time cimport CLOCK_REALTIME
from posix.time cimport clock_gettime
from posix.time cimport timespec


cdef inline double current_time_ms() noexcept nogil:
    # CLOCK_REALTIME in milliseconds; same clock as ``TrpcContext.recv_time``.
    cdef timespec ts
    clock_gettime(CLOCK_REALTIME, &ts)
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0


cdef inline double ms_to_sec(double ms) noexcept nogil:
    return ms / 1000.0


cdef inline long ms_to_us(long ms) noexcept nogil:
    return ms * 1000
