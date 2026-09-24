# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3

# Package-level cimport entry: ``from trpc_sdk.utils cimport current_time_ms``.
# Relative cimports inside a package __init__.pxd resolve against the parent
# package, so absolute module paths are required here. Only ``cdef inline``
# helpers are re-exported, so cimporting this never imports trpc_sdk.utils at
# runtime and stays safe to use from trpc_sdk.abc.

from trpc_sdk.utils._cython._time_utils cimport current_time_ms
from trpc_sdk.utils._cython._time_utils cimport ms_to_sec
from trpc_sdk.utils._cython._time_utils cimport ms_to_us
