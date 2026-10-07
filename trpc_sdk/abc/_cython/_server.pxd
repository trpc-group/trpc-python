# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Cython base for Service. Python ABC cannot be the first base of a cdef class,
# so this extension type is the C-level contract. handle stays a Python async
# method and is not declared here. register takes object because ServiceDesc
# is a Python dataclass, not a Cython type identifier.


cdef class BaseService:
    cpdef void register(self, object service_desc, bint fix_rpc_name)
    cpdef void run(self)
