# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False


cpdef tuple get_app_server_service(str service_name)
cpdef str get_method_from_rpc_func(str rpc_name)
