# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False


cpdef tuple get_app_server_service(str service_name):
    """Extract the app, server, and service components from a service name.

    The canonical service-name format is
    ``trpc.{app}.{server}.{service}``.

    Args:
        service_name: Service name to parse.

    Returns:
        A tuple containing the app, server, and service names.
    """
    cdef list all_names = service_name.split(".")
    cdef Py_ssize_t len_names = len(all_names)

    if len_names > 3:
        return all_names[1], all_names[2], ".".join(all_names[3:])
    if len_names == 3:
        return all_names[0], all_names[1], all_names[2]
    if len_names == 2:
        return all_names[0], all_names[1], ""
    return service_name, "", ""


cpdef str get_method_from_rpc_func(str rpc_name):
    """Extract the method name from a canonical RPC name.

    The canonical RPC-name format is
    ``/trpc.{app}.{server}.{service}/{method}``.

    Args:
        rpc_name: RPC name to parse.

    Returns:
        The extracted method name, or the original value if the RPC name does
        not use the canonical format.
    """
    cdef list all_names

    if not rpc_name or rpc_name[0] != "/":
        return rpc_name

    all_names = rpc_name.split("/")
    if len(all_names) != 3 or all_names[1].count(".") != 3:
        return rpc_name
    return all_names[2]
