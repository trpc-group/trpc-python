#!python
#cython: language_level=3, boundscheck=False, wraparound=False

from typing import Any, Callable, Tuple

from trpc.exceptions import TRPCBaseError
from trpc.trpc_abc.context cimport Context

cdef class FilterChain:
    cdef:
        cdef list filters
        cdef bint is_server
