#!python
#cython: language_level=3, boundscheck=False, wraparound=False

cdef class Parser:
    cpdef void feeds_data(self, bytes data)
    cpdef bytes read(self)
    cpdef void reset(self)
