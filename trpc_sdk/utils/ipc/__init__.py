# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Inter-process primitives shared by every tRPC-Python process.

A pipe here is a Unix-domain socketpair, and it is always created **before
fork** so both ends are inherited by the child. After fork each side owns one
end and must drop the other with :meth:`PipeConnection.close_extra_conn`: until
the unused fd is closed the kernel still counts a live reference, and the peer
never observes EOF when the process on the other side dies.

:class:`PipeEnd` names which end a process owns. The convention across the
framework is that the child keeps ``LEFT`` and the process that forked it keeps
``RIGHT``, so the two sides of one pair can be wrapped independently from the
same ``socket_pair`` tuple.

The package holds no process-model knowledge: who creates which pipe, and what
gets framed onto it, belongs to :mod:`trpc_sdk.runtime` and
:mod:`trpc_sdk.transport`.
"""

from ._constants import PipeEnd
from ._constants import PipeKind
from ._pipe import PipeConnection
from ._pipe import create_pipe

__all__ = [
    "PipeConnection",
    "PipeEnd",
    "PipeKind",
    "create_pipe",
]
