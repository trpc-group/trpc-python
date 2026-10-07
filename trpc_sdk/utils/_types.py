# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type aliases for :mod:`trpc_sdk.utils`.

Address, IP, and port aliases are used by :mod:`trpc_sdk.utils._address`
and :mod:`trpc_sdk.utils._network`. Timer records live with the timer
code in :mod:`trpc_sdk.utils._timer`.
"""

from __future__ import annotations

from typing import Tuple
from typing import Union

# ``(host, port)`` as used by asyncio transports and listen endpoints.
# Unix sockets reuse the tuple with port unused.
AddrType = Tuple[str, int]

# Values accepted by :mod:`ipaddress` (dotted string, packed bytes, or int).
IpType = Union[int, str, bytes]
PortType = Union[int, str, bytes]
