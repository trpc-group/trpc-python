# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Constants shared by the network utils."""

import socket
from typing import Union

# Map ip type to socket type
IP_TYPE_MAP = {4: socket.AF_INET, 6: socket.AF_INET6}

IpType = Union[int, str, bytes]
# Map port type to socket type
PortType = Union[int, str, bytes]
# Unix type
UNIX_TYPE = "unix"
