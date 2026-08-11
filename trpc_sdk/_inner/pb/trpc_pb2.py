# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from google.protobuf import __version__ as protobuf_version

from packaging.version import Version

_v = Version(protobuf_version)

if Version("2.5") <= _v < Version("3.20.0"):
    from ._v3_trpc_pb2 import *
elif Version("4.21.0") <= _v < Version("5.26.0"):
    from ._v4_trpc_pb2 import *
elif _v >= Version("5.26.0"):
    from ._v5_trpc_pb2 import *
else:
    raise ImportError(
        f"unsupported protobuf version: {protobuf_version}; "
        "require 3.x, >=4.21,<5.26, or >=5.26"
    )
