# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Module entry for ``python3 -m examples.plugin.main``."""

import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from trpc_main import main

if __name__ == "__main__":
    sys.exit(main())
