# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Main entry point for the application."""

import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from trpc_main import main

# Kept only so existing tests still import this module. See trpc_main.
if __name__ == '__main__':
    sys.exit(main())
