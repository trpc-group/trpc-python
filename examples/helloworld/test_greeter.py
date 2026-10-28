# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Test all."""

import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from test_service_rpc import main as test_run

# Kept only so existing tests still import this module. See test_run.
if __name__ == "__main__":
    test_run()
