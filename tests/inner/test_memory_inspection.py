# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import glob
import os
import tracemalloc

from trpc_sdk._inner.memory_inspection import MemoryInspector


class TestMemoryInspection:

    def test_take_diff(self):
        inspector = MemoryInspector()
        inspector.start_tracing()
        try:
            inspector.take_stats()
        finally:
            tracemalloc.stop()

    def test_print_to_file(self):
        inspector = MemoryInspector(filename_prefix="test_tracemalloc", line_limit=10)
        inspector.start_tracing()
        try:
            inspector.dump_to_file()
        finally:
            tracemalloc.stop()
            for path in glob.glob("test_tracemalloc_*.txt"):
                os.remove(path)
