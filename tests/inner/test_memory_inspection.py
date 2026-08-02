# -*- coding: utf-8 -*-

from trpc_sdk._inner.memory_inspection import MemoryInspecter


class TestMemoryInspection:

    def test_take_diff(self):
        inspecter = MemoryInspecter()
        inspecter.start()
        inspecter.take_diff()

    def test_print_to_file(self):
        inspecter = MemoryInspecter(filename_prefix="test_tracemalloc", line_limit=10)
        inspecter.start()
        inspecter.print_to_file()
