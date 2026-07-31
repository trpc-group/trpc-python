# -*- coding: utf-8 -*-
import os
import tracemalloc

from trpc.log import logger


class MemoryInspecter:
    """内存分配检视器"""

    _DEFAULT_KEY_TYPE = "lineno"

    def __init__(self, filename_prefix: str = "tracemalloc", line_limit: int = 50) -> None:
        """
        :param filename_prefix: debug信息保存的文件名前缀，完整文件名格式为<prefix>_<pid>_<index>.txt
        :param line_limit: 保存到文件中的内存分配信息行数上线，-1为不限
        """
        self.filename_prefix = filename_prefix
        self.line_limit = line_limit
        self._pre_snapshot = None
        self._saved_index = 0

    def start(self):
        """开始记录该进程的内存分配情况"""
        tracemalloc.start()

    def take_diff(self):
        """收集目前进程状态到上一次执行该函数时状态的diff

        如果第一次执行，则为当前内存状态
        """
        snapshot = tracemalloc.take_snapshot()
        if self._pre_snapshot:
            diff = snapshot.compare_to(self._pre_snapshot, self._DEFAULT_KEY_TYPE)
        else:
            diff = snapshot.statistics(self._DEFAULT_KEY_TYPE)
        self._pre_snapshot = snapshot
        return diff

    def _gen_file_name(self, prefix: str) -> str:
        full_name = f"{prefix}_{os.getpid()}_{self._saved_index}.txt"
        self._saved_index += 1
        return full_name

    def print_to_file(self):
        try:
            self._print_to_file()
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"print memory alloc info to file failed, error: {exc}")

    def _print_to_file(self):
        """将内存分配信息保存到文件中"""
        top_stats = self.take_diff()
        filename = self._gen_file_name(self.filename_prefix)
        with open(filename, "w") as fin:
            for each in top_stats[:self.line_limit]:
                fin.write(str(each))
                fin.write("\n")
