# -*- coding: utf-8 -*-
"""Task info of user define"""

from typing import NamedTuple, Callable, List, Any, Dict


class Task:
    """Task进程"""

    def __init__(self,
                 handler: Callable[[List[Any]], List[Any]],
                 name: str = "",
                 initializer: Callable[[None], None] = None,
                 batch_size: int = 0,
                 thread_num: int = 0,
                 model_path: str = "",
                 model_info: Any = None,
                 use_c_task: bool = False,
                 c_task_driver_params: Dict[str, Any] = None,
                 on_fork: Callable[[None], None] = None,
                 task_num: int = 1,
                 pause_heartbeat: bool = False,
                 task_stream_handler: NamedTuple = None):
        """
        初始化

        :param handler: handler 函数
        :param setter: setter 函数
        :param initializer: 初始化函数
        :param use_c_task: 是否使用 ctask
        """
        self.initializer = initializer
        self.handler = handler
        self.name = name
        self.batch_size = batch_size
        self.model_path = model_path
        self.thread_num = thread_num
        self.model_info = model_info
        self.use_c_task = use_c_task
        self.c_task_driver_params = c_task_driver_params or {}
        self.on_fork = on_fork
        self.task_num = task_num
        self.pause_heartbeat = pause_heartbeat
        self.task_stream_handler = task_stream_handler
        if self.task_stream_handler is not None:
            self.use_c_task = False
