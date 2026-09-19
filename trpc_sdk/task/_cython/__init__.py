# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython framing and queues of the worker/task IPC protocol."""

from ._task_frame import inner_decode_task_req  # pyright: ignore[reportMissingModuleSource]
from ._task_frame import inner_decode_task_rsp  # pyright: ignore[reportMissingModuleSource]
from ._task_frame import inner_encode_task_req  # pyright: ignore[reportMissingModuleSource]
from ._task_frame import inner_encode_task_rsp  # pyright: ignore[reportMissingModuleSource]
from ._task_queue import co_consume_batch_req  # pyright: ignore[reportMissingModuleSource]
from ._task_queue import consume_batch_req  # pyright: ignore[reportMissingModuleSource]
from ._task_queue import consume_batch_rsp  # pyright: ignore[reportMissingModuleSource]
from ._task_queue import receive_pkg_from_worker  # pyright: ignore[reportMissingModuleSource]
from ._task_queue import task_init  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "co_consume_batch_req",
    "consume_batch_req",
    "consume_batch_rsp",
    "inner_decode_task_req",
    "inner_decode_task_rsp",
    "inner_encode_task_req",
    "inner_encode_task_rsp",
    "receive_pkg_from_worker",
    "task_init",
]
