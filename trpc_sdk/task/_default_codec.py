# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-wide default task body codec.

The framing between a worker and a task child is fixed, but the body is
opaque to it: business code chooses how its own objects become bytes by
installing a codec here. Both processes must install the same one.

The :class:`~trpc_sdk.abc.TaskCodec` contract lives in ``trpc_sdk.abc``.
Importing this module installs :class:`DefaultTaskCodec` (pickle) into the
process-wide slot, so it is already active before the server forks.
Business code that needs another format calls :func:`set_default_codec`
afterwards, still before the fork.
"""

import pickle
from typing import Optional

from trpc_sdk.abc import TaskCodec


DEFAULT_TASK_CODEC: Optional[TaskCodec] = None


def set_default_codec(obj: TaskCodec):
    """Install the process-wide task codec.

    Must be called before the server forks its task children so worker and
    task agree on the encoding.

    Args:
        obj: Codec used for every task body in this process.
    """
    global DEFAULT_TASK_CODEC
    DEFAULT_TASK_CODEC = obj


def get_default_codec() -> Optional[TaskCodec]:
    """Return the installed task codec.

    Returns:
        The codec, or ``None`` when no task feature was configured.
    """
    return DEFAULT_TASK_CODEC


class DefaultTaskCodec(TaskCodec):
    """Task codec backed by :mod:`pickle`.

    Bodies never leave the machine: they only cross the IPC pipe between a
    worker and its task child, both of which are the same deployment.
    """

    def encode(self, obj) -> bytes:
        """Serialize one request or response body.

        Args:
            obj: Object handed to, or returned by, the task handler.

        Returns:
            The pickled body bytes.
        """
        return pickle.dumps(obj)

    def decode(self, data: bytes) -> object:
        """Deserialize one request or response body.

        Args:
            data: Pickled body bytes taken from the frame.

        Returns:
            The object to hand to the task handler or the caller.
        """
        return pickle.loads(data)


set_default_codec(DefaultTaskCodec())
