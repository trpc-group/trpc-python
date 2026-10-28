# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Greeter methods for the ResNet50 example.

``SayHello`` reads the session stored on this serving loop. ``SayHi`` sends
the same image to ``normal_task``. Peer mode leaves ``task_enabled`` false
and does not register that task.
"""

from __future__ import annotations

import os
import sys
from typing import List
from typing import Sequence

from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.task import send_to_task

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_hellotrt import pb  # noqa: E402
from trpc_trpc_python_group_hellotrt import rpc  # noqa: E402

_IMAGE_SHAPE = (3, 224, 224)
_IMAGE_SIZE = 3 * 224 * 224
_LOGITS = 1000
_INPUT_KEY = "input"
_OUTPUT_KEY = "logits"

# Set by trpc_main before serve(). Peer does not register a task.
task_enabled = True


def image_from_tensor_data(msg: pb.TensorData) -> List[float]:
    """Read the ``input`` tensor as a flattened ``[3, 224, 224]`` image.

    Args:
        msg: Request ``TensorData``.

    Returns:
        ``3 * 224 * 224`` floats in NCHW order.

    Raises:
        ValueError: The ``input`` tensor is missing or has another shape.
    """
    if _INPUT_KEY not in msg.data:
        raise ValueError(f"tensor data has no {_INPUT_KEY!r}")
    tensor = msg.data[_INPUT_KEY]
    shape = tuple(int(dim.size) for dim in tensor.tensor_shape.dim)
    values = list(tensor.float_val)
    if shape != _IMAGE_SHAPE or len(values) != _IMAGE_SIZE:
        raise ValueError(f"input shape {shape} length {len(values)}, want {_IMAGE_SHAPE}")
    return values


def tensor_data_from_logits(logits: Sequence[float]) -> pb.TensorData:
    """Pack one row of 1000 logits under the key ``logits``.

    Args:
        logits: One forward result.

    Returns:
        ``TensorData`` with a ``DT_FLOAT`` tensor of shape ``[1000]``.

    Raises:
        ValueError: ``logits`` is not length 1000.
    """
    if len(logits) != _LOGITS:
        raise ValueError(f"logits length {len(logits)}, want {_LOGITS}")
    msg = pb.TensorData()
    tensor = msg.data[_OUTPUT_KEY]
    tensor.dtype = pb.DT_FLOAT
    tensor.float_val.extend(logits)
    dim = tensor.tensor_shape.dim.add()
    dim.size = _LOGITS
    return msg


class GreeterServicer(rpc.GreeterServicer):
    """Runs ResNet50 on the serving loop, or hands the image to the task."""

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Forward one image on this serving loop's session.

        Args:
            ctx: Request context. Unused; the session hangs off the loop.
            request: ``TensorData`` with an ``input`` image.

        Returns:
            ``logits`` of shape ``[1000]``.

        Raises:
            RuntimeError: This thread has no serving-loop session.
            ValueError: The input shape is not ``[3, 224, 224]``.
        """
        del ctx
        info = current_handler_thread()
        if info is None or "session" not in info.state:
            raise RuntimeError("no ResNet50 session on this serving loop")
        logits = info.state["session"].infer(image_from_tensor_data(request.msg))
        logger.info("SayHello logits %s", len(logits))
        reply = pb.HelloReply()
        reply.msg.CopyFrom(tensor_data_from_logits(logits))
        return reply

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Send the image to ``normal_task`` and return its logits.

        Args:
            ctx: Request context. The task pipe belongs to this serving loop.
            request: ``TensorData`` with an ``input`` image.

        Returns:
            ``logits`` of shape ``[1000]`` from the task batch.

        Raises:
            RuntimeError: This layout did not register ``normal_task``.
        """
        if not task_enabled:
            raise RuntimeError("normal_task is not registered in peer mode")
        raw = await send_to_task(ctx, "normal_task", request.msg.SerializeToString())
        reply = pb.HelloReply()
        reply.msg.ParseFromString(raw)
        logger.info("SayHi logits %s", len(reply.msg.data[_OUTPUT_KEY].float_val))
        return reply
