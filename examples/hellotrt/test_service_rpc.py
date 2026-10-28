# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client for the ResNet50 greeter.

``python test_service_rpc.py [--port N]`` talks to a server that is already
listening. It does not start one and does not load YAML. The layout sweep
is ``check_modes.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "stub")))  # noqa

from trpc_trpc_python_group_hellotrt import pb  # noqa: E402
from trpc_trpc_python_group_hellotrt import rpc  # noqa: E402

_SERVICE = "trpc.trpc_python_group.hellotrt.Greeter"
_TIMEOUT_MS = 30000
_ATOL = 1e-3
_LOGITS = 1000
_IMAGE_SHAPE = (3, 224, 224)


def make_image() -> List[float]:
    """Build one deterministic ``[3, 224, 224]`` image in ``[0, 1]``.

    Returns:
        ``3 * 224 * 224`` floats in NCHW order.
    """
    size = 3 * 224 * 224
    return [((index * 17) % 251) / 250.0 for index in range(size)]


def make_request() -> pb.HelloRequest:
    """Build a request whose ``input`` tensor is :func:`make_image`.

    Returns:
        ``HelloRequest`` with a ``DT_FLOAT`` image of shape ``[3, 224, 224]``.
    """
    req = pb.HelloRequest()
    tensor = req.msg.data["input"]
    tensor.dtype = pb.DT_FLOAT
    values = make_image()
    tensor.float_val.extend(values)
    for size in _IMAGE_SHAPE:
        dim = tensor.tensor_shape.dim.add()
        dim.size = size
    return req


def _options(port: int) -> Options:
    """Build call options that do not load YAML.

    Args:
        port: TCP port of the already-running server.

    Returns:
        Options for one unary call.
    """
    return (Options().set_target(f"ip://127.0.0.1:{port}").set_serialization_type(
        SerializationType.PB).set_network("tcp").set_protocol("trpc").set_namespace("Development").set_timeout(
            _TIMEOUT_MS))


def _logits(reply: pb.HelloReply) -> List[float]:
    """Read the ``logits`` tensor.

    Args:
        reply: One greeter reply.

    Returns:
        The float values of ``logits``.

    Raises:
        AssertionError: The tensor is missing or is not shape ``[1000]``.
    """
    if "logits" not in reply.msg.data:
        raise AssertionError("reply has no logits")
    tensor = reply.msg.data["logits"]
    shape = tuple(int(dim.size) for dim in tensor.tensor_shape.dim)
    values = list(tensor.float_val)
    if shape != (_LOGITS,) or len(values) != _LOGITS:
        raise AssertionError(f"logits shape {shape} length {len(values)}")
    return values


def _close(left: Sequence[float], right: Sequence[float]) -> bool:
    """Return whether two logit rows match within ``1e-3``.

    Args:
        left: First row.
        right: Second row.

    Returns:
        True when the rows have the same length and each pair is within
        the absolute tolerance.
    """
    if len(left) != len(right):
        return False
    return all(abs(a - b) <= _ATOL for a, b in zip(left, right))


async def run_calls(port: int, say_hi: bool = True) -> Tuple[List[float], Optional[List[float]]]:
    """Call ``SayHello`` and, when requested, ``SayHi``.

    Both replies must be 1000-d and a different shape from the ``[3, 224, 224]``
    input. When both run, their logits must match within ``1e-3``.

    Args:
        port: TCP port of the already-running server.
        say_hi: Also call ``SayHi``. Peer passes false.

    Returns:
        ``SayHello`` logits, and ``SayHi`` logits or ``None``.

    Raises:
        AssertionError: A reply has the wrong shape, or the two rows differ.
    """
    proxy = rpc.GreeterClientProxyImpl(_SERVICE)
    hello_reply, hi_reply = await _call(proxy, port, say_hi)
    hello = _logits(hello_reply)
    print(f"\n  test_async_say_hello logits {len(hello)}\n", flush=True)
    hi: Optional[List[float]] = None
    if hi_reply is not None:
        hi = _logits(hi_reply)
        print(f"\n  test_async_say_hi logits {len(hi)}\n", flush=True)
        if not _close(hello, hi):
            raise AssertionError("SayHello and SayHi logits differ by more than 1e-3")
        print("allclose ok", flush=True)
    return hello, hi


async def _call(proxy, port: int, say_hi: bool) -> Tuple[pb.HelloReply, Optional[pb.HelloReply]]:
    """Issue the calls. ``SayHi`` runs together with ``SayHello`` when enabled.

    Each call gets its own request and options so the two in-flight calls
    do not share a message.

    Args:
        proxy: Greeter client.
        port: TCP port of the already-running server.
        say_hi: Also call ``SayHi``.

    Returns:
        The hello reply and, when requested, the hi reply.
    """
    hello_task = proxy.asyncSayHello(TrpcContext(), make_request(), _options(port))
    if not say_hi:
        return await hello_task, None
    hi_task = proxy.asyncSayHi(TrpcContext(), make_request(), _options(port))
    hello_reply, hi_reply = await asyncio.gather(hello_task, hi_task)
    return hello_reply, hi_reply


def main(argv: Optional[List[str]] = None) -> None:
    """Call the server on ``--port`` (default 8000), including ``SayHi``.

    Args:
        argv: Arguments without the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="hellotrt ResNet50 client")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    asyncio.run(run_calls(args.port, say_hi=True))


if __name__ == "__main__":
    main()
