# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""hellotrt server.

Each serving loop loads its own ResNet50 in ``on_start``. Layouts other
than peer also register ``normal_task``, whose initializer loads another
session after fork. ``Session.load`` is not called in this process before
``serve()``.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List
from typing import Optional

# Set before Torch is imported. A thread pool started in the parent is
# inherited by every child.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import yaml  # noqa: E402

from trpc_sdk.abc import TaskInfo  # noqa: E402
from trpc_sdk.http import register_static  # noqa: E402
from trpc_sdk.plugin import register_task  # noqa: E402
from trpc_sdk.runtime import register_handler_thread_hook  # noqa: E402
from trpc_sdk.server import ServerOption  # noqa: E402
from trpc_sdk.server import create_server  # noqa: E402
from trpc_sdk.utils import inc_io_wait_count  # noqa: E402

_HERE = os.path.abspath(os.path.dirname(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
_STUB = os.path.join(_HERE, "stub")
if _STUB not in sys.path:
    sys.path.insert(0, _STUB)

import greeter  # noqa: E402
from session import Session  # noqa: E402
from trpc_trpc_python_group_hellotrt import pb  # noqa: E402
from trpc_trpc_python_group_hellotrt import rpc  # noqa: E402

DEFAULT_CONF_PATH = os.path.join(_HERE, "trpc_python.yaml")
DOCS_PATH = os.path.abspath(os.path.join(_HERE, "../../docs"))
_TASK_NAME = "normal_task"

# Filled by the task initializer, in the task process only.
_task_session: Optional[Session] = None


def _is_peer(conf_path: str) -> bool:
    """Return whether this YAML is multi-process merge (peer).

    An empty ``io_handle_type`` on ``multi_process`` means separate, not peer.
    Peer is spelled ``merge`` or ``peer``.

    Args:
        conf_path: YAML the server will load.

    Returns:
        True when this process must not call ``register_task``.
    """
    with open(conf_path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    model = (data.get("global") or {}).get("processmodel") or {}
    mode = model.get("mode") or ""
    kind = model.get("io_handle_type") or ""
    return mode == "multi_process" and kind in ("merge", "peer")


def _on_start(info) -> None:
    """Load one ResNet50 onto this serving loop.

    Runs on the loop's thread after fork. The line is printed only after
    the weights are in memory.

    Args:
        info: Serving-loop identity. ``state["session"]`` keeps the module.
    """
    info.state["session"] = Session.load()
    print(f"SESSION_LOAD {info.index} {info.count} {os.getpid()}", flush=True)


def _task_initializer() -> None:
    """Load the task-process session, then let the server bind.

    ``inc_io_wait_count`` is what ``server.io_wait_count: 1`` waits for, so
    the port stays closed until this ResNet50 is in memory.
    """
    global _task_session  # pylint: disable=global-statement
    _task_session = Session.load()
    print(f"TASK_SESSION {os.getpid()}", flush=True)
    inc_io_wait_count()


def _task_handler(reqs: List[bytes]) -> List[bytes]:
    """Forward a batch of images in one call and return one logits row each.

    Args:
        reqs: Serialized ``TensorData`` messages, at most ``batch_size``.

    Returns:
        Serialized ``TensorData`` messages, same length and order as ``reqs``.

    Raises:
        RuntimeError: The initializer did not run.
        ValueError: An image is not ``[3, 224, 224]``.
    """
    if _task_session is None:
        raise RuntimeError("task session was not loaded")
    images = []
    for raw in reqs:
        msg = pb.TensorData()
        msg.ParseFromString(raw)
        images.append(greeter.image_from_tensor_data(msg))
    rows = _task_session.infer_batch(images)
    if len(rows) != len(reqs):
        raise RuntimeError(f"batch returned {len(rows)} rows for {len(reqs)} images")
    return [greeter.tensor_data_from_logits(row).SerializeToString() for row in rows]


def serve(conf_path: str) -> None:
    """Register the hook and, except in peer mode, the task, then serve.

    Args:
        conf_path: Framework YAML. Peer is detected from ``processmodel``
            before ``register_task``, which peer rejects at fork.
    """
    peer = _is_peer(conf_path)
    greeter.task_enabled = not peer
    register_handler_thread_hook(on_start=_on_start)
    if not peer:
        register_task(_TASK_NAME,
                      TaskInfo(name=_TASK_NAME, handler=_task_handler, initializer=_task_initializer, batch_size=5))
    server = create_server(conf_path, ServerOption())
    rpc.register_GreeterServicer_server(server, greeter.GreeterServicer())
    register_static("/static", DOCS_PATH)
    server.serve()


def main(argv: Optional[List[str]] = None) -> None:
    """Parse ``--conf`` and serve.

    Args:
        argv: Arguments without the program name. ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(description="hellotrt ResNet50 server")
    parser.add_argument("--conf", "-c", default=DEFAULT_CONF_PATH, help="server YAML")
    args = parser.parse_args(argv)
    conf_path = os.path.abspath(args.conf)
    if not os.path.exists(conf_path):
        print(f"conf_path: {conf_path} not exists", flush=True)
        sys.exit(1)
    print(f"conf_path: {conf_path}", flush=True)
    serve(conf_path)


if __name__ == "__main__":
    main()
