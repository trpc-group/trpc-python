# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared helpers used across tRPC-Python packages.

Public symbols are re-exported here. Callers should ``from trpc_sdk.utils
import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.utils._constants` — :class:`TcpState`, :data:`UNIX_TYPE`,
  :data:`DEFAULT_BACKLOG`, :class:`RegisterStatus` and
  :data:`TRPC_SHARE_MEM_LOCK`
- :mod:`~trpc_sdk.utils._dict_to_pb` — fill a protobuf message from a dict
- :mod:`~trpc_sdk.utils._types` — address/IP/port aliases
- :mod:`~trpc_sdk.utils._address` — parse/format listen and peer addresses
- :mod:`~trpc_sdk.utils._network` — IP conversion, port-availability probes,
  and post-fork listen-socket constructors (:func:`create_new_socket`)
- :mod:`~trpc_sdk.utils._loop_lib` — event-loop policy via :func:`get_async_lib`
- :mod:`~trpc_sdk.utils._timer` — :class:`TimerType`, :class:`TimerScope`,
  :class:`TimerTaskInfo`, timer tasks and per-process / per-thread executors
- :mod:`~trpc_sdk.utils._queue` — async hand-off queue
- :mod:`~trpc_sdk.utils._time_utils` — millisecond / microsecond clocks
- :mod:`~trpc_sdk.utils._str_utils` — :func:`str2bool`
- :mod:`~trpc_sdk.utils._singleton` — :func:`singleton`
- :mod:`~trpc_sdk.utils._process_sync` — cross-process sync primitives:
  the IO-worker startup barrier (:func:`inc_io_wait_count` /
  :func:`io_ready_reached`), the register flag
  (:func:`register_enable` / :func:`register_spinlock`) and the registry
  heartbeat pause counter (:func:`pause_heartbeat` /
  :func:`resume_heartbeat` / :func:`should_heartbeat`)
- :mod:`~trpc_sdk.utils.ipc` — pre-fork socketpair pipes
  (:class:`PipeConnection`, :class:`PipeEnd`, :class:`PipeKind`,
  :func:`create_pipe`)

Importing this package constructs the ``multiprocessing.Value`` / ``Lock``
objects in :mod:`~trpc_sdk.utils._process_sync`, so it must happen before
the first ``fork``.

:class:`TimerHook` lives in :mod:`trpc_sdk.abc`. The process-role accessors
(``get_process_type``, ``is_worker_process``, ...) are re-exported from
:mod:`trpc_sdk.common`.
"""

from __future__ import annotations

from trpc_sdk.common import get_current_process_var
from trpc_sdk.common import get_process_name
from trpc_sdk.common import get_process_type
from trpc_sdk.common import is_task_process
from trpc_sdk.common import is_timer_process
from trpc_sdk.common import is_worker_process
from trpc_sdk.common import set_current_process_var

from ._address import convert_address_to_str
from ._address import get_addr_from_transport
from ._address import get_address_from_str
from ._address import make_addr_info_to_bytes
from ._address import parse_addr_info_from_bytes
from ._address import set_addr_to_ctx
from ._address import set_addr_to_ctx_from_transport
from ._constants import DEFAULT_BACKLOG
from ._constants import TRPC_SHARE_MEM_LOCK
from ._constants import RegisterStatus
from ._constants import TcpState
from ._constants import UNIX_TYPE
from ._dict_to_pb import EXTENSION_CONTAINER
from ._dict_to_pb import TYPE_CALLABLE_MAP
from ._dict_to_pb import dict_to_protobuf
from ._loop_lib import get_async_lib
from ._loop_lib import get_or_create_event_loop
from ._loop_lib import is_uvloop_enabled
from ._network import bytes_to_ip
from ._network import close_socket
from ._network import create_inet_socket
from ._network import create_new_socket
from ._network import create_probe_socket
from ._network import create_unix_socket
from ._network import find_free_port
from ._network import get_ip_with_nic
from ._network import get_tcp_state
from ._network import int2ip
from ._network import ip2int
from ._network import ip_to_bytes
from ._network import is_close_wait
from ._network import is_ipv4
from ._network import is_ipv6
from ._network import is_port_valid
from ._network import is_tcp_port_available
from ._network import is_udp_port_available
from ._network import is_unix_socket
from ._network import is_valid_socket
from ._network import join_host_port
from ._network import set_close_wait
from ._network import set_default_close_wait
from ._network import set_no_close_wait
from ._network import set_tcp_keep_alive
from ._network import set_tcp_no_delay
from ._network import unix_path_valid
from ._network import unlink_unix_socket_file
from ._process_sync import REGISTER_STATUE
from ._process_sync import get_register_status
from ._process_sync import inc_io_wait_count
from ._process_sync import io_ready_reached
from ._process_sync import pause_heartbeat
from ._process_sync import register_enable
from ._process_sync import register_spinlock
from ._process_sync import remove_lock_file
from ._process_sync import resume_heartbeat
from ._process_sync import set_register_status
from ._process_sync import should_heartbeat
from ._process_sync import wait_notify_in_io
from ._process_sync import warm_up
from ._queue import Queue
from ._singleton import singleton
from ._str_utils import str2bool
from ._time_utils import now_ms
from ._time_utils import now_us
from ._timer import TimerExecutor
from ._timer import TimerScope
from ._timer import TimerTask
from ._timer import TimerTaskInfo
from ._timer import TimerType
from ._timer import register_timer_task
from ._timer import registered_timer_tasks
from ._types import IpType
from ._types import PortType
from .ipc import PipeConnection
from .ipc import PipeEnd
from .ipc import PipeKind
from .ipc import create_pipe

__all__ = [
    "IpType",
    "PortType",
    "UNIX_TYPE",
    "TcpState",
    "TimerType",
    "TimerScope",
    "TimerTaskInfo",
    "convert_address_to_str",
    "get_addr_from_transport",
    "get_address_from_str",
    "make_addr_info_to_bytes",
    "parse_addr_info_from_bytes",
    "set_addr_to_ctx",
    "set_addr_to_ctx_from_transport",
    "int2ip",
    "ip2int",
    "ip_to_bytes",
    "bytes_to_ip",
    "is_ipv4",
    "is_ipv6",
    "is_unix_socket",
    "is_port_valid",
    "is_tcp_port_available",
    "is_udp_port_available",
    "unix_path_valid",
    "create_probe_socket",
    "create_new_socket",
    "create_inet_socket",
    "create_unix_socket",
    "DEFAULT_BACKLOG",
    "unlink_unix_socket_file",
    "get_ip_with_nic",
    "join_host_port",
    "find_free_port",
    "is_valid_socket",
    "is_close_wait",
    "get_tcp_state",
    "set_no_close_wait",
    "set_close_wait",
    "set_default_close_wait",
    "set_tcp_no_delay",
    "set_tcp_keep_alive",
    "close_socket",
    "Queue",
    "now_ms",
    "now_us",
    "str2bool",
    "dict_to_protobuf",
    "EXTENSION_CONTAINER",
    "TYPE_CALLABLE_MAP",
    "singleton",
    "TimerTask",
    "TimerExecutor",
    "register_timer_task",
    "registered_timer_tasks",
    "set_current_process_var",
    "get_current_process_var",
    "get_process_type",
    "get_process_name",
    "is_worker_process",
    "is_task_process",
    "is_timer_process",
    "get_async_lib",
    "is_uvloop_enabled",
    "get_or_create_event_loop",
    "TRPC_SHARE_MEM_LOCK",
    "RegisterStatus",
    "REGISTER_STATUE",
    "warm_up",
    "remove_lock_file",
    "get_register_status",
    "set_register_status",
    "register_enable",
    "register_spinlock",
    "wait_notify_in_io",
    "io_ready_reached",
    "inc_io_wait_count",
    "pause_heartbeat",
    "resume_heartbeat",
    "should_heartbeat",
    "PipeConnection",
    "PipeEnd",
    "PipeKind",
    "create_pipe",
]
