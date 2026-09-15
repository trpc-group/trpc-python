# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared helpers used across tRPC-Python packages.

Public symbols are re-exported here. Callers should ``from trpc_sdk.utils
import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.utils._constants` — :class:`ProcessType`, :class:`TcpState`,
  :class:`TimerType`, :data:`UNIX_TYPE`
- :mod:`~trpc_sdk.utils._types` — IP/port aliases and timer records
- :mod:`~trpc_sdk.utils._address` — parse/format listen and peer addresses
- :mod:`~trpc_sdk.utils._network` — IP conversion and port-availability probes
- :mod:`~trpc_sdk.utils._loop_lib` — event-loop policy and ``asynclib``
- :mod:`~trpc_sdk.utils._timer` — timer tasks and executors
- :mod:`~trpc_sdk.utils._timer_process` — dedicated timer processes
- :mod:`~trpc_sdk.utils._multi_process` — process-local type/name accessors
- :mod:`~trpc_sdk.utils._queue` — async hand-off queue
- :mod:`~trpc_sdk.utils._time_utils` — millisecond / microsecond clocks
- :mod:`~trpc_sdk.utils._str_utils` — :func:`strtobool`
- :mod:`~trpc_sdk.utils._singleton` — :func:`singleton`
- :mod:`~trpc_sdk.utils._testing` — :func:`run_async_test`

:class:`TimerHook` lives in :mod:`trpc_sdk.abc`.
"""

from __future__ import annotations

from ._address import convert_address_to_str
from ._address import get_addr_from_transport
from ._address import get_address_from_str
from ._address import make_addr_info_to_bytes
from ._address import parse_addr_info_from_bytes
from ._address import set_addr_to_ctx_from_transport
from ._constants import ProcessType
from ._constants import TcpState
from ._constants import TimerType
from ._constants import UNIX_TYPE
from ._loop_lib import asynclib
from ._loop_lib import get_or_create_event_loop
from ._multi_process import get_current_process_var
from ._multi_process import get_process_name
from ._multi_process import get_process_type
from ._multi_process import is_dispatcher_process
from ._multi_process import is_task_process
from ._multi_process import is_timer_process
from ._multi_process import is_worker_process
from ._multi_process import set_current_process_var
from ._network import bytes_to_ip
from ._network import close_socket
from ._network import create_probe_socket
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
from ._queue import Queue
from ._singleton import singleton
from ._str_utils import strtobool
from ._testing import run_async_test
from ._time_utils import now_ms
from ._time_utils import now_us
from ._timer import TimerExecutor
from ._timer import TimerTask
from ._timer import register_timer_task
from ._timer_process import TimerProcess
from ._timer_process import get_register_timer_process
from ._timer_process import register_timer_process
from ._types import IpType
from ._types import PortType
from ._types import TimerProcessInfo
from ._types import TimerTaskInfo

__all__ = [
    "IpType",
    "PortType",
    "UNIX_TYPE",
    "ProcessType",
    "TcpState",
    "TimerType",
    "TimerTaskInfo",
    "TimerProcessInfo",
    "convert_address_to_str",
    "get_addr_from_transport",
    "get_address_from_str",
    "make_addr_info_to_bytes",
    "parse_addr_info_from_bytes",
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
    "strtobool",
    "singleton",
    "TimerTask",
    "TimerExecutor",
    "TimerProcess",
    "register_timer_task",
    "register_timer_process",
    "get_register_timer_process",
    "run_async_test",
    "set_current_process_var",
    "get_current_process_var",
    "get_process_type",
    "get_process_name",
    "is_worker_process",
    "is_dispatcher_process",
    "is_task_process",
    "is_timer_process",
    "asynclib",
    "get_or_create_event_loop",
]
