# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Heartbeat monitoring between a worker and the task processes it forks.

The worker builds one socketpair per task in :func:`init_healthy_checker`,
before the fork. After the fork each task sends heartbeats from a ``TASK``
child callback, and the worker attaches :class:`MasterHeartbeatConnection`
to its own loop in :func:`start_heartbeat_monitor`. Any other child that
inherited the pipes closes them with :func:`close_heartbeat_pipes`.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import struct
import time
from dataclasses import dataclass
from functools import partial
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import ProcessType
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.common import register_child_callback
from trpc_sdk.config import TrpcConfig
from trpc_sdk.ipc import PipeConnection
from trpc_sdk.ipc import PipeEnd
from trpc_sdk.ipc import PipeKind
from trpc_sdk.ipc import create_pipe
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin
from ._constants import MAGIC_VAL
from ._constants import PKG_LEN
from ._types import TaskInfo


TimeoutCallBack = Callable[[int], None]

_timeout_handles: Dict[ProcessType, TimeoutCallBack] = {}

# 1. If the process uses coroutines like await asyncio.sleep(1), timeouts can be cancelled
#    since coroutines don't block inherently, so heartbeat remains normal
# 2. If the process uses time.sleep(1) which causes blocking, coroutines cannot cancel this type of blocking,
#    but heartbeat can still detect it


def register_timeout_callback(type: ProcessType, callback: TimeoutCallBack) -> None:
    """Register a timeout callback function for a specific process type.

    Args:
        type: The process type to register callback for
        callback: The callback function to execute when timeout occurs
    """
    _timeout_handles[type] = callback


@dataclass
class PipeRecvData:
    """Data class for received data from child process."""
    magic_val: int = MAGIC_VAL
    pid: int = -1
    current_recv_time: int = 0
    ret: bool = False


@dataclass
class HealthyOptions:
    """Configuration options for healthy process monitoring and communication."""
    process_type: ProcessType = ProcessType.UNKNOWN
    name: str = ""
    master_pipe: Optional[PipeConnection] = None
    child_pipe: Optional[PipeConnection] = None
    heartbeat_interval: int = 0
    max_check_interval: int = 0


class MasterHeartbeatConnection(asyncio.Protocol):
    """Heartbeat connection protocol for master process to monitor child processes."""

    def __init__(self, options: HealthyOptions):
        """Initialize master heartbeat connection with monitoring options.

        Args:
            options: Configuration options for heartbeat monitoring
        """
        super().__init__()
        # Transport to the child; set in ``connection_made``.
        self._conn: Optional[asyncio.Transport] = None
        # Partial heartbeat frames awaiting a full PKG_LEN packet.
        self._recv_buffer = b''
        # Unix timestamp carried by the last decoded heartbeat; ``None`` until
        # the first one arrives.
        self.last_recv_time: Optional[int] = None
        self._heartbeat_interval: int = options.heartbeat_interval
        self._idle_heartbeat_interval: int = options.max_check_interval
        # Child pid learned from the heartbeat payload, not from fork.
        self._child_pid: Optional[int] = None
        # Idle-watchdog task; created in ``connection_made``.
        self._task: Optional[asyncio.Task] = None
        self._is_running = True
        self._timeout_cnt = 0
        self._is_over_timeout = False
        self.name: str = options.name
        self.master_pipe: Optional[PipeConnection] = options.master_pipe
        self.child_pipe: Optional[PipeConnection] = options.child_pipe
        self.process_type: ProcessType = options.process_type

    def _run_timeout_handler(self, pid: int) -> None:
        """Execute the registered timeout handler for this process type.

        Args:
            pid: The process ID to pass to the timeout handler
        """
        global _timeout_handles
        if self.process_type in _timeout_handles:
            _timeout_handles[self.process_type](pid)

    def connection_made(self, transport: asyncio.Transport) -> None:
        """Called when connection is established with child process.

        Args:
            transport: The transport object for the connection
        """
        self._conn = transport
        if self._task is None:
            self._task = asyncio.create_task(self._check_idle())

    def _handle_timeout(self, current_recv_time: int, heartbeat_interval: int, info: str) -> bool:
        """Check if child process has timed out based on received heartbeat.

        Args:
            current_recv_time: Current timestamp for comparison
            heartbeat_interval: Expected heartbeat interval in seconds
            info: Context information for logging

        Returns:
            bool: True if timeout occurred, False otherwise
        """
        if self.last_recv_time is None or self._child_pid is None:
            return False
        if current_recv_time - self.last_recv_time > heartbeat_interval:
            self._timeout_cnt += 1
            self._is_over_timeout = True
            msg = f"master {info} ({self.name}-{self._child_pid}) timeout,"
            msg += f" last recv time: {self.last_recv_time},"
            msg += f" now time: {current_recv_time},"
            msg += f" diff: {(current_recv_time - self.last_recv_time)},"
            msg += f" heartbeat_interval: {heartbeat_interval}"
            msg += f" timeout_cnt: {self._timeout_cnt}"
            logger.warning(msg)
            self._run_timeout_handler(self._child_pid)
            return True
        return False

    async def _check_idle(self) -> None:
        """Continuously check for idle timeouts in child processes."""
        while self._is_running:
            self._handle_timeout(int(time.time()), self._idle_heartbeat_interval, "wait timeout")
            await asyncio.sleep(1)

    def data_received(self, data: bytes) -> None:
        """Handle data received from child process.

        Args:
            data: Raw bytes data received from child process
        """
        self._recv_buffer += data
        try:
            recv_data = self._decode()
            if recv_data.ret:
                self._child_pid = recv_data.pid
                if self.last_recv_time:
                    if self._is_over_timeout is False:
                        # Avoid multiple timeout processing
                        self._handle_timeout(recv_data.current_recv_time, self._heartbeat_interval, "recv from child")
                    else:
                        self._is_over_timeout = False
                self.last_recv_time = recv_data.current_recv_time
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"master recv from child ({self.name}) failed: {e}")

    def _decode(self) -> PipeRecvData:
        """Decode received data packet from child process.

        Returns:
            PipeRecvData: Decoded data structure with process information
        """
        recv_data = PipeRecvData()
        if len(self._recv_buffer) < PKG_LEN:
            return recv_data
        recv_data.magic_val = struct.unpack('>H', self._recv_buffer[:2])[0]
        if recv_data.magic_val != MAGIC_VAL:
            logger.error(f"Invalid magic value: {recv_data.magic_val}")
            self._recv_buffer = self._recv_buffer[2:]
            return recv_data
        recv_data.pid = struct.unpack('>I', self._recv_buffer[2:6])[0]
        recv_data.current_recv_time = struct.unpack('>I', self._recv_buffer[6:PKG_LEN])[0]
        recv_data.ret = True
        self._recv_buffer = self._recv_buffer[PKG_LEN:]
        return recv_data

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Handle connection loss with child process.

        Args:
            exc: Exception that caused connection loss, if any
        """
        if self._task:
            self._is_running = False
            self._task.cancel()
        if self._conn and self._conn.is_closing():
            self._conn.close()
        logger.info(f"master heartbeat connection lost: {exc}")


class ChildHeartbeatConnection(asyncio.Protocol):
    """Heartbeat connection protocol for child processes to send heartbeats to master."""

    def __init__(self, options: HealthyOptions) -> None:
        """Initialize child heartbeat connection with monitoring options.

        Args:
            options: Configuration options for heartbeat monitoring
        """
        super().__init__()
        # Transport to the master; set in ``connection_made``.
        self._conn: Optional[asyncio.Transport] = None
        # Heartbeat sender task; created in ``connection_made``.
        self._task: Optional[asyncio.Task] = None
        self._is_running = True
        self.process_type: ProcessType = options.process_type
        self._heartbeat_interval: int = options.heartbeat_interval
        self.name: str = options.name

    def connection_made(self, transport: asyncio.Transport) -> None:
        """Called when connection is established with master process.

        Args:
            transport: The transport object for the connection
        """
        self._conn = transport
        if self._task is None:
            self._task = asyncio.create_task(self._send())

    async def _send(self) -> None:
        """Continuously send heartbeat messages to master process."""
        while self._is_running:
            await asyncio.sleep(self._heartbeat_interval)
            pack_data = struct.pack('>H', MAGIC_VAL)
            pack_data += struct.pack('>I', os.getpid())
            current_recv_time = time.time()
            pack_data += struct.pack('>I', int(current_recv_time))
            if self._conn and not self._conn.is_closing():
                self._conn.write(pack_data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Handle connection loss with master process.

        Args:
            exc: Exception that caused connection loss, if any
        """
        if self._task:
            self._is_running = False
            self._task.cancel()
        if self._conn and self._conn.is_closing():
            self._conn.close()
        logger.info(f"child heartbeat connection lost: {exc} for {self.process_type} in {self.name}")


class HealthyChecker:
    """Manages healthy checking for multiple process types and their instances."""

    def __init__(self):
        """Initialize the HealthyChecker with empty options dictionary."""
        self._options: Dict[ProcessType, List[HealthyOptions]] = {}

    @property
    def options(self) -> Dict[ProcessType, List[HealthyOptions]]:
        """Get all healthy checking options by process type.

        Returns:
            Dictionary mapping process types to their healthy options
        """
        return self._options

    def make_options(self, process_type: ProcessType, name: str) -> None:
        """Create a new HealthyOptions instance for monitoring.

        Args:
            process_type: The process type to monitor
            name: The name identifier for the process
        """
        if process_type not in self._options:
            self._options[process_type] = []
        pipe = create_pipe(PipeKind.TCP)
        pipe[0].setblocking(False)
        pipe[1].setblocking(False)
        master_pipe = PipeConnection(pipe, PipeEnd.LEFT)
        child_pipe = PipeConnection(pipe, PipeEnd.RIGHT)
        heartbeat_interval = TrpcConfig.server_config().heartbeat_interval
        self._options[process_type].append(
            HealthyOptions(process_type=process_type,
                            name=name,
                            master_pipe=master_pipe,
                            child_pipe=child_pipe,
                            heartbeat_interval=heartbeat_interval,
                            max_check_interval=heartbeat_interval * 8))


_init_healthy_checker: Optional[HealthyChecker] = None


def init_healthy_checker(task_list: List[TaskInfo]) -> None:
    """Initialize the global healthy checker for this worker's task children.

    Args:
        task_list: Task records this worker is about to fork. Heartbeat
            pipes must exist before that fork so the children inherit them.
    """
    if not TrpcConfig.server_config().heartbeat_interval:
        logger.debug(f"heartbeat_interval is not set")
        return

    global _init_healthy_checker
    if _init_healthy_checker is None:
        _init_healthy_checker = HealthyChecker()

    for task in task_list:
        _init_healthy_checker.make_options(ProcessType.TASK, task.name)


def _all_options() -> List[HealthyOptions]:
    """Every heartbeat pipe built by :func:`init_healthy_checker`, in creation order."""
    if _init_healthy_checker is None:
        return []
    options: List[HealthyOptions] = []
    for per_type in _init_healthy_checker.options.values():
        options.extend(per_type)
    return options


def start_heartbeat_monitor(loop: asyncio.AbstractEventLoop) -> None:
    """Watch every task's heartbeat on ``loop``; call in the worker that forked them.

    Runs after the last fork, so no child inherits the monitor's transports.
    Drops the child end of each pipe first: holding it would keep the
    monitor's end from reaching EOF when that task dies. A no-op when
    ``heartbeat_interval`` is unset.

    Args:
        loop: The worker's event loop, created but not yet running.
    """
    for option in _all_options():
        option.master_pipe.close_extra_conn()
        coro = loop.create_connection(partial(MasterHeartbeatConnection, option),
                                      sock=option.master_pipe.get_socket())
        loop.run_until_complete(coro)


def close_heartbeat_pipes(keep: Optional[str] = None) -> Optional[HealthyOptions]:
    """Close the heartbeat pipes this process inherited but does not own.

    Every child forked after :func:`init_healthy_checker` inherits both ends
    of every heartbeat pipe. A task keeps the child end of its own pipe and
    closes everything else; any other child (an rpc task, for instance)
    closes all of them.

    Args:
        keep: Name of the task whose child end stays open, or ``None`` to
            close every pipe.

    Returns:
        The options for ``keep``, with its monitor end already closed, or
        ``None`` when nothing was kept.
    """
    kept: Optional[HealthyOptions] = None
    for option in _all_options():
        if keep is not None and option.name == keep:
            option.child_pipe.close_extra_conn()
            kept = option
            continue
        option.child_pipe.socket_pair[0].close()
        option.child_pipe.socket_pair[1].close()
    return kept


def _start_heartbeat_sender() -> None:
    """Send heartbeats from this task process to the worker that forked it.

    Registered as a ``TASK`` child callback, so it runs in every task after
    its loop is created and before plugins are initialized.
    """
    if not TrpcConfig.server_config().heartbeat_interval:
        return
    name = getattr(multiprocessing.current_process(), "name", "")
    option = close_heartbeat_pipes(keep=name)
    if option is None:
        logger.error(f"heartbeat pipe not found for task {name}")
        return
    logger.info(f"start heartbeat sender in task {name}")
    loop = get_or_create_event_loop()
    coro = loop.create_connection(partial(ChildHeartbeatConnection, option), sock=option.child_pipe.get_socket())
    loop.run_until_complete(coro)


@register_plugin(PluginType.USER_DEFINED, "healthy_checker", init_pos=PluginInitPos.WORKER)
class HealthyCheckerPlugin(BasePlugin):
    """Placeholder kept so configs naming ``healthy_checker`` still load.

    Heartbeat monitoring no longer goes through a plugin: the worker that
    forks the tasks starts the monitor itself after the last fork (see
    :func:`start_heartbeat_monitor`), and each task starts its sender from a
    child callback.
    """


register_child_callback(_start_heartbeat_sender, ProcessType.TASK)
