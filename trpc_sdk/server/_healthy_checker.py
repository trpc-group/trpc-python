# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Healthy checker module for monitoring process health and communication"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import struct
import threading
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
from trpc_sdk.common import ProcessType
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.common import register_child_callback
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin
from ._constants import MAGIC_VAL
from ._constants import PKG_LEN
from ._constants import PipeEnd
from ._constants import PipeKind
from ._pipe import PipeConnection
from ._pipe import create_pipe
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


def _run_checker_thread_in_master() -> None:
    """Run the healthy checker thread in the master process.

    Continuously monitors all child processes for heartbeat messages and handles timeouts.
    """
    global _init_healthy_checker
    while _init_healthy_checker is None:
        # Wait for initialization to complete
        time.sleep(1)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    all_processes: List[HealthyOptions] = []
    for v in _init_healthy_checker.options.values():
        all_processes.extend(v)
    for process in all_processes:
        process.master_pipe.close_extra_conn()
        coro = get_or_create_event_loop().create_connection(partial(MasterHeartbeatConnection, process),
                                                            sock=process.master_pipe.get_socket())
        loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except (SystemExit, KeyboardInterrupt):
        loop.stop()


@register_plugin(PluginType.USER_DEFINED, "healthy_checker", init_pos=PluginInitPos.MASTER)
class HealthyCheckerPlugin(BasePlugin):
    """Plugin for managing healthy checking functionality in TRPC framework."""

    def __init__(self):
        """Initialize the HealthyCheckerPlugin with no active thread."""
        # Daemon thread started once by ``on_master_start``; stays ``None`` when
        # ``heartbeat_interval`` is unset.
        self.checker_thread: Optional[threading.Thread] = None

    def on_master_start(self) -> None:
        """Called when master process starts - initializes the healthy checker thread."""
        if not TrpcConfig.server_config().heartbeat_interval:
            logger.debug(f"heartbeat_interval is not set")
            return
        if self.checker_thread is not None:
            return
        logger.info(f"open checker thread in master")
        self.checker_thread = threading.Thread(target=_run_checker_thread_in_master)
        self.checker_thread.daemon = True
        self.checker_thread.start()

    def on_worker_start(self) -> None:
        """Called when worker process starts - placeholder method (no initialization needed)."""
        pass


def _run_checker_thread_in_worker(type: ProcessType) -> None:
    """Run the healthy checker thread in worker/dispatcher/task processes.

    Args:
        type: The type of process (WORKER, DISPATCHER, TASK)
    """
    heartbeat_interval = TrpcConfig.server_config().heartbeat_interval
    if not heartbeat_interval:
        logger.debug(f"heartbeat_interval is not set")
        return
    current_process = multiprocessing.current_process()
    name = getattr(current_process, "name", "")
    options: List[HealthyOptions] = _init_healthy_checker.options[type]
    target_option = None
    for option in options:
        if option.name in name:
            target_option = option
            break
    if target_option is None:
        logger.error(f"process {type} not found in worker {name}")
        return
    target_option.child_pipe.close_extra_conn()
    logger.info(f"open checker thread in worker {name}")
    #                                                   sock=target_option.child_pipe.get_socket())
    coro = get_or_create_event_loop().create_connection(partial(ChildHeartbeatConnection, target_option),
                                                        sock=target_option.child_pipe.get_socket())
    get_or_create_event_loop().run_until_complete(coro)


register_child_callback(partial(_run_checker_thread_in_worker, ProcessType.WORKER),
                                  ProcessType.WORKER)
register_child_callback(partial(_run_checker_thread_in_worker, ProcessType.DISPATCHER),
                                  ProcessType.DISPATCHER)
register_child_callback(partial(_run_checker_thread_in_worker, ProcessType.TASK),
                                  ProcessType.TASK)
