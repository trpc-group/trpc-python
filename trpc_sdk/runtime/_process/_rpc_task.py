# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Handler process forked by the separate-mode I/O worker."""

from __future__ import annotations

import sys
import traceback
from functools import partial
from multiprocessing import Process
from typing import TYPE_CHECKING
from typing import Any
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import ProcessType
from trpc_sdk.log import flush_logs
from trpc_sdk.log import logger
from trpc_sdk.transport import RpcTaskConnection
from trpc_sdk.utils import PipeEnd
from .._constants import AddressKind
from .._constants import RPC_TASK_SENDER_PREFIX
from .._listeners import classify_address
from .._listeners import create_http_msg_handle
from .._listeners import http_runner_factory
from .._threadmodel import HandlerThreadInfo
from .._threadmodel import ThreadModel
from .._threadmodel import run_start_hooks
from .._threadmodel import run_stop_hooks
from .._threadmodel import set_current_handler_thread
from .._worker import BaseWorker

if TYPE_CHECKING:
    from ._separate_worker import RpcTaskPipes


def rpc_task_sender_name(index: int) -> str:
    """Return the task-mesh sender name of one rpc task process."""
    return f"{RPC_TASK_SENDER_PREFIX}:{index}"


class RpcTaskProcess(BaseWorker, Process):
    """Decode frames and run handlers for a separate-mode I/O worker.

    ``TCP`` frames arrive over the request pipe. ``UDP``, ``HTTP`` and
    ``USER`` addresses are listened on in this process with ``SO_REUSEPORT``.
    """

    _main_loop_only = True

    def __init__(self, index: int, pipes: List[RpcTaskPipes], ready_flags: Any, adapter) -> None:
        """Record this child's index and pre-fork pipes; bind nothing yet."""
        super().__init__(f"rpc_task-{index}", ProcessType.WORKER)
        self._bind_process_info(adapter)

        self.index: int = index
        self.worker_id = index
        self.rpc_task_num: int = len(pipes)
        self._all_pipes: List[RpcTaskPipes] = pipes
        self.worker_pipe_conn, self.rpcz_pipe_conn = pipes[index].endpoints(PipeEnd.LEFT)
        self._ready_flags = ready_flags
        self._controlled = True
        self.drain_seconds = max(0, int(getattr(adapter.opts, "close_wait_time", 0) or 0)) / 1000.0
        # User protocol transports, destroyed after ``on_stop``.
        self._user_transports: List[BaseServerTransport] = []
        self._thread_info: Optional[HandlerThreadInfo] = None

    def _task_pipe_owners(self) -> List[str]:
        """Claim this child's sender identity in the task mesh."""
        return [rpc_task_sender_name(self.index)]

    def _init_plugins(self) -> None:
        """Initialize every plugin; exit with status 1 on failure."""
        try:
            super()._init_plugins()
        except Exception as err:  # pylint: disable=broad-except
            self._fail_startup("plugin initialization failed", err)

    def _prepare_to_serve(self) -> None:
        """Wire IPC, run hooks, bind own listeners, and publish readiness."""
        try:
            self._install_thread_model()
            self._restore_task_mesh_after_fork()
            self._close_unowned_task_pipes()
            self._close_sibling_pipes()
            self._connect_tasks()
            self._attach_request_pipes()
            self._run_start_hooks()
            if self._early_exit:
                return
            self._start_own_listeners()
            self._start_timers()
            self._ready_flags[self.index] = 1
        except Exception as err:  # pylint: disable=broad-except
            self._fail_startup("startup failed", err)

    def _build_thread_model(self) -> ThreadModel:
        """Main loop only; every rpc task binds the same addresses, so ``SO_REUSEPORT`` is always on."""
        return ThreadModel.main_loop(self.loop, worker=self, reuse_port=True)

    def _fail_startup(self, what: str, err: BaseException) -> None:
        """Log a startup failure and exit with status 1."""
        logger.error("[rpc task] RpcTaskProcess-%s %s: %r, trace: %s", self.index, what, err, traceback.format_exc())
        flush_logs()
        sys.exit(1)

    def _close_sibling_pipes(self) -> None:
        """Close every inherited pipe end this child does not own."""
        for index, pipes in enumerate(self._all_pipes):
            if index != self.index:
                pipes.close()
        self.worker_pipe_conn.close_extra_conn()
        self.rpcz_pipe_conn.close_extra_conn()

    def _attach_request_pipes(self) -> None:
        """Attach the request pipe; keep the RPCZ pipe write-only."""
        handle = partial(RpcTaskConnection,
                         app=self,
                         handlers=self.handlers,
                         timeouts=self.timeouts,
                         tran_opts=self.tran_opts)
        coro = self.loop.create_connection(handle, sock=self.worker_pipe_conn.get_socket())
        self.loop.run_until_complete(coro)

    def _run_start_hooks(self) -> None:
        """Run serving-loop start hooks for this rpc task."""
        info = HandlerThreadInfo(index=self.index, count=self.rpc_task_num, loop=self.loop)
        set_current_handler_thread(info)
        self.loop.run_until_complete(run_start_hooks(info))
        self._thread_info = info

    def _start_own_listeners(self) -> None:
        """Listen on every address but the ``TCP`` ones, on this process's loop.

        ``TCP`` addresses are the worker's, which forwards their frames here.
        The rest are served with ``SO_REUSEPORT``, since every rpc task binds
        them.
        """
        for i, _ in enumerate(self.addresses):
            kind = classify_address(self, i)
            if kind in (AddressKind.TCP, AddressKind.UNKNOWN):
                continue
            if kind is AddressKind.USER:
                self._user_transports.append(self._listen_on(i))
                continue
            if kind is AddressKind.HTTP:
                self.transports[i].set_runner_factory(http_runner_factory(self, i, create_http_msg_handle(self, i)))
            self._listen_on(i)

    def _stop_accepting(self) -> None:
        """Stop the transports this process listens with from accepting."""
        self._stop_listening()

    def _on_graceful_exit(self) -> None:
        """Stop accepting, timers and hooks, then release the transports and what they served with.

        Every step takes what it releases out of ``self`` first, so a second
        call does nothing.
        """
        self._stop_listening()
        timer, self.timer = self.timer, None
        if timer is not None:
            timer.stop()
        info, self._thread_info = self._thread_info, None
        try:
            if info is not None:
                self.loop.run_until_complete(run_stop_hooks(info))
            self.loop.run_until_complete(self._release_listeners())
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[rpc task] RpcTaskProcess-%s exit failed: %s", self.index, repr(err), exc_info=True)
        logger.info("[rpc task] RpcTaskProcess-%s exited", self.index)

    async def _release_listeners(self) -> None:
        """Destroy the user protocol transports, then release aiohttp runners and UDP endpoints; idempotent."""
        transports, self._user_transports = self._user_transports, []
        await self._destroy_transports(transports)
        if self.thread_model is not None:
            await self.thread_model.release_resources(self.thread_model.acceptor)
