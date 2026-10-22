# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for the tRPC-Python runtime (process and thread model)."""

from __future__ import annotations

from enum import Enum
from enum import unique


@unique
class Network(str, Enum):
    """ServiceOption listen-address network names from YAML.

    Matched with ``str.find`` against ``service.network``, which may be a
    composite such as ``tcp,http``. TCP and HTTP both use stream sockets;
    UDP uses datagram sockets.
    """

    TCP = "tcp"
    UDP = "udp"
    HTTP = "http"


@unique
class AddressKind(Enum):
    """Which listener serves one listen address.

    Decided by :func:`~trpc_sdk.runtime._listeners.classify_address`, in the
    order the members are listed: a registered user protocol first, then the
    network type. Every mode uses the same decision; they differ only in
    *which process* listens on each kind.
    """

    # A starter registered with ``register_protocol_handle`` binds it.
    USER = "user"
    # tRPC frames over a stream socket; cut into frames by a parser.
    TCP = "tcp"
    # tRPC over datagrams; each serving loop binds with SO_REUSEPORT.
    UDP = "udp"
    # aiohttp application.
    HTTP = "http"
    # No listener matches; a configuration error.
    UNKNOWN = "unknown"


# Process models for ServerOption.mode, translated from YAML `global.processmodel`.
# merge: the worker accepts and runs handlers in one process. Its thread model
# comes from `io_thread_num` / `handle_thread_num`; see `trpc_sdk/server/_option_resolver.py`.
MODE_MERGE = "merge"
# separate: the worker only does I/O and forwards frames to `worker_num`
# rpc task processes, which is where business code runs.
MODE_SEPARATE = "separate"
# peer: `worker_num` equal worker processes, each binding every endpoint with
# SO_REUSEPORT. The process the application started is worker-0 and forks
# the others; nothing is forwarded between them.
MODE_PEER = "peer"

# Thread models of a merge-mode worker (ServerOption.io_handle_type). merge:
# every serving thread does I/O and runs handlers. separate: I/O loops hand
# requests to dedicated handle threads. Also the values of YAML
# `global.processmodel.io_handle_type`.
IO_HANDLE_TYPE_MERGE = "merge"
IO_HANDLE_TYPE_SEPARATE = "separate"

# Accepted spellings of a mode or `io_handle_type` that are not the canonical
# name, mapped to it.
MODE_SPELLINGS = {"seperate": MODE_SEPARATE}

# Values of YAML `global.processmodel.mode`.
# single_process: merge mode, threads from `io_num` / `handle_num`.
# multi_process: separate mode (`io_handle_type: separate`) or peer mode
# (`io_handle_type: merge`, also spelled `peer`).
PROCESS_MODEL_SINGLE = "single_process"
PROCESS_MODEL_MULTI = "multi_process"

# Logged once by worker-0 before forking; SO_REUSEPORT cannot tell a second
# instance from a sibling worker.
PEER_STARTUP_WARNING = ("peer mode: %s workers, all endpoints use SO_REUSEPORT; "
                        "a second instance on this host will bind silently")

# Environment variables for feature switches.

# Environment variable name. An exact "1" enables the native path; anything
# else (including "true") is off.
ENV_C_TASK = "TRPC_C_TASK"
ENV_ENABLED = "1"

# Heartbeat IPC between master and child processes.
# Packed as big-endian: magic (2 bytes) + pid (4) + unix timestamp (4).

# Frame header identifier; distinguishes heartbeat packets from JSON payloads.
MAGIC_VAL = 0x1234
# Packed heartbeat frame length in bytes.
PKG_LEN = 10

# Producer name of the worker process itself when it serves on a single loop.
# Merge mode with several loop threads numbers them ``worker:0``, ``worker:1``,
# and so on, because each loop needs its own socketpair to a task: an asyncio
# transport may only be written from the loop that created it.
WORKER_PRODUCER_PREFIX = "worker"
# Producer name prefix of the rpc task processes a separate-mode worker forks.
RPC_TASK_PRODUCER_PREFIX = "rpc_task"

# ``SchedulerParams.target`` values of the separate thread model, so one
# custom selector can tell its three choices apart.
# A new TCP connection picks an I/O loop.
SCHEDULE_TARGET_IO_CONN = "io_conn"
# A unary request picks a handle thread.
SCHEDULE_TARGET_HANDLE_REQ = "handle_req"
# A new HTTP connection picks a handle thread.
SCHEDULE_TARGET_HANDLE_CONN = "handle_conn"
# ``server.mode: separate``: one frame picks an rpc task process.
SCHEDULE_TARGET_RPC_TASK = "rpc_task"

# Thread lifecycle.

# Seconds all serving threads get, together, to run their stop hooks and exit
# once the main loop stopped. A thread still busy then is left behind; it is a
# daemon, so it does not keep the process alive.
LOOP_THREAD_STOP_TIMEOUT = 5.0

# Process lifecycle.

# Seconds between polls of the task-ready counter while the worker waits for
# ``io_wait_count`` notifications, before it binds any address.
IO_WAIT_POLL_INTERVAL = 1.0
# Seconds a child gets to shut down after noticing its parent died, before it
# is killed with ``os._exit``. Long enough for a normal SIGTERM path, short
# enough that a task stuck in inference does not hold a GPU for long.
PARENT_EXIT_GRACE_SECONDS = 5.0
# Seconds a separate-mode child gets, past its drain deadline, before the I/O
# process kills it with SIGKILL. Also the whole budget of a
# task process, and of every child when startup is aborted.
CHILD_EXIT_GRACE_SECONDS = 5.0
# Seconds between checks of whether the separate-mode children have exited.
CHILD_EXIT_POLL_INTERVAL = 0.05
# Seconds between in-flight checks while draining. Reserved for draining until
# nothing is in flight, which separate mode does not do yet: it waits the full
# ``close_wait_time``.
DRAIN_POLL_INTERVAL = 0.05
