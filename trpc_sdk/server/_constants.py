# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for tRPC-Python Server Module."""

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


# Process models for ServerOption.mode (YAML server.mode).
# merge: the worker accepts and runs handlers in one process, spreading
# connections over `worker_thread_num` event-loop threads.
MODE_MERGE = "merge"
# separate: the worker only does I/O and forwards frames to `rpc_task_num`
# rpc task processes, which is where business code runs.
MODE_SEPARATE = "separate"

# Environment variables for feature switches.

# Environment variable name. An exact "1" enables the native path; anything
# else (including "true") is off.
ENV_C_TASK = "TRPC_C_TASK"
ENV_ENABLED = "1"

# ServerOption defaults (YAML ``server:``, one per Server process).

# Process model. merge keeps a request in one process; separate forks rpc
# tasks to run handlers, which is what a CPU-bound service needs.
DEFAULT_MODE = MODE_MERGE
# Worker (or executor) process count. YAML default is CPU core count.
DEFAULT_WORKER_NUM = 1
# Event-loop threads inside a merge-mode worker. YAML default is CPU core
# count; 1 keeps everything on the main thread and skips the fd hand-off.
DEFAULT_WORKER_THREAD_NUM = 1
# Rpc task processes forked by a separate-mode worker. YAML default is CPU
# core count.
DEFAULT_RPC_TASK_NUM = 1
# Number of ServiceOption entries; filled as len(server.service).
DEFAULT_SERVICE_NUM = 1
# Task-process batch wait, in milliseconds.
DEFAULT_TASK_WAIT_INTERVAL = 100
# Max tasks pulled in one task-process batch.
DEFAULT_TASK_BATCH_SIZE = 1
# Task-process inbound queue capacity.
DEFAULT_TASK_QUEUE_SIZE = 1000
# Consumer threads inside each task process.
DEFAULT_TASK_CONSUMER_NUM = 1
# If True, master restarts a crashed worker/task child.
DEFAULT_REBOOT_CHILD = False
# Graceful-shutdown wait on SIGTERM, in milliseconds.
DEFAULT_CLOSE_WAIT_TIME = 0

# ServiceOption defaults (YAML ``server.service[]``, one per listen endpoint).

# Naming-service namespace: Production vs Development.
DEFAULT_NAMESPACE = "Development"
# Naming-service environment name.
DEFAULT_ENV_NAME = "defaultEnvName"
# Polaris set name; empty unless global.enable_set is Y.
DEFAULT_SET_NAME = ""
# RPC service name, e.g. trpc.app.server.Service.
DEFAULT_SERVICE_NAME = "defaultServiceName"
# Cron spec for a timer service; empty means not a timer.
# Format: "second minute hour day month weekday", e.g. "0 */5 * * * ?".
DEFAULT_TIMER_SPEC = ""
# Bind address as (host, port), or (unix_path, 0) for UNIX sockets.
DEFAULT_ADDRESS = ()
# Socket family/type from YAML (tcp / unix / ...).
DEFAULT_SOCKET_TYPE = ""
# Request timeout in milliseconds; also copied to transport idle timeout.
DEFAULT_TIMEOUT = 0
# If True, do not inherit the upstream caller's remaining timeout.
DEFAULT_DISABLE_REQUEST_TIMEOUT = False
# Queue-wait timeout in milliseconds before a request is dropped.
DEFAULT_QUEUE_WAIT_TIMEOUT = 0
# Queue-wait circuit-breaker policy (0 = QueueWaitPolicy.DEFAULT; see YAML
# queue_wait_policy). Compared with ServiceOption.queue_wait_policy.
DEFAULT_QUEUE_WAIT_POLICY = 0
# Override serialization; -1 means use the protocol default.
DEFAULT_SERIALIZATION_TYPE = -1
# Override compression; -1 means use the protocol default.
DEFAULT_COMPRESS_TYPE = -1
# Wire protocol name: "trpc", "http", ...
DEFAULT_PROTOCOL = "trpc"
# Whether a custom handler is set.
DEFAULT_HANDLER_SET = False
# Delay before registering, in milliseconds.
DEFAULT_REGISTRY_DELAY = 0
# Registry control: 0 = off, 1 = on.
DEFAULT_REGISTRY_OPTION = 0
# Stream flow-control window size; 0 means protocol default.
DEFAULT_MAX_WINDOW_SIZE = 0
# If True, run stream filters on this service.
DEFAULT_ENABLE_STREAM_FILTER = False

# Heartbeat IPC between master and child processes.
# Packed as big-endian: magic (2 bytes) + pid (4) + unix timestamp (4).

# Frame header identifier; distinguishes heartbeat packets from JSON payloads.
MAGIC_VAL = 0x1234
# Packed heartbeat frame length in bytes.
PKG_LEN = 10

# Listen / accept loop.

# Seconds the accept loop stays paused after the process ran out of file
# descriptors or kernel memory. Accepting again immediately would just spin on
# the same error, since the listen socket stays readable the whole time.
ACCEPT_RETRY_DELAY = 1.0

# Sockets accepted per readable event before yielding back to the loop. Bounds
# how long one burst of connections can starve the rest of the main loop.
# Matched to LISTEN_BACKLOG so one readable event can drain the kernel queue.
ACCEPT_BURST = 1024

# Pending-connection queue depth of the public listen sockets. Acceptor
# endpoints get their backlog from ``create_server_socket``; asyncio front
# ends pass this to ``create_server``.
LISTEN_BACKLOG = 1024

# Producer name of the worker process itself when it serves on a single loop.
# Merge mode with several loop threads numbers them ``worker:0``, ``worker:1``,
# and so on, because each loop needs its own socketpair to a task: an asyncio
# transport may only be written from the loop that created it.
WORKER_PRODUCER_PREFIX = "worker"
# Producer name prefix of the rpc task processes a separate-mode worker forks.
RPC_TASK_PRODUCER_PREFIX = "rpc_task"
