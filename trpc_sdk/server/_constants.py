# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for tRPC-Python Server Module."""

from __future__ import annotations

from trpc_sdk.runtime import MODE_MERGE


# ServerOption defaults (YAML ``server:``, one per Server process).

# Process model. merge keeps a request in one process; separate forks rpc
# tasks to run handlers, which is what a CPU-bound service needs; peer runs
# N equal worker processes.
DEFAULT_MODE = MODE_MERGE
# Processes of separate mode (rpc tasks) and peer mode (workers, worker-0
# included); merge does not read it. ``0`` means
# ``trpc_sdk.config.default_worker_num()``, read where it is used, never at import.
DEFAULT_WORKER_NUM = 0
# Thread model of a merge-mode worker: "merge", "separate", or "" to infer it
# from ``handle_thread_num``.
DEFAULT_IO_HANDLE_TYPE = ""
# Serving threads of a worker. In merge they do both I/O and handlers; 1
# keeps everything on the main thread.
DEFAULT_IO_THREAD_NUM = 1
# Handle threads of the separate thread model; 0 means not configured.
DEFAULT_HANDLE_THREAD_NUM = 0
# Unary requests in flight per serving loop; 0 means unlimited.
DEFAULT_HANDLER_MAX_CONCURRENCY = 0
# Per-loop FIFO capacity once the in-flight limit is reached.
DEFAULT_HANDLER_QUEUE_SIZE = 1024
# Set SO_REUSEPORT on the TCP listen sockets of merge and separate mode (and on
# merge-mode HTTP). Off by default so a second instance on the same port fails
# to bind instead of silently sharing.
DEFAULT_TCP_REUSE_PORT = False
# Milliseconds every serving / I/O thread, or every separate- or peer-mode
# child, gets to finish its startup (task pipes, ``on_start`` hooks) before
# the process gives up. YAML ``0`` means this default.
DEFAULT_READY_TIMEOUT_MS = 300000

# Thresholds above which a configured count is reported as probably
# misconfigured; the value is still used.

# ``global.processmodel.io_num`` / ``handle_num`` threads per CPU core (single_process).
THREADS_PER_CPU_WARN = 4
# ``global.processmodel.handle_num`` processes per CPU (multi_process).
WORKERS_PER_CPU_WARN = 2
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
# Processes of a task that sets no ``task_num`` itself (YAML
# ``global.processmodel.task_num``).
DEFAULT_TASK_NUM = 1
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
