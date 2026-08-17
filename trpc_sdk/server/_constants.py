# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for tRPC-Python Server Module."""

# Which end of a socketpair this process owns.
# LEFT_PORT uses socket_pair[0]; RIGHT_PORT uses socket_pair[1].
LEFT_PORT = 1
RIGHT_PORT = 2

# Socketpair protocol type passed to create_pipe().
# TCP_PIPE -> SOCK_STREAM; UDP_PIPE -> SOCK_DGRAM (used by C dispatcher).
TCP_PIPE = 1
UDP_PIPE = 2

# ServiceOption / listen-address network names (substring-matched in YAML).
# TCP and HTTP both use stream sockets; UDP uses datagram sockets.
NETWORK_TCP = "tcp"
NETWORK_UDP = "udp"
NETWORK_HTTP = "http"

# Process models for ServerOption.mode (YAML server.mode).
# dispatcher: one process listens/accepts and forwards frames to workers.
MODE_DISPATCHER = "dispatcher"
# multi-listener: each worker listens itself; no dedicated dispatcher process.
MODE_MULTI_LISTENER = "multi-listener"

# Environment variables for feature switches.

# Environment variable names. An exact "1" enables the native path; anything
# else (including "true") is off.
ENV_C_DISPATCHER = "TRPC_C_DISPATCHER"
ENV_USE_TRPCIO = "TRPC_USE_TRPCIO"
ENV_C_TASK = "TRPC_C_TASK"
ENV_ENABLED = "1"

# ServerOption defaults (YAML ``server:``, one per Server process).

# Process model: dispatcher (one listener + N workers) or multi-listener.
DEFAULT_MODE = MODE_DISPATCHER
# Worker (or executor) process count. YAML default is CPU core count.
DEFAULT_WORKER_NUM = 1
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
# Queue-wait circuit-breaker policy (0 = off; see YAML queue_wait_policy).
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
