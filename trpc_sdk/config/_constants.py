# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Default values for framework config and the config decoder."""

from __future__ import annotations

# Default path for the bind-info dump file written when save_bind_info is enabled.
DEFAULT_BIND_INFO_FILE: str = "./bind_info.log"

# Rpcz samples a span when span_id % N == 0. 0 disables sampling.
DEFAULT_RPCZ_SAMPLE_RATE: int = 1000
# Per-second sampling low water mark. Sampling stays on while the rate is below this.
DEFAULT_RPCZ_LOWER_WATER_LEVEL: int = 50
# Per-second sampling high water mark. Sampling pauses once the rate exceeds this.
DEFAULT_RPCZ_HIGH_WATER_LEVEL: int = 200
# Interval at which rpcz collects sampling data, in milliseconds.
DEFAULT_RPCZ_COLLECT_INTERVAL_MS: int = 500
# Interval at which rpcz drops expired sampling data, in milliseconds.
DEFAULT_RPCZ_REMOVE_INTERVAL_MS: int = 1000
# Default number of summary spans returned by the admin rpcz endpoint.
DEFAULT_RPCZ_PRINT_SPANS_NUM: int = 10
# Span cache expiration time, in seconds.
DEFAULT_RPCZ_CACHE_EXPIRE_INTERVAL_S: int = 60

# Stream flow-control receive window size. 65535 is the 16-bit unsigned maximum.
DEFAULT_STREAM_MAX_WINDOW_SIZE: int = 65535

# Timeout ratio threshold. The queue circuit breaker trips when the fraction of
# timed-out requests in the current window exceeds this value.
DEFAULT_QUEUE_CB_RATIO: float = 0.1
# Minimum sample count before queue circuit-breaker statistics take effect.
DEFAULT_QUEUE_CB_THRESHOLD: int = 100
# Queue circuit-breaker statistics window size, in milliseconds (1 minute).
DEFAULT_QUEUE_CB_INTERVAL_MS: int = 60 * 1000

# Max debug-info lines written by memory inspection. -1 means unlimited.
DEFAULT_MEMORY_INSPECTION_LINE_LIMIT: int = 50
# Filename prefix for memory-inspection debug dumps.
DEFAULT_MEMORY_INSPECTION_FILENAME_PREFIX: str = "tracemalloc"

# Worker count used when neither the cgroup CPU quota nor the CPU count is known.
DEFAULT_WORKER_NUM_FALLBACK: int = 2
# Server read-buffer size, 8 MiB.
DEFAULT_MAX_READ_BUF_SIZE: int = 8 * 1024 * 1024
# Task-process batch wait duration, in milliseconds.
DEFAULT_TASK_WAIT_INTERVAL_MS: int = 5
# Task-process queue capacity.
DEFAULT_TASK_QUEUE_SIZE: int = 1000

# Default client transport network type.
DEFAULT_CLIENT_NETWORK: str = "tcp"
# Default client application protocol.
DEFAULT_CLIENT_PROTOCOL: str = "trpc"
# Connection establishment timeout, in milliseconds.
DEFAULT_CLIENT_DIAL_TIMEOUT_MS: float = 200.0
# Idle connection timeout, in seconds.
DEFAULT_CLIENT_IDLE_TIME_S: float = 60.0
# Maximum number of idle connections kept in the client pool.
DEFAULT_CLIENT_MAX_IDLE_CONN: int = 5

# Default text encoding for decoder ``code`` option when unset.
DEFAULT_ENCODING: str = "utf8"

# Socket-family label stored on service config (``socket_type``). This is the
# YAML/string value compared by callers, not ``socket.AF_UNIX``.
UNIX_TYPE: str = "unix"
