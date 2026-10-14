# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants of the server listen transports."""

from __future__ import annotations

# Metadata key under which ``TrpcServerTransport._msg_handler`` stores the
# ``ServerConnection`` a request arrived on (the :class:`DefaultIoConnection` for TCP,
# the per-datagram peer for UDP), so handlers can reach it.
SERVER_CONN_KEY = "_trpc_server_conn"

# Seconds the accept loop stays paused after the process ran out of file
# descriptors or kernel memory. Accepting again immediately would just spin on
# the same error, since the listen socket stays readable the whole time.
ACCEPT_RETRY_DELAY = 1.0

# Sockets accepted per readable event before yielding back to the loop. Bounds
# how long one burst of connections can starve the rest of the main loop.
# Matched to the default ``BaseServerTransportOption.backlog`` so one readable
# event can drain the kernel queue.
ACCEPT_BURST = 1024
