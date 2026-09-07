# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Unix-domain socketpairs used as IPC pipes between server processes.

Every pipe is created in the master **before fork**, so both ends are inherited
by the children. After fork each side owns one end and must drop the other:
until the unused fd is closed the kernel still counts a live reference and the
peer never observes EOF when the process on the other side dies.

The master builds one such pipe per relationship it needs: dispatcher to each
worker, a side channel per worker for RPCZ spans, worker to each task process,
and a broadcast pipe per worker for inter-worker messages.
"""

from __future__ import annotations

import socket
from typing import Optional
from typing import Tuple

from trpc_sdk.log import logger
from ._constants import PipeEnd
from ._constants import PipeKind


def create_pipe(proto: PipeKind = PipeKind.TCP) -> Tuple[socket.socket, socket.socket]:
    """Create a Unix-domain socket pair used as an IPC pipe.

    Args:
        proto: Pipe type. ``PipeKind.TCP`` creates a stream socket pair;
            ``PipeKind.UDP`` creates a datagram socket pair. Defaults to
            ``PipeKind.TCP``.

    Returns:
        A 2-tuple of connected sockets ``(left, right)``.
    """
    if proto == PipeKind.UDP:
        socket_pair = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM, 0)
    elif proto == PipeKind.TCP:
        socket_pair = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    return socket_pair


class PipeConnection:
    """One endpoint of a Unix-domain socketpair used for IPC after fork.

    ``process_type`` selects which end this process owns:
    ``PipeEnd.LEFT`` uses ``socket_pair[0]``, ``PipeEnd.RIGHT`` uses
    ``socket_pair[1]``. After fork, call ``close_extra_conn()`` so this
    process drops the unused end; otherwise the kernel still sees a live fd
    and the peer never gets EOF.
    """

    def __init__(self, socket_pair: Tuple[socket.socket, socket.socket], process_type: PipeEnd) -> None:
        """Bind this process to one end of ``socket_pair``.

        Args:
            socket_pair: 2-tuple of connected sockets from ``create_pipe()``.
            process_type: ``PipeEnd.LEFT`` or ``PipeEnd.RIGHT``.
        """
        if socket_pair is None or len(socket_pair) != 2:
            raise ValueError("socket_pair must be a 2-tuple of connected sockets")

        # The end this process owns. Declared Optional because every method
        # re-checks it, but a successfully constructed instance always has one.
        self.process_socket: Optional[socket.socket] = None
        # Both ends, kept so ``close_extra_conn`` can drop the peer's fd.
        self.socket_pair: Tuple[socket.socket, socket.socket] = socket_pair
        # ``PipeEnd.LEFT`` or ``PipeEnd.RIGHT``; selects which end is owned.
        self.process_type: PipeEnd = process_type

        if self.process_type == PipeEnd.LEFT:
            self.process_socket = socket_pair[0]
        elif self.process_type == PipeEnd.RIGHT:
            self.process_socket = socket_pair[1]
        else:
            raise ValueError(f"invalid process_type: {process_type}")

    def get_socket(self) -> socket.socket:
        """Return the socket this process owns."""
        return self.process_socket

    def send(self, data: bytes) -> None:
        """Send ``data`` on this process's end of the pipe.

        Args:
            data: Bytes to write.
        """
        if self.process_socket is None:
            raise Exception("None process_socket in pipe connection")
        try:
            self.process_socket.sendall(data)
        except BlockingIOError:
            logger.warning("send data to pipe failed: %s", data)

    def recv(self, length: int) -> Optional[bytes]:
        """Receive up to ``length`` bytes from this process's end of the pipe.

        Args:
            length: Maximum number of bytes to read.

        Returns:
            Bytes read, or ``None`` if the non-blocking socket has no data.
        """
        if self.process_socket is None:
            raise Exception("None process_socket in pipe connection")
        try:
            return self.process_socket.recv(length)
        except BlockingIOError:
            return None

    def fileno(self) -> int:
        """Return the fd of this process's socket, for poll/select."""
        if self.process_socket is None:
            raise Exception("None process_socket in pipe connection")
        return self.process_socket.fileno()

    def close_extra_conn(self) -> None:
        """Close the unused end inherited across fork.

        LEFT keeps ``[0]`` and closes ``[1]``; RIGHT keeps ``[1]`` and closes ``[0]``.
        """
        try:
            if self.process_type == PipeEnd.LEFT:
                self.socket_pair[1].close()
            elif self.process_type == PipeEnd.RIGHT:
                self.socket_pair[0].close()
        except Exception as err:  # pylint: disable=broad-except
            logger.warning("close extra pipe conn failed: %s", err)

    def close(self) -> None:
        """Close this process's end of the pipe."""
        if self.process_socket is None:
            raise Exception("None process_socket in pipe connection")
        self.process_socket.close()
