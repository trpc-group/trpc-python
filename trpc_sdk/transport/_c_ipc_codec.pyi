# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the Cython inner-IPC codec.

Production dispatcher / worker / task paths import these symbols from
``trpc_sdk.transport._c_ipc_codec``. The readable twin is
:mod:`trpc_sdk.transport._ipc_codec`, which covers only ``magic + length``.
"""

from __future__ import annotations

from typing import List
from typing import Tuple

_UDP_PACKAGE_HEADER_LEN: int
_GLOBAL_MAX_IPC_SIZE: int


class InnerStreamParser:
    """Split a byte stream into complete inner-IPC frames.

    A frame is magic (ushort) plus total length (uint32) followed by the
    payload. ``read`` returns every complete package currently buffered;
    an incomplete tail stays in the buffer across ``feeds_data``.
    """

    def feeds_data(self, data: bytes) -> None:
        """Append ``data`` to the growable parse buffer.

        Args:
            data: Incremental stream bytes from the IPC pipe.
        """
        ...

    def read(self) -> List[bytes]:
        """Return every complete frame and compact leftover bytes.

        Returns:
            Complete packages including the inner header. Empty when the
            buffer has no full frame yet.

        Raises:
            ServerDecodeError: The leading magic is not ``INNER_MAGIC_VALUE``.
        """
        ...

    def reset(self) -> None:
        """Drop buffered bytes without freeing the allocation."""
        ...


class InnerDatagramParser:
    """Read ``DataHeader.flags`` from one C-dispatcher UDP datagram.

    One datagram is one frame; this checker does not reassemble chunks.
    """

    def feeds_data(self, data: bytes) -> int:
        """Return the UDP package flag from the datagram header.

        Does not buffer and does not check magic.

        Args:
            data: One complete UDP IPC datagram.

        Returns:
            A :class:`~trpc_sdk.transport._types.UDPPackageType` value.
        """
        ...

    def reset(self) -> None:
        """Reset parser bookkeeping. The datagram path has no buffer."""
        ...


class PipeDataHeader:
    """Decoded ``DataHeader`` plus body for one C-dispatcher datagram.

    Fields are flattened for Python. ``body`` starts at
    ``sizeof(DataHeader)`` and runs to ``header.len``.
    """

    fd: int
    session_id: int
    recv_time: float
    port_id: int
    worker_id: int
    magic_num: int
    body: bytes


def encode_worker_msg_req(from_worker: int, data: bytes) -> bytes:
    """Encode one inter-worker broadcast frame.

    Layout is magic (2) + total length (4) + ``from_worker`` (4) + payload.

    Args:
        from_worker: Sender worker id written after the inner header.
        data: Payload bytes, not including a header.
    """
    ...


def decode_worker_msg_req(data: bytes) -> Tuple[int, bytes]:
    """Decode one inter-worker broadcast frame.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(from_worker, payload)``.
    """
    ...


def inner_encode_req(
    fd: int,
    port: int,
    session_id: int,
    recv_time: int,
    remote_ip: bytes,
    data: bytes,
) -> bytes:
    """Encode one stream-IPC request from the Python dispatcher.

    The ``+28`` prefix is the historical hand-packed layout, not
    ``sizeof(DataHeader)``.

    Args:
        fd: Client connection fd forwarded to the worker.
        port: Listen port that accepted the client.
        session_id: Connection-lifetime id of the client TCP.
        recv_time: Receive timestamp as an integer microsecond count.
        remote_ip: Packed peer address bytes.
        data: Request body.
    """
    ...


def inner_encode_data_header_req(
    fd: int,
    port: int,
    session_id: int,
    recv_time: int,
    remote_ip: bytes,
    data: bytes,
) -> bytes:
    """Encode one datagram-IPC request using ``DataHeader``.

    Args:
        fd: Client connection fd forwarded to the worker.
        port: Unused in the packed header; kept for call-site symmetry.
        session_id: Connection-lifetime id of the client TCP.
        recv_time: Receive timestamp written into ``DataHeader.recv_time``.
        remote_ip: Unused in the packed header; kept for call-site symmetry.
        data: Request body placed after the ``DataHeader``.
    """
    ...


def inner_decode_req(data: bytes) -> Tuple[int, int, int, int, bytes, bytes]:
    """Decode one stream-IPC request.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(fd, port, session_id, recv_time, remote_ip, body)``.
    """
    ...


def inner_decode_data_header_req(data: bytes) -> PipeDataHeader:
    """Decode one datagram-IPC request into a :class:`PipeDataHeader`.

    Args:
        data: One complete UDP IPC datagram.
    """
    ...


def inner_encode_rsp(fd: int, session_id: int, timeout: int, data: bytes) -> bytes:
    """Encode one stream-IPC response from a worker to the dispatcher.

    The ``+18`` prefix is magic, len, fd, session, and an unused timeout
    slot kept for the historical frame layout.

    Args:
        fd: Client connection fd the dispatcher uses to look up the TCP.
        session_id: Connection-lifetime id that must match before write.
        timeout: Unused timeout slot kept for the historical frame layout.
        data: Response body.
    """
    ...


def inner_encode_data_header_rsp(
    fd: int,
    session_id: int,
    worker_id: int,
    flag: int,
    data: bytes,
) -> bytes:
    """Encode one datagram-IPC response using ``DataHeader``.

    ``flag`` is CHUNK / CHUNK_END so the C dispatcher can reassemble.

    Args:
        fd: Client connection fd forwarded back to the C dispatcher.
        session_id: Session id used to reassemble CHUNK / CHUNK_END.
        worker_id: Worker id written into the header.
        flag: A :class:`~trpc_sdk.transport._types.UDPPackageType` value.
        data: One datagram body, possibly a slice of a larger response.
    """
    ...


def inner_decode_rsp(data: bytes) -> Tuple[int, int, int, bytes]:
    """Decode one stream-IPC response.

    Args:
        data: A complete inner-IPC package.

    Returns:
        ``(fd, session_id, timeout, body)``.
    """
    ...
