# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""IP, port, and socket helpers used by the server engine and connection pool.

Port probes here are *availability* checks, not format-only validators:
:func:`is_tcp_port_available` fails if something already accepts TCP on
that address, and :func:`is_udp_port_available` fails if bind is refused.
:func:`create_probe_socket` builds a short-lived socket for those probes
only; listen sockets are created by :func:`create_new_socket`.

The listen-socket constructors (:func:`create_new_socket`,
:func:`create_inet_socket`, :func:`create_unix_socket`) run in the child
after fork. The returned socket is already bound (and, for stream sockets,
listening) and marked non-blocking so asyncio can wrap it with
``create_server`` / ``create_datagram_endpoint``.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import struct
import sys
from contextlib import closing
from typing import Tuple

import netifaces

from trpc_sdk.common import TransportSocket
from trpc_sdk.log import logger
from ._constants import DEFAULT_BACKLOG
from ._constants import TcpState
from ._constants import UNIX_TYPE
from ._types import IpType
from ._types import PortType


# Maps ``ipaddress.IPv*Address.version`` (4 / 6) onto ``socket.AF_*``.
# Used only by :func:`create_probe_socket` for short-lived port probes;
# listen sockets are created by :func:`create_new_socket`.
_IP_TYPE_MAP = {4: socket.AF_INET, 6: socket.AF_INET6}


def int2ip(ip: int) -> str:
    """Convert an IPv4 address stored as an integer to dotted text.

    Args:
        ip: Address as a host-order integer, e.g. ``2130706433``.

    Returns:
        Dotted or compressed IP string.
    """
    return str(ipaddress.ip_address(ip))


def ip2int(ip: str) -> int:
    """Convert a textual IP address to an integer.

    Args:
        ip: Dotted IPv4 or compressed IPv6 string.

    Returns:
        Host-order integer form of ``ip``.
    """
    return int(ipaddress.ip_address(ip))


def ip_to_bytes(ip: str) -> bytes:
    """Pack a textual IP address into network-order bytes.

    Args:
        ip: Dotted IPv4 or compressed IPv6 string.

    Returns:
        4-byte IPv4 or 16-byte IPv6 packed form.
    """
    return ipaddress.ip_address(ip).packed


def bytes_to_ip(ip_bytes: bytes) -> str:
    """Decode packed IP bytes to a textual address.

    Args:
        ip_bytes: 4-byte IPv4 or 16-byte IPv6 packed form.

    Returns:
        Dotted or compressed IP string.
    """
    return str(ipaddress.ip_address(ip_bytes))


def is_ipv4(ip: IpType) -> bool:
    """Return whether ``ip`` is a valid IPv4 address.

    Invalid values are logged and treated as False rather than raised.

    Args:
        ip: Address as int, str, or packed bytes.

    Returns:
        True only when :mod:`ipaddress` parses ``ip`` as IPv4.
    """
    try:
        return ipaddress.ip_address(ip).version == 4
    except ValueError:
        logger.error(f"{ip} is not valid addr")
        return False


def is_ipv6(ip: IpType) -> bool:
    """Return whether ``ip`` is a valid IPv6 address.

    Invalid values are logged and treated as False rather than raised.

    Args:
        ip: Address as int, str, or packed bytes.

    Returns:
        True only when :mod:`ipaddress` parses ``ip`` as IPv6.
    """
    try:
        return ipaddress.ip_address(ip).version == 6
    except ValueError:
        logger.error(f"{ip} is not valid addr")
        return False


def is_unix_socket(socket_type: str) -> bool:
    """Return whether a config socket-type label is UNIX domain.

    Compares against :data:`~trpc_sdk.utils._constants.UNIX_TYPE`, not
    ``socket.AF_UNIX``.

    Args:
        socket_type: Family label from service config.

    Returns:
        True when ``socket_type`` equals :data:`UNIX_TYPE`.
    """
    return socket_type == UNIX_TYPE


def is_port_valid(port: PortType) -> bool:
    """Return whether ``port`` is an integer in ``[0, 65535]``.

    Args:
        port: Port number or a value convertible with ``int()``.

    Returns:
        True when the converted integer is a legal TCP/UDP port.
    """
    return int(port) >= 0 and int(port) <= 65535


def is_tcp_port_available(ip: IpType, port: PortType) -> bool:
    """Return whether ``(ip, port)`` can be used as a TCP listen address.

    First checks the numeric range, then probes with a TCP connect:
    a successful ``connect_ex`` means something is already listening.

    Args:
        ip: Bind address to probe.
        port: Bind port to probe.

    Returns:
        True when the port is in range and not occupied.
    """
    if not is_port_valid(port):
        logger.error(f"tcp port valid check failed, port:{port} is invalid")
        return False

    return _probe_port_available(ip, port, is_udp=False)


def is_udp_port_available(ip: IpType, port: PortType) -> bool:
    """Return whether ``(ip, port)`` can be used as a UDP bind address.

    First checks the numeric range, then probes with a UDP bind:
    bind failure means the address is not available.

    Args:
        ip: Bind address to probe.
        port: Bind port to probe.

    Returns:
        True when the port is in range and bind succeeds.
    """
    if not is_port_valid(port):
        logger.error(f"udp port valid check failed, port:{port} is invalid")
        return False

    return _probe_port_available(ip, port, is_udp=True)


def _probe_port_available(ip: IpType, port: PortType, is_udp: bool) -> bool:
    """Probe whether ``(ip, port)`` is free for TCP or UDP.

    TCP uses ``connect_ex``: return code 0 means a peer accepted the
    handshake, so the port is occupied. UDP uses ``bind``: success means
    the datagram port is free. The probe socket is always closed.

    Args:
        ip: Address to probe.
        port: Port to probe.
        is_udp: True to probe with a datagram socket; False for stream.

    Returns:
        True when the probe concludes the port is available.
    """
    protocol = "udp" if is_udp else "tcp"
    socket_tmp = None
    try:
        socket_tmp = create_probe_socket(ip, is_udp=is_udp)
        if is_udp:
            socket_tmp.bind((ip, port))
            return True
        if socket_tmp.connect_ex((ip, port)) == 0:
            logger.error(
                f"tcp port valid check failed, (ip:{ip} port:{port}) has been occupied")
            return False
        return True
    except Exception as err:  # pylint: disable=broad-except
        logger.error(
            f"{protocol} port valid check failed, (ip:{ip} port:{port}) appear err: {err}")
        return False
    finally:
        if socket_tmp:
            socket_tmp.close()


def unix_path_valid(unix_path: str) -> bool:
    """Return whether a UNIX socket path can be bound.

    Unlinks a stale socket file first. Fails only when the path still
    exists after unlink (for example a regular file that cannot be removed).

    Args:
        unix_path: Filesystem path of the UNIX socket.

    Returns:
        True when the path is clear for bind.
    """
    is_unlink = unlink_unix_socket_file(unix_path)
    if not is_unlink:
        logger.error(f"unix path valid check failed, {unix_path} has existed")
        return False
    return True


def create_probe_socket(ip: IpType, is_udp: bool) -> socket.socket:
    """Create an unbound inet socket for a port-availability probe.

    Family is taken from ``ip`` (IPv4 vs IPv6). This is not the listen
    socket used by the dispatcher; see
    :func:`create_new_socket` for that.

    Args:
        ip: Address whose version selects ``AF_INET`` / ``AF_INET6``.
        is_udp: True for ``SOCK_DGRAM``; False for ``SOCK_STREAM``.

    Returns:
        A new socket. The caller owns close.
    """
    addr = ipaddress.ip_address(ip)
    return socket.socket(_IP_TYPE_MAP[addr.version], socket.SOCK_DGRAM if is_udp else socket.SOCK_STREAM)


def create_new_socket(address: Tuple[str, int],
                      socket_type: str,
                      reuse_port: bool = False,
                      stream: bool = True,
                      backlog: int = DEFAULT_BACKLOG) -> socket.socket:
    """Create and bind the listen socket for one service address.

    Args:
        address: ``(host, port)`` for inet, or ``(unix_path, unused_port)``
            for a UNIX socket. Only ``address[0]`` is used for UNIX.
        socket_type: Socket family name; UNIX sockets are detected by
            :func:`is_unix_socket`.
        reuse_port: When True, set ``SO_REUSEPORT`` so every executor in
            multi-listener mode can bind the same inet address.
        stream: True for TCP / UNIX stream; False for UDP datagram.
        backlog: ``listen()`` backlog for stream sockets.

    Returns:
        A non-blocking, already-bound socket. Stream sockets are listening.
    """
    if is_unix_socket(socket_type):
        return create_unix_socket(address, backlog)
    return create_inet_socket(address, reuse_port, stream, backlog)


def create_inet_socket(address: Tuple[str, int],
                       reuse_port: bool = False,
                       stream: bool = True,
                       backlog: int = DEFAULT_BACKLOG) -> socket.socket:
    """Create, bind, and optionally listen on an IPv4 or IPv6 socket.

    Args:
        address: ``(host, port)`` to bind. The host decides AF_INET vs AF_INET6.
        reuse_port: When True, set ``SO_REUSEPORT``.
        stream: True for ``SOCK_STREAM`` (TCP); False for ``SOCK_DGRAM`` (UDP).
        backlog: ``listen()`` backlog; ignored for UDP.

    Returns:
        A non-blocking bound socket. TCP sockets also have NODELAY, keepalive,
        and lingering disabled, and are already listening.
    """
    sock = None
    stream_option = socket.SOCK_STREAM if stream else socket.SOCK_DGRAM
    if is_ipv4(address[0]):
        sock = socket.socket(socket.AF_INET, stream_option)
    elif is_ipv6(address[0]):
        sock = socket.socket(socket.AF_INET6, stream_option)
    if stream:
        sock.setblocking(False)
        set_tcp_no_delay(sock)
        set_no_close_wait(sock)
        set_tcp_keep_alive(sock)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuse_port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(address)
    if stream:
        sock.listen(backlog)
    sock.setblocking(False)
    return sock


def create_unix_socket(address: Tuple[str, int], backlog: int = DEFAULT_BACKLOG) -> socket.socket:
    """Create and listen on a UNIX-domain stream socket.

    Unlinks ``address[0]`` first when that path already exists, so a stale
    socket file from a previous process does not make bind fail.

    Args:
        address: ``(unix_path, unused_port)``; only the path is bound.
        backlog: ``listen()`` backlog.

    Returns:
        A non-blocking listening UNIX stream socket.
    """
    if os.path.exists(address[0]):
        unlink_unix_socket_file(address[0])
    stream_option = socket.SOCK_STREAM
    sock = socket.socket(socket.AF_UNIX, stream_option)
    sock.bind(address[0])
    sock.listen(backlog)
    sock.setblocking(False)
    return sock


def unlink_unix_socket_file(unix_path: str) -> bool:
    """Remove a leftover UNIX socket file if present.

    ``os.unlink`` of a missing path is treated as success. Failure is
    returned only when the path still exists after ``OSError``.

    Args:
        unix_path: Filesystem path of the UNIX socket.

    Returns:
        True when the path does not exist afterwards.
    """
    try:
        os.unlink(unix_path)
    except OSError:
        if os.path.exists(unix_path):
            return False
    return True


def get_ip_with_nic(nic: str) -> str:
    """Return the first address of a network interface.

    Prefers IPv4; falls back to IPv6 when the NIC has no IPv4 address.

    Args:
        nic: Interface name such as ``eth0``.

    Returns:
        Address string, or empty when the NIC is missing or lookup fails.
    """
    if nic not in netifaces.interfaces():
        return ""
    try:
        nic_ip = ""
        nic_addrs = netifaces.ifaddresses(nic)
        if netifaces.AF_INET in nic_addrs:
            nic_ip = nic_addrs[netifaces.AF_INET][0]['addr']
        elif netifaces.AF_INET6 in nic_addrs:
            nic_ip = nic_addrs[netifaces.AF_INET6][0]['addr']

        return nic_ip
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"parse nic err: {err}")
        return ""


def join_host_port(host: str, port: int) -> str:
    """Format ``host:port``, bracketing IPv6 hosts.

    Args:
        host: Hostname or IP.
        port: Port number.

    Returns:
        ``host:port`` or ``[ipv6]:port``.
    """
    if host.find(':') != -1:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def find_free_port() -> int:
    """Bind ``('', 0)`` and return the ephemeral TCP port the kernel assigned.

    Returns:
        A currently unused TCP port on the local machine.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def is_valid_socket(sock: socket.socket) -> bool:
    """Return whether ``sock`` still has a live file descriptor.

    Args:
        sock: Socket-like object; objects without ``fileno`` are invalid.

    Returns:
        True when ``fileno() >= 0``.
    """
    if hasattr(sock, 'fileno'):
        return sock.fileno() >= 0
    return False


def is_close_wait(sock: socket.socket) -> bool:
    """Return whether a TCP socket is in ``CLOSE_WAIT``.

    Skipped on Darwin: ``TCP_INFO`` is not available on the Python
    versions this package supports there. Non-TCP sockets return False.

    Args:
        sock: Connected TCP socket.

    Returns:
        True only when Linux ``TCP_INFO`` reports ``TCP_CLOSE_WAIT``.
    """
    if sock.proto != socket.IPPROTO_TCP:
        return False
    if sys.platform == "darwin":
        return False
    tcp_info = sock.getsockopt(socket.SOL_TCP, socket.TCP_INFO, 20)
    return tcp_info[0] == TcpState.TCP_CLOSE_WAIT


def get_tcp_state(sock: socket.socket) -> str:
    """Return the ``TCP_INFO`` state name of a TCP socket.

    Args:
        sock: Socket that supports ``TCP_INFO``.

    Returns:
        Name of a :class:`~trpc_sdk.utils._constants.TcpState` member.
    """
    tcp_info = sock.getsockopt(socket.SOL_TCP, socket.TCP_INFO, 20)
    return TcpState(tcp_info[0]).name


def set_no_close_wait(sock: socket.socket) -> None:
    """Disable lingering so ``close`` sends RST instead of waiting.

    Args:
        sock: Socket to configure.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))


def set_close_wait(sock: socket.socket, delay_time: int) -> None:
    """Enable lingering with a timeout in seconds.

    Args:
        sock: Socket to configure.
        delay_time: Linger timeout passed to ``SO_LINGER``.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, delay_time))


def set_default_close_wait(sock: socket.socket) -> None:
    """Restore default linger (``l_onoff = 0``).

    Args:
        sock: Socket to configure.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))


def set_tcp_no_delay(sock: socket.socket) -> None:
    """Disable Nagle on a TCP socket.

    Args:
        sock: TCP socket to configure.
    """
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def set_tcp_keep_alive(sock: socket.socket) -> None:
    """Enable kernel TCP keepalive.

    Args:
        sock: TCP socket to configure.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


def close_socket(sock) -> None:
    """Close a socket, including :class:`~trpc_sdk.common.TransportSocket`.

    Prefer ``sock.close()``. Cython transport sockets expose the real
    fd on ``_sock`` and have no usable ``close``.

    Args:
        sock: Socket or transport-socket wrapper.
    """
    if hasattr(sock, 'close'):
        try:
            sock.close()
        except TypeError as err:
            logger.debug(f"not support close {err}")
        return

    if isinstance(sock, TransportSocket) and hasattr(sock, '_sock'):
        sock._sock.close()
        return

    logger.error(f"sock is {sock} not close method")
