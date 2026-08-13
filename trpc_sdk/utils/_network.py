# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Network address related operations"""

import os
import enum
import struct
import socket
import sys
import ipaddress
from contextlib import closing

import netifaces

from trpc_sdk.log import logger
from trpc_sdk.common import TransportSocket

from ._constants import IP_TYPE_MAP
from ._constants import UNIX_TYPE
from ._constants import IpType
from ._constants import PortType


def int2ip(ip: int) -> str:
    """Convert integer ip to string.
    Args:
        ip: ip of type int
    Returns:
        str
    """
    return str(ipaddress.ip_address(ip))


def ip2int(ip: str) -> int:
    """Convert string ip to integer.
    Args:
        ip: ip of type str
    Returns:
        int
    """
    return int(ipaddress.ip_address(ip))


def ip_to_bytes(ip: str) -> bytes:
    """Convert string ip to bytes.
    Args:
        ip: ip of type str
    Returns:
        bytes
    """
    return ipaddress.ip_address(ip).packed


def bytes_to_ip(ip_bytes: bytes) -> str:
    """Convert bytes ip to string.
    Args:
        ip_bytes: bytes of ip
    Returns:
        str
    """
    return str(ipaddress.ip_address(ip_bytes))


def is_ipv4(ip: IpType) -> bool:
    """Check if ip is ipv4.
    Args:
        ip: ip of type IpType
    Returns:
        bool
    """
    try:
        return ipaddress.ip_address(ip).version == 4
    except ValueError:
        logger.error(f"{ip} is not valid addr")
        return False


def is_ipv6(ip: IpType) -> bool:
    """Check if ip is ipv6.
    Args:
        ip: ip of type IpType
    Returns:
        bool
    """
    try:
        return ipaddress.ip_address(ip).version == 6
    except ValueError:
        logger.error(f"{ip} is not valid addr")
        return False


def is_unix_socket(socket_type: str) -> bool:
    """Check if socket type is unix.
    Args:
        socket_type: socket type of type str
    Returns:
        bool
    """
    return socket_type == UNIX_TYPE


def port_valid(port: PortType) -> bool:
    """Check if port is valid.
    Args:
        port: port of type PortType
    Returns:
        bool
    """
    return int(port) >= 0 and int(port) <= 65535


def tcp_port_valid(ip: IpType, port: PortType) -> bool:
    """
    Check if tcp port is valid.
    Args:
        ip: ip of type IpType
        port: port of type PortType
    Returns:
        bool
    """
    if not port_valid(port):
        logger.error(f"tcp port valid check failed, port:{port} is invalid")
        return False

    return real_tcp_port_valid(ip, port)


def udp_port_valid(ip: IpType, port: PortType) -> bool:
    """
    Check if udp port is valid.
    Args:
        ip: ip of type IpType
        port: port of type PortType
    Returns:
        bool
    """
    if not port_valid(port):
        logger.error(f"udp port valid check failed, port:{port} is invalid")
        return False

    return real_udp_port_valid(ip, port)


def real_tcp_port_valid(ip: IpType, port: PortType) -> bool:
    """Check if tcp port is valid.
    Args:
        ip: ip of type IpType
        port: port of type PortType
    Returns:
        bool
    """
    socket_tmp = None
    try:
        socket_tmp = new_inet_socket(ip, is_udp=False)
        if 0 == socket_tmp.connect_ex((ip, port)):
            logger.error(f"tcp port valid check failed, (ip:{ip} port:{port}) has been occupied")
            return False
        return True
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"tcp port valid check failed, (ip:{ip} port:{port}) appear err: {err}")
        return False
    finally:
        if socket_tmp:
            socket_tmp.close()


def real_udp_port_valid(ip: IpType, port: PortType) -> bool:
    """Check if udp port is valid.
    Args:
        ip: ip of type IpType
        port: port of type PortType
    Returns:
        bool
    """
    socket_tmp = None
    try:
        socket_tmp = new_inet_socket(ip, is_udp=True)
        socket_tmp.bind((ip, port))
        return True
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"udp port valid check failed, (ip:{ip} port:{port}) appear err: {err}")
        return False
    finally:
        if socket_tmp:
            socket_tmp.close()


def unix_path_valid(unix_path: str) -> bool:
    """Check if unix path is valid.
    Args:
        unix_path: unix path of type str
    Returns:
        bool
    """
    is_unlink = unlink_unix_socket_file(unix_path)
    if not is_unlink:
        logger.error(f"unix path valid check failed, {unix_path} has existed")
        return False
    return True


def new_inet_socket(ip: str, is_udp: bool) -> socket.socket:
    """Build socket.
    Args:
        ip: ip of type str
        is_udp: whether is udp
    Returns:
        socket.socket
    """
    addr = ipaddress.ip_address(ip)
    return socket.socket(IP_TYPE_MAP[addr.version], socket.SOCK_DGRAM if is_udp else socket.SOCK_STREAM)


def unlink_unix_socket_file(unix_path: str) -> bool:
    """Check if unix socket file is exists.
    Args:
        unix_path: unix socket file of type str
    Returns:
        bool
    """
    try:
        os.unlink(unix_path)
    except OSError:
        if os.path.exists(unix_path):
            return False
    return True


def find_free_port() -> int:
    """
    Find free port.
    Returns:
        int
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class TcpState(enum.IntEnum):
    """Tcp state.
    """
    TCP_ESTABLISHED = 1
    TCP_SYN_SENT = enum.auto()
    TCP_SYN_RECV = enum.auto()
    TCP_FIN_WAIT1 = enum.auto()
    TCP_FIN_WAIT2 = enum.auto()
    TCP_TIME_WAIT = enum.auto()
    TCP_CLOSE = enum.auto()
    TCP_CLOSE_WAIT = enum.auto()
    TCP_LAST_ACK = enum.auto()
    TCP_LISTEN = enum.auto()
    TCP_CLOSING = enum.auto()


def is_valid_socket(sock: socket.socket) -> bool:
    """Check if socket is valid.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    if hasattr(sock, 'fileno'):
        return sock.fileno() >= 0
    return False


def is_close_wait(sock: socket.socket) -> bool:
    """Check close wait.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    if sock.proto != socket.IPPROTO_TCP:
        return False
    # mac in python312以后，才支持获取TCP_INFO，当前还不支持312版本，先返回False不做此项检查
    if sys.platform == "darwin":
        return False
    tcp_info = sock.getsockopt(socket.SOL_TCP, socket.TCP_INFO, 20)
    return tcp_info[0] == TcpState.TCP_CLOSE_WAIT


def get_tcp_state(sock: socket.socket) -> str:
    """Get info of tcp.
    Args:
        sock: socket of type socket.socket
    Returns:
        str
    """
    tcp_info = sock.getsockopt(socket.SOL_TCP, socket.TCP_INFO, 20)
    return TcpState(tcp_info[0]).name


def set_no_close_wait(sock: socket.socket) -> bool:
    """Set no close wait.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))


def set_close_wait(sock: socket.socket, delay_time: int) -> bool:
    """Set close wait.
    Args:
        sock: socket of type socket.socket
        delay_time: delay time of type int
    Returns:
        bool
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, delay_time))


def set_default_close_wait(sock: socket.socket) -> bool:
    """Set default close wait.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))


def set_tcp_no_delay(sock: socket.socket) -> bool:
    """Set no delay.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def set_tcp_keep_alive(sock: socket.socket) -> bool:
    """Set keep alive.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


def close_socket(sock) -> bool:
    """Close socket.
    Args:
        sock: socket of type socket.socket
    Returns:
        bool
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
