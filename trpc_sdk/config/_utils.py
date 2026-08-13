# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Config utils"""

import netifaces


def join_host_port(host: str, port: int) -> str:
    """Build ip port.
    Args:
        host: host of type str
        port: port of type int
    Returns:
        str
    """
    if host.find(':') != -1:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def get_ip_with_nic(nic: str) -> str:
    """
    Get ip with nic.
    Args:
        nic: nic of type str
    Returns:
        str
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
        from trpc_sdk.log import logger
        logger.error(f"parse nic err: {err}")
        return ""
