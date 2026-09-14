# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Dict lookup helpers and URL small tools used by the HTTP package.

:func:`reverse_dict` and :func:`find_from_dict` exist so the content-type,
content-encoding and HTTP-status tables in :mod:`trpc_sdk.http._types` can
be inverted and queried without repeating the same two functions.
:func:`check_url` and :func:`get_host` operate on an already-parsed
:class:`~urllib.parse.ParseResult`.
"""

from __future__ import annotations

from typing import Mapping
from typing import Optional
from typing import TypeVar
from urllib.parse import ParseResult


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


def reverse_dict(origin: Mapping[_KT, _VT]) -> Mapping[_VT, _KT]:
    """Return a new mapping with keys and values swapped.

    Duplicate values keep the last key that mapped to them, matching
    the historical ``defines._reverse_dict`` behaviour. That is why
    ``application/protobuf`` (and not ``application/x-protobuf``) is
    the content type returned for :attr:`~trpc_sdk.codec.SerializationType.PB`.

    Args:
        origin: Mapping to invert.

    Returns:
        A new mapping from values to keys.
    """
    reversed_map: dict[_VT, _KT] = {}
    for key, value in origin.items():
        reversed_map[value] = key
    return reversed_map


def find_from_dict(
        origin: Mapping[_KT, _VT],
        key: _KT,
        default: Optional[_VT] = None,
) -> Optional[_VT]:
    """Look ``key`` up in ``origin``, returning ``default`` if missing.

    The HTTP lookup helpers in :mod:`trpc_sdk.http._types` bind ``origin``
    with :func:`functools.partial`, so callers pass only ``key`` and
    an optional ``default``.

    Args:
        origin: Mapping to search.
        key: Key to look up.
        default: Value returned when ``key`` is absent.

    Returns:
        The mapped value, or ``default``.
    """
    return origin.get(key, default)


def check_url(url: ParseResult) -> bool:
    """Return whether ``url`` has both scheme and netloc.

    Kept so existing subclasses that imported this helper keep working.

    Args:
        url: Already-parsed URL.

    Returns:
        ``True`` when both fields are non-empty.
    """
    return url.scheme != "" and url.netloc != ""


def get_host(url: ParseResult) -> str:
    """Return ``hostname:port`` when a port is set, else ``hostname``.

    Kept so existing subclasses that imported this helper keep working.

    Args:
        url: Already-parsed URL.

    Returns:
        Host string suitable for a ``Host`` header.
    """
    if url.port:
        return f"{url.hostname}:{url.port}"
    return url.hostname
