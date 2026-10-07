# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Async context managers wrapping an HTTP request coroutine.

:class:`RequestContextManager` is what :class:`~trpc_sdk.http.Client`
returns from ``get`` / ``post`` / ``request``. ``async with`` awaits
the request and :meth:`TrpcHttpResponse.release` on exit so the
aiohttp response is closed.
"""

from __future__ import annotations

from typing import Coroutine
from typing import Generator


class BaseRequestContextManager(Coroutine):
    """Coroutine wrapper that also works as an async context manager."""

    __slots__ = ("_coro", "_resp")

    def __init__(self, coro: Coroutine) -> None:
        self._coro = coro

    def send(self, arg: None):
        return self._coro.send(arg)

    def throw(self, arg: BaseException) -> None:  # type: ignore
        self._coro.throw(arg)  # type: ignore

    def close(self) -> None:
        return self._coro.close()

    def __await__(self) -> Generator:
        ret = self._coro.__await__()
        return ret

    def __iter__(self) -> Generator:
        return self.__await__()

    async def __aenter__(self):
        self._resp = await self._coro
        return self._resp


class RequestContextManager(BaseRequestContextManager):
    """Request context manager that releases the HTTP response on exit."""

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._resp.release()
