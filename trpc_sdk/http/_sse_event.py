# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server-Sent Events event model."""

from __future__ import annotations

import json
from typing import Any
from typing import Optional


class SSEEvent:
    """One dispatched event from a ``text/event-stream`` response."""

    def __init__(
        self,
        event: Optional[str] = None,
        data: Optional[str] = None,
        id: Optional[str] = None,  # pylint: disable=redefined-builtin
        retry: Optional[int] = None,
    ) -> None:
        """Initialize an SSE event; missing fields fall back to spec defaults.

        Args:
            event: Event type. Empty or ``None`` becomes ``"message"``.
            data: Event payload. ``None`` becomes ``""``.
            id: Last event ID. ``None`` becomes ``""``.
            retry: Reconnection time in milliseconds, if the stream sent one.
        """
        if not event:
            event = "message"

        if data is None:
            data = ""

        if id is None:
            id = ""

        self._event = event
        self._data = data
        self._id = id
        self._retry = retry

    @property
    def event(self) -> str:
        """Event type, ``"message"`` by default."""
        return self._event

    @property
    def data(self) -> str:
        """Event payload; multi-line ``data:`` fields are joined with ``\\n``."""
        return self._data

    @property
    def id(self) -> str:
        """Last event ID seen on the stream, ``""`` if none."""
        return self._id

    @property
    def retry(self) -> Optional[int]:
        """Reconnection time in milliseconds, or ``None``."""
        return self._retry

    def json(self) -> Any:
        """Parse :attr:`data` as JSON.

        Returns:
            The decoded JSON value.

        Raises:
            json.JSONDecodeError: ``data`` is not valid JSON.
        """
        return json.loads(self.data)

    def __repr__(self) -> str:
        """Show ``event`` plus every field that differs from its default."""
        parts = [f"event={self.event!r}"]
        if self.data != "":
            parts.append(f"data={self.data!r}")
        if self.id != "":
            parts.append(f"id={self.id!r}")
        if self.retry is not None:
            parts.append(f"retry={self.retry!r}")
        return f"SSEEvent({', '.join(parts)})"
