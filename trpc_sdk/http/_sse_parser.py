# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Incremental ``text/event-stream`` decoder."""

from __future__ import annotations

from typing import List
from typing import Optional

from ._constants import SSE_NEWLINE_CHARS
from ._sse_event import SSEEvent


class SSEDecoder:
    """Stateful SSE decoder: text chunks to lines, lines to events.

    :meth:`split_lines` and :meth:`flush_lines` turn arbitrarily split text
    into whole lines, carrying partial lines and a trailing ``\\r`` across
    chunks. :meth:`feed_line` accumulates fields and dispatches an
    :class:`SSEEvent` on each blank line.
    """

    def __init__(self) -> None:
        """Start with no pending fields and an empty line buffer."""
        self._event_type = ""
        self._data_lines: List[str] = []
        self._last_event_id = ""
        self._retry: Optional[int] = None
        self._line_buffer: List[str] = []
        self._pending_cr: bool = False

    def feed_line(self, line: str) -> Optional[SSEEvent]:
        """Consume one line of the event stream.

        Args:
            line: A single line without its terminator.

        Returns:
            The dispatched event when ``line`` is blank and at least one
            field is pending, otherwise ``None``.

        Note:
            Per the spec, ``_last_event_id`` survives dispatch while the
            other fields are reset.
        """
        if not line:
            if (not self._event_type and not self._data_lines and not self._last_event_id and self._retry is None):
                return None

            event = SSEEvent(
                event=self._event_type,
                data="\n".join(self._data_lines),
                id=self._last_event_id,
                retry=self._retry,
            )

            self._event_type = ""
            self._data_lines = []
            self._retry = None

            return event

        if line.startswith(":"):
            return None

        field, _, value = line.partition(":")

        if value.startswith(" "):
            value = value[1:]

        if field == "event":
            self._event_type = value
        elif field == "data":
            self._data_lines.append(value)
        elif field == "id":
            if "\0" in value:
                pass
            else:
                self._last_event_id = value
        elif field == "retry":
            try:
                self._retry = int(value)
            except (TypeError, ValueError):
                pass
        else:
            pass  # Ignore unknown fields

        return None

    def split_lines(self, text: str) -> List[str]:
        """Split a text chunk into complete lines.

        A trailing ``\\r`` is held back because the next chunk may start
        with ``\\n``. An unterminated last line is buffered and prefixed to
        the first line of the next chunk.

        Args:
            text: Raw decoded text from the stream.

        Returns:
            Complete lines, possibly empty.
        """
        if self._pending_cr:
            text = "\r" + text
            self._pending_cr = False
        if text.endswith("\r"):
            self._pending_cr = True
            text = text[:-1]

        if not text:
            return []

        ends_with_newline = text[-1] in SSE_NEWLINE_CHARS
        lines = text.splitlines()

        if len(lines) == 1 and not ends_with_newline:
            self._line_buffer.append(lines[0])
            return []

        if self._line_buffer:
            lines = ["".join(self._line_buffer) + lines[0]] + lines[1:]
            self._line_buffer = []

        if not ends_with_newline:
            self._line_buffer = [lines.pop()]

        return lines

    def flush_lines(self) -> List[str]:
        """Drain whatever is buffered at end of stream.

        Returns:
            One line holding the buffered text (possibly ``""`` if only a
            ``\\r`` was pending), or an empty list when nothing is buffered.
        """
        if not self._line_buffer and not self._pending_cr:
            return []

        lines = ["".join(self._line_buffer)]
        self._line_buffer = []
        self._pending_cr = False
        return lines
