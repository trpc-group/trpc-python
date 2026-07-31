# -*- coding: utf-8 -*-
"""Core span model for rpcz."""

import json
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict

if TYPE_CHECKING:
    from typing import List


class SpanType(str, Enum):
    """Supported rpcz span types."""

    SERVER = "server"
    CLIENT = "client"


@dataclass
class Span:
    """Rpcz span core data structure.

    Notes:
    - Keep only server/client span types.
    - Use explicit timestamp names based on filter points.
    - Timestamp unit is microseconds.
    """

    span_id: int = 0
    span_type: SpanType = SpanType.SERVER

    full_method_name: str = ""
    remote_name: str = ""
    remote_endpoint: str = ""

    request_size: int = 0
    response_size: int = 0
    error_code: int = 0
    timeout_ms: int = 0

    # server path timestamps
    server_pre_sched_recv_msg_us: int = 0
    server_post_sched_recv_msg_us: int = 0
    server_pre_rpc_invoke_us: int = 0
    server_post_rpc_invoke_us: int = 0
    server_pre_sched_send_msg_us: int = 0
    server_post_sched_send_msg_us: int = 0
    server_pre_io_send_msg_us: int = 0
    server_post_io_send_msg_us: int = 0

    # client path timestamps
    client_pre_rpc_invoke_us: int = 0
    client_post_rpc_invoke_us: int = 0
    client_pre_io_send_msg_us: int = 0
    client_post_io_send_msg_us: int = 0
    client_post_sched_send_msg_us: int = 0
    client_post_sched_recv_msg_us: int = 0

    # Child spans for route/forward scenario.
    sub_spans: "List[Span]" = field(default_factory=list)

    # Extra fields for future compatibility.
    extra: Dict[str, Any] = field(default_factory=dict)

    async def append_sub_span(self, sub_span: "Span") -> None:
        """Append one child span."""
        self.sub_spans.append(sub_span)

    async def get_sub_spans(self) -> "List[Span]":
        """Return a snapshot copy of child spans."""
        return list(self.sub_spans)

    def _first_log_us(self) -> int:
        """Return first timestamp used for brief span info."""
        if self.span_type == SpanType.SERVER:
            return self.server_pre_sched_recv_msg_us
        return self.client_pre_rpc_invoke_us

    def _last_log_us(self) -> int:
        """Return last timestamp used for brief span info."""
        if self.span_type == SpanType.SERVER:
            return self.server_post_io_send_msg_us
        return self.client_post_rpc_invoke_us

    def brief_span_info(self) -> Dict[str, Any]:
        """Return brief json-style span info for admin listing."""
        first_log_us = self._first_log_us()
        last_log_us = self._last_log_us()
        cost_us = max(0, last_log_us - first_log_us)
        return {
            "first_log_us": first_log_us,
            "cost_us": cost_us,
            "span_type": "S" if self.span_type == SpanType.SERVER else "C",
            "span_id": self.span_id,
            "full_method_name": self.full_method_name,
            "request_size": self.request_size,
            "response_size": self.response_size,
            "framework_result": "ok" if self.error_code == 0 else "failed",
            "error_code": self.error_code,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert span into a JSON-serializable dict."""
        data = asdict(self)
        data["span_type"] = self.span_type.value
        data.pop("timeout_ms", None)

        if self.span_type == SpanType.SERVER:
            data.pop("client_pre_rpc_invoke_us", None)
            data.pop("client_post_rpc_invoke_us", None)
            data.pop("client_pre_io_send_msg_us", None)
            data.pop("client_post_io_send_msg_us", None)
            data.pop("client_post_sched_send_msg_us", None)
            data.pop("client_post_sched_recv_msg_us", None)
        else:
            data.pop("server_pre_sched_recv_msg_us", None)
            data.pop("server_post_sched_recv_msg_us", None)
            data.pop("server_pre_rpc_invoke_us", None)
            data.pop("server_post_rpc_invoke_us", None)
            data.pop("server_pre_sched_send_msg_us", None)
            data.pop("server_post_sched_send_msg_us", None)
            data.pop("server_pre_io_send_msg_us", None)
            data.pop("server_post_io_send_msg_us", None)

        # Apply the same field policy recursively for nested spans.
        serialized_sub_spans = []
        for sub_span in self.sub_spans:
            if isinstance(sub_span, Span):
                serialized_sub_spans.append(sub_span.to_dict())
            elif isinstance(sub_span, dict):
                try:
                    serialized_sub_spans.append(Span.from_dict(sub_span).to_dict())
                except Exception:  # pylint: disable=broad-except
                    continue
        data["sub_spans"] = serialized_sub_spans
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Span":
        """Build span from dict data."""
        payload = dict(data or {})
        span_type = payload.get("span_type", SpanType.SERVER.value)
        payload["span_type"] = SpanType(span_type)
        raw_sub_spans = payload.get("sub_spans", []) or []
        payload["sub_spans"] = [
            cls.from_dict(sub_span) if isinstance(sub_span, dict) else sub_span for sub_span in raw_sub_spans
            if isinstance(sub_span, (dict, Span))
        ]
        return cls(**payload)

    def encode(self) -> bytes:
        """Encode span into bytes for IPC/network transport."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def decode(cls, data: bytes) -> "Span":
        """Decode bytes back to a span object."""
        payload = json.loads(data.decode("utf-8"))
        return cls.from_dict(payload)
