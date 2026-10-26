English | [中文](sse.zh_CN.md)

# HTTP SSE Client

`trpc_sdk.http.SSEClient` is built on the [HTTP Client](http.md). It consumes a server-pushed `text/event-stream` (Server-Sent Events, SSE) response and parses the byte stream into `SSEEvent` objects that the caller iterates asynchronously.

An event ends at a blank line (`\n\n`). Inside an event, each line is one field (`data`, `event`, `id`, or `retry`). The field name and value are split at the first colon `:`. When one event has several `data` lines, the framework joins them with `\n` into one string.

## Basic Use

`client.request_events(...)` sends the request and yields events:

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.http import SSEClient

async def read_events() -> None:
    """Print each SSE data field from a GET stream."""
    client = SSEClient()
    ctx = TrpcContext()
    options = [Options().set_timeout(10000)]
    async for event in client.request_events(ctx, "GET", "http://127.0.0.1:8080/sse", options):
        print(event.event, event.data)
```

- **`SSEClient(name="", encoding="utf-8")`.** `name` is passed to the inner `http.Client` and used to match a client entry in `trpc_python.yaml`. `encoding` is the character encoding used to decode the byte stream into text. The default is `"utf-8"`.
- **`request_events(ctx, method, url, options, data=..., headers=...)`.** This method forces the underlying HTTP request to be a stream, then yields `SSEEvent` values whose `data` is non-empty. `options` is a list of `Options`, appended after the base options generated from the URL. `data` is the body. `headers` are extra request headers.

## `SSEEvent` Fields and Methods

| Attribute / method | Meaning |
| --- | --- |
| `event.data` | Payload string. If the server sent no `data` (an empty string), `request_events` skips the event |
| `event.event` | Event type. `"message"` when the server did not set it |
| `event.id` | The most recent event ID. `""` when the server did not set it |
| `event.retry` | Reconnect interval suggested by the server, in milliseconds (`int`). `None` when the server did not set it |
| `event.json()` | Parse `event.data` as JSON and return the Python object. Raises `json.JSONDecodeError` when the content is not valid JSON |

- **Response type check.** Before events are read, the client checks the `Content-Type` response header. If it does not contain `text/event-stream`, the call raises `TypeError`.
- **No automatic reconnect.** `event.retry` only records the value the server sent. The client does not reconnect at that interval. When the underlying connection drops, the `async for` iteration ends.

## Getting an `SSEEventSource` Directly

Use `sse_iter_events` when you need to inspect the HTTP status or response headers before reading events. It yields an `SSEEventSource`. The underlying HTTP response stays open until that event source is finished:

```python
async for source in client.sse_iter_events(ctx, "POST", "http://127.0.0.1:8080/sse", data=b"{}"):
    print(source.response.status)
    async for event in source.aiter_sse():
        print(event.id, event.data)
```

`source.response` is the `TrpcHttpResponse` described in the [HTTP Client](http.md). `source.aiter_sse()` also checks `Content-Type` and skips events whose `data` is empty.
