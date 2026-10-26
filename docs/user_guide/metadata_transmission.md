English | [中文](metadata_transmission.zh_CN.md)

# Metadata Propagation

The `trans_info` field in the tRPC protocol frame header carries a set of key-value pairs across processes (the key is a `str`, and the value is `bytes`):

- **On pack:** the codec writes `client_meta_data` from the `TrpcMessage` into `trans_info` in the frame header.
- **On unpack:** the server stores `trans_info` from the request frame in `server_meta_data`. The client stores `trans_info` returned in the response frame in `client_meta_data`.

> **In-process metadata is not the same as cross-process metadata.**
> `with_meta_data` / `get_meta_data` / `pop_meta_data` on `TrpcContext` read and write `TrpcContext.meta_data`. That map is **visible only on this context inside the current process and is not encoded into the network frame** (an HTTP service stores its `request` and `response` objects in that local dict; see [HTTP Server](server/http.md)). Cross-process propagation must use `Options.set_metadata`, or `client_meta_data` / `server_meta_data` on the `message`, as described below.

## Passing Metadata Downstream from the Client

To send metadata to the callee, prefer `Options.set_metadata(key, val)` on the call options (`val` must be `bytes`). You can also call `message.set_client_meta_data(...)` directly before the call:

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.client import Options

ctx = TrpcContext()
get_message(ctx).set_client_meta_data({"trace-id": b"abc"})

options = (
    Options()
    .set_target("ip://127.0.0.1:8000")
    .set_protocol("trpc")
    .set_metadata("key", b"value")
)
```

The two APIs differ, and context cloning changes what is sent:

1. **Merge one key, or replace the whole map.** `Options.set_metadata(key, val)` merges into the current `client_meta_data` while call options are assembled (for a duplicate key, the value passed to this `set_metadata` wins). `message.set_client_meta_data(dict)` replaces the whole dict.
2. **What `need_clone_ctx` does:**
   - The generated stub proxy defaults to `need_clone_ctx=True`. When the context is cloned, the framework copies **`server_meta_data`** from the original context into **`client_meta_data`** on the new outbound message. Existing `client_meta_data` on the original message is **not** copied.
   - `Options.set_metadata` runs after the clone, so it takes effect whether cloning is on or off. To set the outbound map directly with `get_message(ctx).set_client_meta_data(...)` before the call, or to read `client_meta_data` brought back by the downstream response from the original `ctx` after the call returns, construct the proxy with `need_clone_ctx=False`.

## Reading Upstream Metadata and Passing It On

After the server unpacks the request, upstream `trans_info` is in `server_meta_data` on the current message:

```python
from trpc_sdk.abc import get_message

async def SayHello(self, ctx, request):
    incoming = get_message(ctx).server_meta_data or {}
    trace_id = incoming.get("trace-id", b"")
    ...
```

If the next downstream call does not clone the context (`need_clone_ctx=False`, or a direct `Client.invoke`), the framework does not copy `server_meta_data` onto the outbound request. To keep passing it down, call `Options.set_metadata` or `set_client_meta_data` on the outbound `message`.

## Reserved Metadata Keys

The codec parses the following two keys on its own and maps them onto dedicated attributes of `TrpcMessage`. Do not treat them as ordinary business fields:

| Reserved key | What the framework does |
| --- | --- |
| `trpc-dyeing-key` | Dyeing key. After unpacking, it is written to `message.dyeing_key` |
| `trpc-env` | Environment passthrough. After unpacking, it is written to `message.env_transfer` |

A streaming RPC uses the same `trans_info` fields on the `INIT` frame that opens the stream.
