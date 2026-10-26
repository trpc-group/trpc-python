English | [中文](protocol.zh_CN.md)

# How to Write a Protocol Plugin

In tRPC-Python, one protocol name (the built-in names are `trpc` and `http`) ties together four extension slots: server and client **frame splitting (Checker/Framer)** and server and client **codec**.

This page implements a custom `line` protocol that splits a message on the newline `\n`, and shows how those four slots attach to the framework's default TCP transport. Like every other plugin, a protocol plugin is registered when the module that defines it is imported. Import that module from the service entry before `create_server` (or before the client loads configuration).

## Call Path

![Client encode, server decode, business, server encode, client decode](images/codec-cs.png)

One RPC moves through the following steps. The gray boxes on both sides of "network" in the figure are the codec. Frame splitting happens before the bytes enter the codec. Serialization happens between the codec and the business handler.

- **Client send and receive (`_invoke_backend`)**:
  1. On send: follow the types on `message` through **serialize**, then **compress**, then **`codec.encode_data`**. The `body` passed to `encode_data` is already serialized and compressed `bytes`.
  2. On receive: **`checker.read_frame`** splits one complete response frame off the connection, then **`codec.decode_data`**, then **decompress**, then **deserialize**.
- **Server receive and reply (`Service.handle`)**:
  1. On receive: after the TCP connection delivers bytes, the **`checker`** splits out one complete request frame. `Service.handle` then runs **`await codec.decode_data`**, decompresses by `message.compress_type`, deserializes by `message.serialization_type`, and enters the filter chain and the business function.
  2. On reply: **serialize**, then **compress**, then **`codec.encode_data`**, and write the result back to the network.

### How the `protocol` Setting Selects Plugins

The `protocol` string in YAML is the registered name shared by the four slots:

- **Server**: `ServiceOption.set_protocol(protocol)` looks up and installs `get_server_codec` and `get_server_parser` under that name. When the service is created, if that protocol name has no `SERVER_TRANSPORT` of its own, `create_server` uses the base `TrpcServerTransport` (the framework's built-in TCP/UDP listener).
- **Client**: `Options.set_protocol(protocol)` looks up `CLIENT_CODEC`, `CLIENT_CHECKER`, and `CLIENT_TRANSPORT` under that name. If no client transport of the same name is registered, the default `trpc` client transport stays in place.

A custom message format on top of TCP, such as `line` on this page, therefore **only needs the four checker and codec plugins**. Transport can keep using the framework's built-in TCP.

## 1. Server Checker (`SERVER_CHECKER`)

![Server: split frames, decode, deserialize, business, serialize, encode](images/server-codec-pipeline.png)

The cell labeled **Framer** in the figure is the abstract base class `BaseServerProtocolParser` (imported from `trpc_sdk.abc`). Implement these three methods:

- `feed_data(data: bytes) -> None`: append newly received bytes to this connection's buffer.
- `read() -> List[bytes]`: split out every complete frame that is ready, return them, and remove them from the buffer. Return an empty list `[]` when no complete frame is ready.
- `reset() -> None`: clear the buffer.

**Why registration goes through a factory.** Each new TCP connection makes the lower protocol object (in `_tcp_protocol.pyx`, the dispatcher, or the executor) call `checker.create()` for a parser that belongs to that connection, then `feed_data` and `read` from `data_received`. What you register as `PluginType.SERVER_CHECKER` must be a function that returns a `BasePluginFactory`. `create()` on that factory hands each connection its own buffer.

> `feed_data` on the `BaseServerProtocolParser` base class writes into an internal buffer that pure Python cannot read. A custom parser written in pure Python should keep its own `bytearray` and override `feed_data`, `read`, and `reset`.

`LineServerParser` below splits on `b"\n"`. The frame keeps the trailing `\n`, and the codec strips it later.

```python
from typing import List

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin


class LineServerParser(BaseServerProtocolParser):
    """Split a TCP stream into newline-terminated frames."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed_data(self, data: bytes) -> None:
        self._buf.extend(data)

    def read(self) -> List[bytes]:
        frames = []
        while True:
            index = self._buf.find(b"\n")
            if index < 0:
                break
            frames.append(bytes(self._buf[: index + 1]))
            del self._buf[: index + 1]
        return frames

    def reset(self) -> None:
        self._buf.clear()


class LineCheckerFactory(BasePluginFactory):
    """Hand out one line parser per connection."""

    def create(self) -> LineServerParser:
        return LineServerParser()


@register_plugin(PluginType.SERVER_CHECKER, "line")
def setup_line_checker() -> LineCheckerFactory:
    return LineCheckerFactory()
```

**Compared with the built-in implementations:**

- **`trpc`**: The built-in `TrpcServerProtocolChecker` splits frames from the fixed 16-byte header. It checks magic `2352` and reads the total length `package_len`. It does not parse the protobuf header at this stage.
  ![tRPC 16-byte frame header, magic number, and total length](images/trpc-frame.png)
- **`http`**: `ServerHttpChecker` only occupies the slot. HTTP messages are parsed by the underlying `aiohttp`, so they do not go through the framework's TCP `data_received` frame-splitting path.

## 2. Client Checker (`CLIENT_CHECKER`)

![Client: serialize, encode, network, split frames, decode, deserialize](images/client-codec-pipeline.png)

On the client receive path, bytes first pass the cell labeled **Framer**, which is `BaseClientProtocolParser`, then `decode_data` and deserialization.

Unlike the server, a client checker subclasses `BaseClientProtocolParser` (itself a `BasePlugin`) and is registered directly as a class, so the registry holds one instance. Implement `async def read_frame(self, connection) -> bytes`. The client TCP transport `await`s that method to read one complete response frame from a pooled connection (`PooledConnection`).

`PooledConnection.read(n)` asynchronously reads exactly `n` bytes. The snippet below reads 1 byte at a time until `b"\n"`, to show the idea. When the connection closes or the read fails, the exception is turned into `ClientDecodeError`:

```python
from trpc_sdk.abc import BaseClientProtocolParser
from trpc_sdk.abc import PluginType
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.CLIENT_CHECKER, "line")
class LineClientParser(BaseClientProtocolParser):
    """Read one newline-terminated frame from a pooled connection."""

    async def read_frame(self, connection) -> bytes:
        chunks = []
        while True:
            try:
                piece = await connection.read(1)
            except Exception as err:
                raise ClientDecodeError("line connection closed") from err
            chunks.append(piece)
            if piece == b"\n":
                return b"".join(chunks)
```

> **Compared with the built-in implementations**: `TrpcClientParser` reads the 16-byte header first. When the magic is not `2352`, it immediately calls `conn.close(True)` to drop the connection (the byte stream is no longer aligned to the next frame), then raises `ClientDecodeError`. `ClientHttpChecker.read_frame` raises `NotImplementedError` directly, because HTTP responses are read by `aiohttp`.

## 3. Server and Client Codecs (`SERVER_CODEC` / `CLIENT_CODEC`)

A codec subclasses `BaseCodec` and implements a pair of methods. The first argument of both is `TrpcContext`:

- `encode_data(context, body) -> bytes`
- `decode_data(context, buffer) -> bytes`

**Sync and async signatures differ:**

- **Server `decode_data` must be `async def`**, because `Service.handle` calls it with `await codec.decode_data(...)`.
- **Client `decode_data`, and `encode_data` on both sides, are ordinary functions (`def`)**. The framework does not `await` those three calls. Return `bytes` directly.

### A Codec for the `line` Protocol

The `line` frame is simple:

- `encode_data` appends one `\n` to the already serialized `body`.
- `decode_data` strips the trailing `\n` from the frame the checker handed over and returns the `body` bytes. `rstrip(b"\n")` removes every trailing newline, so the original `body` itself must not end with `\n`.

Server `decode_data` also sets two fields on `message`:

1. **The routed method name (`server_rpc_name`)**. `Service.handle` looks up the registered RPC handler with `message.get_server_rpc_name()`. The name looks like `/service/method`. If the service was registered with `fix_rpc_name=True`, the `service` span is replaced by the service `name` from YAML. A `line` frame carries no method name, so the example below fixes it as `"/trpc.app.server.Greeter/SayHello"`. A real protocol can put the method name in the frame header and parse it in `decode_data`.
2. **The serialization type (`serialization_type`)**. A new `message` defaults to serialization type `0`, which is protobuf. To send and receive raw bytes without protobuf, call `message.set_serialization_type(SerializationType.Noop)` in `decode_data`.

```python
from typing import Optional

from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.plugin import register_plugin
from trpc_sdk.serialization import SerializationType


@register_plugin(PluginType.SERVER_CODEC, "line")
class LineServerCodec(BaseCodec):
    """Newline-terminated server codec."""

    def encode_data(self, context: TrpcContext, body: Optional[bytes]) -> bytes:
        return (body or b"") + b"\n"

    async def decode_data(self, context: TrpcContext, buffer: Optional[bytes]) -> bytes:
        message = get_message(context)
        message.set_server_rpc_name("/trpc.app.server.Greeter/SayHello")
        message.set_serialization_type(SerializationType.Noop)
        return (buffer or b"").rstrip(b"\n")


@register_plugin(PluginType.CLIENT_CODEC, "line")
class LineClientCodec(BaseCodec):
    """Newline-terminated client codec."""

    def encode_data(self, context: TrpcContext, body: Optional[bytes]) -> bytes:
        return (body or b"") + b"\n"

    def decode_data(self, context: TrpcContext, buffer: Optional[bytes]) -> bytes:
        return (buffer or b"").rstrip(b"\n")
```

> A few more notes:
>
> - **How the HTTP codec works**: The built-in `ServerHttpCodec` does not assemble a TCP frame. It takes the `aiohttp` request and response from `message`. `decode_data` reads the body from the request and ignores `buffer`. `encode_data` writes the status code and response headers onto the response and returns the body.
> - **Streaming codecs**: `encode_stream` / `decode_stream` on `BaseCodec` are for the tRPC streaming protocol. An ordinary unary protocol does not implement them.

## 4. Serialization and Compression (Optional)

Serialization and compression are two plugin slots independent of the protocol:

- **Serialization (`PluginType.SERIALIZATION`)**: Subclass `BaseSerializer` and implement `serialize` and `deserialize`. The registered name is the integer value of `SerializationType` (`PB = 0`, `JCE = 1`, `JSON = 2`, `FlatBuffer = 3`, `Noop = 4`).
- **Compression (`PluginType.COMPRESS`)**: Subclass `BaseCompress` and implement `compress` and `decompress`. The registered name is the integer value of `CompressType`. The default, no compression, is `CompressType.Noop = 0`.

The `line` example has no serialization-type field in the header, so both sides agree on `SerializationType.Noop` (value `4`) and exchange raw bytes:

- **Server**: `LineServerCodec.decode_data` above already sets `SerializationType.Noop`. The `Noop` deserializer writes the raw bytes into the `data` attribute of a `trpc_sdk.serialization.Body`.
- **Client**: Set `serialization: 4` in YAML, or call `Options().set_serialization_type(SerializationType.Noop)` on the call.

## 5. A Custom Transport or a Custom Listener (Optional)

If the protocol still runs on the framework's standard TCP or UDP event loop, as `line` does, **do not** implement the two items below. Use them only when the plugin must take over the network read/write or the listen loop.

1. **Replace transport (`SERVER_TRANSPORT` / `CLIENT_TRANSPORT`)**: Register a transport plugin under the same protocol name. Replacing `CLIENT_TRANSPORT` to call a third-party database SDK is covered in [Database Plugin](database.md).
2. **Take over the server listen loop (`register_protocol_handle`)**: Import `register_protocol_handle(protocol, starter)` from `trpc_sdk.server` or `trpc_sdk.runtime`, and call it before `serve()` forks child processes (usually at import time). After it is registered, the framework marks addresses that use this protocol as user-owned and no longer starts the built-in TCP or HTTP listener. It calls `starter(worker, index)` instead, and the plugin `bind`s on the event loop itself. In `separate` multi-process mode several processes bind the same address, so the socket needs `SO_REUSEPORT`.

## 6. Configuration

Import the module that defines `LineCheckerFactory`, `LineClientParser`, `LineServerCodec`, and `LineClientCodec` from the service entry, then configure it as follows.

### Server

```yaml
server:
  service:
    - name: trpc.app.server.Greeter
      protocol: line
      network: tcp
      ip: 127.0.0.1
      port: 8000
```

### Client

Besides `protocol: line`, the client needs a target address `target` (`ip://` is a naming-service selector; see [Naming Plugin](naming.md)) and `serialization: 4` (`SerializationType.Noop`):

```yaml
client:
  service:
    - name: trpc.app.server.Greeter
      protocol: line
      network: tcp
      target: ip://127.0.0.1:8000
      serialization: 4
```

In code, set the protocol with `Options().set_protocol("line")`. If the client calls `set_protocol("line")` before the plugin module is imported, no `CLIENT_CODEC` named `"line"` is found, `set_protocol` returns without doing anything, and the later call raises `ClientEncodeError`.
