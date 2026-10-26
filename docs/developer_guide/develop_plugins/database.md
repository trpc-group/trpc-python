English | [中文](database.zh_CN.md)

# How to Write a Database Plugin

When a call to Redis, MySQL, or another store should reuse the RPC client's naming route (`Selector`), timeout control, and client filters (metrics, tracing, and so on), wrap the third-party database SDK into the `Client.invoke` path under a custom `protocol`. tRPC-Python ships Redis, MySQL, and Kafka plugins in the ecosystem repository [python-database](https://github.com/trpc-ecosystem/python-database).

The overall workflow is:

1. **Pass the command via `req_head`**: Database SDKs usually take structured commands or arguments rather than serialized wire bytes. The caller passes empty bytes `b""` as the request body (with `SerializationType.Noop`) and attaches the command object via `Options().set_req_head(command)`, which places it on `msg.client_req_head`.
2. **Register a pass-through `CLIENT_CODEC`**: Register a codec that returns the byte buffer unchanged so the protocol slot is valid.
3. **Execute the SDK call in `CLIENT_TRANSPORT`**: At the end of the client filter chain, `round_trip` reads the resolved `(host, port)` from `options.address` and the command object from `msg.client_req_head`, invokes the third-party database SDK, and writes the result back onto the command object.

Keep the roles of these two configuration fields distinct:
- `client.service.protocol` selects the `CLIENT_CODEC` and `CLIENT_TRANSPORT` plugins (routing the call into your database transport).
- `client.service.target` uses its URI scheme (such as `ip://` or `polar://`) to select a `NAMING_SELECTOR` plugin, which only resolves the target into a concrete `(host, port)` address.

The walkthrough below registers a generic protocol named `store` without tying it to a specific database engine.

## Why Both Codec and Transport Must Share the Protocol Name

When `protocol: store` is set in `trpc_python.yaml` or via call options, `Options.set_protocol("store")` looks up plugins in this exact order:

1. **Look up `CLIENT_CODEC`**: Calls `get_client_codec(protocol)`. **If no client codec is registered under that name, `set_protocol` returns immediately** without updating the parser or transport. The options instance keeps the default `trpc` transport with `opts.codec = None`, and `Client.invoke` subsequently raises `ClientEncodeError("client codec empty")` before reaching the transport layer.
2. **Look up `CLIENT_CHECKER`**: Once a codec exists, calls `get_client_parser(protocol)`. Because the third-party database SDK manages its own framing and connections, you do not need to register a client checker (`set_parser(None)` has no effect on a custom `round_trip`).
3. **Look up `CLIENT_TRANSPORT`**: Calls `get_client_transport(protocol)`. If a transport instance is found, it replaces the default `trpc` transport initialized in `_create_client_options`.

Therefore, **you must register both `CLIENT_CODEC` and `CLIENT_TRANSPORT` under `store`**. Registering only the transport will cause `protocol: store` in YAML to be ignored.

## 1. Implement the Codec (`CLIENT_CODEC`)

Because the command object travels in `msg.client_req_head`, the codec does not frame or parse packets—it simply returns the byte payload as-is.

The first argument to both `encode_data` and `decode_data` is the call `context`. Keep both methods as **ordinary synchronous functions** (not `async def`), because `_invoke_backend` calls them directly without `await`.

```python
from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.CLIENT_CODEC, "store")
class StoreCodec(BaseCodec):
    """Pass the body through without framing."""

    def encode_data(self, context, body: bytes) -> bytes:
        return body or b""

    def decode_data(self, context, buffer: bytes) -> bytes:
        return buffer or b""
```

## 2. Implement the Transport (`CLIENT_TRANSPORT`)

Subclass `BaseClientTransport` and implement the async method `round_trip(ctx, req_data, options)`. It sits at the innermost point of the client `filter` chain. Every configured client interceptor wraps this `round_trip`.

Inside `round_trip`, everything needed to call the database SDK is available:
- **Target address**: `options.address` is a `(host, port)` tuple populated by `load_node_config` after the `Selector` resolves `target`.
- **Password**: If `password` is configured under the service entry in YAML, it is available at `options.password`.
- **Command object**: Retrieved from `ctx.get_message().client_req_head` (while `req_data` is `b""`).

```python
from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.CLIENT_TRANSPORT, "store")
class StoreTransport(BaseClientTransport):
    """Run one already-installed SDK call for a store command."""

    async def round_trip(self, ctx, req_data: bytes, options):
        message = ctx.get_message()
        command = message.client_req_head
        host, port = options.address
        # Invoke the third-party database SDK here using host, port, options.password, and command.
        command.result = None
        return b""
```

Return empty bytes `b""`. After `round_trip` returns, `_invoke_backend` calls `codec.decode_data`. When the decoded response body has length `0`, the framework skips deserialization and does not fill a response object.

## 3. Configuration (`trpc_python.yaml`)

Configure the downstream storage service under `client.service`:

- `name`: The callee service name. When `callee` is omitted, `parse_client_config` keys the client configuration table by `name`.
- `protocol`: Set to `store` to select `StoreCodec` and `StoreTransport`.
- `target`: The target endpoint. `ip://127.0.0.1:6379` uses the built-in `ip` selector, which parses the IP and port directly without DNS lookup.
- `password` (optional): Authentication secret, forwarded to `options.password` in `round_trip`.

```yaml
client:
  service:
    - name: trpc.app.server.store
      network: tcp
      protocol: store
      target: ip://127.0.0.1:6379
      timeout: 1000
```

## 4. Calling from Business Code

When calling `Client().invoke()` from business code, pay attention to three details:

1. **Set the callee service name on the message before calling `invoke`**: `get_options` loads YAML configuration (`load_client_config`) using `msg.callee_service_name` *before* applying the per-call `Options` list. Always call `msg.set_callee_service_name("trpc.app.server.store")` prior to `invoke`. If you only pass `Options().set_service_name(...)`, the YAML lookup has already happened with an empty name, and the `protocol` and `target` from YAML will be missed.
2. **Use `SerializationType.Noop` with an empty request body**: `NoopSerializer.serialize` only accepts `bytes` or `Body` instances, so a custom `StoreCommand` object cannot be passed as the request body `req`. Pass `b""` as the request body, `Body` as the response class, and attach `command` via `Options().set_req_head(command)` (which `update_message` copies to `msg.client_req_head`).
3. **Import the plugin module at startup**: Make sure the module defining `StoreCodec` and `StoreTransport` is imported before making calls so `@register_plugin` executes.

```python
from trpc_sdk.abc import clone_client_message
from trpc_sdk.client import Client
from trpc_sdk.client import Options
from trpc_sdk.serialization import Body
from trpc_sdk.serialization import SerializationType


class StoreCommand:
    def __init__(self, statement: str):
        self.statement = statement
        self.result = None


async def execute(ctx, statement: str):
    ctx, _ = clone_client_message(ctx)
    ctx.get_message().set_callee_service_name("trpc.app.server.store")
    command = StoreCommand(statement)
    await Client().invoke(
        ctx,
        b"",
        Body,
        [
            Options()
            .set_req_head(command)
            .set_serialization_type(SerializationType.Noop),
        ],
    )
    return command.result
```

If you are not loading configuration from YAML, chain `.set_protocol("store").set_target("ip://127.0.0.1:6379")` onto the same `Options()` instance.
