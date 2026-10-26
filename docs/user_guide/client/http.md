English | [中文](http.zh_CN.md)

# HTTP Client

This page explains how to send a standard HTTP request by URL with `trpc_sdk.http.Client`. Underneath, the client uses the same selector and the same client filter chain as a unary RPC.

If you already have a protobuf stub and only want the transport protocol to be HTTP (generic HTTP RPC), see the [Client Development Guide](overview.md).

## Basic Use

`Client` provides `get`, `post`, and a general `request`. Each method returns an async context manager. `async with` sends the request and closes the underlying aiohttp response when the block exits:

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.http import Client

async def post_json() -> None:
    """POST a body and read the response as text."""
    cli = Client()
    ctx = TrpcContext()
    options = [Options().set_timeout(3000)]
    async with cli.post(ctx, "http://127.0.0.1:8001/test", options, data=b"hello") as rsp:
        print(rsp.status)
        print(await rsp.text())
```

The first string argument of `request` is any HTTP method, for example `"PUT"` or `"DELETE"`. `get` sends no body:

```python
async with cli.get(ctx, "http://127.0.0.1:8001/health") as rsp:
    body = await rsp.json()

async with cli.request(ctx, "PUT", "http://127.0.0.1:8001/item", data=b"{}") as rsp:
    raw = await rsp.read()
```

The URL must include a scheme and a host, for example `"http://127.0.0.1:8001/test"`. A relative path such as `"/test"` raises `InvalidURL`.

## How the URL Is Parsed and Matched

On each request, `Client` first builds a base `Options` from the URL, then appends the caller's `options` list and applies that list in order. A `set_*` in the caller's list therefore overrides the value derived from the URL. For this client the protocol is fixed as `"http"`, and the serialization type is fixed as `SerializationType.Noop`. The body is sent as raw bytes.

### 1. Scheme and Addressing

- **When the scheme is `trpc`** (for example `trpc://trpc.test.helloworld.Greeter/SayHello`): the host is the callee service name. It is handed to `TrpcSelector` for node selection and used to match YAML. The URL path is the RPC method name of this call.
- **When the scheme is anything else** (for example `http://127.0.0.1:8001/test`): the framework sets `set_target("scheme://host")`. In the example, `target` is `"http://127.0.0.1:8001"`. The URL path is still written as the RPC method name.

```python
async with cli.get(ctx, "trpc://trpc.test.helloworld.Greeter/SayHello") as rsp:
    text = await rsp.text()
```

### 2. Matching a YAML Client Entry

The `name` passed to `Client(name)` is the lookup key in `client.service[]` of `trpc_python.yaml`. When it is omitted or `""`, the key is the URL's `host:port`:

```python
cli = Client("example.http")
```

The matching configuration:

```yaml
client:
  service:
    - name: example.http
      timeout: 1000
      protocol: http
```

With `Client("example.http")`, a URL of `"http://127.0.0.1:8001/..."` still loads the YAML entry named `"example.http"`. The `target` is still `scheme://host` from the URL itself.

## Reading the Response

By default the response is streaming (`stream=True`). When the `async with` block starts, the body has not been read yet. `rsp` is a `TrpcHttpResponse`. Common attributes and methods:

| Attribute / method | Result |
| --- | --- |
| `rsp.status` | HTTP status code (`int`) |
| `rsp.header` | Response header map |
| `await rsp.json()` | Parse the body as a JSON object |
| `await rsp.text()` | Decode the body as text |
| `await rsp.read()` | Read the remaining body bytes (`bytes`) |
| `await rsp.pb(msg)` | Deserialize the body into the existing protobuf message `msg` |
| `async for chunk in rsp.stream_data()` | Iterate the raw response bytes in chunks |

`await rsp.pb(msg)` writes into the `msg` instance you passed in. Its return value is unused:

```python
reply = pb.HelloReply()
async with cli.post(ctx, "http://127.0.0.1:8001/pb", data=req.SerializeToString()) as rsp:
    await rsp.pb(reply)
```

- Passing `stream=False` to `get`, `post`, or `request` reads the full body into `bytes` before the call returns. `json`, `text`, `pb`, `read`, and `stream_data` in the table above are for the default `stream=True` mode.
- Other aiohttp request arguments can be passed as keyword arguments. The framework forwards them to the underlying `ClientSession`. For session reuse and short connections, see [Client Connection Modes](connection_mode.md).
