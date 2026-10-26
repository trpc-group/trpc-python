English | [中文](http.zh_CN.md)

# HTTP Server

When a service's `protocol` is `http` in `trpc_python.yaml`, the framework listens with `aiohttp` and serves HTTP/1.1. HTTP/2 and HTTP/3 are not supported.

The same HTTP service configuration supports two styles. Use either one, or both together. A runnable example is in [examples/http](../../../examples/http/README.md):

- **Generic HTTP RPC.** The interface is still defined in protobuf and generated as stub code. Only the transport changes to HTTP.
- **Standard HTTP.** No `.proto` file. Register routes with the `@route` decorator and handle the HTTP request and response yourself.

> **TLS.** To serve HTTPS, set both `tls_key` (private key path) and `tls_cert` (certificate path) on that `service`.

## 1. Generic HTTP RPC

The interface and registration are the same as for a [unary RPC service](overview.md): write a `.proto`, generate stub code, then call `create_server()`, `register_*_server()`, and `server.serve()`. The only difference is `protocol: http` in YAML:

```yaml
server:
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      ip: 127.0.0.1
      port: 9002
      network: tcp
      protocol: http
      timeout: 5000
```

### URL Path

The RPC method path is the HTTP URL path. For `SayHello` in the configuration above, the address is:

```text
http://127.0.0.1:9002/trpc.trpc_python_group.helloworld.Greeter/SayHello
```

When the service is registered with `fix_rpc_name=True`, the service-name span in the URL path is replaced by that `service`'s YAML `name`.

### Request Body and Serialization

- **GET.** The query string is the request payload and is deserialized as `Form`.
- **Other methods, such as POST.** The framework reads the HTTP body. The default deserialization is `JSON`. The `Content-Type` request header selects another format:

| `Content-Type` | Serialization |
| --- | --- |
| `application/json` | JSON (default) |
| `application/protobuf`, `application/x-protobuf`, `application/pb`, `application/proto` | Protobuf |
| `application/x-www-form-urlencoded` | Form |
| `application/octet-stream` | Raw bytes |

### Headers and Error Codes

- **Request control headers.** tRPC call-control fields travel in HTTP headers with a `trpc-` prefix, including `trpc-timeout`, `trpc-caller`, `trpc-callee`, `trpc-request-id`, and `trpc-trans-info`. `Content-Type` and `Content-Encoding` keep their standard HTTP names.
- **Response error codes.** A framework error code is returned in `trpc-ret`. A business error code is returned in `trpc-func-ret`.
- **Filters and fallback.** Every path that `@route` does not match falls through to the generic HTTP RPC entry. `aiohttp` first attaches the request object to the context. The server filter chain then runs as usual, and the transport skips tRPC binary-frame decompression and the default protobuf deserialization.

## 2. Standard HTTP

Standard HTTP does not need a `.proto` or generated stub code. Register handlers with the `@route` decorator.

> `@route` records the route when the module is imported. Import the module that defines the routes **before** `create_server()`.

```python
from aiohttp import web
from aiohttp.web_request import Request

from trpc_sdk.abc import TrpcContext
from trpc_sdk.http import get_http_response
from trpc_sdk.http import register_static
from trpc_sdk.http import route
from trpc_sdk.http import set_http_rsp_header


@route("/normal_http")
async def normal_http(ctx: TrpcContext, request: Request) -> web.Response:
    return web.json_response({"data": "json"})


@route("/bind_service_name", service_name="trpc.trpc_python_group.helloworld.Normal")
async def bind_service_name(ctx: TrpcContext, request: Request) -> web.Response:
    rsp = get_http_response(ctx)
    rsp.headers["url_path"] = request.path
    set_http_rsp_header(ctx, header={"test": "data"})
    return web.json_response({"data": "json"})


register_static("/static", "/path/to/files")
```

### Registering a Route and Binding a Service (`@route`)

`route(path, method="*", service_name=None)`:

- **`method`.** The default is `"*"`, which matches any HTTP method (GET, POST, and so on).
- **`service_name`.**
  - When it is omitted (`None`), every `service` in the configuration with `protocol: http` mounts this route.
  - When it is set, only the `service` whose YAML `name` matches exactly mounts it.

### Handling the Request and Building the Response

The handler signature is `async def handler(ctx: TrpcContext, request: Request)`. It receives the call context and an `aiohttp` `Request`. Two return styles are supported:

1. **Return a `web.StreamResponse` directly** (including `web.Response` and `web.json_response`). The framework uses that object as the HTTP response.
2. **Return `bytes` or `str`.** The framework writes them into the body of a response it prepared ahead of time. In this mode:
   - `get_http_response(ctx)` returns that prepared response, so the handler can change its status code or headers.
   - `set_http_rsp_header(ctx, header=...)` records headers. The framework merges them into the final response after the handler returns.

### Static Files and Match Order

`register_static(prefix, path_to_folder)` mounts a local directory at a URL prefix.

An incoming HTTP request is matched in this order:

1. An exact `@route`
2. A directory registered with `register_static`
3. The generic HTTP RPC fallback

> **Filter chain.** Standard HTTP routes also enter the server filter chain. The `req` passed to `filter.handle(ctx, req, rsp)` is the `aiohttp` `Request`, not a protobuf message.
