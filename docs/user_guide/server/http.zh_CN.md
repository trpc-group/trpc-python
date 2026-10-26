# HTTP 服务端使用指南

当在 `trpc_python.yaml` 中将服务的 `protocol` 设为 `http` 时，框架会基于 `aiohttp` 监听并提供 HTTP/1.1 服务（暂不支持 HTTP/2 与 HTTP/3）。

同一套 HTTP 服务配置支持两种开发模式，可按需选择或混合使用（完整可运行示例请参考 [examples/http](../../../examples/http/README.md)）：
- **泛 HTTP RPC**：接口仍由 Protobuf 定义并生成桩代码，底层传输协议切换为 HTTP。
- **标准 HTTP 服务**：无需编写 `.proto` 文件，直接通过 `@route` 装饰器注册路由并自行处理 HTTP 请求与响应。

> **启用 TLS**：若需开启 HTTPS，只需在对应 `service` 配置下同时填写 `tls_key`（私钥路径）和 `tls_cert`（证书路径）即可。

## 1. 泛 HTTP RPC

泛 HTTP RPC 的接口定义与服务注册方式与 [一元 RPC 服务](overview.zh_CN.md) 完全一致：编写 `.proto` 并生成桩代码，依次调用 `create_server()`、`register_*_server()` 和 `server.serve()`。唯一的区别是在 YAML 配置中将 `protocol` 设为 `http`：

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

### URL 路径映射规则

RPC 方法路径直接映射为 HTTP 请求的 URL Path。以上述配置中的 `SayHello` 方法为例，其请求地址为：

```text
http://127.0.0.1:9002/trpc.trpc_python_group.helloworld.Greeter/SayHello
```

如果在注册服务时传入了 `fix_rpc_name=True`，则 URL Path 中的服务名部分会被替换为该 `service` 在 YAML 中配置的 `name`。

### 请求体与序列化格式

- **GET 请求**：框架将 URL 中的查询字符串（Query String）作为请求载荷，按 `Form` 格式反序列化。
- **其他请求方法（如 POST）**：框架读取 HTTP 请求体（Body），默认按 `JSON` 格式反序列化，也可通过请求头 `Content-Type` 指定其他序列化格式：

| `Content-Type` 请求头 | 对应的序列化方式 |
| --- | --- |
| `application/json` | JSON（默认） |
| `application/protobuf`、`application/x-protobuf`、`application/pb`、`application/proto` | Protobuf |
| `application/x-www-form-urlencoded` | Form 表单 |
| `application/octet-stream` | 原始字节流（Raw Bytes） |

### 协议头与错误码传递

- **请求控制头**：tRPC 调用控制字段通过带 `trpc-` 前缀的 HTTP Header 传递，包括 `trpc-timeout`、`trpc-caller`、`trpc-callee`、`trpc-request-id`、`trpc-trans-info` 等；`Content-Type` 和 `Content-Encoding` 则保持标准 HTTP 头名称不变。
- **响应错误码**：框架层错误码通过响应头 `trpc-ret` 返回，业务逻辑错误码通过响应头 `trpc-func-ret` 返回。
- **拦截器与回退机制**：所有未被 `@route` 匹配到的请求路径，最终都会回退（Fallback）到泛 HTTP RPC 入口。此时 `aiohttp` 会先将请求对象挂载到上下文中，随后正常执行服务端拦截器链（Filter Chain），并在传输层跳过 tRPC 二进制帧解压与默认 Protobuf 反序列化流程。

## 2. 标准 HTTP 服务

标准 HTTP 服务不需要定义 `.proto` 或生成桩代码，直接通过 `@route` 装饰器注册处理函数即可。

> **注意**：`@route` 装饰器在模块导入（`import`）阶段完成路由登记，因此请确保在调用 `create_server()` **之前** 先 `import` 定义了路由的业务模块。

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

### 路由注册与服务绑定（`@route`）

`route(path, method="*", service_name=None)` 的参数规则如下：
- **`method`**：默认值为 `"*"`，表示匹配任意 HTTP 请求方法（GET、POST 等）。
- **`service_name`**：
  - 未指定（`None`）时，配置文件中所有 `protocol: http` 的 `service` 都会挂载该路由。
  - 显式指定时，仅挂载到 YAML 中 `name` 与之完全匹配的那个 `service` 上。

### 请求处理与响应构造

路由处理函数的签名为 `async def handler(ctx: TrpcContext, request: Request)`，接收调用上下文与 `aiohttp` 的 `Request` 对象。返回值支持以下两种方式：

1. **直接返回 `web.StreamResponse`（含 `web.Response`、`web.json_response` 等）**：框架直接将其作为最终的 HTTP 响应返回。
2. **返回 `bytes` 或 `str`**：框架将其写入预先创建的默认响应对象的 Body 中。在此模式下：
   - 可调用 `get_http_response(ctx)` 获取该预创建的响应对象，直接修改其状态码或 Header。
   - 也可调用 `set_http_rsp_header(ctx, header=...)` 记录响应头，框架会在处理函数返回后将其合并到最终响应中。

### 静态文件挂载与路由匹配优先级

通过 `register_static(prefix, path_to_folder)` 可将本地目录挂载到指定的 URL 前缀下。

当收到 HTTP 请求时，框架按以下优先级顺序进行匹配：
1. 精确匹配的 `@route` 路由
2. `register_static` 注册的静态文件目录
3. 泛 HTTP RPC 兜底入口

> **关于拦截器链**：标准 HTTP 路由同样会经过服务端拦截器链（Filter Chain）。需要注意的是，此时传入拦截器 `filter.handle(ctx, req, rsp)` 的 `req` 参数是 `aiohttp` 的 `Request` 对象，而非 Protobuf 消息实例。
