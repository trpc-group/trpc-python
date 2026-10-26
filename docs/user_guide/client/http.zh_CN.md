# HTTP 客户端使用指南

本文介绍如何使用 `trpc_sdk.http.Client` 直接通过 URL 发起标准 HTTP 请求。该客户端底层复用与一元 RPC 相同的路由选点（Selector）和客户端拦截器链（Filter Chain）。

> 若你使用的是 Protobuf 生成的桩代码，仅希望将底层传输协议切换为 HTTP（即泛 HTTP RPC），请参考 [客户端开发向导](overview.zh_CN.md)。

## 基本用法

`Client` 提供了 `get`、`post` 以及通用的 `request` 方法。这些方法均返回异步上下文管理器，通过 `async with` 发起请求，并在退出上下文块时自动关闭底层的 `aiohttp` 响应对象：

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

调用 `request` 时可通过第一个字符串参数指定任意 HTTP 方法（如 `"PUT"`、`"DELETE"`）；`get` 方法不携带请求体：

```python
async with cli.get(ctx, "http://127.0.0.1:8001/health") as rsp:
    body = await rsp.json()

async with cli.request(ctx, "PUT", "http://127.0.0.1:8001/item", data=b"{}") as rsp:
    raw = await rsp.read()
```

> 注意：传入的 URL 必须包含完整的 `scheme` 和 `host`（例如 `"http://127.0.0.1:8001/test"`），若仅传入相对路径（如 `"/test"`）会抛出 `InvalidURL` 异常。

## URL 解析与配置匹配规则

发起请求时，`Client` 会先根据传入的 URL 自动生成一组基础 `Options`，随后将调用方传入的 `options` 列表追加在后面依次执行（因此调用方在 `options` 中设置的 `set_*` 项会覆盖从 URL 推导出的默认值）。在 HTTP 客户端中，协议固定设为 `"http"`，序列化类型固定为 `SerializationType.Noop`（请求体按原始字节发送）。

### 1. URL Scheme 与寻址推导

- **当 `scheme` 为 `trpc` 时**（如 `trpc://trpc.test.helloworld.Greeter/SayHello`）：
  框架将 URL 中的 `host` 部分作为被调服务名交给 `TrpcSelector` 进行名字服务选点，并按该名称匹配 YAML 配置；URL 的路径部分（Path）则作为本次调用的 RPC 方法名。
- **当 `scheme` 为其他值时**（如 `http://127.0.0.1:8001/test`）：
  框架自动设置 `set_target("scheme://host")`（上例中 `target` 为 `"http://127.0.0.1:8001"`），URL 的路径部分同样写入 RPC 方法名。

```python
async with cli.get(ctx, "trpc://trpc.test.helloworld.Greeter/SayHello") as rsp:
    text = await rsp.text()
```

### 2. 匹配 YAML 中的客户端配置

构造 `Client(name)` 时传入的 `name` 参数用于作为查找键，去 `trpc_python.yaml` 的 `client.service[]` 中匹配配置（若不传或传入空字符串 `""`，则默认使用 URL 中的 `host:port` 作为查找键）：

```python
cli = Client("example.http")
```

对应的 YAML 配置示例：

```yaml
client:
  service:
    - name: example.http
      timeout: 1000
      protocol: http
```

当显式指定了 `Client("example.http")` 时，即使请求 URL 写为 `"http://127.0.0.1:8001/..."`，框架依然会按 `"example.http"` 查找并加载上述 YAML 配置，而目标地址 `target` 仍由 URL 自身的 `scheme://host` 决定。

## 读取响应内容

默认情况下，HTTP 响应按流式模式（`stream=True`）返回，即进入 `async with` 块时响应体尚未被读取。`rsp`（`TrpcHttpResponse`）提供的常用属性与方法如下：

| 属性 / 方法 | 返回结果说明 |
| --- | --- |
| `rsp.status` | HTTP 状态码（`int`） |
| `rsp.header` | HTTP 响应头映射 |
| `await rsp.json()` | 将响应体解析为 JSON 对象 |
| `await rsp.text()` | 将响应体解码为文本字符串 |
| `await rsp.read()` | 读取剩余的全部响应体字节（`bytes`） |
| `await rsp.pb(msg)` | 将响应体反序列化并填充到已创建的 Protobuf 消息对象 `msg` 中 |
| `async for chunk in rsp.stream_data()` | 按块（Chunk）异步迭代读取原始响应字节流 |

使用 `await rsp.pb(msg)` 时，数据会直接写入传入的 `msg` 实例中，无需接收其返回值：

```python
reply = pb.HelloReply()
async with cli.post(ctx, "http://127.0.0.1:8001/pb", data=req.SerializeToString()) as rsp:
    await rsp.pb(reply)
```

补充说明：
- 若在调用 `get` / `post` / `request` 时传入关键字参数 `stream=False`，框架会在返回前预先将完整响应体读取为 `bytes`（上表中的 `json`、`text`、`pb`、`read`、`stream_data` 方法均针对默认的 `stream=True` 模式设计）。
- 其他额外的 `aiohttp` 请求参数可通过关键字参数（`**kwargs`）直接传入，框架会将其透传给底层的 `ClientSession`。关于 Session 复用与短连接配置，请参考 [客户端连接模式](connection_mode.zh_CN.md)。
