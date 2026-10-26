# HTTP SSE 客户端使用指南

`trpc_sdk.http.SSEClient` 构建在 [HTTP 客户端](http.zh_CN.md) 之上，用于消费服务端推送的 `text/event-stream`（Server-Sent Events，SSE）流式响应，并将字节流按 SSE 规范解析为结构化的 `SSEEvent` 对象供业务异步迭代。

SSE 协议格式说明：每条事件以空行（`\n\n`）作为结束分隔符；事件内部每行包含一个字段（支持 `data`、`event`、`id`、`retry`），字段名与字段值之间以第一个冒号 `:` 分隔；若同一条事件包含多行 `data`，框架会自动使用换行符 `\n` 将它们拼接为一个字符串。

## 基本用法

使用 `client.request_events(...)` 发起请求并按事件异步迭代：

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

参数说明：
- **`SSEClient(name="", encoding="utf-8")`**：`name` 会传递给内部持有的 `http.Client`，用于在 `trpc_python.yaml` 中匹配客户端配置；`encoding` 指定将字节流解码为文本时使用的字符编码（默认为 `"utf-8"`）。
- **`request_events(ctx, method, url, options, data=..., headers=...)`**：该方法会强制将底层 HTTP 请求设为流式模式，并逐个产出（yield）`data` 非空的 `SSEEvent` 事件。其中 `options` 为 `Options` 列表（追加在由 URL 生成的基础选项之后），`data` 为请求体，`headers` 为自定义请求头。

## `SSEEvent` 事件属性与方法

| 属性 / 方法 | 含义说明 |
| --- | --- |
| `event.data` | 事件载荷字符串。若服务端发送的事件未包含 `data`（为空字符串），`request_events` 会自动跳过该事件 |
| `event.event` | 事件类型名称；若服务端未显式指定，默认为 `"message"` |
| `event.id` | 最近一次收到的事件 ID；若服务端未指定则为空字符串 `""` |
| `event.retry` | 服务端建议的重连间隔（毫秒，整型）；若服务端未指定则为 `None` |
| `event.json()` | 将 `event.data` 按 JSON 解析并返回对应 Python 对象；若内容不是合法 JSON 则抛出 `json.JSONDecodeError` |

注意事项：
- **响应类型校验**：开始读取事件时，客户端会检查 HTTP 响应头中的 `Content-Type`。若其中不包含 `text/event-stream`，会直接抛出 `TypeError`。
- **不自动重连**：`event.retry` 仅记录服务端下发的建议值，客户端不会按该间隔自动发起重连；当底层连接断开后，`async for` 迭代即正常结束。

## 进阶：直接获取 `SSEEventSource`

如果需要在读取事件前先检查 HTTP 响应状态码或响应头，可以使用 `sse_iter_events`。它会产出一个 `SSEEventSource` 实例，在该事件源迭代结束前，底层 HTTP 响应会一直保持打开状态：

```python
async for source in client.sse_iter_events(ctx, "POST", "http://127.0.0.1:8080/sse", data=b"{}"):
    print(source.response.status)
    async for event in source.aiter_sse():
        print(event.id, event.data)
```

其中 `source.response` 即为 [HTTP 客户端](http.zh_CN.md) 中的 `TrpcHttpResponse` 对象；调用 `source.aiter_sse()` 同样会执行 `Content-Type` 校验并自动跳过 `data` 为空的事件。
