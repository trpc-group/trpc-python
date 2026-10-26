# 附件传输（Attachment）

在 tRPC 协议的一元调用中，除了经过序列化和压缩的业务包体（Body）外，还可以在帧末尾附带一段原始字节流（Attachment）。这段字节流**不经过序列化与压缩**，发送端将其直接拼接在业务 Body 之后发出，接收端则根据协议帧头中的 `attachment_size` 字段将其切分出来。

> 注意：附件传输仅适用于 `trpc` 协议，HTTP 协议不支持该机制。

附件的读写函数位于 `trpc_sdk.codec` 模块，直接操作当前 `TrpcContext` 内挂载的 `TrpcMessage`：
- 写入时：支持传入 `bytes` 或 `str`（传入 `str` 时会自动按 UTF-8 编码为 `bytes`）。
- 读取时：默认返回 `bytes`（未携带附件时返回空字节 `b""`）；若指定 `is_str=True`，则会按 UTF-8 解码为字符串后返回。

## 客户端发送与接收附件

客户端在发起调用前通过 `set_request_attachment` 设置请求附件。

需要注意上下文克隆行为：桩代码生成的 Proxy 默认开启了 `need_clone_ctx=True`。在克隆上下文时，框架会将已设置的请求附件（`request_attachment`）一并复制到克隆出的新 `message` 上；但服务端返回的响应附件（`response_attachment`）会写入实际发起调用的那份克隆 `message` 中。因此，**如果希望在调用返回后直接从原本传入的 `ctx` 中读取响应附件，需在构造 Proxy 时传入 `need_clone_ctx=False`**：

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.codec import get_response_attachment
from trpc_sdk.codec import set_request_attachment

proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
ctx = TrpcContext()
set_request_attachment(ctx, b"request attachment")

options = Options().set_target("ip://127.0.0.1:8000").set_protocol("trpc")
reply = await proxy.asyncSayHello(ctx, req, options)
print(get_response_attachment(ctx, is_str=True))
```

## 服务端读取与回传附件

服务端在业务处理函数中，可通过 `get_request_attachment` 读取客户端发来的附件，并通过 `set_response_attachment` 设置回传给客户端的响应附件：

```python
from trpc_sdk.codec import get_request_attachment
from trpc_sdk.codec import set_response_attachment

async def SayHello(self, ctx, request):
    incoming = get_request_attachment(ctx, is_str=True)
    set_response_attachment(ctx, f"return: {incoming}")
    ...
```

## 注意事项

1. **不会自动向下游透传**：附件的具体格式与语义由业务自行约定，框架不会校验附件内容与业务 Body 的一致性。当服务端继续作为客户端调用下游时，框架**不会**自动将上游请求附件复制到出站请求中；如需继续向下游传递，需显式调用 `set_request_attachment` 写入出站上下文。
2. **协议兼容性与解包异常**：若帧头中的 `attachment_size` 大于接收缓冲区的剩余字节数，拆包将直接报错（服务端抛出 `ServerDecodeError`，客户端抛出 `ClientDecodeError`）。此外，调用链路上每一跳的对端都必须支持识别 `attachment_size` 字段；若对端不支持附件机制，会将业务 Body 与附件字节流当成一整段数据进行 Protobuf 反序列化，从而导致解析异常。
