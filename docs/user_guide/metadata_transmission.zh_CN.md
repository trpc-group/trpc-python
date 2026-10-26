# 链路透传（Metadata 透传）

tRPC 协议帧头中的 `trans_info` 字段用于跨进程传递一组键值对（Key 为 `str`，Value 为 `bytes`）：
- **打包发送时**：编解码器（Codec）会将 `TrpcMessage` 上的 `client_meta_data` 写入协议帧头的 `trans_info`。
- **拆包接收时**：服务端会将请求帧带来的 `trans_info` 存入 `server_meta_data`；客户端则将响应帧回传的 `trans_info` 存入 `client_meta_data`。

> **注意区分进程内元数据与跨进程透传元数据**：
> `TrpcContext` 自身的 `with_meta_data` / `get_meta_data` / `pop_meta_data` 方法读写的是 `TrpcContext.meta_data` 属性，**仅在当前进程的单次上下文中可见，不会被编码进网络协议帧**（HTTP 服务的 `request`、`response` 对象即存放在该本地字典中，详见 [HTTP 服务](server/http.zh_CN.md)）。跨进程透传必须使用下文介绍的 `Options.set_metadata` 或 `message` 上的 `client_meta_data` / `server_meta_data`。

## 客户端向下游传递透传信息

客户端向被调服务发送透传数据时，推荐在调用选项中使用 `Options.set_metadata(key, val)`（其中 `val` 必须为 `bytes` 类型），也可以在发起调用前直接操作 `message.set_client_meta_data(...)`：

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

两者的区别与上下文克隆行为说明：

1. **增量合并 vs 整表替换**：`Options.set_metadata(key, val)` 会在组装调用选项时合并进当前的 `client_meta_data`（若存在同名 Key，以本次 `set_metadata` 传入的值为准）；而 `message.set_client_meta_data(dict)` 则是直接替换整张字典。
2. **上下文克隆（`need_clone_ctx`）的影响**：
   - 生成的桩代码 Proxy 默认设置 `need_clone_ctx=True`。在克隆上下文时，框架会将原上下文中的 **`server_meta_data`** 复制为新出站消息的 **`client_meta_data`**，而原消息上已有的 `client_meta_data` **不会**被复制过去。
   - `Options.set_metadata` 发生在克隆完成之后，因此无论是否开启克隆都能稳定生效。如果希望在调用前直接通过 `get_message(ctx).set_client_meta_data(...)` 设置出站透传字典，或者希望在调用返回后从原来的 `ctx` 上读取下游响应带回的 `client_meta_data`，请在构造 Proxy 时传入 `need_clone_ctx=False`。

## 服务端读取上游透传并继续向下游传递

服务端完成请求拆包后，上游传入的 `trans_info` 存放在当前消息的 `server_meta_data` 中：

```python
from trpc_sdk.abc import get_message

async def SayHello(self, ctx, request):
    incoming = get_message(ctx).server_meta_data or {}
    trace_id = incoming.get("trace-id", b"")
    ...
```

如果在不克隆上下文（如 `need_clone_ctx=False` 或直接调用 `Client.invoke`）的情况下继续调用下游，框架不会自动将 `server_meta_data` 复制到出站请求中；此时若需继续向下游透传，需显式调用 `Options.set_metadata` 或在出站 `message` 上调用 `set_client_meta_data`。

## 框架保留的特殊透传键

以下两个键名由 tRPC 编解码器单独解析并映射到 `TrpcMessage` 的专用属性上，业务代码请勿将它们当作普通自定义透传字段使用：

| 保留键名 | 框架处理行为 |
| --- | --- |
| `trpc-dyeing-key` | 流量染色标识，拆包后自动写入 `message.dyeing_key` |
| `trpc-env` | 环境透传标识，拆包后自动写入 `message.env_transfer` |

此外，流式 RPC 在建立流时的 `INIT` 帧同样使用同一组 `trans_info` 字段传递透传信息。
