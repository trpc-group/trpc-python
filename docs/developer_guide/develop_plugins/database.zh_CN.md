# 怎么开发数据库插件

在 tRPC-Python 中，如果希望访问 Redis、MySQL 等存储时，能够复用 RPC 客户端的名字服务路由（`Selector`）、超时控制以及客户端拦截器（如监控上报、调用链追踪），可以通过自定义 `protocol` 将第三方数据库 SDK 接入 `Client.invoke` 调用链路。tRPC-Python 提供了 Redis、MySQL、Kafka 插件，可以参考生态仓库 [python-database](https://github.com/trpc-ecosystem/python-database)。

整体思路如下：

1. **命令传递**：数据库 SDK 通常接收结构化的命令或参数，而非序列化后的字节流。调用方将请求体设为空字节 `b""`（配合 `SerializationType.Noop`），把真正的命令对象放进 `Options().set_req_head(command)`，通过消息头 `msg.client_req_head` 传给传输层。
2. **协议占槽（`CLIENT_CODEC`）**：注册一个直通（pass-through）的客户端编解码器，对字节流原样透传。
3. **执行调用（`CLIENT_TRANSPORT`）**：在客户端过滤器链末端的 `round_trip` 中，从 `options.address` 拿到路由解析出的 `(host, port)`，从 `msg.client_req_head` 拿到命令对象，调用第三方数据库 SDK 完成读写，并将结果写回命令对象。

需要注意区分两个配置项的职责：
- `client.service.protocol`：决定使用哪套 `CLIENT_CODEC` 和 `CLIENT_TRANSPORT`（即选中上述数据库调用链路）。
- `client.service.target`：其中的 scheme（如 `ip://`、`polar://`）用于选择 `NAMING_SELECTOR` 插件，仅负责将目标解析为具体的节点地址 `(host, port)`。

下面以通用的协议名 `store` 为例说明完整实现（不绑定具体数据库）。

## 为什么必须同时注册 Codec 和 Transport

在 `trpc_python.yaml` 或调用参数中指定 `protocol: store` 时，`Options.set_protocol("store")` 会按以下顺序查找并挂载插件：

1. **查找 `CLIENT_CODEC`**：调用 `get_client_codec(protocol)`。**如果未找到同名 codec，`set_protocol` 会直接返回**，不会继续替换后续的 checker 和 transport。此时 options 仍保留默认的 `trpc` transport，且 `opts.codec` 为 `None`，后续调用 `Client.invoke` 会在进入传输层前抛出 `ClientEncodeError("client codec empty")`。
2. **查找 `CLIENT_CHECKER`**：找到 codec 后，继续调用 `get_client_parser(protocol)` 设置分包器。数据库调用由第三方 SDK 自行管理连接与协议解析，不需要框架做 TCP 切帧，因此无需注册同名 checker（`set_parser(None)` 不会影响自定义 `round_trip`）。
3. **查找 `CLIENT_TRANSPORT`**：调用 `get_client_transport(protocol)`。若存在同名实例，则用它替换 `_create_client_options` 默认设置的 `trpc` transport。

因此，自定义数据库协议时，**必须以相同的协议名同时注册 `CLIENT_CODEC` 和 `CLIENT_TRANSPORT`**；只注册 transport 会导致 `protocol: store` 配置不生效。

## 1. 实现 Codec（`CLIENT_CODEC`）

由于命令对象放在 `msg.client_req_head` 中传递，Codec 不需要做任何组包或解包，只需将字节流原样返回即可。

`encode_data` 和 `decode_data` 的第一个参数均为调用上下文 `context`。注意这两个方法必须是**普通同步函数**（不能是 `async def`），因为客户端 `_invoke_backend` 会直接同步调用它们，不会 `await`。

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

## 2. 实现 Transport（`CLIENT_TRANSPORT`）

继承 `BaseClientTransport` 并实现异步方法 `round_trip(ctx, req_data, options)`。该方法位于客户端 `filter` 链的最内层，所有配置的客户端拦截器都会包裹在这次 `round_trip` 外围执行。

在 `round_trip` 中可以获取以下信息来调用第三方数据库 SDK：
- **目标地址**：`options.address`，格式为 `(host, port)` 元组，由 `Selector` 根据 `target` 解析出节点后通过 `load_node_config` 填入。
- **鉴权口令**：如果在 YAML 的 service 配置下写了 `password`，框架会将其写入 `options.password`。
- **请求命令**：从 `ctx.get_message().client_req_head` 中取出业务侧传入的命令对象（此时 `req_data` 为空字节 `b""`）。

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
        # 在此处调用第三方数据库 SDK，可直接使用 host、port、options.password 和 command
        command.result = None
        return b""
```

执行完毕后直接返回空字节 `b""` 即可。`_invoke_backend` 在拿到返回值后会调用 `codec.decode_data`，当解码出的响应体长度为 `0` 时，框架会自动跳过反序列化步骤，不会去填充响应对象。

## 3. 配置文件（`trpc_python.yaml`）

在 `client.service` 下配置下游存储服务：

- `name`：被调服务名（callee）。未显式配置 `callee` 字段时，`parse_client_config` 会以 `name` 作为键存入客户端配置表。
- `protocol`：填写上面注册的协议名 `store`，用于选中 `StoreCodec` 和 `StoreTransport`。
- `target`：目标地址。例如 `ip://127.0.0.1:6379` 使用内置的 `ip` selector，直接解析 IP 和端口而不走 DNS。
- `password`（可选）：数据库访问口令，可在 `round_trip` 中通过 `options.password` 读取。

```yaml
client:
  service:
    - name: trpc.app.server.store
      network: tcp
      protocol: store
      target: ip://127.0.0.1:6379
      timeout: 1000
```

## 4. 业务侧调用

业务代码通过 `Client().invoke()` 发起调用时，有三个细节需要注意：

1. **先在 `message` 上设置被调服务名**：`get_options` 会先用调用时刻 `msg.callee_service_name` 的值去匹配 YAML 配置（`load_client_config`），随后才执行本次传入的 `Options` 覆盖项。因此必须在 `invoke` 之前调用 `msg.set_callee_service_name("trpc.app.server.store")`。如果只在 `Options().set_service_name(...)` 中设置名字，加载 YAML 的阶段已经结束，配置里的 `protocol` 和 `target` 将无法生效。
2. **使用 `SerializationType.Noop` 与空字节请求体**：`NoopSerializer.serialize` 仅接受 `bytes` 或 `Body` 实例，不能直接把自定义的 `StoreCommand` 当作请求体 `req` 传入。正确做法是请求体传 `b""`、响应类型传 `Body`，并将 `command` 放到 `Options().set_req_head(command)` 中（`update_message` 会将其复制到 `msg.client_req_head`）。
3. **确保插件模块已被导入**：服务启动时必须先 `import` 定义了 `StoreCodec` 和 `StoreTransport` 的模块，触发 `@register_plugin` 完成注册。

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

如果不使用 YAML 配置文件，也可以在传入的 `Options` 上链式调用 `.set_protocol("store").set_target("ip://127.0.0.1:6379")` 来直接指定协议和目标地址。
