# 错误码说明

在 tRPC-Python 中，所有框架级与业务级错误统一表示为 `TrpcException` 异常，包含三个核心属性：`type`（错误类型）、`code`（错误码）和 `msg`（错误描述）。

## 错误类型与构造方法

`type` 取自枚举 `TrpcErrorType`，用于区分错误来源：

| 枚举值 | 名称 | 含义 |
| --- | --- | --- |
| `1` | `FRAMEWORK` | 本端框架错误 |
| `2` | `BUSINESS` | 业务逻辑错误 |
| `3` | `CALLEE_FRAMEWORK` | 下游（被调端）框架错误 |

`code` 取自枚举 `TrpcErrorCode`（标准协议错误码在运行时从 `trpc.TrpcRetCode` 加载，数值与 `trpc.proto` 保持一致）：

- **抛出业务错误**：使用 `create_business_error(code, msg)` 构造异常，此时 `type` 固定为 `BUSINESS`，`code` 由业务自行定义。
- **抛出框架错误**：使用 `create_frame_error(code, msg)`，或直接抛出下文表格中对应的具体异常子类。

当服务端业务处理函数抛出 `TrpcException` 时，框架会将其错误码与描述编码进响应帧（响应包体为空）返回给客户端，当前 Worker 进程不会崩溃，继续正常处理后续请求。若抛出的是其他非 `TrpcException` 的普通 Python 异常，框架会将其包装为 `UnknownError`（错误码 `999`）返回。

```python
from trpc_sdk.exceptions import create_business_error

async def SayHello(self, ctx, request):
    if not request.msg:
        raise create_business_error(10001, "empty msg")
```

## 服务端错误码

| 错误码 | 枚举名称 | 对应异常类 | 说明 |
| --- | --- | --- | --- |
| `0` | `TRPC_INVOKE_SUCCESS` | — | 调用成功 |
| `1` | `TRPC_SERVER_DECODE_ERR` | `ServerDecodeError` | 服务端协议帧解码或反序列化失败 |
| `2` | `TRPC_SERVER_ENCODE_ERR` | `ServerEncodeError` | 服务端响应编码或序列化失败 |
| `11` | `TRPC_SERVER_NOSERVICE_ERR` | — | 请求的目标服务名不存在 |
| `12` | `TRPC_SERVER_NOFUNC_ERR` | `ServerFuncNotFoundError` | 请求的 RPC 方法未注册 |
| `21` | `TRPC_SERVER_TIMEOUT_ERR` | `ServerTimeoutError` | 服务端处理超时或队列等待超时 |
| `22` | `TRPC_SERVER_OVERLOAD_ERR` | — | 服务端过载（协议保留码） |
| `23` | `TRPC_SERVER_LIMITED_ERR` | — | 服务端限流（协议保留码） |
| `24` | `TRPC_SERVER_FULL_LINK_TIMEOUT_ERR` | — | 服务端全链路超时 |
| `31` | `TRPC_SERVER_SYSTEM_ERR` | `ServerSystemError` | 服务端系统内部错误 |
| `41` | `TRPC_SERVER_AUTH_ERR` | — | 服务端鉴权失败（协议保留码） |
| `51` | `TRPC_SERVER_VALIDATE_ERR` | — | 服务端参数校验失败（协议保留码） |

> 注：`22`、`23`、`41`、`51` 为 tRPC 协议标准保留错误码，供过载保护、限流、鉴权和参数校验等外部插件使用（本核心仓库不包含这些插件的内置实现）。

## 客户端错误码

| 错误码 | 枚举名称 | 对应异常类 | 说明 |
| --- | --- | --- | --- |
| `101` | `TRPC_CLIENT_INVOKE_TIMEOUT_ERR` | `ClientTimeoutError` | 客户端调用超时 |
| `102` | `TRPC_CLIENT_FULL_LINK_TIMEOUT_ERR` | — | 客户端全链路超时 |
| `111` | `TRPC_CLIENT_CONNECT_ERR` | `ClientConnectionError` | 客户端建立连接失败 |
| `121` | `TRPC_CLIENT_ENCODE_ERR` | `ClientEncodeError` | 客户端请求编码或序列化失败 |
| `122` | `TRPC_CLIENT_DECODE_ERR` | `ClientDecodeError` | 客户端响应解码或反序列化失败 |
| `123` | `TRPC_CLIENT_LIMITED_ERR` | — | 客户端限流 |
| `124` | `TRPC_CLIENT_OVERLOAD_ERR` | — | 客户端过载 |
| `131` | `TRPC_CLIENT_ROUTER_ERR` | `ClientRouteError` | 客户端名字服务路由选点失败 |
| `141` | `TRPC_CLIENT_NETWORK_ERR` | — | 客户端网络通信异常 |
| `151` | `TRPC_CLIENT_VALIDATE_ERR` | `ClientPoolLimitError` | 客户端连接池达到活跃上限（复用协议码 `151`） |
| `152` | `TRPC_CLIENT_POOL_CLOSED` | `ClientPoolClosedError` | 客户端连接池已关闭（Python 扩展码） |
| `153` | `TRPC_CLIENT_POOL_CONN_CLOSED` | `ClientPoolConnClosedError` | 连接池中的连接已关闭（Python 扩展码） |
| `161` | `TRPC_CLIENT_CANCELED_ERR` | — | 客户端调用被取消 |
| `171` | `TRPC_CLIENT_READ_FRAME_ERR` | — | 客户端读取协议帧失败 |
| `999` | `TRPC_INVOKE_UNKNOWN_ERR` | `UnknownError`、`ClientInvokeError` | 未归类的未知错误 |

> 注：`152` 和 `153` 为 tRPC-Python 针对连接池扩展的错误码，不在标准 `trpc.proto` 定义中；`ClientPoolLimitError` 则使用了协议码 `151`。

## 流式调用错误码

流式 RPC 错误码同样源自 `TrpcRetCode`，其中服务端流错误码区间为 `201`–`255`，客户端流错误码区间为 `301`–`361`，未归类的流式错误为 `1000`（`TRPC_STREAM_UNKNOWN_ERR`）：

| 错误码（服务端 / 客户端） | 枚举名称 | 说明 |
| --- | --- | --- |
| `201` / `301` | `TRPC_STREAM_SERVER_NETWORK_ERR` / `TRPC_STREAM_CLIENT_NETWORK_ERR` | 流式网络传输错误 |
| `211` / `311` | `TRPC_STREAM_SERVER_MSG_EXCEED_LIMIT_ERR` / `TRPC_STREAM_CLIENT_MSG_EXCEED_LIMIT_ERR` | 流式消息大小超出限制 |
| `221` / `321` | `TRPC_STREAM_SERVER_ENCODE_ERR` / `TRPC_STREAM_CLIENT_ENCODE_ERR` | 流式帧编码失败 |
| `222` / `322` | `TRPC_STREAM_SERVER_DECODE_ERR` / `TRPC_STREAM_CLIENT_DECODE_ERR` | 流式帧解码失败 |
| `231`–`234` / `331`–`334` | `TRPC_STREAM_*_WRITE_*` | 流写入异常（依次为：写结束、写缓冲区溢出、写连接关闭、写超时） |
| `251`–`255` / `351`–`355` | `TRPC_STREAM_*_READ_*` / `*_IDLE_TIMEOUT_ERR` | 流读取异常（依次为：读结束、读连接关闭、读空、读超时、流空闲超时） |
| `361` | `TRPC_STREAM_CLIENT_INIT_ERR` | 客户端流初始化失败 |

## Python 框架扩展错误码

以下错误码为 tRPC-Python 运行时特有的扩展错误码：

| 错误码 | 枚举名称 | 对应异常类 | 触发场景说明 |
| --- | --- | --- | --- |
| `200` | `TRPC_TASK_HANDLE_ERR` | `TaskHandleError` | Task 子进程任务处理失败（包括 Worker 找不到对应的 Task 管道连接、Task 进程内等待超时、任务队列已满、返回值不合法或用户任务函数抛出异常） |
| `202` | `TRPC_PLUGIN_INIT_ERR` | `PluginInitError`（`trpc_sdk.plugin`） | 插件初始化失败 |
| `203` | `TRPC_CONFIG_ERR` | `ConfigError`（`trpc_sdk.config`） | 配置文件加载或解析失败 |
