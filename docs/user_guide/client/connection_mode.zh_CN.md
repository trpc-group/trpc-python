# 客户端连接模式

在 tRPC-Python 中，基于 `trpc` 协议的 TCP 和 Unix Domain Socket 传输默认使用**连接池（Connection Pool）**复用长连接；若将 `short_conn` 设为 `True`，则会切换为**短连接模式**（每次请求新建连接，请求结束立即关闭）。

需要注意以下传输层特性：
- **流式调用强制长连接**：流式 RPC 调用在获取连接前会强制将 `short_conn` 设为 `False`，因为同一条流的后续帧收发必须保持在同一条 TCP 连接上。
- **HTTP 独立管理**：HTTP 客户端不使用上述 TCP 连接池，而是通过 `aiohttp.ClientSession` 管理连接（详见文末说明）。

## 1. 连接池模式（默认）

默认情况下，客户端通过连接池管理下游连接。每个事件循环（Event Loop）维护独立的连接池管理器，并按目标下游地址存取连接：请求执行期间独占该连接，请求完成后将其归还至空闲队列供后续请求复用。

连接池参数封装在 `ConnPoolOptions` 中，可通过 YAML 配置文件或代码中的 `Options.set_pool_opts` 设置（两者的字段对应关系如下）：

| YAML 配置字段 | `ConnPoolOptions` 参数名 | 默认值 | 含义说明 |
| --- | --- | --- | --- |
| `dial_timeout` | `dial_timeout` | `200` | 建立连接的超时时间（毫秒） |
| `idle_time` | `idle_timeout` | `60` | 空闲连接在池中的最大保留时间（秒）；`0` 表示不按空闲时间回收 |
| `max_conn_lifetime` | `max_conn_lifetime` | `0` | 连接自创建起的最长存活时间（秒）；`0` 表示不限制 |
| `max_idle_conn` | `max_idle` | `5` | 空闲连接队列的最大容量；`0` 表示不设上限 |
| `max_active_conn` | `max_active` | `0` | 同时存在的最大活跃连接数上限；`0` 表示不限制 |
| `is_wait_active_conn` | `wait` | `false` | 达到最大活跃连接数上限时的行为：`true` 表示排队等待可用连接，`false` 表示直接抛出 `ClientPoolLimitError` |

通过代码设置连接池参数：

```python
from trpc_sdk.client import Options
from trpc_sdk.transport import ConnPoolOptions

options = (
    Options()
    .set_target("ip://127.0.0.1:8000")
    .set_protocol("trpc")
    .set_pool_opts(ConnPoolOptions(max_idle=8, idle_timeout=30, max_active=32, wait=True))
)
```

通过 `trpc_python.yaml` 配置连接池参数：

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: ip://127.0.0.1:8000
      network: tcp
      protocol: trpc
      dial_timeout: 200
      idle_time: 60
      max_conn_lifetime: 0
      max_idle_conn: 5
      max_active_conn: 0
      is_wait_active_conn: false
```

## 2. 短连接模式

当 `short_conn` 设为 `True` 时，该次请求会使用 `ShortConnFactory`：每次请求前新建一条 TCP 连接，收到响应后立即关闭连接，不会将其放入空闲队列。适用于低频单次调用，或下游服务不支持在单条连接上连续处理多个请求的场景。

通过代码开启短连接（优先级高于 YAML）：

```python
options = Options().set_target("ip://127.0.0.1:8000").set_protocol("trpc").set_short_conn(True)
```

通过 YAML 开启短连接：

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: ip://127.0.0.1:8000
      protocol: trpc
      short_conn: true
```

## 3. HTTP 客户端的连接复用

HTTP 客户端底层基于 `aiohttp.ClientSession` 实现连接管理，不经过上述 tRPC TCP 连接池：

- **默认共享 Session**：默认情况下，所有 HTTP 请求共用同一个 `ClientSession` 实例。
- **按 URL 隔离 Session（`disable_reuse_http_session: true`）**：开启后，`ClientSession` 会按目标 URL 分别创建与维护，不同目标地址之间不再共用同一个 Session。
- **单次短连接（`short_conn: true`）**：开启后，每次 HTTP 请求结束时会立即关闭当前使用的 `ClientSession`。

通过代码配置：

```python
options = (
    Options()
    .set_protocol("http")
    .set_disable_reuse_http_session(True)
)
```

通过 YAML 配置：

```yaml
client:
  service:
    - name: example.http
      protocol: http
      disable_reuse_http_session: true
```

关于直接通过 URL 发起 HTTP 请求的用法，请参考 [HTTP 客户端使用指南](http.zh_CN.md)。
