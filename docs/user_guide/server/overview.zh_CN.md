# tRPC-Python 服务端开发向导

tRPC-Python 服务端基于 `asyncio` 构建。业务处理函数编写为 `async def` 协程，由框架在事件循环中调度执行。

本文按照开发一个服务端应用的完整脉络展开：**服务选型 -> 配置监听端点（Naming Service） -> 定义接口并注册实现（Proto Service） -> 挂载插件与拦截器 -> 调整进程模型与超时策略**。

- 完整可运行的一元 RPC 示例：[examples/helloworld](../../../examples/helloworld/trpc_main.py)
- 底层进程与线程分工原理：[架构设计](../../developer_guide/architecture.zh_CN.md)

## 1. 服务选型

框架内置支持四类服务。其中前三类通过 Protobuf 定义接口并由 `trpc create` 命令生成桩代码，第四类（标准 HTTP）直接通过装饰器注册路由，无需编写 IDL：

- **tRPC 一元服务**：配置 `protocol: trpc`，一请求一响应的标准 RPC 模式。
- **tRPC 流式服务**：配置 `protocol: trpc`，支持在单条连接上持续收发数据流（客户端流、服务端流、双向流）。详见 [流式服务](stream.zh_CN.md)。
- **泛 HTTP RPC**：配置 `protocol: http`，底层传输切换为 HTTP，但接口仍由 Protobuf 定义，业务实现方式与一元服务一致。详见 [HTTP 服务](http.zh_CN.md)。
- **标准 HTTP 服务**：配置 `protocol: http`，使用 `@route` 注册 `aiohttp` 处理函数，自行读写 HTTP 请求与响应。详见 [HTTP 服务](http.zh_CN.md)。

**补充说明：**
- **HTTP 与 TLS**：HTTP 服务底层基于 `aiohttp`（HTTP/1.1，暂不支持 HTTP/2 与 HTTP/3）。若需开启 TLS，在对应 `service` 配置中同时指定 `tls_key` 和 `tls_cert` 即可。
- **接入自定义协议**：若需对接其他私有或第三方协议，可参考 [协议插件开发](../../developer_guide/develop_plugins/protocol.zh_CN.md) 实现对应的 `codec` 与 `checker`，再将 `service.protocol` 设为注册的协议名。
- **进程内定时任务**：定时任务不属于对外暴露的 RPC 接口，通过 `@register_timer_task` 注册后，在服务进程的事件循环中按固定间隔或 Cron 表达式触发。详见 [定时器](timer.zh_CN.md)。

## 2. 配置监听端点（Naming Service）

在配置文件中，`server.service` 列表下的每一项代表一个独立的监听端点（Naming Service），包含监听地址、网络类型与协议。客户端通过 `name` 进行服务发现与寻址，命名格式通常遵循 `trpc.{app}.{server}.{service}`。

调用 `create_server()` 时，框架会为 `server.service` 中的每一项创建一个 `Service` 实例。以下是 [examples/helloworld/trpc_python.yaml](../../../examples/helloworld/trpc_python.yaml) 中的核心配置项：

```yaml
server:
  app: test
  server: Greeter
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      ip: 127.0.0.1
      port: 8000
      network: tcp
      protocol: trpc
      timeout: 5000
```

- `ip` / `port`：服务端绑定的监听地址与端口。
- `network`：传输层网络类型，可选 `tcp` 或 `udp`。
- `protocol`：应用层协议，内置支持 `trpc` 或 `http`。
- `timeout`：单次请求的处理超时时间（单位：毫秒）。

如果接入并配置了名字服务注册插件（Registry），服务进程在完成端口绑定并开始监听后，会自动将该 `name` 与监听地址注册到名字服务系统。

## 3. 定义与注册服务（Proto Service）

Proto Service 是在代码中实现的一组 RPC 方法集合。调用生成的 `register_*_server` 函数时，框架会将 Proto Service 的方法表绑定到上述 Naming Service 上。

### 3.1 基于 IDL 的服务开发（三步流程）

tRPC 一元服务、流式服务和泛 HTTP RPC 均遵循以下三步开发流程：

**第一步：编写 `.proto` 文件**

注意 `package` 名称会作为前缀拼入最终生成的 RPC 方法路径中：

```protobuf
syntax = "proto3";

package trpc.trpc_python_group.helloworld;

option py_generic_services = true;

service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
  rpc SayHi (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string msg = 1;
}

message HelloReply {
  string msg = 1;
}
```

**第二步：生成 Python 桩代码**

```bash
trpc create --lang=python -p helloworld.proto --rpconly -f
```

生成的 `rpc.py` 中包含服务端基类 `GreeterServicer` 以及注册函数 `register_GreeterServicer_server`。默认生成的 RPC 方法路径格式为 `/trpc.trpc_python_group.helloworld.Greeter/SayHello`。

**第三步：实现业务方法并启动服务**

代码结构与 [examples/helloworld/trpc_main.py](../../../examples/helloworld/trpc_main.py) 一致：

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.server import create_server

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc


class GreeterServicer(rpc.GreeterServicer):
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hi: " + request.msg
        return rsp


def serve(conf_path: str) -> None:
    server = create_server(conf_path)
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()
```

**关键机制说明：**
- **`create_server` 与 `serve` 的分工**：`create_server(conf_path)` 仅负责读取解析 YAML 并初始化内存中的 `Service` 对象，此时尚未绑定端口或 `fork` 子进程；直到调用 `server.serve()` 时，才会真正绑定监听地址并启动事件循环。
- **处理函数必须为 `async def`**：框架内部不维护用于执行同步阻塞函数的线程池，若将业务方法定义为普通同步函数（`def`），在请求到达时会报错失败。

### 3.2 多 Service 端口配置与 `fix_rpc_name`

`server.service` 下可以配置多个监听端点（分别监听不同端口或协议）。调用生成的 `register_*_server(server, servicer)` 时，框架会将同一份方法表注册到当前 `server` 下的**所有** `Service` 实例上。

- **开启 `fix_rpc_name=True`**：每个 `Service` 上的方法路径前缀会被替换为该 `Service` 在 YAML 中配置的 `name`（即变为 `/<YAML service.name>/<method>`）。这样每个监听端口都能以各自独立的 `name` 响应调用（需确保客户端请求的服务名与 YAML 中的 `name` 一致）。
- **注册多个不同的 Proto Service**：若对两个不同的 Proto 生成的 `register_*_server` 分别调用一次，两份方法表会同时挂载到每一个 `Service` 端点上（生成的注册函数不会按名称只挂载到某一个特定端口）。

### 3.3 标准 HTTP 服务

标准 HTTP 服务无需生成桩代码，在 YAML 中设置 `protocol: http` 后，使用 `@route` 装饰器注册路径即可。详见 [HTTP 服务端使用指南](http.zh_CN.md)。

## 4. 插件体系

插件在其定义模块被 `import` 时自动完成注册。因此，若使用了自定义或外部插件，必须在调用 `create_server()` **之前**先 `import` 对应的插件模块。

配置读取方式分为两类：
- 框架结构化字段（如 `server.filter`、`service.protocol`）由框架自动解析。
- 顶层 `plugins:` 配置段作为一个完整的字典保存，由各个插件内部调用 `get_plugins_config()` 自行读取。插件开发与注册方式详见 [插件开发基础](../../developer_guide/develop_plugins/plugin.zh_CN.md)。

按用途与依赖关系，插件可分为三类：
- **独立插件**：如协议（Protocol）、压缩（Compressor）、序列化（Serializer），不依赖外部基础设施，按需加载即可（参见 [协议插件](../../developer_guide/develop_plugins/protocol.zh_CN.md)）。
- **治理插件**：如名字服务、远程日志、配置中心，需对接外部系统。使用时需在入口处 `import` 插件模块，并在 `plugins:` 下填写对应的配置。
- **任务插件（Task）**：用于将 CPU 密集型、阻塞型或批量计算任务卸载到独立的 `task` 子进程池中执行（参见 [任务插件](../../developer_guide/develop_plugins/task.zh_CN.md)）。注意：多进程 `merge`（即 `peer`）模式下不支持挂载任务插件。

框架默认已内置并自动注册了以下基础插件：

| 插件类型 | 注册名称 | 功能说明 |
| --- | --- | --- |
| 日志后端 | `loguru` | 默认日志实现 |
| 日志输出 | `console`、`file` | `console` 输出到标准错误（stderr），`file` 输出到本地文件 |
| 配置来源 | `file` | 从本地文件系统读取配置文件 |
| 配置解码 | `yaml`、`json` | 将配置文件内容解析为字典 |

## 5. 服务端拦截器（Filter）

拦截器（Filter）以洋葱模型包裹单次 RPC 的业务处理过程。在 YAML 列表中排在前面的拦截器位于最外层：**其前置逻辑最先执行，后置逻辑最后执行**。

`create_server()` 会先加载全局的 `server.filter`，随后将各服务端点私有的 `server.service[].filter` 追加在后面。列表中填写的名称必须是已注册的拦截器，否则服务启动会报错。拦截器的编写方式与执行顺序详见 [过滤器插件](../../developer_guide/develop_plugins/filter.zh_CN.md)。

## 6. 业务错误处理

在业务处理函数中返回业务错误时，应抛出 [`TrpcException`](../../../trpc_sdk/exceptions/_trpc_exception.py)（可通过辅助函数 `create_business_error(code, msg)` 快速构造）：

```python
from trpc_sdk.exceptions import create_business_error

async def SayHello(self, ctx, request):
    if not request.msg:
        raise create_business_error(10001, "empty msg")
    ...
```

- 当处理函数抛出 `TrpcException` 时，框架会将其编码为携带对应错误码、包体为空的响应返回给客户端，当前 Worker 进程继续正常处理后续请求。
- 若抛出其他未捕获的 Python 异常，框架会将其包装为 `UnknownError` 并同样编码为错误响应返回。完整错误码列表请参考 [错误码说明](../error_code.zh_CN.md)。

## 7. 测试验证

本仓库使用 `pytest` 进行测试，服务端相关的注册与调用测试位于 `tests/server` 目录下。启动本地服务后，也可参考 [快速开始](../../quick_start.zh_CN.md) 使用客户端脚本发起联调调用。

## 8. 进程模型、超时与生命周期配置

### 8.1 进程与线程模型

进程与线程模型在 `global.processmodel` 段下配置。若不配置该段，默认采用**单进程、单事件循环**模式，适合绝大多数量级较轻或刚起步的服务。当需要更高并发能力或隔离阻塞调用时，可按目标场景选择对应配置：

| 适用场景与效果 | `global.processmodel` 配置组合 |
|---|---|
| **最简模式（默认）**：单进程、单事件循环 | 无需配置 |
| **单进程多线程合并模式**：多个事件循环线程，每个线程同时负责网络收发包与业务 Handler 执行 | `mode: single_process`<br>`io_handle_type: merge`<br>`io_num: N`, `handle_num: N` |
| **单进程多线程分离模式**：将网络 I/O 线程与业务 Handler 线程分离，避免 Handler 中的耗时操作拖慢收发包 | `mode: single_process`<br>`io_handle_type: separate`<br>`io_num: N`, `handle_num: M` |
| **多进程对等模式（Peer）**：多进程绕过 GIL 限制，各子进程独立监听并直接处理请求，进程间无转发开销 | `mode: multi_process`<br>`io_handle_type: merge`<br>`handle_num: N` |
| **多进程分离模式（Separate）**：多进程绕过 GIL 限制，由主进程统一负责网络接入并分发给后端的业务子进程 | `mode: multi_process`<br>`io_handle_type: separate`<br>`handle_num: N` |

**模型细节说明：**
- 各配置字段的详细定义见 [框架配置](../framework_conf.zh_CN.md) 的 `global` 章节；不同模型的底层架构原理见 [架构设计](../../developer_guide/architecture.zh_CN.md)。
- `server.handler_max_concurrency`（最大并发数）与 `server.handler_queue_size`（队列长度）仅在 **`single_process` + `separate`** 模式下生效。
- 在 **多进程 `separate`** 模式下，主（I/O）进程仅接管 `tRPC-over-TCP` 的网络收发；UDP 与 HTTP 协议仍由各个业务子进程（rpc task）自行绑定端口监听。
- 在 **多进程 `merge`（Peer）** 模式下，所有子进程通过 `SO_REUSEPORT` 共同绑定同一批监听地址，该模式下不支持挂载 `task` 任务插件。

### 8.2 请求超时与队列等待检查

- **处理超时（`service.timeout`）**：定义该服务端点处理单次请求的时间预算（单位：毫秒）。若上游请求帧头中携带了超时时间，且未开启 `disable_request_timeout: true`，则框架会取“上游剩余超时”与 `service.timeout` 中**更小（更严格）**的一个作为本次请求的超时上限。
- **队列等待检查（`check_timeout: true`）**：开启后，框架会在请求真正进入业务 Handler 之前，检查其在队列中已经等待的耗时：
  - 当 `queue_wait_policy: 0` 时：只要排队等待时间达到上限 `queue_wait_timeout`，即直接向客户端返回 `ServerTimeoutError`，不再执行业务 Handler。若 `queue_wait_timeout` 设为 `0`，则以本次请求合并后的总超时作为排队上限（若合并后的总超时也为 `0`，则跳过此项检查）。
  - 当 `queue_wait_policy: 1` 时：除了排队时间达到上限外，还需满足滑动窗口处于过载状态，才会直接丢弃请求并返回超时。

### 8.3 空闲连接回收（`check_idle_timeout`）

`check_idle_timeout` 用于设置 TCP 空闲连接超时时间（单位：毫秒，`0` 表示关闭空闲检查）。当值大于 `0` 时，若某条 TCP 连接在指定时间内没有任何数据收发，框架会自动将其关闭。

- 在多进程 `separate` 模式下，空闲连接检查在主 I/O 进程中执行；而由业务子进程自行监听的 UDP、HTTP 及自定义协议连接不经过此项检查。
- 当某个 `Service` 注册了流式 RPC 方法后，框架会自动关闭该 `Service` 的传输层空闲超时检查，以防止流式长连接被误断开。

### 8.4 名字服务延迟注册与 HTTP 包体限制

- **延迟注册（`registry_delay`）**：单位为毫秒，默认为 `0`。名字服务注册发生在端口开始监听之后；若服务启动后有较慢的预热或初始化逻辑、暂不希望立即接收外部流量，可调大该值以推迟注册时机。当 `registry_option` 设为 `1` 时，进程会改为等待共享的就绪标记（Ready Flag），而不再按 `registry_delay` 固定休眠。
- **HTTP 请求体大小限制（`max_rbuf_size`）**：对应底层 `aiohttp` 应用的 `client_max_size` 参数，默认值为 8 MiB（8388608 字节），用于限制单次 HTTP 请求体的最大字节数。

### 8.5 进程退出信号回调（SIGTERM）

服务进程收到 `SIGTERM` 信号时，可触发业务注册的清理回调。回调函数不接收参数，支持多次注册并按注册顺序依次执行（单个回调抛出异常不会中断后续回调的执行）。

> **注意**：该回调直接在信号处理路径上同步执行，因此**严禁包含阻塞操作**。

```python
from trpc_sdk.runtime import set_signal_handle

def on_server_exit() -> None:
    ...

def on_worker_exit() -> None:
    ...

set_signal_handle(on_server_exit, on_worker_exit)
```

- `master_handle`（第一个参数）：在启动服务的主进程退出收尾时执行。在 `merge` 模式下紧挨着 Worker 回调之前执行；在 `separate` 模式下会等待所有业务子进程（rpc task）全部退出后再执行。
- `worker_handle`（第二个参数）：在承载请求处理的 Worker 子进程退出时执行。

### 8.6 多进程 Fork 生命周期回调

在多进程模式下，`server.serve()` 会将 `multiprocessing` 的启动方式设为 `fork`。由于父进程中已启动的线程、正在运行的协程以及持有的锁无法在 `fork` 出的子进程中安全沿用，若需要在 `fork` 前后执行资源清理或重建，请在调用 `serve()` **之前**通过 `register_at_fork` 和 `register_child_callback` 注册无参回调函数：

```python
from trpc_sdk.common import ProcessType
from trpc_sdk.common import register_at_fork
from trpc_sdk.common import register_child_callback

def before_fork() -> None:
    ...

def after_parent() -> None:
    ...

def after_any_child() -> None:
    ...

def after_worker() -> None:
    ...

register_at_fork(before=before_fork, after_in_parent=after_parent, after_in_child=after_any_child)
register_child_callback(after_worker, ProcessType.WORKER)
```

各回调的触发时机如下：
- `before`：父进程执行 `fork` **之前**触发。
- `after_in_parent`：父进程完成 `fork` **之后**在父进程内触发。
- `after_in_child`：每个子进程被 `fork` 出来后立即在子进程内触发。
- `register_child_callback(func, process_type)`：紧接在 `after_in_child` 之后触发，但支持按子进程角色过滤——指定 `ProcessType.WORKER` 时仅在业务 Worker 子进程中执行，指定 `ProcessType.TASK` 时仅在 Task 任务子进程中执行。
- `register_before_loop_run_in_child(func)`：在子进程完成所有插件初始化之后、正式进入事件循环 `loop.run_forever()` 之前触发。
