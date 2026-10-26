# tRPC-Python 框架配置指南

tRPC-Python 框架在进程启动时读取一份 YAML 配置文件。调用 `create_server(conf_path)` 时，内部会执行 `TrpcConfig.load`，将 `global`、`server`、`client` 三个配置段解析并填充到进程内的配置对象中；而 `plugins` 段则作为原始字典保留，供各个插件自行通过 `get_plugins_config()` 读取。修改配置文件后需重启进程方可生效。

在示例项目 [examples/helloworld/trpc_main.py](../../examples/helloworld/trpc_main.py) 中，启动脚本通过命令行参数 `--conf` 接收配置文件路径（默认读取同目录下的 `trpc_python.yaml`）。注意 `--conf` 是业务入口脚本自行解析的参数，`create_server` 函数本身直接接收配置文件路径字符串。

配置文件由四个顶层段落组成：

| 顶层段落 | 职责说明 |
| --- | --- |
| `global` | 全局环境信息（命名空间、环境名、Set 名）、`uvloop` 开关、`rpcz` 采样参数、进程与线程模型、监听端口落盘配置 |
| `server` | 当前进程作为服务端时的配置：应用与服务名、超时与排队控制、监听端点列表（`service`）、拦截器、Admin 管理端口等 |
| `client` | 当前进程作为客户端调用下游时的配置：公共默认项、各下游服务（`service`）的寻址、超时、拦截器与连接池参数 |
| `plugins` | 插件专属配置字典，内部键名与结构由各插件自行定义 |

配置项缺省规则：未填写的字符串默认为空字符串 `""`，整数默认为 `0`，布尔值默认为 `false`，列表默认为空列表 `[]`。下文仅列出当前框架实际读取并生效的字段；关于进程模型与超时机制的详细说明，请参考 [服务端开发向导](server/overview.zh_CN.md) 与 [超时控制](timeout_control.zh_CN.md)。

## 1. `global` 全局配置

```yaml
global:
  namespace: Development            # 环境命名空间（如 Development / Production）
  env_name: test                    # 子环境名称
  container_name: ""                # 容器名称
  local_ip: ""                      # 本机 IP 地址
  enable_set: false                 # 是否开启 Set 路由
  full_set_name: ""                 # 完整 Set 名称（enable_set 为 true 时生效）
  enable_uvloop: false              # 是否启用 uvloop 事件循环加速
  save_bind_info:                   # 监听端口落盘配置
    enable: false                   # 是否将绑定的 name/ip/port 写入文件
    file_name: ./bind_info.log      # 落盘文件路径
  rpcz:                             # rpcz 调用链采样与缓存配置
    sample_rate: 1000               # 采样步长（span_id % sample_rate == 0 时采样，0 关闭）
    lower_water_level: 50           # 每秒采样数低水位线
    high_water_level: 200           # 每秒采样数高水位线
    sample_minimum_timecost: 0      # 最小采样耗时阈值（毫秒）
    collect_interval_ms: 500        # 采样数据收集间隔（毫秒）
    remove_interval_ms: 1000        # 过期 Span 清理间隔（毫秒）
    print_spans_num: 10             # Admin 接口默认返回的 Span 摘要条数
    cache_expire_interval: 60       # Span 内存缓存过期时间（秒）
    service_whitelist: []           # 采样服务名白名单（空列表表示不过滤）
  processmodel:                     # 进程与线程模型配置
    mode: single_process            # 运行模式：single_process（单进程）或 multi_process（多进程）
    io_handle_type: merge           # I/O 与业务处理关系：merge（合并）或 separate（分离）
    io_num: 1                       # I/O 线程数（单进程）或 I/O 进程数（多进程分离模式固定为 1）
    handle_num: 1                   # 业务线程数（单进程）或业务子进程数（多进程，0 表示按 CPU 核数）
    task_num: 1                     # 异步 Task 插件默认启动的子进程数
```

### 基础环境与事件循环

- **环境与 Set 路由**：`namespace` 和 `env_name` 会写入每个服务端 `Service` 的运行选项中。当 `enable_set` 设为 `true` 时，`full_set_name` 会作为当前服务的 Set 名称，并在客户端发起调用时随选点选项传递给 `Selector`（本仓库不包含具体的名字服务实现，Set 与多环境路由逻辑由外部 `Selector` 插件决定）。
- **`enable_uvloop`**：设为 `true` 且运行环境已安装 `uvloop` 包时，服务事件循环将使用 `uvloop` 加速。
- **`save_bind_info`**：当 `enable: true` 时，`create_server` 会将每个 `service` 绑定的 `name ip port` 信息追加写入 `file_name` 指定的文件中（默认为 `./bind_info.log`），便于外部脚本获取动态绑定的端口。

### `global.rpcz` 调用链诊断配置

`global.rpcz` 用于配置 `rpcz` 的采样与缓存参数。注意：要使 `rpcz` 生效，还需将拦截器名称 `rpcz` 加入 `server.filter` 或 `client.filter` 列表中（详见 [过滤器插件](../developer_guide/develop_plugins/filter.zh_CN.md)，查询接口见 [管理命令](admin.zh_CN.md)）。

| 字段 | 默认值 | 含义说明 |
| --- | --- | --- |
| `sample_rate` | `1000` | 按比例采样步长，仅当 `span_id % sample_rate == 0` 时采样；设为 `0` 则关闭按比例采样 |
| `lower_water_level` | `50` | 每秒采样数低水位线：当每秒已采样数低于此值时，即使未命中 `sample_rate` 比例也会保留 Span |
| `high_water_level` | `200` | 每秒采样数高水位线：达到此上限后暂停额外采样 |
| `sample_minimum_timecost` | `0` | 最小耗时阈值（毫秒）：耗时低于该值的 Span 不予保留 |
| `print_spans_num` | `10` | Admin 接口 `GET /cmds/rpcz` 返回的 Span 摘要条数 |
| `cache_expire_interval` | `60` | Span 在内存中的缓存保留时间（秒） |
| `service_whitelist` | `[]` | 服务名白名单；为空列表时表示不按服务名过滤 |

### `global.processmodel` 进程与线程模型

`global.processmodel` 用于统一配置服务的进程与线程模型（若不配置 `mode`，默认为单进程、单事件循环）。


| `mode` | `io_handle_type` | 运行效果说明 |
| --- | --- | --- |
| `single_process` | `merge`（默认） | **单进程合并模式**：启动 `io_num` 个线程，每个线程既负责网络 I/O 又执行业务 Handler。要求 `io_num` 与 `handle_num` 保持一致（若只配其一则自动取相同值；若配置不一致会打印 Warning 并以 `handle_num` 为准） |
| `single_process` | `separate` | **单进程线程分离模式**：在单个进程内启动 `io_num` 个 I/O 线程 + `handle_num` 个业务 Handle 线程（`handle_num` 必须大于 `0`） |
| `multi_process` | `separate`（默认） | **多进程主从分离模式**：1 个主 I/O 进程负责收发包与切帧，通过管道分发给 `handle_num` 个 `RpcTaskProcess` 业务子进程（此时 `io_num` 仅支持 `1`，配置其他值会打印 Warning 并按 `1` 处理） |
| `multi_process` | `merge` | **多进程对等模式**：启动 `handle_num` 个对等 Worker 进程，各自通过 `SO_REUSEPORT` 绑定全部监听地址（此时 `io_num` 不生效） |

补充说明：
- 在多进程模式（`multi_process`）下，若 `handle_num` 设为 `0`，框架会自动按当前机器的可用 CPU 核心数启动对应数量的子进程。
- `task_num` 用于指定未单独配置进程数的异步任务（Task 插件）默认启动的子进程数，设为 `0` 或省略时默认为 `1`。

## 2. `server` 服务端配置

```yaml
server:
  app: test                         # 应用名
  server: Greeter                   # 服务进程名
  bin_path: /usr/local/trpc/bin/    # 可执行文件目录
  conf_path: /usr/local/trpc/conf/  # 配置文件目录
  data_path: /usr/local/trpc/data/  # 数据文件目录
  handler_max_concurrency: 0        # 单 Handle 线程最大并发请求数（0 表示不限制，仅单进程分离模式生效）
  handler_queue_size: 1024          # 达到并发上限后的溢出等待队列长度
  tcp_reuse_port: false             # TCP 监听套接字是否开启 SO_REUSEPORT
  ready_timeout: 0                  # 工作线程或子进程启动就绪超时（毫秒，0 默认按 300000）
  close_wait_time: 0                # 优雅退出等待在途请求完成的时间（毫秒）
  io_wait_count: 0                  # 开始监听前需等待的 inc_io_wait_count 次数（0 表示不等待）
  heartbeat_interval: 0             # 父进程检查子进程心跳的间隔（秒，0 表示关闭）
  check_timeout: false              # 是否开启服务端全链路超时检查
  check_idle_timeout: 0             # 连接空闲超时检查间隔（毫秒，0 表示关闭）
  queue_wait_timeout: 0             # 请求在队列中的最大排队等待时间（毫秒）
  queue_wait_policy: 0              # 队列超时策略（0 直接丢弃/报错，或配合 queue_cb 熔断）
  queue_cb:                         # 队列超时熔断配置
    ratio: 0.1                      # 触发熔断的超时比例阈值
    threshold: 100                  # 统计窗口内的最小请求样本数
    interval: 60000                 # 统计时间窗口大小（毫秒）
  registry_delay: 0                 # 服务启动后延迟注册到名字服务的时间（毫秒）
  registry_option: 0                # 服务注册开关（0 关闭，1 开启）
  max_rbuf_size: 8388608            # 单连接最大读缓冲区大小（字节，默认 8 MiB）
  filter: []                        # 服务端公共拦截器列表
  admin:                            # Admin 管理服务配置
    ip: 127.0.0.1                   # Admin 监听 IP
    port: 9028                      # Admin 监听端口
  memory_inspection:                # 内存分配快照诊断配置（通过 SIGUSR2 触发）
    enabled: false                  # 是否启用 tracemalloc 内存监控
    filename_prefix: tracemalloc    # 快照输出文件名前缀
    line_limit: 50                  # 每次快照输出的最大统计行数
  service:                          # 服务端监听端点列表
    - name: trpc.trpc_python_group.helloworld.Greeter  # 注册服务名（trpc.{app}.{server}.{service}）
      ip: 127.0.0.1                 # 监听 IP 地址
      port: 8000                    # 监听端口（0 表示由系统随机分配空闲端口）
      network: tcp                  # 传输层网络类型（tcp / udp）
      protocol: trpc                # 应用层协议名（trpc / http 或自定义协议）
      timeout: 5000                 # 单次请求处理超时预算（毫秒）
      disable_request_timeout: false  # 是否忽略上游请求帧传入的剩余超时
```

### 服务端公共字段说明

- **基础标识与目录**：`app` 和 `server` 分别为当前服务的应用名和进程名；`bin_path`、`conf_path`、`data_path` 为业务约定的路径字段，解析后挂载在 `ServerConfig` 对象上供业务读取。
- **并发与队列限制**：`handler_max_concurrency` 和 `handler_queue_size` 仅在 `single_process` + `io_handle_type: separate`（单进程线程分离）模式下生效（各模式架构详见 [架构设计](../developer_guide/architecture.zh_CN.md)）。
- **优雅退出与心跳**：
  - `close_wait_time`（毫秒）：收到退出信号后，进程先停止 `accept` 新连接，并在该时间窗口内继续将正在处理中的请求响应写回。
  - `heartbeat_interval`（秒）：大于 `0` 时，父进程按该间隔检查子进程心跳；为 `0` 表示不开启心跳检查。
- **启动屏障（`io_wait_count`）**：大于 `0` 时，持有监听套接字的进程会等 task 进程累计调用 `inc_io_wait_count()` 达到该次数后才绑定地址，例如 Task 加载模型期间不对外提供服务。peer 模式没有 task，会打 warning 并忽略该值。
- **管理服务（`admin`）**：当 `admin.ip` 和 `admin.port` 同时非空时，根进程在 `fork` 之后启动独立线程提供 Admin HTTP 服务（详见 [管理命令](admin.zh_CN.md)）。
- **内存快照诊断（`memory_inspection`）**：当 `enabled: true` 时，Worker 进程会在事件循环启动前开启 Python 标准库的 `tracemalloc`。运行期间向该 Worker 进程发送 `SIGUSR2` 信号，会在当前工作目录下生成 `{filename_prefix}_{pid}_{序号}.txt` 文件：首次收到信号时输出自启动以来的内存分配统计，后续每次收到信号时输出相对于上一次快照的内存增量。`line_limit` 控制每次写入的统计行数上限（默认 `50`），`filename_prefix` 默认为 `tracemalloc`。
- **超时与其它控制**：`check_timeout`、`queue_wait_timeout`、`queue_wait_policy`、`queue_cb` 的配合使用见 [超时控制](timeout_control.zh_CN.md)；`registry_delay` 与 `max_rbuf_size` 说明见 [服务端开发向导](server/overview.zh_CN.md)。

### `server.service[]` 监听端点字段

| 字段 | 含义说明 |
| --- | --- |
| `name` | 注册到名字服务上的服务名，常见格式为 `trpc.{app}.{server}.{service}` |
| `ip` / `port` | 监听的 IP 地址与端口；当 `port` 设为 `0` 时，由操作系统自动分配空闲端口 |
| `nic` | 监听网卡名称；若与 `ip` 同时存在，优先使用 `ip` |
| `address` | 完整监听地址字符串；配置后会覆盖 `ip:port` |
| `network` | 传输层网络类型，支持 `tcp` 或 `udp` |
| `protocol` | 承载协议名称，内置支持 `trpc`、`http`，或填写已注册的自定义协议插件名 |
| `socket_type` | 套接字类型，默认为 `net`（网络套接字）；设为 `unix` 时使用 Unix Domain Socket，路径由 `unix_path` 指定 |
| `timeout` | 当前服务的单次请求处理超时预算（毫秒） |
| `disable_request_timeout` | 设为 `true` 时忽略请求帧中上游传入的剩余超时，仅使用本服务的 `timeout` |
| `idletime` | 单条连接的空闲超时时间（毫秒）；`server.check_idle_timeout` 为服务级别的另一项空闲检查开关 |
| `filter` | 当前服务专属的拦截器名称列表，会追加在公共 `server.filter` 列表之后 |
| `tls_key` / `tls_cert` | HTTP 服务的 TLS 私钥与证书文件路径，必须成对配置 |
| `registry` | 名字服务注册中心插件名称 |
| `max_window_size` | 流式 RPC 的接收窗口大小（字节），默认为 `65535` |
| `enable_stream_filter` | 流式 RPC 是否经过拦截器的 `run` 方法 |

## 3. `client` 客户端配置

```yaml
client:
  timeout: 1000                     # 客户端兜底超时时间（毫秒，仅未匹配到 service 条目时生效）
  namespace: Development            # 默认被调环境命名空间
  filter: []                        # 客户端公共拦截器列表
  service:                          # 下游被调服务配置列表
    - name: trpc.test.helloworld.Greeter  # 下游服务名（未配 callee 时也作为匹配键）
      network: tcp                  # 传输层网络类型（tcp / udp）
      protocol: trpc                # 应用层协议名（trpc / http 或自定义协议）
      timeout: 800                  # 当前下游服务的单次调用超时（毫秒）
      target: ip://127.0.0.1:8000   # 目标寻址 URI（scheme://endpoint）
```

### 配置匹配与继承规则

- **配置查找键**：框架加载 `client.service[]` 时，若配置项填写了 `callee` 则以 `callee` 作为查找键，否则以 `name` 作为查找键。
- **默认值与 `client.timeout` 的特殊规则**：当按被调服务名在 `client.service[]` 中**未找到**匹配项时，会回退使用顶层 `client` 段上的公共配置（此时 `client.timeout` 作为兜底超时生效）。但如果在 `client.service[]` 中匹配到了对应条目，而该条目自身省略了 `timeout` 字段，则该条目的 `timeout` 保持为 `0`（表示本次调用不从客户端额外设置截止时间，而**不会**继承 `client.timeout`）。

### `client.service[]` 常用字段

| 字段 | 含义说明 |
| --- | --- |
| `callee` | 桩代码写入 `message` 的被调服务名（用于匹配本条 YAML 配置） |
| `name` | 目标服务在名字服务上的注册名称 |
| `target` | 目标寻址 URI，格式为 `scheme://endpoint`（内置支持 `ip`、`http`、`https`、`dns`、`dsn`） |
| `namespace` / `env_name` / `set_name` | 传递给路由 `Selector` 的命名空间、目标环境名与目标 Set 名 |
| `network` / `protocol` | 网络类型与协议名，默认分别为 `tcp` 和 `trpc` |
| `timeout` | 客户端单次调用超时时间（毫秒）；`0` 表示不从客户端额外设置截止时间 |
| `serialization` / `compression` | 序列化与压缩类型的整数枚举值；`0` 分别代表 Protobuf（`PB`）与不压缩（`Noop`） |
| `filter` | 当前下游服务专属的客户端拦截器列表，追加在公共 `client.filter` 之后 |
| `discovery` / `loadbalance` / `circuitbreaker` | 未配置 `target`、走 `TrpcSelector` 组合链时使用的服务发现、负载均衡与熔断器插件名 |
| `password` | 下游服务的鉴权口令，供自定义协议传输层（如数据库插件）通过 `options.password` 读取 |
| `short_conn` | 设为 `true` 时使用短连接（每次请求新建连接，用完即关，不进连接池） |
| `dial_timeout` | 建立连接的超时时间（毫秒），默认 `200` |
| `idle_time` | 连接池中空闲连接的最大保留时间（秒），默认 `60` |
| `max_idle_conn` | 连接池最多保留的空闲连接数，默认 `5` |
| `max_active_conn` / `is_wait_active_conn` | 连接池最大活跃连接数上限，以及达到上限后是否排队等待 |
| `max_conn_lifetime` | 连接池中连接自创建起的最长存活时间（秒），`0` 表示不限制 |
| `disable_reuse_http_session` | HTTP 客户端是否禁止跨 URL 复用同一个 `ClientSession` |
| `enable_stream_filter` | 客户端流式调用是否经过拦截器的 `run` 方法 |

关于连接池与短连接的具体行为请参考 [客户端连接模式](client/connection_mode.zh_CN.md)，客户端完整调用流程请参考 [客户端开发向导](client/overview.zh_CN.md)。

## 4. `plugins` 插件与日志配置

`plugins` 是一个自由格式的字典。框架本身不会根据 `plugins` 下的键名自动实例化对象；插件需在代码被 `import` 时完成注册，并在初始化阶段自行读取 `plugins` 中对应的配置段。

### 内置日志配置（`plugins.log`）

框架默认日志后端基于 `loguru` 实现。在 `plugins.log` 下可直接生效的配置项为平铺的 `writer` 和 `level`：

```yaml
plugins:
  log:                              # 日志插件配置
    default:                        # 默认 Logger 名称（对应输出列表）
      - writer: console             # 输出目标插件名（内置支持 console / file）
        level: info                 # 日志级别（trace / debug / info / warning / error / fatal）
```

- **`level` 可选值**：`trace`、`debug`、`info`、`warning`、`error`、`fatal`（其中 `trace` 和 `debug` 在底层均映射为标准 `logging.DEBUG`）。
- 注意：嵌套结构的 `format_config`、`writer_config` 当前不会被框架自动转换为配置对象，具体字段限制及自定义 Writer 开发请参考 [日志插件开发](../developer_guide/develop_plugins/log.zh_CN.md)。

在业务代码中，可直接导入默认的 `logger` 使用；若希望在日志中携带当前请求的上下文维度字段，可先调用 `with_context_fields(ctx, ...)`，再使用 `*_context` 系列函数打印日志：

```python
from trpc_sdk.log import info_context
from trpc_sdk.log import logger
from trpc_sdk.log import with_context_fields

logger.info("listening")
with_context_fields(ctx, {"trace_id": "abc"})
info_context(ctx, "say hello %s", request.msg)
```

此外，可通过 `get_logger(name)` 获取 `plugins.log` 下配置的具名 Logger（若名称未登记则自动回退至 `default`）。服务运行期间如需动态调整日志级别，可通过 [管理命令](admin.zh_CN.md) 的 `PUT /cmds/loglevel` 接口操作。

> 若业务需要加载自定义的外部业务配置文件（而非 `trpc_python.yaml`），请使用 `ConfigLoaderFactory`，详见 [配置插件开发](../developer_guide/develop_plugins/config.zh_CN.md)。
