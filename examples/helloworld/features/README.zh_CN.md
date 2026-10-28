# helloworld 特性

本文档说明演示用户协议、管理命令、HTTP 路由，以及单进程多线程、多进程 peer、多进程 separate 这几种例子。

## 特性一览

| 路径 | 说明 | 自检 |
| --- | --- | --- |
| [user_protocol.py](user_protocol.py) | 注册 `protocol: user`，任意 UDP 数据报回复 `user:<index>:<pid>` | [check_user.py](check_user.py) 核对默认服务的 `8090` |
| [admin/](admin/) | 管理 HTTP API，并加一条 `GET /cmds/whoami` | [admin/check.py](admin/check.py)，merge / peer / separate |
| [http/](http/) | `@route` 示例，挂到默认服务的 HTTP 端口上 | 默认服务里这段导入是注释，没有独立检查 |
| [http/thread/](http/thread/) | 单进程多线程上的 HTTP RPC 和 `@route` | [http/thread/check.py](http/thread/check.py) |
| [thread/](thread/) | 单进程多线程上的 TCP、UDP 和用户协议 | [thread/check.py](thread/check.py) |
| [peer/](peer/) | 多进程 merge：每个 worker 自己监听全部地址 | [peer/check.py](peer/check.py) |
| [separate/](separate/) | 多进程 separate：I/O 进程收 TCP 并转发给 rpc task | [separate/check.py](separate/check.py) |

同名的 `separate` 是两套布局。[thread/conf/separate.yaml](thread/conf/separate.yaml) 和 [http/thread/conf/separate.yaml](http/thread/conf/separate.yaml) 是 `single_process` + `io_handle_type: separate`，一个进程里用 I/O 线程和处理线程。[separate/](separate/) 是 `multi_process` + `io_handle_type: separate`，一个 I/O 进程加若干 rpc task 子进程。

<a id="user-protocol"></a>

## 用户协议

[user_protocol.py](user_protocol.py) 在导入时 `register_protocol_handle("user", ...)`。配置里有 `protocol: user` 的服务时，运行时在 0 号服务循环上调用一次启动函数，绑一个带 `SO_REUSEPORT` 的 UDP 套接字。任意数据报都回 `user:<index>:<pid>`。在默认服务和下面的单进程线程布局里，`index` 是 `0`，`pid` 是服务进程。

默认 [trpc_main.py](../trpc_main.py) 在 `serve()` 前已经导入它。[trpc_python.yaml](../trpc_python.yaml) 里 `trpc.trpc_python_group.helloworld.GreeterUser` 监听 `127.0.0.1:8090`。

[check_user.py](check_user.py) 用这份默认配置拉起 `trpc_main.py`，向 `8090` 发一个数据报，要求回复等于 `user:0:<服务进程 pid>`，再向进程组发 `SIGTERM`，task 子进程随服务一起退出。

```bash
python3 features/check_user.py
```

成功时打印 `ok user 127.0.0.1:8090 pid=<pid>`。

[peer/](peer/) 和 [separate/](separate/) 各自在 servicer 里注册了同名协议，回复格式相同。peer 每个 worker 绑一个套接字，separate 每个 rpc task 绑一个套接字，`index` 是该 worker 或 rpc task 的序号。

<a id="admin"></a>

## 管理命令

[admin/server.py](admin/server.py) 打开框架的管理 HTTP API，并注册自定义命令 `whoami`。根进程在 fork 之后用单独的线程和事件循环监听管理端口，提供 `/version`、`/cmds/*`，并在 `/_/ws` 接受 worker 的 WebSocket。每个 worker 进程再连回这条 WebSocket。task 进程不连接。本示例不注册 `py_task`。

`GET /cmds/whoami` 向每条还开着的 WebSocket 发 `whoami {"request_id": "..."}`，worker 回自己的 `os.getpid()`。根进程的 pid 会出现，是因为它自己也作为 worker 连了一条 WebSocket。

| 配置 | 进程模型 | 管理端口 | 业务端口 | `whoami` 的 pid 数 |
| --- | --- | --- | --- | --- |
| [admin/conf/merge.yaml](admin/conf/merge.yaml) | `single_process` + `merge` | `19420` | `18420` | 1，根进程 |
| [admin/conf/peer.yaml](admin/conf/peer.yaml) | `multi_process` + `merge`，`handle_num: 2` | `19421` | `18421` | 2，worker-0 和 worker-1 |
| [admin/conf/separate.yaml](admin/conf/separate.yaml) | `multi_process` + `separate`，`handle_num: 2` | `19422` | `18422` | 3，I/O 根进程加两个 rpc task |

业务端口上注册了 `GreeterServicer`。检查脚本不调用它，也没有打开 `greeter.task_enabled`。

```bash
python3 features/admin/check.py
python3 features/admin/check.py merge
```

不带参数时三种模式依次检查。核对 `/version`、`/cmds`（含 `/cmds/whoami`）、读写 `/cmds/loglevel`、`/cmds/config` 里的管理端口、`/cmds/debug/language` 为 `python`、`/cmds/rpcz` 的 `spans` 是列表，以及 `whoami` 的 pid 个数。这三份配置没有打开 rpcz filter，`spans` 一般是空列表。成功时打印 `ok: merge`、`ok: peer`、`ok: separate`。

<a id="http"></a>

## HTTP 路由

[http/_server.py](http/_server.py) 用 `@route` 注册路由，导入时打印 `=========== register http ==================`，并把仓库的 `docs/` 挂到 `/static`。

| 路径 | 挂到哪些服务 | 响应 |
| --- | --- | --- |
| `/normal_http` | 配置里每一个 HTTP 服务 | JSON `{"data": "json"}` |
| `/bind_service_name` | 只挂 `trpc.trpc_python_group.helloworld.Greeter1` | 同上，并设置响应头 `url_path` |
| `/route_with_rsp_header` | 每一个 HTTP 服务 | 正文 `user-defined test`，响应头 `test` 为 `data`，同样有 `url_path` |
| `/static` | 每一个 HTTP 服务 | 仓库 `docs/` 下的静态文件 |

装饰器没有写 `method`，默认接受任意方法。[http/_client.py](http/_client.py) 的 `HTTP_TEST` 对 `127.0.0.1:8002` 发 `SayHello`、`SayHi`，以及上面三条路由的 POST；对 `127.0.0.1:8003` 发 `/normal_http` 和 `/route_with_rsp_header`。`SayHello` 的 `msg` 是 `hello: hello`，`SayHi` 是 `hi: hello`。

默认 [trpc_main.py](../trpc_main.py) 里 `import features.http` 是注释。[trpc_python.yaml](../trpc_python.yaml) 里 `Greeter1`（`8002`，`http`）和 `Greeter2`（`8003`，`http`）也是注释。要跑这组路由，打开这两处，再打开 [test_service_rpc.py](../test_service_rpc.py) 里的 `test_http_client`，先起 `trpc_main.py`，再跑客户端。

[http/thread/server.py](http/thread/server.py) 故意不导入 `features.http`。那份服务有自己的 `/thread_info`，和这里的路由不要装进同一个进程。

<a id="thread"></a>

## 单进程线程

[thread/server.py](thread/server.py) 按 YAML 选择线程模型。进程模型都是 `single_process`。回复正文是 `info:<index>:<count>:<ident>:<on_main>:<thread_name>`，用来标明处理这次请求的服务线程。每个服务线程启动时打一行 `HOOK_START`，退出时打 `HOOK_STOP`。

[thread/conf/merge.yaml](thread/conf/merge.yaml)：`io_handle_type: merge`，`io_num` 和 `handle_num` 都是 `3`。主线程接受连接，`trpc-loop-*` 读包并执行 handler。一个 TCP 监听套接字。`protocol: trpc` 的 UDP 在每个服务线程上各有一个套接字。用户协议只在 `trpc-loop-0` 上绑一次。

| 服务 | 地址 | network | protocol |
| --- | --- | --- | --- |
| `GreeterTcp` | `127.0.0.1:18100` | `tcp` | `trpc` |
| `GreeterUdp` | `127.0.0.1:18101` | `udp` | `trpc` |
| `GreeterUser` | `127.0.0.1:18103` | `udp` | `user` |

[thread/conf/separate.yaml](thread/conf/separate.yaml)：`io_handle_type: separate`，`io_num: 2`，`handle_num: 3`。主线程和 `trpc-io-*` 做 I/O，`trpc-handle-*` 跑 handler。TCP 仍是一个监听套接字。`protocol: trpc` 的 UDP 跟 I/O 循环，用户协议绑在 `trpc-handle-0`。端口是 `18110`（TCP）、`18111`（UDP trpc）、`18113`（UDP user）。

```bash
python3 features/thread/check.py --conf features/thread/conf/merge.yaml
python3 features/thread/check.py --conf features/thread/conf/separate.yaml
```

成功时分别打印 `ok merge serving=3 io_thread_num=3 merge.yaml` 和 `ok separate serving=3 io_thread_num=2 separate.yaml`。检查线程数、线程 id、监听套接字、TCP / UDP / 用户协议 / HTTP 的回复，以及退出码和每个服务线程的 `HOOK_STOP`。这份 YAML 没有 HTTP 服务时，HTTP 那一步会跳过。

<a id="http-thread"></a>

## 单进程 HTTP 线程

[http/thread/server.py](http/thread/server.py) 使用同一套线程检查，传输只有 HTTP。`@route("/thread_info")` 和 HTTP RPC 的 `SayHello` 都返回上面的 `info:...`。一条 HTTP 连接上的请求落在同一个服务线程。

| 配置 | 模型 | 地址 |
| --- | --- | --- |
| [http/thread/conf/merge.yaml](http/thread/conf/merge.yaml) | `merge`，`io_num` / `handle_num` 为 `3` / `3` | `127.0.0.1:18102`，`protocol: http` |
| [http/thread/conf/separate.yaml](http/thread/conf/separate.yaml) | `separate`，`io_num` / `handle_num` 为 `2` / `3` | `127.0.0.1:18112`，`protocol: http` |

merge 时主线程接受连接，把连接交给 `trpc-loop-*`。separate 时主线程接受连接，把连接交给 `trpc-handle-*`，`trpc-io-*` 不跑 HTTP handler。两种配置都只有一个监听套接字。

```bash
python3 features/http/thread/check.py --conf features/http/thread/conf/merge.yaml
python3 features/http/thread/check.py --conf features/http/thread/conf/separate.yaml
```

成功时的 `ok` 行与上一节相同，配置文件名换成对应的 `merge.yaml` 或 `separate.yaml`。

<a id="peer"></a>

## peer

[peer/server.py](peer/server.py) 使用 `multi_process` + `io_handle_type: merge`，`handle_num: 2`。当前进程是 worker-0，fork 出 worker-1。每个 worker 用 `SO_REUSEPORT` 绑定全部地址。YAML 里 `tcp_reuse_port: false` 会被忽略，peer 仍然打开 `SO_REUSEPORT`。

| 服务 | 地址 | network | protocol |
| --- | --- | --- | --- |
| `GreeterA` | `127.0.0.1:18320` | `tcp` | `trpc` |
| `GreeterB` | `127.0.0.1:18321` | `tcp` | `trpc` |
| `GreeterUdp` | `127.0.0.1:18322` | `udp` | `trpc` |
| `GreeterHttp` | `127.0.0.1:18323` | `tcp` | `http` |
| `GreeterUser` | `127.0.0.1:18324` | `udp` | `user` |

一元 `SayHello` 的 `msg` 是 `rpc:<index>:<count>:<pid>:<on_main>:<service>`。`index` 是 worker 序号，`count` 是 `handle_num`。HTTP 路由是 `GET /peer`，正文与一元回复相同。用户协议回复 `user:<index>:<pid>`。每个 tRPC-over-TCP 服务还有一个服务端流方法 `StreamSay`。未命名的 timer 在每个 worker 上跑，名为 `worker:0` 的 timer 只在 worker-0 上跑。

peer 不支持 Task。设置 `TASK=1` 时会在 fork 前拒绝启动。`SCHEDULER=1` 和 `IO_WAIT=1` 会被忽略。`SYNC=1` 注册一个普通 `def` handler，启动时告警，`SayHello` 仍然有回复。请求 `msg` 为 `info:<秒>` 时，handler 先睡眠再回复，供检查脚本在请求还在处理时发 `SIGTERM`。

```bash
python3 features/peer/check.py
```

脚本依次检查正常调用、慢 hook、失败 hook、ready timeout、`SIGTERM`、进程组、杀掉子进程、拒绝 Task、拒绝 unix、`worker_num` 为 1 或 0、忽略 `io_wait` 和 scheduler、同步 handler，以及第二个实例。macOS 的 `SO_REUSEPORT` 不在 worker 之间分摊连接，检查不要求每个 worker 都接到请求，但要求每个 worker 都持有监听套接字。

`second instance` 这一项当前会失败。脚本要求第二个进程绑上同一组端口，并打出 `a second instance on this host will bind silently`。运行时在 `bind` 之前用 `connect` 探测端口，发现 `18320` 已被占用就退出，退出码为 1。其余项会打印 `ok peer ...`。

<a id="separate"></a>

## separate

[separate/server.py](separate/server.py) 使用 `multi_process` + `io_handle_type: separate`，`io_num: 1`，`handle_num: 2`。当前进程做 I/O：TCP 只在这里监听一次，完整帧转发给 rpc task。UDP、HTTP 和用户协议由每个 rpc task 用 `SO_REUSEPORT` 自己绑。`io_num` 不是 1 时会告警，仍然只起一个 I/O 进程。

| 服务 | 地址 | network | protocol | 谁监听 |
| --- | --- | --- | --- | --- |
| `GreeterA` | `127.0.0.1:18220` | `tcp` | `trpc` | I/O 进程 |
| `GreeterB` | `127.0.0.1:18221` | `tcp` | `trpc` | I/O 进程 |
| `GreeterUdp` | `127.0.0.1:18222` | `udp` | `trpc` | 每个 rpc task |
| `GreeterHttp` | `127.0.0.1:18223` | `tcp` | `http` | 每个 rpc task |
| `GreeterUser` | `127.0.0.1:18224` | `udp` | `user` | 每个 rpc task |

一元 `SayHello` 的 `msg` 是 `rpc:<index>:<count>:<pid>:<service>`。`index` 是 rpc task 序号。`msg` 以 `task:` 开头时，请求被转到名为 `echo` 的 task 进程，回复里带那个进程的 pid。`server.io_wait_count: 1` 会等这个 task 的 initializer 调用 `inc_io_wait_count` 之后才绑定地址。HTTP 路由是 `/separate`。服务端流方法同样是 `StreamSay`。未命名 timer 在每个 rpc task 上跑，`worker:0` 只在 0 号 rpc task 上跑。I/O 进程不跑 handler，也不打 `HOOK_START`。

`SCHEDULER=pin0` 把一元 TCP 固定转到 rpc task 0。`SLOW_HOOK`、`FAIL_HOOK`、`SLOW_TASK` 用来改变启动时间或让启动失败。

```bash
python3 features/separate/check.py
```

成功时依次打印 `ok separate happy`、`pin0`、`slow task`、`slow hook`、`fail hook`、`ready timeout`、`sigterm inflight`、`double sigterm`、`process group`、`kill rpc task`、`kill io`、`io_num warns`。macOS 上 UDP、HTTP 和用户协议不要求打到每一个 rpc task。
