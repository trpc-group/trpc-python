# hellosse 示例说明

`@route("/sse")` 写在 [trpc_main.py](trpc_main.py)。服务名 `trpc.test.trpc_python.TestSse`，`protocol: http`，默认监听 `127.0.0.1:8080`。这是 HTTP SSE，不是 tRPC 流。本目录没有 `features/`。

响应状态 200，`Content-Type: text/event-stream`，同时带 `Cache-Control: no-cache` 和 `Connection: keep-alive`。服务写出三帧，`i` 为 0、1、2。每帧的线上内容是 `data: Hello, this is a sse {i} message from the server!\n\n`，写出之后 `sleep(1)`。`CancelledError` 被吞掉，`finally` 里调用 `write_eof`。路由没有写 `service_name`，会挂到每个 HTTP 服务上。客户端使用 `GET`。

## 关键特性

- 一条 `GET /sse` 在同一条连接上连续吐出三帧事件
- 同一条路由跑在五种进程布局上，端口 `8080`–`8084`
- 手动客户端用两种 SSE 读法把三帧读完；`check_modes` 只核对第一帧里的 `sse 0`

## 五种布局

| 配置 | 布局 | 监听 | 说明 |
| --- | --- | --- | --- |
| [trpc_python.yaml](trpc_python.yaml) | `single_process` + `merge`，`io_num` / `handle_num` 为 1 | `127.0.0.1:8080` | 还有一份用不到的 client 块，以及 `./trpc.log`。文件里没有 `close_wait_time`，框架默认是 0 |
| [conf/single_process_merge.yaml](conf/single_process_merge.yaml) | 多线程 merge，`io_num` / `handle_num` 为 3 | `127.0.0.1:8081` | |
| [conf/single_process_separate.yaml](conf/single_process_separate.yaml) | separate-thread，`io_num: 2`，`handle_num: 3` | `127.0.0.1:8082` | |
| [conf/multi_process_separate.yaml](conf/multi_process_separate.yaml) | 多进程 separate，`io_num: 1`，`handle_num: 2` | `127.0.0.1:8083` | |
| [conf/multi_process_merge.yaml](conf/multi_process_merge.yaml) | peer，`handle_num: 2` | `127.0.0.1:8084` | |

`conf/` 里有 `close_wait_time: 2000`。这些配置只打控制台日志。没有任何 Unix 套接字。

## 关键文件

- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认旁边的 YAML。导入时就注册 `/sse`
- [trpc_sse_client.py](trpc_sse_client.py)：`--port`，默认 `8080`。不负责起服务
- [check_modes.py](check_modes.py)：五种布局各读到第一帧就停。没有命令行参数

## 调用关系

```text
trpc_sse_client.py / check_modes.py
└── GET http://127.0.0.1:<port>/sse
    └── trpc_main.sse
```

## 关键代码解释

- 客户端用 `SSEClient.request_events` 和 `sse_iter_events` 各开一条连接，`asyncio.gather` 一起跑。超时 10000 毫秒。打印出来的是事件的 `data`，也就是 `Hello, this is a sse {i} message from the server!`
- `check_modes` 只用 `request_events`，读到第一帧 `data` 里含 `sse 0` 就取消。不要求读完三帧，也不要求打到每一个 worker。一条 SSE 连接留在接住它的那个 serving 循环上
- `check_modes` 拉起的子进程工作目录是仓库根。默认 YAML 的 `./trpc.log` 因此写在仓库根。手动在 `examples/hellosse` 里执行 `python3 trpc_main.py` 时，这个文件写在当前目录

## 环境要求

- Python `>=3.8`（[pyproject.toml](../../pyproject.toml) 的 `requires-python`）
- macOS 或 Linux
- 在仓库根目录执行 `pip install -e .`。安装说明见 [README.zh_CN.md](../../README.zh_CN.md)

## 运行步骤

在 `examples/hellosse` 起服务：

```bash
python3 trpc_main.py
python3 trpc_main.py --conf=conf/single_process_merge.yaml
```

另一个终端，服务已在听，仍在该目录：

```bash
python3 trpc_sse_client.py
python3 trpc_sse_client.py --port 8081
```

其余三份配置同样用 `--conf`，端口依次是 `8082`、`8083`、`8084`。

五种布局在仓库根目录一次跑完。`check_modes` 自己拉起服务：

```bash
python -m examples.hellosse.check_modes
```

## 运行结果

服务启动时打印 `conf_path: ...`。

手动客户端对每个事件打印 `test_http_client_sse got body: ...` 和 `test_http_client_with_sse_iter got body: ...`。读完三帧是两条连接、六行。两条连接一起跑，行的先后不固定。没有断言。

`check_modes` 每种布局先打印 `== <name> <yaml> :<port>`，通过后打印 `ok <name>`。全部通过是 `all layouts passed`，退出码 0。否则打印 `failed: ...`，退出码 1，并带上该布局日志末 80 行。日志在系统临时目录，文件名是 `hellosse-<name>.log`。停进程是进程组 SIGTERM，7 秒后 SIGKILL。默认 YAML 的 `close_wait_time` 是 0，`conf/` 是 2000 毫秒，停止预算仍是 7 秒。

## 适用场景

- 确认 `GET /sse` 在单循环下能吐出事件
- 换四种布局后确认仍能收到含 `sse 0` 的第一帧

## 需要注意什么

1. 端口 `8080`–`8084` 要空闲
2. macOS 接受 `SO_REUSEPORT` 但不分流。多进程布局只证明有一个进程接住了这条连接
3. 没有 `requirements.txt`。客户端导入 `trpc_sdk.http.SSEClient`，按仓库根 `pip install -e .`
4. 这不是 `SayHelloRsp` 那种 tRPC 服务端流
