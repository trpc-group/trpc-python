# hellostream 示例说明

本目录是 tRPC Greeter：一元调用，加上客户端流、服务端流、双向流。默认服务名 `trpc.py_stream.helloworld.Greeter`，TCP 监听 `127.0.0.1:8000`。同一份配置还监听 Unix 套接字 `/tmp/hellostream-merge.sock`，服务名 `trpc.py_stream.helloworld.UnixGreeter`。

`HELLOSTREAM_STRESS=1` 时，注册的实现换成 `StressGreeter`。`trpc_main.py` 调用 `register_GreeterServicer_server(..., fix_rpc_name=True)`，一元方法会按每个服务名改写路径，Unix 服务名因此也能挂上这些方法。流式方法路径仍留在 `Greeter` 上。

过滤器样例在 [features/filter/](features/filter/)，默认 `trpc_main.py` 没有导入它。

## 关键特性

- 四种调用：`SayHello`、`SayHelloReq`、`SayHelloRsp`、`SayHelloAll`
- 五份进程布局，TCP 端口 `8000`–`8040`
- 其中四份同时提供 Unix 套接字；peer 只监听 TCP
- `check_modes` 按布局把这三种流式调用和 Unix 上的一元、服务端流各跑一遍

## 五种布局

TCP 服务名都是 `trpc.py_stream.helloworld.Greeter`。带 Unix 的四份，套接字服务名都是 `trpc.py_stream.helloworld.UnixGreeter`。Unix 的 `network` 仍是 `tcp`，另加 `socket_type: unix`。

| 检查名 | 配置 | 进程模型 | io_num | handle_num | TCP | Unix |
| --- | --- | --- | --- | --- | --- | --- |
| `merge` | [trpc_python.yaml](trpc_python.yaml) | `single_process` + `merge` | 1 | 1 | `127.0.0.1:8000` | `/tmp/hellostream-merge.sock` |
| `single_process_merge` | [conf/single_process_merge.yaml](conf/single_process_merge.yaml) | `single_process` + `merge`，多线程 | 3 | 3 | `127.0.0.1:8010` | `/tmp/hellostream-threads.sock` |
| `single_process_separate` | [conf/single_process_separate.yaml](conf/single_process_separate.yaml) | `single_process` + `separate` | 2 | 3 | `127.0.0.1:8020` | `/tmp/hellostream-separate-thread.sock` |
| `multi_process_separate` | [conf/multi_process_separate.yaml](conf/multi_process_separate.yaml) | `multi_process` + `separate` | 1 | 2 | `127.0.0.1:8030` | `/tmp/hellostream-separate.sock` |
| `multi_process_merge` | [conf/multi_process_merge.yaml](conf/multi_process_merge.yaml) | `multi_process` + `merge`（peer） | 未设置 | 2 | `127.0.0.1:8040` | 无 |

多线程 merge 要求 `io_num` 与 `handle_num` 相同；不一致时会被改写成 `handle_num`。separate-thread 把一条流的帧留在同一个 handle 线程，下标是 `(session_id + stream_id) % handle_num`。多进程 separate 里，启动的那个进程只切帧并转发，`handle_num` 个 rpc-task 进程跑 handler，一条流留在同一个 rpc task。peer 由 worker-0 再 fork 出 `handle_num - 1` 个进程，每个 worker 用 `SO_REUSEPORT` 绑定同一地址；YAML 里的 `tcp_reuse_port` 保持 `false`。

peer 只监听 TCP。每个 worker 都会删掉同一个套接字文件，因为 `create_unix_socket` 在 bind 前 unlink，并且不设置 `SO_REUSEPORT`，所以这份配置不写 Unix 服务。

## 四种调用

默认实现是 `GreeterServicer`。

| 方法 | 形态 | 回复 |
| --- | --- | --- |
| `SayHello` | 一元 | `HelloReply.message = request.name` |
| `SayHelloReq` | 客户端流 | 收到结束（`EOFError`）后一帧 `SayHelloReq done`，再 `send_and_close` |
| `SayHelloRsp` | 服务端流 | 三帧 `rsp: 0`、`rsp: 1`、`rsp: 2`，每帧之后 `sleep(1)` |
| `SayHelloAll` | 双向流 | 每个请求回一帧 `rsp data`，`EOFError` 时结束 |

`StressGreeter` 的流式回复是 `pid=<pid> tid=<tid> n=<n>`，没有每帧 sleep。`SayHello` 仍回显 `name`。`SayHelloRsp` 的帧数来自请求名里的 `count=`。`SayHelloReq` 的 `n` 是收到的数据帧数；`SayHelloAll` 的 `n` 来自请求名开头的 `n=`。

## 关键文件

- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认旁边的 [trpc_python.yaml](trpc_python.yaml)
- [greeter.py](greeter.py)、[stress_greeter.py](stress_greeter.py)：默认实现和压测实现
- [test_service_rpc.py](test_service_rpc.py)：`--port`，默认 `8000`。只调用已经起来的服务
- [check_modes.py](check_modes.py)：五种布局各跑一遍
- [stress.py](stress.py)：压测入口。自己拉起服务，见文末注意
- [requirements.txt](requirements.txt)：依赖列表，和本仓库的安装方式不一致，见文末注意

## 调用关系

```text
test_service_rpc.py / check_modes.py
└── :<port>  trpc.py_stream.helloworld.Greeter
    ├── SayHelloReq
    ├── SayHelloRsp
    └── SayHelloAll
check_modes.py（四份非 peer 配置）
└── UnixGreeter
    ├── SayHello
    └── SayHelloRsp
```

`check_modes` 的 TCP 检查只跑上面三种流。Unix 检查只跑 `SayHello` 和 `SayHelloRsp`。

## 关键代码解释

直接跑 `python3 trpc_main.py` 时，解释器会把脚本所在目录放进 `sys.path`。`python -m examples.hellostream.trpc_main` 不会。`trpc_main.py` 因此把自己的目录和 `stub/` 都插进 `sys.path`，两种启动方式都能 `import greeter` 和桩包。

`fix_rpc_name=True` 只改写一元方法名里的服务段。`Greeter` 上的 `SayHello` 仍是 `/trpc.py_stream.helloworld.Greeter/SayHello`；`UnixGreeter` 上的是 `/trpc.py_stream.helloworld.UnixGreeter/SayHello`。流式方法注册名保持 `/trpc.py_stream.helloworld.Greeter/SayHelloReq`、`SayHelloRsp`、`SayHelloAll`。客户端建流时用的也是这三条 `Greeter` 路径，所以 Unix 上的 `SayHelloRsp` 仍然打到 `Greeter` 的流式方法。

`_build_servicer()` 在 `serve()` 里、fork 之前构造。`HELLOSTREAM_STRESS=1` 时构造 `StressGreeter`，子进程继承这个实例，不会再构造一次。

`test_service_rpc.py` 不读 YAML。调用选项写在代码里：目标 `ip://127.0.0.1:<port>`，协议 `trpc`，超时 15000 毫秒。`SayHelloRsp` 三帧之间各停 1 秒，所以超时留得比这三秒更长。

`check_modes` 没有命令行参数。它用 `python -m examples.hellostream.trpc_main --conf ...` 拉起服务，工作目录是仓库根，一次只起一份布局。进程以新会话启动，停的时候对整个进程组发 SIGTERM，7 秒后仍在就 SIGKILL。有 Unix 的布局在进程组退出后删除套接字文件。

## 环境要求

- Python `>=3.8`（[pyproject.toml](../../pyproject.toml) 的 `requires-python`）
- macOS 或 Linux
- 在仓库根目录执行 `pip install -e .`。安装说明见仓库 [README.zh_CN.md](../../README.zh_CN.md)

## 运行步骤

服务可以在 `examples/hellostream` 启动：

```bash
python3 trpc_main.py
python3 trpc_main.py --conf=conf/single_process_merge.yaml
```

也可以在仓库根目录用模块方式指定另一份配置：

```bash
python -m examples.hellostream.trpc_main --conf examples/hellostream/conf/single_process_separate.yaml
```

`--conf` 指向的文件不存在时，会打印该路径并改用旁边的 `trpc_python.yaml`。默认文件也不存在则退出码 1。

客户端要对着已经起来的服务。在 `examples/hellostream`：

```bash
python3 test_service_rpc.py
python3 test_service_rpc.py --port 8010
```

五种布局在仓库根目录：

```bash
python -m examples.hellostream.check_modes
```

日志写到临时目录下的 `hellostream-<name>.log`（`tempfile.gettempdir()`）。macOS 上这通常是 `$TMPDIR`。Unix 套接字路径是配置里写死的 `/tmp/...`。

## 运行结果

直接启动服务时，标准输出先有一行 `conf_path: <配置绝对路径>`，然后进程停在 `serve()`。

`test_service_rpc` 通过时不打印成功横幅，退出码 0。它断言：`SayHelloReq` 的结束帧是 `SayHelloReq done`；`SayHelloRsp` 依次是 `rsp: 0`、`rsp: 1`、`rsp: 2`；`SayHelloAll` 在两次发送后得到两帧 `rsp data`。对不上就抛 `AssertionError`。`SayHelloRsp` 大约要 3 秒。

`check_modes` 每种布局先打印一行 `== <name> <yaml 文件名> :<port>`，有 Unix 时后面再跟上套接字路径。TCP 三种检查通过后，四份带 Unix 的布局再做 `SayHello`（请求名 `unix`，回复 `unix`）和同样的三帧 `SayHelloRsp`。该项通过打印 `ok <name>`。五份都通过时打印 `all layouts passed`，退出码 0。

某一份失败时，立刻打印错误和该布局日志的末 80 行，然后继续剩下的布局。全部跑完后打印 `failed: <name>, ...`，退出码 1。服务没起来时，错误正文是 `<name>: server did not become ready on 127.0.0.1:<port>`；Unix 套接字没起来时是 `<name>: unix socket did not become ready at <path>`。

## 适用场景

- 确认四种流式 RPC 在单循环下可用
- 换 YAML 后确认同一套三形检查仍能通过
- 确认四份可单次绑定的布局能提供 Unix 一元调用和一条服务端流

## 需要注意什么

1. 端口 `8000`–`8040` 要空闲。peer 只监听 `127.0.0.1:8040`，不监听 Unix
2. [stress.py](stress.py) 在仓库根目录执行，自己拉起服务。`HELLOSTREAM_STRESS=1` 时换成 `StressGreeter`。`--mode` 选一种布局，`--all` 跑全部五种。剖面是 `bidi`、`client_stream`、`stop_under_load`；前两个的 p99 要小于 10 秒才算过。`stop_under_load` 在流还开着时对进程组发 SIGTERM，停的过程中客户端报错是预期。Unix 路径不参与压测
