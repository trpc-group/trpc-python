# helloworld 示例

本文说明默认一元 Greeter，不展开四种进程布局。`GreeterServicer` 实现 `SayHello` 和 `SayHi`。`SayHello` 的回复 `msg` 是 `hello: ` 加上请求的 `msg`；`SayHi` 是 `hi: ` 加上请求的 `msg`。

[trpc_python.yaml](trpc_python.yaml) 里，服务 `trpc.trpc_python_group.helloworld.Greeter` 监听 `127.0.0.1:8000`，`tcp` / `trpc`。同一份配置还有 `trpc.trpc_python_group.helloworld.GreeterUser`，监听 `127.0.0.1:8090`，`udp` / `user`。进程模型是 `single_process` + `merge`，`io_num`、`handle_num`、`task_num` 都是 1。

## 关键特性

| 服务名 | 地址 | network | protocol | 行为 |
| --- | --- | --- | --- | --- |
| `trpc.trpc_python_group.helloworld.Greeter` | `127.0.0.1:8000` | `tcp` | `trpc` | 一元 `SayHello`、`SayHi` |
| `trpc.trpc_python_group.helloworld.GreeterUser` | `127.0.0.1:8090` | `udp` | `user` | 任意数据报回复 `user:0:<pid>` |

| 配置项 | 值 |
| --- | --- |
| `global.processmodel.mode` | `single_process` |
| `global.processmodel.io_handle_type` | `merge` |
| `io_num` / `handle_num` / `task_num` | `1` / `1` / `1` |

YAML 里 `8001`、`8002`、`8003` 和 TLS 都是注释，当前进程不监听这些端口。Unix 套接字的注释指向 [hellostream](../hellostream/)，本示例不监听 `/tmp/trpc.sock`。

用户协议的注册、回包，以及拉起本服务核对 `8090` 的检查，见 [features/README.zh_CN.md](features/README.zh_CN.md#user-protocol)。默认 [trpc_main.py](trpc_main.py) 在 `serve()` 前已经 `import features.user_protocol`。HTTP 路由的导入仍是注释。

## 关键文件

- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认旁边的 `trpc_python.yaml`；`create_server` 前调用 `greeter.register_py_task()` 并打开 `greeter.task_enabled`，`register_GreeterServicer_server(..., fix_rpc_name=True)` 之后 `serve()`
- [main.py](main.py)：转调 `trpc_main.main`，给 `python -m examples.helloworld.main` 用
- [greeter.py](greeter.py)：`SayHello`、`SayHi`，以及 `py_task` 的注册函数、initializer 和回显 handler。metadata、attachment 仍是注释
- [test_service_rpc.py](test_service_rpc.py)、[test_greeter.py](test_greeter.py)：客户端。`test_greeter.py` 只调用前者的 `main`
- [features/user_protocol.py](features/user_protocol.py)：在 `serve()` 前导入，注册 `protocol: user`

## 调用关系

```text
test_service_rpc.py / test_greeter.py
└── 127.0.0.1:8000  trpc.trpc_python_group.helloworld.Greeter
    ├── SayHello
    │   └── send_to_task("py_task", msg)  task 子进程原样回显
    └── SayHi
features/check_user.py
└── 127.0.0.1:8090  GreeterUser
    └── user:0:<pid>
```

## 关键代码解释

直接跑 `python3 trpc_main.py` 时，解释器把脚本所在目录放进 `sys.path`，因此可以 `import greeter`。`python3 -m examples.helloworld.trpc_main` 时 `__name__` 仍是 `__main__`，[trpc_main.py](trpc_main.py) 里 `if __name__ != '__main__'` 不会把 helloworld 目录加进 `sys.path`，`import greeter` 会失败。模块方式要跑 `python3 -m examples.helloworld.main`：[main.py](main.py) 会先把该目录加进 `sys.path`，再以普通导入的方式加载 `trpc_main`。

`py_task` 由 [trpc_main.py](trpc_main.py) 在 `create_server` 之前注册，`serve()` 按 `task_num: 1` fork 出一个 `py_task:0` 子进程，并在子进程里先跑 initializer。`SayHello` 在 `greeter.task_enabled` 为真时 `await send_to_task(ctx, "py_task", request.msg)`；handler 在子进程里拿到一批请求，原样返回，结果回到服务循环后打成日志。只导入 `greeter` 不会注册 task，`task_enabled` 默认是假，所以 [features/admin](features/admin/) 这类自带入口、不拉起 task 的宿主不会去发 task。

客户端用绝对路径 `TrpcConfig.load` 这份 YAML。真正的 target 仍是写死的 `ip://127.0.0.1:8000`，不读 YAML 里的监听地址。

`run_all` 只 `await test_rpc()`。`sync_test` 的同步调用在 `main` 里是注释，默认不会执行。`test_Greeter_SayHello()` 和 `test_Greeter_SayHi()` 在 import 时就创建好协程，`test_rpc` 用 `asyncio.gather` 并发。`asyncSayHello` 的 `req.msg` 是 `hello`；`asyncSayHi` 不设置 `msg`。断言只要求返回值不是 `None`。

## 环境要求

- Python `>=3.8`（[pyproject.toml](../../pyproject.toml) 的 `requires-python`）
- macOS 或 Linux
- 在仓库根目录执行 `pip install -e .`

安装说明见仓库 [README.zh_CN.md](../../README.zh_CN.md)。

## 运行步骤

在 `examples/helloworld`：

```bash
python3 trpc_main.py
python3 trpc_main.py --conf=trpc_python.yaml
```

另一个终端，仍在该目录，等服务已经在听：

```bash
python3 test_service_rpc.py
python3 test_greeter.py
```

仓库根目录：

```bash
python3 -m examples.helloworld.main
python3 -m examples.helloworld.test_service_rpc
```

UDP `8090` 的检查不在上面这两步里，见 [features/README.zh_CN.md](features/README.zh_CN.md#user-protocol)。

## 运行结果

服务进程先打印 `conf_path: ` 加上实际使用的配置路径，再打一条 `load config` 日志，然后是 `forked child py_task:0 as pid <pid>` 和子进程的 `initializer py_task done, pid <pid>`。

客户端打印：

```text
=== test_rpc start===
```

然后两行：

```text
test_Greeter_SayHello recv result is ...
test_Greeter_SayHi recv result is ...
```

最后：

```text
=== test_rpc end===
```

源码里的格式是 `"\n=== test_rpc start=== \n"`、`"\n  test_Greeter_SayHello recv result is {ret}\n"`、`"\n  test_Greeter_SayHi recv result is {ret}\n"`、`"\n=== test_rpc end=== \n"`。两条结果行并发，顺序不固定。

`SayHello` 的 `msg` 是 `hello: hello`。`SayHi` 的请求没有设置 `msg`，回复是 `hi: `。`SayHello` 里还有一条内容为 `test log` 的 error 日志，以及 `test log in SayHello req: hello`；`SayHi` 打 `test log in SayHi`。这些是示例日志，调用本身仍然返回上面的回复。`SayHello` 发给 task 之后，服务日志还有 `py_task echo: 'hello'`；task 不改变对外回复。

异常时客户端打错误日志并执行 `assert 0`。

## 适用场景

- 确认桩代码注册、一元 `SayHello` / `SayHi` 和默认单循环配置可用
- 作为其它 feature 的宿主：HTTP 路由要打开 [trpc_main.py](trpc_main.py) 里的注释；进程布局换成自己的 `server.py`

## 需要注意什么

1. 仓库根目录的 [run_example.sh](../../run_example.sh) 会等 `8000`–`8003` 和 `/tmp/trpc.sock`。当前 YAML 只会让它报 `failed to become ready`。用上面的命令启动。
2. `task_num: 1` 拉起一个 `py_task` 子进程，默认客户端的 `SayHello` 会打到它。旧示例里的 `c_task` 和 `c_driver_task` 已删除：当前运行时只有 Python 的 task 进程，原生 task driver 不在构建里。
3. 端口 `8000` 和 `8090` 要空闲。
