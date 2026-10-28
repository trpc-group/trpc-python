# 用户自定义插件示例

这个例子演示 `PluginType.USER_DEFINED`。框架不会按这个类型去调用插件，业务在 `SayHello` 里自己 `get_plugin`。

`SayHello` 的回复是 `hello: ` 加上请求的 `msg`，再接 ` token=` 和 yaml 里的 token。请求 `ping`、token 为 `secret` 时，回复是 `hello: ping token=secret`。`SayHi` 不读插件，回复是 `hi: ` 加上请求的 `msg`。

服务名 `trpc.trpc_python_group.helloworld.Greeter`，监听 `127.0.0.1:8200`，`tcp` / `trpc`。桩代码用 [helloworld](../helloworld/stub) 里的同一份 Greeter，本目录不另放 proto。端口不用 `8000`，和 helloworld 可以同时听。

进程模型是 `single_process` + `merge`。插件的 `init_pos` 用默认 `WORKER`，在处理请求的进程里由 `init_worker_plugins()` 建实例。

## 注册和配置

[plugin.py](plugin.py) 在 import 时执行 `@register_plugin(PluginType.USER_DEFINED, "business")`。这时只有一份 spec，没有实例。[trpc_main.py](trpc_main.py) 在 `create_server` 之前 `import plugin`。装饰器不跑，服务就登记不到这个名字，后面的 `get_plugin` 是 `None`。

`init()` 在 worker 启动、YAML 已经加载之后调用。它自己读 `get_plugins_config()`，取出 `plugins.business.token`。框架不会因为 yaml 里有 `business` 这个键就去创建插件。键名是插件自己定的，这里和注册名放在一起。

```yaml
plugins:
  business:
    token: secret
```

`plugins.log` 仍由框架读取。`business` 这一段不是。

## 关键文件

- [plugin.py](plugin.py)：`BusinessPlugin`。`init` 保存 token，`apply` 拼回复
- [greeter.py](greeter.py)：`SayHello` 调用 `get_plugin`；`SayHi` 不调用
- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认旁边的 `trpc_python.yaml`。先 import 插件，再 `register_GreeterServicer_server(..., fix_rpc_name=True)`，然后 `serve()`
- [main.py](main.py)：给 `python3 -m examples.plugin.main` 用。直接跑 `python3 -m examples.plugin.trpc_main` 时，脚本目录不在 `sys.path` 上，`import greeter` 会失败
- [trpc_python.yaml](trpc_python.yaml)：`127.0.0.1:8200` 和 `plugins.business.token`
- [test_service_rpc.py](test_service_rpc.py)：客户端。target 写死 `ip://127.0.0.1:8200`

## 调用关系

```text
test_service_rpc.py
└── 127.0.0.1:8200  trpc.trpc_python_group.helloworld.Greeter
    ├── SayHello   get_plugin("business")  →  hello: ping token=secret
    └── SayHi      不读插件                 →  hi: ping
```

## 运行步骤

仓库根目录先执行 `pip install -e .`。然后在 `examples/plugin`：

```bash
python3 trpc_main.py
```

另一个终端，仍在该目录，等服务已经在听：

```bash
python3 test_service_rpc.py
```

仓库根目录：

```bash
python3 -m examples.plugin.main
python3 -m examples.plugin.test_service_rpc
```

`SayHello` 打印 `hello: ping token=secret`，`SayHi` 打印 `hi: ping`。
