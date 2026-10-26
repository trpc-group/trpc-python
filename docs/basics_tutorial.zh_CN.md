[English](./basics_tutorial.md) | 中文

## 基础教程

在[快速开始](./quick_start.zh_CN.md)中，你已经跑通了 tRPC-Python 的 helloworld。那一章跳过了服务是怎么定义、配置和扩展的。这里按开发时的顺序补上这些部分：

- 如何用 protobuf 定义 tRPC 服务
- `trpc_python.yaml` 怎么写
- 拦截器和插件
- 流式 RPC 与 HTTP

服务定义使用 Protocol Buffers v3。可以参考它的 [Python 教程](https://protobuf.dev/getting-started/pythontutorial/)。

### 定义服务

新服务先在 protobuf 里声明。下面声明了一个名为 `MyService` 的服务：

```protobuf
service MyService {
}
```

方法写在 `service` 里面。helloworld 的 `Greeter` 有一个方法 `SayHello`，参数是 `HelloRequest`，返回 `HelloReply`：

```protobuf
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string msg = 1;
}

message HelloReply {
  string msg = 1;
}
```

方法声明以 `{}` 结束。方法体可以是空的。

### 编写客户端和服务端代码

protobuf 给出的是语言无关的服务定义。用 [trpc 命令行工具](https://github.com/trpc-group/trpc-cmdline) 把它生成 Python 桩代码。在 proto 文件所在目录执行：

```bash
$ trpc create --lang=python -p helloworld.proto --rpconly -f
```

`$ trpc create -h` 可以查看其余选项。完整工程见 [helloworld](/examples/helloworld)。如果生成的 `rpc.py` 仍从旧的 `trpc` 包导入，请改成从 `trpc_sdk` 导入，对照写在[快速开始](./quick_start.zh_CN.md)里。

桩代码分成服务端和客户端。服务端要你实现的是 `GreeterServicer`：

```python
class GreeterServicer:
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        raise NotImplementedError("Method not implemented!")
```

业务实现继承这个类，填上 `SayHello`：

```python
class GreeterServicer(rpc.GreeterServicer):
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp
```

进程入口用 `create_server` 读配置，再把实现注册进去。`create_server` 只加载配置并组好 service，监听发生在 `serve`：

```python
from trpc_sdk.server import create_server

server = create_server(conf_path)
rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
server.serve()
```

客户端桩提供异步方法 `asyncSayHello`。`Options` 可以直接传给它。下面的写法与 helloworld 的客户端示例一致：

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options

proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
req = pb.HelloRequest()
req.msg = "world"
options = (
    Options()
    .set_target("ip://127.0.0.1:8000")
    .set_protocol("trpc")
    .set_network("tcp")
)
rsp = await proxy.asyncSayHello(TrpcContext(), req, options)
```

桩上还有同步方法 `SayHello`。后面的例子继续使用异步调用。

### 框架配置

上面的客户端用 `set_target` 指定了服务端地址。服务端的监听地址写在 `trpc_python.yaml` 里。

启动时把这份文件的路径传给 `create_server`，它会调用 `TrpcConfig.load` 读入配置。helloworld 默认使用示例目录下的 `trpc_python.yaml`，也可以在命令行指定：

```bash
$ python examples/helloworld/trpc_main.py --conf=<路径>
```

改 `ip`、`port`、`protocol`、`timeout` 之后，重新启动进程即可，`SayHello` 的实现不用改。

教程用到的服务端配置如下。`name` 与生成桩里的服务名 `trpc.trpc_python_group.helloworld.Greeter` 一致：

```yaml
server:
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      ip: 127.0.0.1
      port: 8000
      network: tcp
      protocol: trpc
      timeout: 5000
```

客户端也可以在 `client.service` 里写 `target`。同一次调用如果又执行了 `set_target`，以这次调用传入的地址为准。

### 拦截器和插件

tRPC-Python 把跨请求的逻辑放在拦截器里，把可替换的模块放在插件里。拦截器本身也是一种插件。

#### 拦截器

拦截器为洋葱模型。处理请求时，先执行外层的前置逻辑，再进入业务方法，返回时再执行后置逻辑。

客户端和服务端使用同一个 `BaseFilter`。`run` 处理一元调用，`run_stream` 处理流式调用。下面是一个只包住下一层的服务端拦截器：

```python
from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterAsyncGenHandleType
from trpc_sdk.abc import FilterAsyncGenReturnType
from trpc_sdk.abc import FilterHandleType
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "my_filter")
class MyFilter(BaseFilter):
    async def run(
        self,
        ctx: TrpcContext,
        req,
        next_handle: FilterHandleType,
    ) -> FilterReturnType:
        rsp, err = await next_handle()
        return rsp, err

    async def run_stream(
        self,
        ctx: TrpcContext,
        req,
        next_handle: FilterAsyncGenHandleType,
    ) -> FilterAsyncGenReturnType:
        raise NotImplementedError
```

`next_handle()` 之前的代码是前置逻辑，之后是后置逻辑。`run` 返回 `(rsp, err)`。

这个拦截器不做流式处理，所以 `run_stream` 抛出 `NotImplementedError`。流式调用什么时候会进入 `run_stream`，见后面的流式 RPC。

客户端拦截器的写法相同，注册类型换成 `PluginType.CLIENT_FILTER_COMPONENT`。

`@register_plugin` 在模块被 import 时执行。服务入口要在 `create_server` 之前导入这个模块，例如 `import my_filter as _`。装饰器的默认 `init_pos` 是 `WORKER`。

配置文件用键 `filter` 列出拦截器的注册名：

```yaml
server:
  filter:
    - my_filter
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      filter:
        - service_only_filter
client:
  filter:
    - client_filter
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      filter:
        - client_service_filter
```

服务端在 `create_server` 里先装上 `server.filter`，再追加该 service 自己的 `filter`。列表里靠前的拦截器是洋葱外层，请求进入时先执行。某个名字没有对应的 `SERVER_FILTER_COMPONENT` 插件时，`create_server` 抛出 `PluginInitError`。

客户端先把全局 `client.filter` 放在该 service 的 `filter` 前面。调用时 `Options.set_filter` 或 `set_filter_list` 默认再追加在这两段之后。传入 `override=True` 时，这次调用只保留参数里的拦截器实例。调用处传入的是实例：

```python
options = Options().set_target("ip://127.0.0.1:8000").set_filter(MyClientFilter())
```

#### 插件

插件在 import 时用 `@register_plugin(PluginType, name)` 注册。被装饰的类一般继承 `BasePlugin`。框架在对应的进程阶段调用 `init()`，再调用 `start()`。`BaseFilter` 是 `BasePlugin` 的子类，所以上一节的拦截器注册已经是插件注册。

使用别人写好的插件时，在服务入口 import 那个模块，装饰器才会执行。helloworld 的 `trpc_main.py` 里被注释掉的 `import ... as _` 就是在做这件事。

`trpc_python.yaml` 里的 `plugins:` 会整块放进 `get_plugins_config()`。框架按插件自己的实现决定要不要读取这段配置。

- 名字服务通过 `target` 的 scheme 找到 `NAMING_SELECTOR`。`plugins` 段不会解码进 selector。见[名字服务插件](./developer_guide/develop_plugins/naming.zh_CN.md)。
- 启动文件固定使用数据源 `file` 和解码器 `yaml`。`plugins` 段不会用来创建 config 插件。见[配置插件](./developer_guide/develop_plugins/config.zh_CN.md)。

拦截器配合插件的方式是：import 完成注册后，把注册名写进 `filter` 列表。

### 多协议支持

[快速开始](./quick_start.zh_CN.md)里的调用是一次请求对应一次响应。tRPC-Python 还支持流式 RPC 和 HTTP。

#### 流式 RPC

流式 RPC 让客户端和服务端分多次交换数据，有三种模式。

客户端流里，客户端依次发送多个包，服务端全部收到后再返回一个包。服务端流里，一个客户端请求可以收到多次回包。双向流里，两边都可以给对方发送数据，并且保持顺序。

示例在 [hellostream](/examples/hellostream)。protobuf 里用 `stream` 标出流式方法：

```protobuf
service Greeter {
  rpc SayHelloReq (stream HelloRequest) returns (HelloReply) {}
  rpc SayHelloRsp (HelloRequest) returns (stream HelloReply) {}
  rpc SayHelloAll (stream HelloRequest) returns (stream HelloReply) {}
}
```

`stream` 只出现在请求前，该方法是客户端流；只出现在返回值前，是服务端流；两边都有，是双向流。

客户端流里，客户端调用 `await proxy.asyncSayHelloReq(ctx, options)`，多次 `send`，最后 `close_and_recv`。服务端实现 `SayHelloReq(self, stream)`，循环 `recv`。`EOFError` 表示对端已经 `close_and_recv`，这时用 `send_and_close` 回一个包并结束：

```python
async def SayHelloReq(self, stream):
    while True:
        try:
            req = await stream.recv()
        except EOFError:
            rsp = pb.HelloReply()
            rsp.message = "done"
            await stream.send_and_close(rsp)
            return
```

hellostream 的请求字段是 `name`，回复字段是 `message`。

服务端流里，服务端实现 `SayHelloRsp(self, request, stream)`，多次 `send`，方法返回表示发送结束。客户端调用 `asyncSayHelloRsp(ctx, req, options)` 后循环 `recv`，`EOFError` 表示结束。

双向流里，客户端调用 `asyncSayHelloAll` 后可以交叉 `send` 和 `recv`，发完调用 `close_send`。服务端实现 `SayHelloAll(self, stream)`，同样 `recv` 和 `send`。对端关闭时，`recv` 抛出 `EOFError`。

监听配置和一元 RPC 相同，service 的 `protocol` 仍是 `trpc`。要让流式调用进入拦截器的 `run_stream`，service 配置 `enable_stream_filter: true`，客户端调用 `set_enable_stream_filter(True)`。

#### 自定义 HTTP 路由

把 service 的 `protocol` 写成 `http` 后，这个端口由 aiohttp 接入。路由用 `trpc_sdk.http.route` 注册，并在 `serve` 之前 import 定义路由的模块：

```yaml
server:
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter1
      ip: 127.0.0.1
      port: 8080
      network: tcp
      protocol: http
```

```python
from aiohttp import web
from trpc_sdk.http import route


@route("/normal_http")
async def normal_http(ctx, request):
    return web.json_response({"data": "json"})
```

处理函数的参数是 `(ctx, request)`，`request` 是 aiohttp 的 `Request`。

不写 `service_name` 时，配置里每个 HTTP service 都会挂上这条路由。写成 `@route("/bind", service_name="trpc.trpc_python_group.helloworld.Greeter1")` 时，路由只挂到这一个 service。

匹配时先看这些用户路由。没有命中的路径再交给 RPC 处理。

#### 用 HTTP 调用已有 RPC

同一个 `Greeter`，把该 service 的 `protocol` 从 `trpc` 改成 `http` 后，可以用 HTTP 调用桩里的方法。请求路径就是方法名。`Content-Type: application/json` 时，正文按 JSON 反序列化，字段名是 proto 里的 `msg`：

```bash
$ curl -X POST -H "Content-Type: application/json" \
  -d '{"msg": "world"}' \
  127.0.0.1:8000/trpc.trpc_python_group.helloworld.Greeter/SayHello
```

### 进一步阅读

- 回顾[快速开始](./quick_start.zh_CN.md)里的 helloworld 运行步骤。
- 阅读 [helloworld 示例](/examples/helloworld) 和 [hellostream 示例](/examples/hellostream) 的完整代码。
- 开发名字服务插件见[名字服务插件](./developer_guide/develop_plugins/naming.zh_CN.md)。
- 开发配置插件见[配置插件](./developer_guide/develop_plugins/config.zh_CN.md)。
