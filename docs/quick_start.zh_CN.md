[English](quick_start.md) | 中文

## 快速开始

### 准备工作

- **[Python](https://www.python.org/downloads/)**，版本应该大于等于 3.8。
- **C 编译工具链**（如 gcc 或 clang），安装框架时需要编译 Cython 扩展。
- **[tRPC 命令行工具](https://github.com/trpc-group/trpc-cmdline)**，用于从 protobuf 生成 Python 桩代码。

### 获取示例代码

示例代码是 tRPC-Python 仓库的一部分。
克隆仓库，创建虚拟环境并安装框架：
```bash
$ git clone --depth 1 git@github.com:trpc-group/trpc-python.git
$ cd trpc-python
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -e .
```

### 执行示例

1. 启动服务端，它会按 `examples/helloworld/trpc_python.yaml` 监听 `127.0.0.1:8000`：
   ```bash
   $ python examples/helloworld/trpc_main.py
   ```
2. 打开另一个终端，激活同一个虚拟环境后执行客户端代码：
   ```bash
   $ python examples/helloworld/test_greeter.py
   ```
   你会在客户端输出中发现 `hello: hello` 字样。

恭喜你！你已经成功地在 tRPC-Python 框架中执行了客户端-服务端应用示例。

### 更新 protobuf

可以看到，protobuf `examples/helloworld/stub/trpc_trpc_python_group_helloworld/helloworld.proto` 定义了服务 `Greeter`：
```protobuf
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
  rpc SayHi (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string msg = 1;
  repeated bytes msg_list = 2;
}

message HelloReply {
  string msg = 1;
  repeated bytes msg_list = 2;
}
```
它有两个方法 `SayHello` 和 `SayHi`。它们的参数都是 `HelloRequest`，返回一个 `HelloReply`。

现在，我们加入一个新的方法 `SayHelloAgain`，使用相同的参数和返回值。
```protobuf
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
  rpc SayHi (HelloRequest) returns (HelloReply) {}
  rpc SayHelloAgain (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string msg = 1;
  repeated bytes msg_list = 2;
}

message HelloReply {
  string msg = 1;
  repeated bytes msg_list = 2;
}
```

在 proto 文件所在目录执行以下命令，重新生成 tRPC 桩代码：
```bash
$ trpc create --lang=python -p helloworld.proto --rpconly -f
```
在「准备工作」一节，我们已经安装了命令行工具 `trpc`。

### 更新并执行服务端和客户端

在服务端 `examples/helloworld/greeter.py` 的 `GreeterServicer` 中，加入以下代码来实现 `SayHelloAgain`：
```python
    async def SayHelloAgain(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        logger.info("got SayHelloAgain request: %s", request.msg)
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg + " again!"
        return rsp
```

在客户端 `examples/helloworld/test_service_rpc.py`，参照 `test_Greeter_SayHello` 加入以下代码来调用 `SayHelloAgain`：
```python
async def test_Greeter_SayHelloAgain():
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = 'world'
    options = Options().set_target('ip://127.0.0.1:8000').set_protocol('trpc').set_serialization_type(SerializationType.PB).set_network('tcp')
    ret = await proxy.asyncSayHelloAgain(TrpcContext(), req, options)
    print(ret.msg)


async_test.append(test_Greeter_SayHelloAgain())
```

按「执行示例」一节重启服务端，再执行一遍客户端，你可能看到 `hello: world again!` 出现在客户端输出中。

### 下一步

- 了解 [tRPC 设计原理](https://github.com/trpc-group/trpc)。
- 阅读 [基础教程](./basics_tutorial.zh_CN.md) 来更深入地了解 tRPC-Python。
- 浏览 [helloworld 示例](/examples/helloworld) 的完整代码。
