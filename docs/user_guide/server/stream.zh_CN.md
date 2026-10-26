# 流式服务使用指南

与一元 RPC（等待完整请求包到达后才进入业务逻辑、一次性返回响应）不同，流式 RPC 支持在单条连接上持续收发数据流，实现边接收边处理。

流式服务同样使用 tRPC 协议（YAML 中配置 `protocol: trpc`），服务创建与启动流程与 [一元服务](overview.zh_CN.md) 完全一致：依次调用 `create_server()`、`register_*_server()` 和 `server.serve()`。完整可运行示例请参考 [examples/hellostream/greeter.py](../../../examples/hellostream/greeter.py)。

## 1. 三种流式 RPC 模式

在 `.proto` 文件中，通过在请求或响应类型前添加 `stream` 关键字，即可声明对应的流式模式：

```protobuf
syntax = "proto3";

package trpc.py_stream.helloworld;

service Greeter {
  // 客户端流：客户端持续发送请求流，服务端最终返回单个响应
  rpc SayHelloReq (stream HelloRequest) returns (HelloReply) {}
  // 服务端流：客户端发送单个请求，服务端持续返回响应流
  rpc SayHelloRsp (HelloRequest) returns (stream HelloReply) {}
  // 双向流：客户端与服务端均可独立、持续地收发数据流
  rpc SayHelloAll (stream HelloRequest) returns (stream HelloReply) {}
}

message HelloRequest {
  string name = 1;
}

message HelloReply {
  string message = 1;
}
```

生成桩代码的命令与一元服务相同：

```bash
trpc create --lang=python -p helloworld.proto --rpconly -f
```

### 服务端方法签名与收发规范

三种流式模式生成的服务端方法签名与交互方式各有不同：

- **客户端流（Client Streaming）**：
  - 方法仅接收一个 `stream` 参数。
  - 在循环中调用 `await stream.recv()` 逐条读取客户端发来的消息。
  - 当客户端正常结束发送并关闭流时，`stream.recv()` 会抛出 `EOFError`；此时服务端应调用 `await stream.send_and_close(rsp)` 发送唯一的一条汇总响应并结束本次流式调用。
- **服务端流（Server Streaming）**：
  - 方法接收两个参数：第一个是客户端发来的单个请求对象 `request`，第二个是 `stream`。
  - 服务端可通过多次调用 `await stream.send(rsp)` 向客户端持续推送响应消息，函数返回时框架自动关闭服务端流。
- **双向流（Bidirectional Streaming）**：
  - 方法仅接收一个 `stream` 参数。
  - 服务端可自由交替或并发调用 `await stream.recv()` 与 `await stream.send(rsp)`。捕获到 `EOFError` 表示客户端已正常关闭其发送方向。

```python
import asyncio

from trpc_sdk.log import logger

from trpc_py_stream_helloworld import pb
from trpc_py_stream_helloworld import rpc
from trpc_py_stream_helloworld import rpc_stream


class GreeterServicer(rpc.GreeterServicer):
    async def SayHelloReq(self, stream: rpc_stream.GreeterServerStream.SayHelloReq):
        """Client streaming example."""
        while True:
            try:
                req = await stream.recv()
                logger.info("request: %s", req.name)
            except EOFError:
                rsp = pb.HelloReply()
                rsp.message = "SayHelloReq done"
                await stream.send_and_close(rsp)
                return

    async def SayHelloRsp(self, request: pb.HelloRequest, stream: rpc_stream.GreeterServerStream.SayHelloRsp):
        """Server streaming example."""
        for i in range(3):
            rsp = pb.HelloReply()
            rsp.message = "rsp: " + str(i)
            await stream.send(rsp)
            await asyncio.sleep(1)

    async def SayHelloAll(self, stream: rpc_stream.GreeterServerStream.SayHelloAll):
        """Bidirectional streaming example."""
        while True:
            try:
                req = await stream.recv()
            except EOFError:
                return
            rsp = pb.HelloReply()
            rsp.message = "rsp: " + req.name
            await stream.send(rsp)
```

> **异常关闭说明**：如果客户端发送了携带错误状态的关闭帧（非正常结束），`await stream.recv()` 会抛出 `ServerSystemError` 而非 `EOFError`。

## 2. 拦截器、空闲连接与多进程路由

- **流式拦截器（`enable_stream_filter`）**：
  流式数据帧在进入超时检查和队列等待检查之前，就会直接分发给已注册的流式处理方法。默认情况下流式调用**不经过**服务端拦截器链（`enable_stream_filter` 默认为 `false`）。若希望拦截器包裹流式方法的执行，需在对应 `service` 配置中显式设置 `enable_stream_filter: true`；开启后，调用仍会进入 `filters.handle`，但此时传给拦截器的 `req` 参数为 `None`。详见 [过滤器插件](../../developer_guide/develop_plugins/filter.zh_CN.md)。
- **自动禁用空闲连接超时**：
  一旦某个 `Service` 注册了流式方法，框架会自动关闭该 `Service` 的传输层空闲连接超时检查，防止长耗时或低频收发的流式连接被 [空闲连接回收机制](overview.zh_CN.md) 误断开。
- **多进程分离模式下的流亲和性**：
  在 `multi_process` + `separate` 进程模式下，主 I/O 进程会根据 `(session_id + stream_id)` 进行哈希绑定，确保同一条流的所有数据帧始终路由到同一个后端业务子进程（rpc task）处理。关于进程模型的详细分工，请参考 [架构设计](../../developer_guide/architecture.zh_CN.md)。
