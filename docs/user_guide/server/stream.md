English | [中文](stream.zh_CN.md)

# Streaming Service

A unary RPC waits until the whole request packet has arrived, then runs the business logic and returns one response. A streaming RPC keeps sending and receiving on one connection, so the handler can process data as it arrives.

A streaming service still uses the tRPC protocol (`protocol: trpc` in YAML). Creating and starting it is the same as for a [unary service](overview.md): `create_server()`, then `register_*_server()`, then `server.serve()`. A runnable example is in [examples/hellostream/greeter.py](../../../examples/hellostream/greeter.py).

## 1. Three Streaming RPC Modes

In the `.proto` file, the `stream` keyword on the request or response type selects the mode:

```protobuf
syntax = "proto3";

package trpc.py_stream.helloworld;

service Greeter {
  // Client stream: the client keeps sending, and the server returns one response.
  rpc SayHelloReq (stream HelloRequest) returns (HelloReply) {}
  // Server stream: the client sends one request, and the server keeps returning.
  rpc SayHelloRsp (HelloRequest) returns (stream HelloReply) {}
  // Bidi stream: both sides keep sending and receiving independently.
  rpc SayHelloAll (stream HelloRequest) returns (stream HelloReply) {}
}

message HelloRequest {
  string name = 1;
}

message HelloReply {
  string message = 1;
}
```

The generate command is the same as for a unary service:

```bash
trpc create --lang=python -p helloworld.proto --rpconly -f
```

### Server Method Signatures

The three modes generate different server signatures:

- **Client streaming**:
  - The method takes only a `stream`.
  - Loop on `await stream.recv()` to read each message from the client.
  - When the client finishes sending and closes the stream normally, `stream.recv()` raises `EOFError`. The server then calls `await stream.send_and_close(rsp)` to send the single summary response and end the call.
- **Server streaming**:
  - The method takes two arguments. The first is the single client request object `request`. The second is `stream`.
  - Call `await stream.send(rsp)` more than once to push responses. When the function returns, the framework closes the server stream.
- **Bidirectional streaming**:
  - The method takes only a `stream`.
  - `await stream.recv()` and `await stream.send(rsp)` can alternate or run concurrently. `EOFError` means the client has closed its send direction normally.

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

> If the client sends a close frame that carries an error, rather than a normal end, `await stream.recv()` raises `ServerSystemError` instead of `EOFError`.

## 2. Filters, Idle Connections, and Multi-Process Routing

- **Streaming filters (`enable_stream_filter`).** A streaming frame is handed to the registered streaming method before the timeout check and the queue-wait check. By default a streaming call **does not** enter the server filter chain (`enable_stream_filter` defaults to `false`). To wrap the streaming method in filters, set `enable_stream_filter: true` on that `service`. After it is turned on, the call still enters `filters.handle`, but the `req` passed to the filter is `None`. See [Filter Plugin](../../developer_guide/develop_plugins/filter.md).
- **Idle timeout is turned off automatically.** Once a `Service` has registered a streaming method, the framework turns off that `Service`'s transport idle-connection timeout, so a long or infrequent stream is not closed by [idle connection reclamation](overview.md).
- **Stream affinity in multi-process separate mode.** With `multi_process` + `separate`, the main I/O process hashes `(session_id + stream_id)` so every frame of one stream stays on the same backend business child process (rpc task). How the processes are split is in [Architecture](../../developer_guide/architecture.md).
