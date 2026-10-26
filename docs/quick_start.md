English | [中文](quick_start.zh_CN.md)

## Quick Start

### Prerequisites

- **[Python](https://www.python.org/downloads/)**, should be greater or equal than 3.8.
- **A C toolchain** (such as gcc or clang), to build the Cython extensions during installation.
- **[tRPC cmdline tools](https://github.com/trpc-group/trpc-cmdline)**, to generate Python stub codes from protobuf.

### Get Example Code

Example code is part of tRPC-Python repo.
Clone the repo, create a virtual environment and install the framework:
```bash
$ git clone --depth 1 git@github.com:trpc-group/trpc-python.git
$ cd trpc-python
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -e .
```

### Run the Example

1. Start the server, which listens on `127.0.0.1:8000` as configured in `examples/helloworld/trpc_python.yaml`:
   ```bash
   $ python examples/helloworld/trpc_main.py
   ```
2. From a different terminal, activate the same virtual environment and run the client:
   ```bash
   $ python examples/helloworld/test_greeter.py
   ```
   You will see `hello: hello` in the client output.

Congratulations! You’ve just run a client-server application with tRPC-Python.

### Update protobuf

As you can see, service `Greeter` is defined in protobuf `examples/helloworld/stub/trpc_trpc_python_group_helloworld/helloworld.proto` as following:
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
It has two methods, `SayHello` and `SayHi`, which both take `HelloRequest` as parameter and return `HelloReply`.

Now, add a new method `SayHelloAgain`, with the same request and response:
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

Regenerate tRPC stub code by running the following command in the directory of the proto file:
```bash
$ trpc create --lang=python -p helloworld.proto --rpconly -f
```
The `trpc` command should be installed by prerequisites.

### Update and Run Server and Client

At server side `examples/helloworld/greeter.py`, add codes to `GreeterServicer` to implement `SayHelloAgain`:
```python
    async def SayHelloAgain(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        logger.info("got SayHelloAgain request: %s", request.msg)
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg + " again!"
        return rsp
```

At client side `examples/helloworld/test_service_rpc.py`, add codes like `test_Greeter_SayHello` to call `SayHelloAgain`:
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

Follow the `Run the Example` section to restart the server and re-run the client, and you will see `hello: world again!` in the client output.

### What's Next

- Learn [tRPC Design](https://github.com/trpc-group/trpc).
- Read [basics tutorial](./basics_tutorial.md) to get deeper into tRPC-Python.
- Browse the full code of the [helloworld example](/examples/helloworld).
