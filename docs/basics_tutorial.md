English | [中文](./basics_tutorial.zh_CN.md)

## Basics Tutorial

In [Quick Start](./quick_start.md), you already ran the tRPC-Python helloworld. That chapter skipped how a service is defined, configured, and extended. This chapter fills in those parts in the order you meet them while developing:

- How to define a tRPC service with protobuf
- How to write `trpc_python.yaml`
- Filters and plugins
- Streaming RPC and HTTP

Service definitions use Protocol Buffers v3. See its [Python tutorial](https://protobuf.dev/getting-started/pythontutorial/).

### Define a Service

Declare a new service in protobuf first. The following declares a service named `MyService`:

```protobuf
service MyService {
}
```

Methods go inside `service`. The helloworld `Greeter` has one method, `SayHello`. Its argument is `HelloRequest`, and it returns `HelloReply`:

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

A method declaration ends with `{}`. The method body can be empty.

### Write the Client and Server

Protobuf gives a language-independent service definition. Use the [trpc command-line tool](https://github.com/trpc-group/trpc-cmdline) to generate Python stub code from it. Run this in the directory that contains the proto file:

```bash
$ trpc create --lang=python -p helloworld.proto --rpconly -f
```

`$ trpc create -h` lists the other options. The full project is [helloworld](/examples/helloworld). If the generated `rpc.py` still imports from the old `trpc` package, change those imports to `trpc_sdk`, as described in [Quick Start](./quick_start.md).

The stub code is split into a server and a client. On the server, you implement `GreeterServicer`:

```python
class GreeterServicer:
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        raise NotImplementedError("Method not implemented!")
```

The business implementation subclasses that class and fills in `SayHello`:

```python
class GreeterServicer(rpc.GreeterServicer):
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp
```

The process entry point reads the configuration with `create_server`, then registers the implementation. `create_server` only loads the configuration and assembles the services. Listening starts in `serve`:

```python
from trpc_sdk.server import create_server

server = create_server(conf_path)
rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
server.serve()
```

The client stub provides the async method `asyncSayHello`. You can pass `Options` to it directly. The following matches the helloworld client example:

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

The stub also has a synchronous method, `SayHello`. The later examples keep using the async call.

### Framework Configuration

The client above uses `set_target` to set the server address. The server listen address is written in `trpc_python.yaml`.

At startup, pass the path of that file to `create_server`. It calls `TrpcConfig.load` to read the configuration. helloworld uses `trpc_python.yaml` in the example directory by default. You can also pass a path on the command line:

```bash
$ python examples/helloworld/trpc_main.py --conf=<path>
```

After you change `ip`, `port`, `protocol`, or `timeout`, restart the process. The `SayHello` implementation stays the same.

The server configuration used in this tutorial is below. `name` matches the service name in the generated stub, `trpc.trpc_python_group.helloworld.Greeter`:

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

The client can also set `target` under `client.service`. If the same call also runs `set_target`, the address passed on that call takes precedence.

### Filters and Plugins

tRPC-Python puts logic that spans requests in filters, and puts replaceable modules in plugins. A filter is itself a kind of plugin.

#### Filters

A filter is like an onion. While handling a request, the outer pre-logic runs first, then the business method, then the post-logic on the way back.

The client and the server use the same `BaseFilter`. `run` handles unary calls, and `run_stream` handles streaming calls. The following server filter only wraps the next layer:

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

Code before `next_handle()` is pre-logic, and code after it is post-logic. `run` returns `(rsp, err)`.

This filter does not handle streaming, so `run_stream` raises `NotImplementedError`. See Streaming RPC below for when a streaming call enters `run_stream`.

A client filter is written the same way. Its registration type is `PluginType.CLIENT_FILTER_COMPONENT`.

`@register_plugin` runs when the module is imported. The service entry point must import that module before `create_server`, for example `import my_filter as _`. The decorator's default `init_pos` is `WORKER`.

The configuration file lists registered filter names under the `filter` key:

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

On the server, `create_server` installs `server.filter` first, then appends that service's own `filter`. Filters earlier in the list are the outer layers of the onion and run first as the request enters. If a name has no matching `SERVER_FILTER_COMPONENT` plugin, `create_server` raises `PluginInitError`.

On the client, the global `client.filter` is placed before that service's `filter`. At call time, `Options.set_filter` or `set_filter_list` appends after those two segments by default. Pass `override=True` and that call keeps only the filter instances in the argument. The call site passes instances:

```python
options = Options().set_target("ip://127.0.0.1:8000").set_filter(MyClientFilter())
```

#### Plugins

A plugin is registered at import time with `@register_plugin(PluginType, name)`. The decorated class usually subclasses `BasePlugin`. The framework calls `init()` at the matching process stage, then calls `start()`. `BaseFilter` is a subclass of `BasePlugin`, so the filter registration in the previous section is already plugin registration.

To use a plugin written by someone else, import that module from the service entry point so the decorator runs. The commented-out `import ... as _` lines in helloworld's `trpc_main.py` do this.

The `plugins:` block in `trpc_python.yaml` is stored as a whole by `get_plugins_config()`. Each plugin's own implementation decides whether to read that configuration.

- A naming service finds `NAMING_SELECTOR` through the scheme of `target`. The `plugins` section is not decoded into the selector. See [Naming plugins](./developer_guide/develop_plugins/naming.md).
- The startup file always uses the `file` data source and the `yaml` decoder. The `plugins` section is not used to create config plugins. See [Config plugins](./developer_guide/develop_plugins/config.md).

Filters work with plugins like this: after the import finishes registration, write the registered name into the `filter` list.

### Multiple Protocols

The call in [Quick Start](./quick_start.md) is one request and one response. tRPC-Python also supports streaming RPC and HTTP.

#### Streaming RPC

Streaming RPC lets the client and the server exchange data in more than one step. There are three modes.

In client streaming, the client sends several packets in order, and the server returns one packet after it has received all of them. In server streaming, one client request can receive several response packets. In bidirectional streaming, both sides can send data to each other, and order is preserved.

The example is [hellostream](/examples/hellostream). Mark a streaming method with `stream` in protobuf:

```protobuf
service Greeter {
  rpc SayHelloReq (stream HelloRequest) returns (HelloReply) {}
  rpc SayHelloRsp (HelloRequest) returns (stream HelloReply) {}
  rpc SayHelloAll (stream HelloRequest) returns (stream HelloReply) {}
}
```

`stream` only before the request means client streaming. `stream` only before the return value means server streaming. `stream` on both sides means bidirectional streaming.

In client streaming, the client calls `await proxy.asyncSayHelloReq(ctx, options)`, calls `send` several times, and finishes with `close_and_recv`. The server implements `SayHelloReq(self, stream)` and loops on `recv`. `EOFError` means the peer has already called `close_and_recv`. Use `send_and_close` then to return one packet and finish:

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

In hellostream, the request field is `name` and the reply field is `message`.

In server streaming, the server implements `SayHelloRsp(self, request, stream)`, calls `send` several times, and returning from the method means sending is finished. The client calls `asyncSayHelloRsp(ctx, req, options)` and then loops on `recv`. `EOFError` means the stream has ended.

In bidirectional streaming, the client calls `asyncSayHelloAll` and can interleave `send` and `recv`, then calls `close_send` when it has finished sending. The server implements `SayHelloAll(self, stream)` and also uses `recv` and `send`. When the peer closes, `recv` raises `EOFError`.

The listen configuration is the same as for a unary RPC, and the service `protocol` is still `trpc`. To send streaming calls into a filter's `run_stream`, set `enable_stream_filter: true` on the service and call `set_enable_stream_filter(True)` on the client.

#### Custom HTTP Routes

After you set a service's `protocol` to `http`, aiohttp serves that port. Register routes with `trpc_sdk.http.route`, and import the module that defines them before `serve`:

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

The handler takes `(ctx, request)`. `request` is an aiohttp `Request`.

When `service_name` is omitted, every HTTP service in the configuration gets this route. With `@route("/bind", service_name="trpc.trpc_python_group.helloworld.Greeter1")`, the route is attached only to that service.

Matching checks these user routes first. Unmatched paths are then handed to RPC handling.

#### Call an Existing RPC over HTTP

For the same `Greeter`, change that service's `protocol` from `trpc` to `http`, and you can call the stub methods over HTTP. The request path is the method name. With `Content-Type: application/json`, the body is deserialized as JSON, and the field name is `msg` from the proto:

```bash
$ curl -X POST -H "Content-Type: application/json" \
  -d '{"msg": "world"}' \
  127.0.0.1:8000/trpc.trpc_python_group.helloworld.Greeter/SayHello
```

### Further Reading

- Review the helloworld run steps in [Quick Start](./quick_start.md).
- Read the full code of the [helloworld example](/examples/helloworld) and the [hellostream example](/examples/hellostream).
- To write a naming plugin, see [Naming plugins](./developer_guide/develop_plugins/naming.md).
- To write a config plugin, see [Config plugins](./developer_guide/develop_plugins/config.md).
