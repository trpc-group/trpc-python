English | [中文](overview.zh_CN.md)

# tRPC-Python Server Development Guide

The tRPC-Python server is built on `asyncio`. A business handler is an `async def` coroutine, and the framework schedules it on the event loop.

This page follows one server from start to finish: **choose a service -> configure a listen endpoint (Naming Service) -> define the interface and register the implementation (Proto Service) -> attach plugins and filters -> tune the process model and timeouts**.

- A runnable unary RPC example: [examples/helloworld](../../../examples/helloworld/trpc_main.py)
- How processes and threads divide the work: [Architecture](../../developer_guide/architecture.md)

## 1. Choosing a Service

The framework ships four kinds of services. The first three define the interface in protobuf and generate stub code with `trpc create`. The fourth, standard HTTP, registers routes with a decorator and does not need an IDL:

- **tRPC unary service.** `protocol: trpc`. One request, one response.
- **tRPC streaming service.** `protocol: trpc`. One connection keeps sending and receiving (client stream, server stream, or bidi stream). See [Streaming Service](stream.md).
- **Generic HTTP RPC.** `protocol: http`. The transport is HTTP, but the interface is still protobuf and the implementation matches a unary service. See [HTTP Server](http.md).
- **Standard HTTP.** `protocol: http`. Register `aiohttp` handlers with `@route` and read and write the HTTP request and response yourself. See [HTTP Server](http.md).

**Also:**

- **HTTP and TLS.** HTTP is served by `aiohttp` (HTTP/1.1). HTTP/2 and HTTP/3 are not supported. For TLS, set both `tls_key` and `tls_cert` on that `service`.
- **A custom protocol.** To speak another private or third-party protocol, implement the `codec` and `checker` as in [Protocol Plugin](../../developer_guide/develop_plugins/protocol.md), then set `service.protocol` to the registered name.
- **In-process timer tasks.** A timer is not an RPC exposed to the outside. Register it with `@register_timer_task`. It runs on the service process event loop at a fixed interval or on a cron expression. See [Timer](timer.md).

## 2. Configuring a Listen Endpoint (Naming Service)

Each item under `server.service` is one listen endpoint (Naming Service): address, network, and protocol. Clients discover it by `name`. A common form is `trpc.{app}.{server}.{service}`.

`create_server()` builds one `Service` for each item in `server.service`. The core fields from [examples/helloworld/trpc_python.yaml](../../../examples/helloworld/trpc_python.yaml) are:

```yaml
server:
  app: test
  server: Greeter
  service:
    - name: trpc.trpc_python_group.helloworld.Greeter
      ip: 127.0.0.1
      port: 8000
      network: tcp
      protocol: trpc
      timeout: 5000
```

- `ip` / `port`: the address and port the server binds.
- `network`: `tcp` or `udp`.
- `protocol`: application protocol. The built-in names are `trpc` and `http`.
- `timeout`: the processing budget of one request, in milliseconds.

If a naming-service registry plugin is configured, the process registers that `name` and the listen address after the port is bound and listening has started.

## 3. Defining and Registering a Service (Proto Service)

A Proto Service is the set of RPC methods implemented in code. The generated `register_*_server` function binds that method table onto the Naming Service above.

### 3.1 An IDL Service in Three Steps

A tRPC unary service, a streaming service, and generic HTTP RPC all follow these three steps.

**Step 1: write the `.proto`.**

The `package` name is prefixed onto the generated RPC method path:

```protobuf
syntax = "proto3";

package trpc.trpc_python_group.helloworld;

option py_generic_services = true;

service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
  rpc SayHi (HelloRequest) returns (HelloReply) {}
}

message HelloRequest {
  string msg = 1;
}

message HelloReply {
  string msg = 1;
}
```

**Step 2: generate the Python stub.**

```bash
trpc create --lang=python -p helloworld.proto --rpconly -f
```

The generated `rpc.py` contains the server base class `GreeterServicer` and the registration function `register_GreeterServicer_server`. The default method path looks like `/trpc.trpc_python_group.helloworld.Greeter/SayHello`.

**Step 3: implement the methods and start the server.**

The layout matches [examples/helloworld/trpc_main.py](../../../examples/helloworld/trpc_main.py):

```python
from trpc_sdk.abc import TrpcContext
from trpc_sdk.server import create_server

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc


class GreeterServicer(rpc.GreeterServicer):
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hello: " + request.msg
        return rsp

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = "hi: " + request.msg
        return rsp


def serve(conf_path: str) -> None:
    server = create_server(conf_path)
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()
```

**How the pieces fit:**

- **`create_server` and `serve`.** `create_server(conf_path)` only reads the YAML and builds `Service` objects in memory. It does not bind a port or `fork` child processes. `server.serve()` binds the listen address and starts the event loop.
- **The handler must be `async def`.** The framework does not keep a thread pool for synchronous blocking functions. A plain `def` fails when a request arrives.

### 3.2 Several Listen Endpoints and `fix_rpc_name`

`server.service` can list several endpoints, each with its own port or protocol. `register_*_server(server, servicer)` registers the same method table onto **every** `Service` on that `server`.

- **`fix_rpc_name=True`.** The method-path prefix on each `Service` is replaced by that `Service`'s YAML `name`, so the path becomes `/<YAML service.name>/<method>`. Each listen port then answers under its own `name`. The service name the client requests must match the YAML `name`.
- **Several Proto Services.** Calling `register_*_server` once for each of two different protos mounts both method tables on every `Service` endpoint. The generated function does not attach a proto to only one port by name.

### 3.3 Standard HTTP

Standard HTTP does not generate stub code. Set `protocol: http` in YAML and register paths with `@route`. See [HTTP Server](http.md).

## 4. Plugins

A plugin is registered when the module that defines it is imported. Import a custom or third-party plugin module **before** `create_server()`.

Configuration is read in two ways:

- Structured framework fields, such as `server.filter` and `service.protocol`, are parsed by the framework.
- The top-level `plugins:` block is kept as one dict. Each plugin reads it with `get_plugins_config()`. How to write and register a plugin is in [Plugin Basics](../../developer_guide/develop_plugins/plugin.md).

By purpose and dependency, plugins fall into three kinds:

- **Standalone plugins**, such as protocol, compression, and serialization. They do not depend on external infrastructure. Load them as needed. See [Protocol Plugin](../../developer_guide/develop_plugins/protocol.md).
- **Governance plugins**, such as naming, remote logs, and a configuration center. They talk to an external system. Import the plugin module at the entry and fill in its block under `plugins:`.
- **Task plugins.** Move CPU-heavy, blocking, or batched work into a separate `task` child-process pool. See [Task Plugin](../../developer_guide/develop_plugins/task.md). Multi-process `merge` (that is, `peer`) mode cannot attach a task plugin.

The framework already registers these built-in plugins:

| Type | Registered name | Behavior |
| --- | --- | --- |
| Log backend | `loguru` | Default log implementation |
| Log output | `console`, `file` | `console` writes to stderr. `file` writes to a local file |
| Config source | `file` | Reads configuration from the local filesystem |
| Config decoder | `yaml`, `json` | Parses file contents into a dict |

## 5. Server Filters

A filter wraps one RPC's business handling in an onion. A name earlier in the YAML list sits on the outside: **its pre-logic runs first, and its post-logic runs last**.

`create_server()` installs the global `server.filter` first, then appends each endpoint's `server.service[].filter`. Every name in the list must already be registered, or the service fails to start. How to write a filter and how the order works are in [Filter Plugin](../../developer_guide/develop_plugins/filter.md).

## 6. Business Errors

To return a business error from a handler, raise [`TrpcException`](../../../trpc_sdk/exceptions/_trpc_exception.py). `create_business_error(code, msg)` builds one:

```python
from trpc_sdk.exceptions import create_business_error

async def SayHello(self, ctx, request):
    if not request.msg:
        raise create_business_error(10001, "empty msg")
    ...
```

- When the handler raises `TrpcException`, the framework encodes a response that carries that error code and an empty body. This worker keeps serving later requests.
- Any other uncaught Python exception is wrapped as `UnknownError` and encoded as an error response the same way. The code table is in [Error Codes](../error_code.md).

## 7. Testing

This repository uses `pytest`. Server registration and call tests live under `tests/server`. After the local service is up, follow [Quick Start](../../quick_start.md) and call it from a client script.

## 8. Process Model, Timeout, and Lifecycle

### 8.1 Process and Thread Model

The process and thread model is configured under `global.processmodel`. With that block omitted, the default is **one process and one event loop**, which fits most small or new services. When you need more concurrency, or need to isolate a blocking call, pick the row that matches the goal:

| When to use it | `global.processmodel` |
|---|---|
| **Simplest (default)**: one process, one event loop | Configure nothing |
| **Single-process, multi-thread merge**: several event-loop threads. Each thread both receives packets and runs handlers | `mode: single_process`<br>`io_handle_type: merge`<br>`io_num: N`, `handle_num: N` |
| **Single-process, multi-thread separate**: I/O threads and handler threads are split, so a slow handler does not slow down receiving | `mode: single_process`<br>`io_handle_type: separate`<br>`io_num: N`, `handle_num: M` |
| **Multi-process peer (Peer)**: several processes bypass the GIL. Each child listens and handles requests itself. Processes do not forward to each other | `mode: multi_process`<br>`io_handle_type: merge`<br>`handle_num: N` |
| **Multi-process separate (Separate)**: several processes bypass the GIL. The main process accepts connections and fans them out to backend business processes | `mode: multi_process`<br>`io_handle_type: separate`<br>`handle_num: N` |

**Details:**

- Field definitions are in the `global` section of [Framework Configuration](../framework_conf.md). How the models are built is in [Architecture](../../developer_guide/architecture.md).
- `server.handler_max_concurrency` and `server.handler_queue_size` take effect only under **`single_process` + `separate`**.
- In **multi-process `separate`** mode, the main (I/O) process does only `tRPC-over-TCP` I/O. UDP and HTTP are listened by each business child process (rpc task) itself.
- In **multi-process `merge` (peer)** mode, every child binds the same set of addresses with `SO_REUSEPORT`. This mode cannot attach a `task` plugin.

### 8.2 Request Timeout and Queue-Wait Checks

- **Processing timeout (`service.timeout`).** The time budget for one request on this endpoint, in milliseconds. When the upstream frame header carries a timeout and `disable_request_timeout: true` is not set, the framework uses the **smaller** of the upstream time remaining and `service.timeout`.
- **Queue-wait check (`check_timeout: true`).** Before the request enters the business handler, the framework checks how long it has already waited in the queue:
  - With `queue_wait_policy: 0`, waiting that reaches `queue_wait_timeout` returns `ServerTimeoutError` immediately and the handler does not run. When `queue_wait_timeout` is `0`, the merged timeout of this request is the upper bound. If that merged timeout is also `0`, this check is skipped.
  - With `queue_wait_policy: 1`, the request is dropped only when the wait has reached the limit **and** the sliding window is overloaded.

### 8.3 Idle Connection Reclamation (`check_idle_timeout`)

`check_idle_timeout` is the TCP idle timeout in milliseconds. `0` turns the check off. When the value is greater than `0`, a TCP connection with no bytes in either direction for that long is closed.

- In multi-process `separate` mode, the idle check runs in the main I/O process. UDP, HTTP, and custom-protocol connections that the business child listens itself do not go through this check.
- After a `Service` registers a streaming RPC method, the framework turns off that `Service`'s transport idle timeout so a long-lived stream is not closed by mistake.

### 8.4 Delayed Naming Registration and the HTTP Body Limit

- **Delayed registration (`registry_delay`).** Milliseconds, default `0`. Naming-service registration happens after listening starts. Increase it when warmup or initialization is slow and the process should not take traffic yet. When `registry_option` is `1`, the process waits for a shared ready flag instead of sleeping for `registry_delay`.
- **HTTP body limit (`max_rbuf_size`).** This is the `aiohttp` application's `client_max_size`. The default is 8 MiB (8388608 bytes). It limits the size of one HTTP request body.

### 8.5 SIGTERM Callbacks

When the service process receives `SIGTERM`, it can run cleanup callbacks registered by the business. A callback takes no arguments. Several callbacks run in registration order. One that raises does not stop the later ones.

> The callback runs synchronously on the signal-handling path. It must not block.

```python
from trpc_sdk.runtime import set_signal_handle

def on_server_exit() -> None:
    ...

def on_worker_exit() -> None:
    ...

set_signal_handle(on_server_exit, on_worker_exit)
```

- `master_handle` (the first argument) runs while the process that started the service is finishing. In `merge` mode it runs immediately before the worker callback. In `separate` mode it waits until every business child process (rpc task) has exited.
- `worker_handle` (the second argument) runs when the worker child that serves requests exits.

### 8.6 Multiprocess Fork Lifecycle Callbacks

In multi-process mode, `server.serve()` sets `multiprocessing`'s start method to `fork`. Threads already started in the parent, coroutines that are running, and locks that are held are not safe to keep using in a forked child. To clean up or rebuild around `fork`, register no-argument callbacks with `register_at_fork` and `register_child_callback` **before** `serve()`:

```python
from trpc_sdk.common import ProcessType
from trpc_sdk.common import register_at_fork
from trpc_sdk.common import register_child_callback

def before_fork() -> None:
    ...

def after_parent() -> None:
    ...

def after_any_child() -> None:
    ...

def after_worker() -> None:
    ...

register_at_fork(before=before_fork, after_in_parent=after_parent, after_in_child=after_any_child)
register_child_callback(after_worker, ProcessType.WORKER)
```

When each callback runs:

- `before`: in the parent, **before** `fork`.
- `after_in_parent`: in the parent, **after** `fork`.
- `after_in_child`: in each child, immediately after that child is forked.
- `register_child_callback(func, process_type)`: immediately after `after_in_child`, filtered by child role. `ProcessType.WORKER` runs only in a business worker child. `ProcessType.TASK` runs only in a task child.
- `register_before_loop_run_in_child(func)`: after the child has finished plugin initialization, and before it enters `loop.run_forever()`.
