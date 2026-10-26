English | [中文](README.zh_CN.md)

# tRPC-Python Framework

[![Python](https://img.shields.io/badge/python-%3E%3D3.8-blue.svg)](https://www.python.org/downloads/)
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](https://github.com/trpc-group/trpc-python/blob/main/LICENSE)
[![Releases](https://img.shields.io/github/release/trpc-group/trpc-python.svg?style=flat-square)](https://github.com/trpc-group/trpc-python/releases)
[![Docs](https://img.shields.io/badge/docs-latest-green)](/docs/README.md)
[![Tests](https://github.com/trpc-group/trpc-python/actions/workflows/ci.yml/badge.svg)](https://github.com/trpc-group/trpc-python/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/trpc-group/trpc-python/branch/main/graph/badge.svg)](https://app.codecov.io/gh/trpc-group/trpc-python/tree/main)

tRPC-Python is the [Python][] language implementation of [tRPC][]. It is a pluggable, high-performance RPC framework that enables rapid development of RPC services through pluggable protocols, filters, and service governance components.

For more information, please refer to the [Quick Start][quick start] and [Detailed Documentation][docs].

## Framework Features

tRPC-Python has the following features:

- **Multi-Service Listening**: Multiple services can be started within a single process, listening on multiple addresses simultaneously.
- **Fully Pluggable Components**: All components are designed to be pluggable, with built-in default implementations for core functionalities that can be replaced on demand; third-party components can also be implemented and registered into the framework.
- **Multi-Protocol Interoperability**: Supports extending any third-party protocol (simply by implementing the protocol's packing and unpacking interfaces). Built-in support for `trpc` and `http` protocols is provided by default and can be switched at any time.
- **Multiple Process Models**: Provides multiple process models that can be flexibly selected via `global.processmodel` (defaults to single-process mode).
- **Code Generation Tool**: Provides the [trpc command-line tool][trpc-cmdline] to generate Python stub code from protobuf with one click.

## Use Cases

- High-performance cross-language microservice communication (based on Protocol Buffers, seamlessly interoperating with the multi-language tRPC ecosystem)
- Streaming data transmission and real-time interaction (supporting client streaming, server streaming, bidirectional streaming RPC, and HTTP SSE)
- AI model inference and compute-intensive services (supporting Tensor transmission and standalone `task` processes to isolate heavy computation from network I/O)
- Unified multi-protocol access and Web services (built-in `trpc` and `http` protocol support, providing custom `@route` routing and custom protocol extensions)
- High-concurrency and multi-core scaling services (supporting single-process, multi-process, I/O and Handle separation, and other process models)
- Pluggable microservice governance integration (supporting naming services, configuration centers, logging, metrics monitoring, task scheduling, and filter extensions)
- Single-process multi-service aggregated deployment (mounting multiple logical services within the same process, listening on TCP, UDP, or Unix Domain Socket simultaneously)

## Quick Start

### Prerequisites

- Python 3.8+ (Python 3.10+ recommended)
- C compiler toolchain (such as `gcc` or `clang`, used to compile Cython extensions during installation)
- [tRPC command-line tool][trpc-cmdline] (optional, used to generate Python stub code from protobuf)

### Installation

#### Install with pip

```bash
pip install trpc-python
```

Install optional extras on demand (such as `uvloop` acceleration and `snappy` compression):

```bash
pip install "trpc-python[fast,snappy]"
```

#### Install with uv

[uv](https://docs.astral.sh/uv/) provides a faster and reproducible installation experience:

```bash
# macOS / Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on macOS: brew install uv

# Install the published package in a virtual environment
uv venv --python 3.12
source .venv/bin/activate
uv pip install trpc-python

# Install optional extras on demand
uv pip install "trpc-python[fast,snappy]"
```

For projects managed by uv:

```bash
# Core capabilities only
uv add trpc-python
# With optional extras
# uv add "trpc-python[fast,snappy]"
```

When developing this repository:

```bash
git clone --depth 1 https://github.com/trpc-group/trpc-python.git
cd trpc-python

# Option 1: Sync dev and test dependencies with uv
uv sync --extra dev --group test

# Option 2: Install using a virtual environment and pip / build script
python3 -m venv .venv
source .venv/bin/activate
./build.sh
```

### Develop a Greeter RPC Service

1. **Define the protobuf service** (`helloworld.proto`) and generate stub code:

```protobuf
syntax = "proto3";
package trpc.trpc_python_group.helloworld;

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

```bash
trpc create --lang=python -p helloworld.proto --rpconly -f
```

2. **Write the server configuration file** (`trpc_python.yaml`):

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

3. **Implement the server and client code**:

```python
# server.py
from trpc_sdk.abc import TrpcContext
from trpc_sdk.server import create_server
from trpc_trpc_python_group_helloworld import pb, rpc


class GreeterServicer(rpc.GreeterServicer):
    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        rsp = pb.HelloReply()
        rsp.msg = f"hello: {request.msg}"
        return rsp


if __name__ == "__main__":
    server = create_server("trpc_python.yaml")
    rpc.register_GreeterServicer_server(server, GreeterServicer(), fix_rpc_name=True)
    server.serve()
```

```python
# client.py
import asyncio

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType
from trpc_trpc_python_group_helloworld import pb, rpc


async def main():
    proxy = rpc.GreeterClientProxyImpl(need_clone_ctx=False)
    req = pb.HelloRequest(msg="hello")
    options = (
        Options()
        .set_target("ip://127.0.0.1:8000")
        .set_protocol("trpc")
        .set_serialization_type(SerializationType.PB)
        .set_network("tcp")
    )
    rsp = await proxy.asyncSayHello(TrpcContext(), req, options)
    print(rsp.msg)


if __name__ == "__main__":
    asyncio.run(main())
```

### Run the Example Service

The repository includes an out-of-the-box `helloworld` example. You can start the server and invoke it from the client directly:

```bash
# Terminal 1: Start the server (listens on 127.0.0.1:8000 by default)
python examples/helloworld/trpc_main.py

# Terminal 2: Run the client call (outputs hello: hello)
python examples/helloworld/test_greeter.py
```

## Related Documentation

- [Quick Start][quick start]
- [Basics Tutorial][basics]
- [Framework Configuration][framework conf]
- [Server Development Guide][server]
- [Client Development Guide][client]
- [Documentation Index][docs]

## Examples

See the [Examples Index][examples] for detailed descriptions.

- [helloworld][helloworld]: Unary Greeter service, listening on `127.0.0.1:8000` by default, plus a custom UDP `8090` protocol example
- [Streaming Service][hellostream]: Covers unary, client streaming, server streaming, and bidirectional streaming, listening on TCP `8000`–`8040`
- [HTTP SSE][hellosse]: `GET /sse` server-sent events with five process layouts, listening on `8080`–`8084`
- [hellotrt][hellotrt]: Tensor echo and `normal_task` task scheduling, listening on `127.0.0.1:8000` by default
- [HTTP][http]: HTTP RPC and custom `@route` routing, listening on `9002` / `9003`

## Plugins

For developing built-in plugins within the framework, please refer to the developer guides:

- [Plugin Development Basics][plugin]
- [Protocol Plugin][protocol]
- [Config Plugin][config]
- [Task Plugin][task]
- [Log Plugin][log]
- [Metrics Plugin][metrics]
- [Database Plugin][database]

For more plugins integrating with external governance components, please refer to the [tRPC Ecosystem Repositories][ecosystem].

## How to Contribute

We warmly welcome community contributions! If you are interested in contributing, please read our [Contribution Guidelines][contributing] and check the unassigned tasks in [Issues][issues]. Claim a task and let's make tRPC-Python better together.

### **Ways to Contribute**

- **Report Bugs or Suggest Features** - Report issues or propose new features via [Issues][issues]
- **Improve Documentation** - Enhance tutorials, examples, and API references to help fellow developers get started quickly
- **Submit PRs** - Participate in bug fixes, new feature development, performance optimizations, or test coverage improvements
- **Extend Plugins & Share Use Cases** - Develop ecosystem plugins or share your RPC service best practices

### **Quick Contribution Setup**

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/trpc-python.git
cd trpc-python

# Install development and test dependencies, then run tests
pip install -e ".[dev,fast,snappy]"
pip install -r requirements-test.txt
pytest

# Make your changes and submit a PR!
```

**Please read** [CONTRIBUTING.md][contributing] for detailed contribution workflows and coding standards.  
**Please follow** [CODE-OF-CONDUCT.md][code-of-conduct] to help maintain a friendly, respectful, and inclusive open-source community.

[Python]: https://www.python.org
[tRPC]: https://github.com/trpc-group/trpc
[trpc-cmdline]: https://github.com/trpc-group/trpc-cmdline
[docs]: /docs/README.md
[quick start]: /docs/quick_start.md
[basics]: /docs/basics_tutorial.md
[framework conf]: /docs/user_guide/framework_conf.md
[server]: /docs/user_guide/server/overview.md
[client]: /docs/user_guide/client/overview.md
[architecture]: /docs/developer_guide/architecture.md
[plugin]: /docs/developer_guide/develop_plugins/plugin.md
[filter]: /docs/developer_guide/develop_plugins/filter.md
[protocol]: /docs/developer_guide/develop_plugins/protocol.md
[config]: /docs/developer_guide/develop_plugins/config.md
[task]: /docs/developer_guide/develop_plugins/task.md
[log]: /docs/developer_guide/develop_plugins/log.md
[metrics]: /docs/developer_guide/develop_plugins/metrics.md
[database]: /docs/developer_guide/develop_plugins/database.md
[naming]: /docs/developer_guide/develop_plugins/naming.md
[ecosystem]: https://github.com/orgs/trpc-ecosystem/repositories
[contributing]: /CONTRIBUTING.md
[code-of-conduct]: /CODE-OF-CONDUCT.md
[issues]: https://github.com/trpc-group/trpc-python/issues
[examples]: /examples/README.md
[helloworld]: /examples/helloworld/README.md
[hellostream]: /examples/hellostream/README.md
[hellosse]: /examples/hellosse/README.md
[hellotrt]: /examples/hellotrt/README.md
[http]: /examples/http/README.md
[helloworld-features]: /examples/README.md#helloworld-features
[hellostream-features]: /examples/README.md#hellostream-features
