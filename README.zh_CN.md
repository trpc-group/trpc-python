[English](README.md) | 中文

# tRPC-Python Framework

[![Python](https://img.shields.io/badge/python-%3E%3D3.8-blue.svg)](https://www.python.org/downloads/)
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](https://github.com/trpc-group/trpc-python/blob/main/LICENSE)
[![Releases](https://img.shields.io/github/release/trpc-group/trpc-python.svg?style=flat-square)](https://github.com/trpc-group/trpc-python/releases)
[![Docs](https://img.shields.io/badge/docs-latest-green)](/docs/README.zh_CN.md)
[![Tests](https://github.com/trpc-group/trpc-python/actions/workflows/ci.yml/badge.svg)](https://github.com/trpc-group/trpc-python/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/trpc-group/trpc-python/branch/main/graph/badge.svg)](https://app.codecov.io/gh/trpc-group/trpc-python/tree/main)

tRPC-Python 是 [tRPC][] 的 [Python][] 语言实现，是一个可插拔的高性能 RPC 框架，支持通过可插拔的协议、过滤器和微服务治理组件快速构建 RPC 服务。

更多信息请参阅：[快速上手][quick start] 以及 [详细文档][docs]。

## 框架特性

tRPC-Python 具有以下特点：

- **多服务监听**：一个进程内可以同时启动多个服务，并监听多个地址。
- **全组件可插拔**：所有组件均采用可插拔设计，内置核心功能的默认实现并支持按需替换；第三方组件也可自行实现并注册到框架中。
- **多协议互通**：支持扩展任意第三方协议（只需实现协议的打包和解包接口），默认支持 `trpc` 和 `http` 协议并可随时切换。
- **多进程模型**：提供多种进程模型，可通过 `global.processmodel` 灵活选择（默认为单进程模式）。
- **代码生成工具**：提供 [trpc 命令行工具][trpc-cmdline]，支持从 protobuf 一键生成 Python 桩代码。

## 使用场景

- 高性能跨语言微服务通信（基于 Protocol Buffers，与 tRPC 多语言生态无缝互通）
- 流式数据传输与实时交互（支持客户端流、服务端流、双向流 RPC 以及 HTTP SSE）
- AI 模型推理与计算密集型服务（支持 Tensor 传输与独立 `task` 进程，隔离重负载计算与网络 I/O）
- 多协议统一接入与 Web 服务（内置 `trpc` 与 `http` 协议支持，提供 `@route` 自定义路由及自定义协议扩展）
- 高并发与多核扩展服务（支持单进程、多进程、I/O 与 Handle 分离等多种进程模型）
- 可插拔的微服务治理接入（支持名字服务、配置中心、日志、监控指标、任务调度与拦截器扩展）
- 单进程多服务聚合部署（支持同一进程内挂载多个逻辑服务，同时监听 TCP、UDP 或 Unix Domain Socket）

## 快速开始

### 前置条件

- Python 3.8+（推荐 Python 3.10+）
- C 编译工具链（如 `gcc` 或 `clang`，安装框架时用于编译 Cython 扩展）
- [tRPC 命令行工具][trpc-cmdline]（可选，用于从 protobuf 生成 Python 桩代码）

### 安装

#### 使用 pip 安装

```bash
pip install trpc-python
```

按需安装扩展能力（如 `uvloop` 加速与 `snappy` 压缩）：

```bash
pip install "trpc-python[fast,snappy]"
```

#### 使用 uv 安装

[uv](https://docs.astral.sh/uv/) 可提供更快、可复现的安装体验：

```bash
# macOS / Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh
# macOS 也可以: brew install uv

# 在虚拟环境中安装已发布的包
uv venv --python 3.12
source .venv/bin/activate
uv pip install trpc-python

# 按需安装扩展能力
uv pip install "trpc-python[fast,snappy]"
```

对于由 uv 管理的项目：

```bash
# 仅安装核心能力
uv add trpc-python
# 需要扩展能力
# uv add "trpc-python[fast,snappy]"
```

开发本仓库时：

```bash
git clone --depth 1 https://github.com/trpc-group/trpc-python.git
cd trpc-python

# 方式一：使用 uv 同步开发与测试依赖
uv sync --extra dev --group test

# 方式二：使用虚拟环境与 pip / 构建脚本安装
python3 -m venv .venv
source .venv/bin/activate
./build.sh
```

### 开发 Greeter RPC 服务

1. **定义 protobuf 服务**（`helloworld.proto`）并生成桩代码：

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

2. **编写服务端配置文件**（`trpc_python.yaml`）：

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

3. **实现服务端与客户端代码**：

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

### 运行示例服务

仓库已内置开箱即用的 `helloworld` 示例，可直接启动服务端并发起客户端调用：

```bash
# 终端 1：启动服务端（默认监听 127.0.0.1:8000）
python examples/helloworld/trpc_main.py

# 终端 2：执行客户端调用（输出 hello: hello）
python examples/helloworld/test_greeter.py
```

## 相关文档

- [快速上手][quick start]
- [基础教程][basics]
- [框架配置][framework conf]
- [服务端开发向导][server]
- [客户端开发向导][client]
- [文档目录][docs]

## 相关示例

详细说明索引见[示例总目录][examples]。

- [helloworld][helloworld]：一元 Greeter 服务，默认监听 `127.0.0.1:8000`，另含自定义协议 UDP `8090` 示例
- [流式服务][hellostream]：涵盖一元、客户端流、服务端流、双向流，监听 TCP `8000`–`8040`
- [HTTP SSE][hellosse]：`GET /sse` 服务端事件推送，提供五种进程布局，监听 `8080`–`8084`
- [hellotrt][hellotrt]：Tensor 回显与 `normal_task` 任务调度，默认监听 `127.0.0.1:8000`
- [HTTP][http]：HTTP RPC 与 `@route` 自定义路由，监听 `9002` / `9003`

## 相关插件

框架内置插件的开发方式请参阅开发指南：

- [插件开发基础][plugin]
- [协议插件][protocol]
- [配置插件][config]
- [任务插件][task]
- [日志插件][log]
- [监控插件][metrics]
- [数据库插件][database]

更多对接外部治理组件的插件请参阅 [tRPC 生态仓库][ecosystem]。

## 如何贡献

我们非常欢迎社区贡献！如果您有兴趣参与建设，请查阅[贡献指南][contributing]并浏览 [Issues][issues] 中未分配的任务。认领一个任务，让我们一起把 tRPC-Python 做得更好。

### **贡献方式**

- **报告 Bug 或提出建议** - 通过 [Issues][issues] 反馈问题或建议新特性
- **改进文档** - 完善教程、示例与 API 说明，帮助更多开发者快速上手
- **提交 PR** - 参与 Bug 修复、新功能开发、性能优化或补充测试用例
- **扩展插件与分享用例** - 开发生态插件或分享您的 RPC 服务实践经验

### **快速贡献设置**

```bash
# Fork 并克隆仓库
git clone https://github.com/YOUR_USERNAME/trpc-python.git
cd trpc-python

# 安装开发与测试依赖，然后运行测试
pip install -e ".[dev,fast,snappy]"
pip install -r requirements-test.txt
pytest

# 进行您的更改并提交 PR！
```

**请阅读** [CONTRIBUTING.zh_CN.md][contributing] 了解详细贡献流程和编码规范。  
**请遵循** [CODE-OF-CONDUCT.zh_CN.md][code-of-conduct] 共同维护友好、尊重、包容的开源社区。

[Python]: https://www.python.org
[tRPC]: https://github.com/trpc-group/trpc
[trpc-cmdline]: https://github.com/trpc-group/trpc-cmdline
[docs]: /docs/README.zh_CN.md
[quick start]: /docs/quick_start.zh_CN.md
[basics]: /docs/basics_tutorial.zh_CN.md
[framework conf]: /docs/user_guide/framework_conf.zh_CN.md
[server]: /docs/user_guide/server/overview.zh_CN.md
[client]: /docs/user_guide/client/overview.zh_CN.md
[architecture]: /docs/developer_guide/architecture.zh_CN.md
[plugin]: /docs/developer_guide/develop_plugins/plugin.zh_CN.md
[filter]: /docs/developer_guide/develop_plugins/filter.zh_CN.md
[protocol]: /docs/developer_guide/develop_plugins/protocol.zh_CN.md
[config]: /docs/developer_guide/develop_plugins/config.zh_CN.md
[task]: /docs/developer_guide/develop_plugins/task.zh_CN.md
[log]: /docs/developer_guide/develop_plugins/log.zh_CN.md
[metrics]: /docs/developer_guide/develop_plugins/metrics.zh_CN.md
[database]: /docs/developer_guide/develop_plugins/database.zh_CN.md
[naming]: /docs/developer_guide/develop_plugins/naming.zh_CN.md
[ecosystem]: https://github.com/orgs/trpc-ecosystem/repositories
[contributing]: /CONTRIBUTING.zh_CN.md
[code-of-conduct]: /CODE-OF-CONDUCT.zh_CN.md
[issues]: https://github.com/trpc-group/trpc-python/issues
[examples]: /examples/README.zh_CN.md
[helloworld]: /examples/helloworld/README.zh_CN.md
[hellostream]: /examples/hellostream/README.zh_CN.md
[hellosse]: /examples/hellosse/README.zh_CN.md
[hellotrt]: /examples/hellotrt/README.zh_CN.md
[http]: /examples/http/README.zh_CN.md
[helloworld-features]: /examples/README.zh_CN.md#helloworld-features
[hellostream-features]: /examples/README.zh_CN.md#hellostream-features
