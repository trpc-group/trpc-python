# 怎么开发名字服务插件

名字服务（Naming）负责在客户端发起调用时，将目标服务名解析并路由到一个具体的网络节点（`Node`）。

名字服务插件同样通过 `@register_plugin` 在模块导入时完成注册。执行 `import trpc_sdk.naming` 会自动注册框架内置的插件；自定义插件所在的模块则需要在服务入口处被 `import`，以确保装饰器执行。

客户端选点有两种工作模式：**通过 `target` 指定单一 `Selector` 插件**，或**不写 `target`、走默认的 `TrpcSelector` 组合链（服务发现 + 路由 + 负载均衡 + 熔断）**。下面分别介绍这两种方式。

## 方式一：通过 `target` 选择 Selector（`NAMING_SELECTOR`）

客户端在 `select_node` 阶段进行选点。当配置了 `target` 时，其格式必须为 `scheme://endpoint`：
- `scheme`：对应 `PluginType.NAMING_SELECTOR` 插件的注册名。
- `endpoint`：`://` 后面的字符串，会作为参数原样传给 `selector.select(endpoint, opt)`。
- 如果 `target` 中缺少 `://`，框架会抛出 `ClientRouteError`。

> 注意：当未配置 `target` 时，客户端会直接实例化 `TrpcSelector`，而不会去插件表里查找名为 `trpc` 的插件。由于 `TrpcSelector` 本身没有使用 `@register_plugin` 注册，若在配置中写 `target: trpc://...` 会报找不到插件。

### 内置 Selector 一览

| scheme | 插件类 | 行为说明 |
| --- | --- | --- |
| `ip`、`http`、`https` | `IpSelector` | 将 `endpoint` 按 `host:port` 解析，不进行 DNS 查询（支持逗号分隔多个地址并随机选取一个）。 |
| `dns` | `DNSSelector` | 将 `endpoint` 按 `host:port` 解析（支持逗号分隔多个地址并随机选取），随后对域名执行 DNS 解析。 |
| `dsn` | `DSNSelector` | 将 `://` 后面的完整字符串直接写入 `Node.address`。例如 `target` 为 `dsn://user:passwd@tcp(ip:port)/db` 时，`Node.address` 即为 `user:passwd@tcp(ip:port)/db`。 |

> **优先级说明**：一旦为某个客户端服务配置了 `target`，该服务下的 `discovery`、`loadbalance`、`circuitbreaker` 配置将被忽略——这三项仅在未配置 `target`、走 `TrpcSelector` 组合链时生效。

### 实现一个自定义 Selector

下面实现一个基于静态字典的 `StaticSelector`：根据传入的服务名返回固定的 `(ip, port)`，`report` 方法不做任何处理（内置的 `IpSelector.report` 也是空实现）。

```python
from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.NAMING_SELECTOR, "static")
class StaticSelector(BaseSelector):
    """Map a service name to one fixed host:port."""

    _nodes = {
        "service1": ("127.0.0.1", 8000),
        "service2": ("127.0.0.1", 8001),
    }

    async def select(self, service_name: str, opt: SelectorOption) -> Node:
        ip, port = self._nodes[service_name]
        return Node(service_name=service_name, ip=ip, port=port)

    async def report(self, node: Node, cost: int, code: int) -> bool:
        return True
```

几点实现细节：
- **`Node` 地址拼接**：当构造 `Node` 时同时传入了 `ip` 和 `port`，初始化逻辑会自动将 `node.address` 补全为 `"ip:port"`。如果像 `DSNSelector` 那样不拆分 IP 和端口，也可以只传 `address`。
- **`report` 回调参数**：`cost` 为调用耗时，`code` 为本次调用的结果码（调用成功时框架传入 `0`，失败时传入对应的错误码）。
- **配置方式**：框架不会自动将 YAML 的 `plugins` 段解析到名字服务插件中。在服务入口 `import` 上述模块后，客户端只需在 YAML 中配置 `target`：

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: static://service1
```

此时 `://` 后面的 `service1` 会作为 `service_name` 传给 `StaticSelector.select`，最终将请求路由到 `127.0.0.1:8000`。

## 方式二：不配置 `target` 时的 `TrpcSelector` 组合链

当客户端配置中未指定 `target` 时，框架使用客户端配置的服务名（即 YAML 中的 `name`）作为目标标识，并交由 `TrpcSelector.select` 按以下顺序完成选点：

1. **确定各环节插件名**：先取进程级的默认插件名，若本次调用的 `SelectorOption` 中指定了覆盖项，则以 `SelectorOption` 为准。
2. **尝试 `discovery.select_node`**：优先调用服务发现插件的 `select_node`。内置的 `IpDiscovery.select_node` 固定返回 `None`，从而继续执行后续步骤；如果自定义 Discovery 在此直接返回了 `Node`，则会跳过后面的列表拉取、路由过滤和负载均衡步骤。
3. **执行标准流水线**：依次调用 `discovery.list`（获取候选节点列表）→ `service_router.filter`（服务路由过滤）→ `loadbalance.select`（负载均衡选出一个节点）。
4. **绑定熔断器**：将当前选用的熔断器实例存入 `node.metadata["circuitbreaker"]`。当 RPC 调用结束触发 `TrpcSelector.report` 时，会 `await` 该熔断器的 `report` 方法。

### 默认子插件与配置覆盖

组合链中四个角色的默认插件如下：

| 角色 | 插件类型（`PluginType`） | 默认注册名 | 内置实现类 |
| --- | --- | --- | --- |
| 服务发现（discovery） | `NAMING_DISCOVERY` | `IPDiscovery` | `IpDiscovery` |
| 负载均衡（load balance） | `NAMING_LOADBALANCE` | `RandomLB` | `RandomLoadBalance` |
| 熔断器（circuit breaker） | `NAMING_CIRCUIT_BREAKER` | `NoopCB` | `NoopCircuitBreaker` |
| 服务路由（service router） | `NAMING_SERVICE_ROUTER` | `NoopSR` | `NoopServiceRouter` |

> 内置的 `IpDiscovery.list` 会直接把服务名当成 `host:port` 进行 DNS 解析（忽略 `namespace`）。如果希望通过逻辑服务名查询节点列表，需要替换为自定义的 Discovery 插件。

你可以通过代码修改进程内所有客户端的默认子插件：

```python
from trpc_sdk.naming import set_default_circuitbreaker
from trpc_sdk.naming import set_default_discovery
from trpc_sdk.naming import set_default_loadbalance
from trpc_sdk.naming import set_default_servicerouter

set_default_discovery("static_discovery")
set_default_loadbalance("RandomLB")
set_default_circuitbreaker("NoopCB")
set_default_servicerouter("NoopSR")
```

也可以在 YAML 中针对单个客户端服务覆盖 `discovery`、`loadbalance` 和 `circuitbreaker`（注：YAML 客户端配置中没有 `servicerouter` 字段，服务路由需通过 `set_default_servicerouter` 或在调用时使用 `Options.set_service_router` 指定）：

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      discovery: static_discovery
      loadbalance: RandomLB
      circuitbreaker: NoopCB
      namespace: Development
```

### 各子插件的实现契约

下面展示如何编写自定义的 `BaseDiscovery` 插件：

```python
from typing import Any
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseDiscovery
from trpc_sdk.abc import Node
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.NAMING_DISCOVERY, "static_discovery")
class StaticDiscovery(BaseDiscovery):
    """Return one fixed node for a known service name."""

    _nodes = {
        "trpc.test.helloworld.Greeter": ("127.0.0.1", 8000),
    }

    async def list(self, service_name: str, namespace: str) -> List[Node]:
        ip, port = self._nodes[service_name]
        return [Node(service_name=service_name, ip=ip, port=port)]

    async def select_node(self, service_name: str, opt: Any) -> Optional[Node]:
        return None
```

组合链中其余三类插件的实现要点：
- **负载均衡（`BaseLoadBalance`）**：实现 `select(service_name, node_list, namespace)`。内置 `RandomLoadBalance` 会从非空列表中随机均匀挑选一个节点；若列表为空则直接报错。
- **服务路由（`BaseServiceRouter`）**：实现 `filter(service_name, nodes, opt)`。内置 `NoopServiceRouter` 原样返回传入的节点列表。若需剔除部分节点，可返回过滤后的新列表（或修改后的原列表）；若返回空列表，则代表无可用节点，后续负载均衡将报错失败。
- **熔断器（`BaseCircuitBreaker`）**：实现 `available` 和 `report` 方法。目前 `TrpcSelector` 选点链路不会主动调用 `available`；调用结束后，`TrpcSelector.report` 会将节点、耗时和结果码转发给熔断器的 `report` 方法（内置 `NoopCircuitBreaker.report` 为空实现），自定义熔断器可在此记录状态。

## 附：ResolveSelector

框架还提供了一个辅助类 `ResolveSelector`（未自动注册到插件表）。它用于从一段 DSN 字符串中提取出服务名，委托给另一个已实例化的 Selector 解析出具体地址后，再将地址拼回原 DSN 字符串中。
