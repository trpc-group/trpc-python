English | [中文](naming.zh_CN.md)

# How to Write a Naming Plugin

A naming service resolves the target service name to one concrete network node (`Node`) when the client makes a call.

Naming plugins register through `@register_plugin` when their module is imported. `import trpc_sdk.naming` registers the built-in plugins. Import a custom plugin's module from the service entry so the decorator runs.

The client selects a node in one of two modes: **one `Selector` plugin chosen by `target`**, or **no `target`, using the default `TrpcSelector` chain (discovery, routing, load balancing, and circuit breaking)**. The two modes are described below.

## Choosing a Selector with `target` (`NAMING_SELECTOR`)

The client selects a node during `select_node`. When `target` is set, it must be `scheme://endpoint`:

- `scheme` is the registered name of a `PluginType.NAMING_SELECTOR` plugin.
- `endpoint` is the string after `://`. It is passed through unchanged to `selector.select(endpoint, opt)`.
- If `target` has no `://`, the framework raises `ClientRouteError`.

> When `target` is omitted, the client constructs `TrpcSelector` directly and does not look up a plugin named `trpc` in the plugin table. `TrpcSelector` itself is not registered with `@register_plugin`, so `target: trpc://...` fails because the plugin cannot be found.

### Built-in Selectors

| scheme | Plugin | Behavior |
| --- | --- | --- |
| `ip`, `http`, `https` | `IpSelector` | Parse `endpoint` as `host:port`, with no DNS lookup. Several addresses separated by commas are accepted, and one is picked at random. |
| `dns` | `DNSSelector` | Parse `endpoint` as `host:port` (comma-separated addresses, one picked at random), then resolve the host with DNS. |
| `dsn` | `DSNSelector` | Write the whole string after `://` into `Node.address`. When `target` is `dsn://user:passwd@tcp(ip:port)/db`, `Node.address` is `user:passwd@tcp(ip:port)/db`. |

> Once a client service has `target`, `discovery`, `loadbalance`, and `circuitbreaker` on that service are ignored. Those three apply only when `target` is omitted and the call goes through the `TrpcSelector` chain.

### Implementing a Custom Selector

The selector below is a `StaticSelector` backed by a static dictionary. It returns a fixed `(ip, port)` for the service name it receives. `report` does nothing. The built-in `IpSelector.report` is empty as well.

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

A few implementation details:

- **How `Node` fills its address**: When `Node` is constructed with both `ip` and `port`, initialization fills `node.address` as `"ip:port"`. To leave the address unsplit, as `DSNSelector` does, pass only `address`.
- **Arguments to `report`**: `cost` is the call duration. `code` is the result code of this call. On success the framework passes `0`. On failure it passes the error code.
- **Configuration**: The framework does not decode the YAML `plugins` section into a naming plugin. After the service entry imports the module above, the client only needs `target` in YAML:

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: static://service1
```

`service1` after `://` is passed to `StaticSelector.select` as `service_name`, and the call is routed to `127.0.0.1:8000`.

## The `TrpcSelector` Chain When `target` Is Omitted

When the client configuration has no `target`, the framework uses the configured client service name (YAML `name`) as the target. `TrpcSelector.select` then picks a node in this order:

1. **Resolve each plugin name.** Start from the process-wide default. If this call's `SelectorOption` sets an override, that override wins.
2. **Try `discovery.select_node`.** Call the discovery plugin's `select_node` first. The built-in `IpDiscovery.select_node` always returns `None`, so the later steps still run. If a custom discovery returns a `Node` here, listing, route filtering, and load balancing are skipped.
3. **Run the standard pipeline.** Call `discovery.list` for the candidate nodes, then `service_router.filter`, then `loadbalance.select` to pick one node.
4. **Attach the circuit breaker.** Store the chosen circuit-breaker instance in `node.metadata["circuitbreaker"]`. When the RPC ends, `TrpcSelector.report` `await`s that circuit breaker's `report`.

### Default Sub-plugins and Overrides

The four roles in the chain use these default plugins:

| Role | Plugin type (`PluginType`) | Default registered name | Built-in class |
| --- | --- | --- | --- |
| discovery | `NAMING_DISCOVERY` | `IPDiscovery` | `IpDiscovery` |
| load balance | `NAMING_LOADBALANCE` | `RandomLB` | `RandomLoadBalance` |
| circuit breaker | `NAMING_CIRCUIT_BREAKER` | `NoopCB` | `NoopCircuitBreaker` |
| service router | `NAMING_SERVICE_ROUTER` | `NoopSR` | `NoopServiceRouter` |

> The built-in `IpDiscovery.list` treats the service name as `host:port` and resolves it with DNS, ignoring `namespace`. To look up a node list by a logical service name, replace it with a custom discovery plugin.

Change the default sub-plugins for every client in the process from code:

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

A single client service can override `discovery`, `loadbalance`, and `circuitbreaker` in YAML. Client YAML has no `servicerouter` field. Set the service router with `set_default_servicerouter`, or with `Options.set_service_router` on the call:

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      discovery: static_discovery
      loadbalance: RandomLB
      circuitbreaker: NoopCB
      namespace: Development
```

### Contracts for the Sub-plugins

A custom `BaseDiscovery` looks like this:

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

The other three plugins in the chain:

- **Load balance (`BaseLoadBalance`)**: Implement `select(service_name, node_list, namespace)`. The built-in `RandomLoadBalance` picks one node uniformly at random from a non-empty list. An empty list fails immediately.
- **Service router (`BaseServiceRouter`)**: Implement `filter(service_name, nodes, opt)`. The built-in `NoopServiceRouter` returns the incoming list unchanged. To drop some nodes, return a filtered new list, or the original list after you change it. An empty list means there is no usable node, and the later load balance fails.
- **Circuit breaker (`BaseCircuitBreaker`)**: Implement `available` and `report`. The current `TrpcSelector` selection path does not call `available`. After the call, `TrpcSelector.report` forwards the node, the cost, and the result code to the circuit breaker's `report`. The built-in `NoopCircuitBreaker.report` is empty. A custom circuit breaker records state there.

## Appendix: ResolveSelector

The framework also provides `ResolveSelector`, which is not registered in the plugin table. It extracts a service name from a DSN string, asks another selector that is already instantiated to resolve an address, and splices that address back into the original DSN string.
