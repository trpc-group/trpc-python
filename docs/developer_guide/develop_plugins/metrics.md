English | [中文](metrics.zh_CN.md)

# How to Write a Metrics Plugin

tRPC-Python provides [Prometheus](https://github.com/trpc-ecosystem/python-metrics-prometheus) in the ecosystem, and the abstract base class [`BaseMetrics`](../../../trpc_sdk/abc/_metrics.py) (registration type `PluginType.METRICS`) for business code or third-party plugins to extend.

Two mechanisms to keep in mind:

1. **YAML is not parsed automatically.** The framework does not read the `plugins.metrics` section of `trpc_python.yaml`. Writing a `metrics` section does not instantiate the plugin, and it does not set it as the default implementation.
2. **Facade forwarding and activation.** The reporting entry is [`trpc_sdk.telemetry`](../../../trpc_sdk/telemetry/_metrics.py). Import from the package: `from trpc_sdk.telemetry import get_metrics, report_attr, set_default_metrics`. Until `set_default_metrics` binds a default instance, every `report_*` function returns `False`.

## Facade Functions and Framework Call Sites

`trpc_sdk.telemetry` provides four report functions. When a default metrics instance is set, they forward the call to that instance. Otherwise they return `False`:

- `report_active(active_data, call_result)`: caller-side (client) metrics.
- `report_passive(passive_data, call_result)`: callee-side (server) metrics.
- `report_attr(key, value, policy=StatPolicy.SUM)`: attribute metrics.
- `report_custom(data)`: custom multi-dimensional metrics.

**What the framework calls:**

- The framework already calls `report_attr`, for example an illegal magic number in the codec, a connection error in transport, and a child process exit in the runtime. After business code calls `set_default_metrics`, those internal metrics enter the plugin.
- `report_active`, `report_passive`, and `report_custom` have no default call sites inside the framework. Business code or a custom filter can call the facade when it needs to report.

## Data Types

The definitions are in [`trpc_sdk/abc/_metrics.py`](../../../trpc_sdk/abc/_metrics.py).

- **`ActiveData`**: caller-side dimensions, including `active_service`, `passive_service`, `passive_ip`, and `ret_code`.
- **`PassiveData`**: callee-side dimensions, including `active_service`, `passive_service`, `passive_interface`, and `ret_code`.
- **`CallResult`**: the call result. `status` is `ReportStatus`: `SUCCESS = 0`, `EXCEPTION = 1`, `TIMEOUT = 2`. `time` is milliseconds.
- **`StatPolicy`**: a string enum: `SUM`, `AVG`, `MAX`, `MIN`, `SET`, `HISTOGRAM`. When `report_attr` omits `policy`, the policy is `StatPolicy.SUM`.
- **`CustomData`**: custom metrics data, with `name`, `dimensions`, `stat_values`, and `tags`. An element of `stat_values` is a `StatValue` (`value`, `count`, `policy`).

All four `report_*` methods return `bool`. Accepting the sample returns `True`.

## Implementation and Integration

The plugin below keeps reported samples in memory and does not depend on an external metrics system. All four abstract methods of `BaseMetrics` must be implemented, or the class cannot be instantiated:

```python
from trpc_sdk.abc import ActiveData
from trpc_sdk.abc import BaseMetrics
from trpc_sdk.abc import CallResult
from trpc_sdk.abc import CustomData
from trpc_sdk.abc import PassiveData
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import StatPolicy
from trpc_sdk.plugin import register_plugin
from trpc_sdk.telemetry import get_metrics
from trpc_sdk.telemetry import set_default_metrics


@register_plugin(PluginType.METRICS, "memory")
class MemoryMetrics(BaseMetrics):
    def __init__(self):
        super().__init__()
        self.active = []
        self.passive = []
        self.attrs = []
        self.custom = []

    def report_active(self, active_data: ActiveData, call_result: CallResult) -> bool:
        self.active.append((active_data, call_result))
        return True

    def report_passive(self, passive_data: PassiveData, call_result: CallResult) -> bool:
        self.passive.append((passive_data, call_result))
        return True

    def report_attr(self, key: str, value: float, policy: StatPolicy) -> bool:
        self.attrs.append((key, value, policy))
        return True

    def report_custom(self, data: CustomData) -> bool:
        self.custom.append(data)
        return True


def install() -> MemoryMetrics:
    metrics = get_metrics("memory")
    set_default_metrics(metrics)
    return metrics
```

### Notes on Integration

1. **Take the singleton from `get_metrics`.** `@register_plugin` only puts the class into the plugin table. The first call to `get_metrics("memory")` (the same as `get_plugin(PluginType.METRICS, "memory")`) constructs the instance and runs `init()` and `start()`. Pass that return value to `set_default_metrics(metrics)`. Constructing another `MemoryMetrics()` and passing it in gives the facade an object that is not the singleton in the plugin table.
2. **When it takes effect, and multiple processes.** Import the module that defines the plugin before calling `install()`. Each worker process keeps its own default metrics instance, so call `install()` in the process that actually reports, usually before it starts handling requests. Do not write a `plugins.metrics` section in YAML.
3. **Check that reports land.** After `install()`, `report_attr("demo", 1)` shows up in `metrics.attrs`. Business code can also build `ActiveData` and `CallResult` and call `report_active`.
