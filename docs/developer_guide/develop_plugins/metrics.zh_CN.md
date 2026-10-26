# 怎么开发监控插件

tRPC-Python 在生态系统中提供 [Prometheus](https://github.com/trpc-ecosystem/python-metrics-prometheus)，并且提供了抽象基类 [`BaseMetrics`](../../../trpc_sdk/abc/_metrics.py)（注册类型为 `PluginType.METRICS`）供业务或第三方插件扩展。

需要注意以下两点机制：
1. **不自动解析 YAML 配置**：框架不会读取 `trpc_python.yaml` 中的 `plugins.metrics` 段。仅在配置文件里编写 `metrics` 配置不会自动实例化插件，也不会将其设为默认监控实现。
2. **门面转发与激活**：监控上报的入口位于 [`trpc_sdk.telemetry`](../../../trpc_sdk/telemetry/_metrics.py)（推荐直接从包顶层导入：`from trpc_sdk.telemetry import get_metrics, report_attr, set_default_metrics`）。在显式调用 `set_default_metrics` 绑定默认实例之前，所有 `report_*` 函数都会直接返回 `False`。

## 门面函数与框架调用点

`trpc_sdk.telemetry` 提供了四个上报函数。当已设置默认监控实例时，它们会将调用转发给该实例；否则直接返回 `False`：

- `report_active(active_data, call_result)`：主调侧（客户端）调用监控上报。
- `report_passive(passive_data, call_result)`：被调侧（服务端）调用监控上报。
- `report_attr(key, value, policy=StatPolicy.SUM)`：属性指标上报。
- `report_custom(data)`：自定义多维监控数据上报。

**框架内部调用情况**：
- 目前框架内部（如 codec 收到非法魔数、transport 发生连接错误、runtime 检测到子进程退出等场景）已经接入了 `report_attr`。一旦业务调用了 `set_default_metrics`，这些内部指标就会自动进入你的插件。
- `report_active`、`report_passive` 和 `report_custom` 在框架内部暂无默认调用点，业务代码或自定义拦截器可以根据需要主动调用门面函数进行上报。

## 核心数据类型

监控相关的数据结构定义在 [`trpc_sdk/abc/_metrics.py`](../../../trpc_sdk/abc/_metrics.py) 中：

- **`ActiveData`**：主调侧维度信息，包含 `active_service`、`passive_service`、`passive_ip`、`ret_code` 等字段。
- **`PassiveData`**：被调侧维度信息，包含 `active_service`、`passive_service`、`passive_interface`、`ret_code` 等字段。
- **`CallResult`**：调用结果，其中 `status` 为 `ReportStatus` 枚举（`SUCCESS = 0`、`EXCEPTION = 1`、`TIMEOUT = 2`），`time` 为耗时（单位：毫秒）。
- **`StatPolicy`**：统计策略字符串枚举，支持 `SUM`、`AVG`、`MAX`、`MIN`、`SET`、`HISTOGRAM`。调用 `report_attr` 不传 `policy` 时默认为 `StatPolicy.SUM`。
- **`CustomData`**：自定义监控数据，包含 `name`、`dimensions`、`stat_values`、`tags`。其中 `stat_values` 的元素为 `StatValue`（包含 `value`、`count`、`policy`）。

四个 `report_*` 抽象方法的返回值类型均为 `bool`，成功接收样本时返回 `True`。

## 实现与接入示例

下面实现一个最小化的内存监控插件，将上报的样本保存在内存列表中，不依赖外部监控系统。注意 `BaseMetrics` 的四个抽象方法必须全部实现，否则无法实例化：

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

### 接入时的注意事项

1. **通过 `get_metrics` 获取单例**：`@register_plugin` 仅将类注册到插件表中。首次调用 `get_metrics("memory")`（等价于 `get_plugin(PluginType.METRICS, "memory")`）时，插件管理器才会构造实例并依次调用其 `init()` 和 `start()`。请直接将该返回值传给 `set_default_metrics(metrics)`；如果自己额外执行 `MemoryMetrics()` 传入，会导致门面持有的对象与插件表中的单例不一致。
2. **生效时机与多进程说明**：在调用 `install()` 前，需确保定义插件的模块已被 `import`。由于每个 Worker 子进程各自维护独立的默认监控实例，需要在实际产生上报的进程中（通常在开始处理请求前）执行 `install()`。无需在 YAML 中编写 `plugins.metrics`。
3. **验证上报**：执行 `install()` 后，调用 `report_attr("demo", 1)` 即可在 `metrics.attrs` 中看到记录；业务侧也可以构造 `ActiveData` 和 `CallResult` 并调用 `report_active` 完成主调上报。
