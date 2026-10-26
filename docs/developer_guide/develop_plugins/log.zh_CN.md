# 怎么开发日志 Writer 插件

在 tRPC-Python 中，框架的日志后端固定为 `loguru`。配置文件中的 `plugins.log` 用于定义各个 logger 的输出目标（sink）列表。如果需要自定义日志输出方式，可以通过扩展点 `PluginType.LOG_WRITER` 实现 Writer 插件（框架内置了 `console` 和 `file` 两种实现）。

插件通过 `@register_plugin` 装饰器在模块导入时完成注册：当执行 `import trpc_sdk.log` 时，包初始化逻辑会加载 [`_loguru.py`](../../../trpc_sdk/log/_loguru.py)，进而导入 [`_writer.py`](../../../trpc_sdk/log/_writer.py)，触发内置 `console` 和 `file` 插件的注册。因此，自定义的 Writer 模块也需要在服务启动入口处被 `import`，确保装饰器在日志初始化前执行。

## 日志后端机制

在 `PluginType.LOG` 类型下有两个内置插件，但运行时工厂固定使用名为 `loguru` 的后端；扩展日志输出通道时，请注册 `PluginType.LOG_WRITER` 而非 `PluginType.LOG`：

- [`DefaultLoggerInitPlugin`](../../../trpc_sdk/log/_logger.py)（注册名 `"default"`）：在 `init()` 阶段调用 `get_log_config()` 加载配置，仅执行一次。
- [`LoguruFactoryPlugin`](../../../trpc_sdk/log/_loguru.py)（注册名 `"loguru"`，即 `DEFAULT_LOG_BACKEND_NAME`）：通过 `create_logger` 根据配置的输出列表创建具体的 logger 实例。

## 配置字段说明

`get_log_config()` 会读取配置文件中的 `plugins.log` 节点，其结构为「logger 名称 → 输出配置列表」的映射。默认 logger 的名称为 `"default"`（`DEFAULT_LOG_NAME`）——注意不要将 YAML 中的默认 logger 名 `"default"` 与上面负责初始化配置的 `DefaultLoggerInitPlugin` 混淆。

列表中的每一项输出配置都对应一个 [`OutputConfig`](../../../trpc_sdk/abc/_log.py) 实例（若包含未知字段，构造时会抛出 `TypeError`）。各字段含义如下：

- `writer`：`PluginType.LOG_WRITER` 插件的注册名（如 `console`、`file`）。
- `level`：日志级别，支持 `trace`、`debug`、`info`、`warning`、`error`、`fatal`。其中 `trace` 和 `debug` 均映射为标准库 `logging.DEBUG`；未配置时默认为 `debug`。
- `formatter`：格式化方式。空字符串或 `"json"` 使用文本布局；`"noop"` 和 `"full_json"` 使用 [`_format.py`](../../../trpc_sdk/log/_format.py) 中对应的 Formatter 类；其他未知名称会回退为 `"json"`。
- `format_config`：对应 [`FormatConfig`](../../../trpc_sdk/abc/_log.py)。Writer 实际读取其中的 `fmt` 和 `time_fmt` 字段。能够直接转换为 `loguru` 原生模板的百分号字段仅包括 `asctime`、`levelname`、`filename`、`lineno`、`process`、`message` 以及 `%(msecs)03d`；若包含其他字段或格式修饰符，则会回退为函数回调形式的 formatter。
- `writer_config`：对应 [`WriterConfig`](../../../trpc_sdk/abc/_log.py)。内置的 `FileWriter` 仅读取其中的 `filename`、`log_path`、`max_size`、`max_backups` 和 `compress`：
  - 文件路径由 `log_path + "/" + filename` 拼接而成（若 `log_path` 为空则直接使用 `filename`）。
  - `max_size` 以 MB 为单位，乘以 `1000000` 后传给 `loguru` 的 `rotation` 参数（为 `0` 时不轮转）。
  - `max_backups` 传给 `loguru` 的 `retention` 参数（为 `0` 时不限制保留份数）。
  - `compress: true` 时启用 `gz` 压缩。
  - `WriterConfig` 上的 `max_age`、`roll_type`、`time_unit` 字段目前未被 `FileWriter` 读取。
- `caller_skip`：定义在 `OutputConfig` 上，默认值为 `2`，当前内置 Writer 未读取该字段。
- `capture_third_party`：是否捕获第三方库日志，必须填写 YAML 布尔值 `true` 或 `false`（注意：带引号的 `"false"` 是非空字符串，在布尔判断时会被视为 `True`）。
- `third_party_level`：第三方库日志级别，缺省时沿用当前输出项的 `level`。

## Writer 插件的调用流程

在创建日志绑定时，[`_LoguruSinkManager.create_binding`](../../../trpc_sdk/log/_loguru.py) 会根据 `output_config.writer` 的名称查找已注册的 `PluginType.LOG_WRITER` 插件：

1. 若未找到同名插件，打印 `cannot find writer plugin` 并跳过该条输出配置。
2. 若找到的插件实例继承自 `LoggerWriterPlugin`，则调用其 `create_log_sink(output_config)` 方法。
3. `create_log_sink` 内部会调用子类实现的 `_get_sink_kwargs(output_config)`；若返回 `None`，同样跳过该条输出。

因此，开发自定义 Writer 只需继承 `LoggerWriterPlugin` 并实现 `_get_sink_kwargs` 方法即可。注意 `LoggerWriterPlugin` 未在 `trpc_sdk.log` 顶层导出，需从 `trpc_sdk.log._writer` 导入。

## 实现与使用示例

我们可以直接参考内置 [`ConsoleWriter`](../../../trpc_sdk/log/_writer.py) 的实现。基类的 `_common_sink_kwargs(output_config)` 已经组装好了 `level`、`format` 以及 `enqueue=True`，子类只需在此基础上补充 `sink` 等 `loguru.logger.add` 所需的参数：

```python
import sys

from trpc_sdk.abc import PluginType
from trpc_sdk.log._writer import LoggerWriterPlugin
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.LOG_WRITER, "console")
class ConsoleWriter(LoggerWriterPlugin):
    def _get_sink_kwargs(self, output_config):
        sink_kwargs = self._common_sink_kwargs(output_config)
        sink_kwargs["sink"] = sys.stderr
        sink_kwargs["colorize"] = False
        return sink_kwargs
```

如果编写自己的 Writer 插件，只需将装饰器中的注册名改为自定义名称（例如 `"stderr"`），并在服务入口导入该模块。

随后在配置文件中指定 `writer` 为对应的注册名。由于同一个 logger 支持同时输出到多个目标，`default` 下配置为一个列表：

```yaml
plugins:
  log:
    default:
      - writer: console
        level: info
```

配置加载完成后，调用 `trpc_sdk.log` 的日志接口（如 `info(...)` 或 `get_logger("default")`）即可将日志写入对应的输出插件。
