English | [中文](log.zh_CN.md)

# How to Write a Log Writer Plugin

The framework log backend is fixed as loguru. `plugins.log` configures the output list of a logger. The extension point is `PluginType.LOG_WRITER`. The built-in outputs are `console` and `file`.

`import trpc_sdk.log` completes registration: package initialization enters [`_loguru.py`](../../../trpc_sdk/log/_loguru.py), that module then imports [`_writer.py`](../../../trpc_sdk/log/_writer.py), and the `console` and `file` decorators run at that moment. Your own writer module must also be imported from the service entry, before log initialization, or the decorator never runs.

## Log Backend

`PluginType.LOG` has two built-in plugins. The runtime factory always takes the one named `loguru`. To extend output, register `PluginType.LOG_WRITER`, not `PluginType.LOG`.

- [`DefaultLoggerInitPlugin`](../../../trpc_sdk/log/_logger.py), registered name `"default"`. `init()` calls `get_log_config()` and runs only once.
- [`LoguruFactoryPlugin`](../../../trpc_sdk/log/_loguru.py), registered name `"loguru"` (`DEFAULT_LOG_BACKEND_NAME`). Its `create_logger` builds a logger from the output list.

## Configuration Fields

`get_log_config()` reads `plugins.log`. The shape is "logger name → output list". `"default"` is the default logger name (`DEFAULT_LOG_NAME`). It shares a name with the init plugin above: one is the logger in yaml, and the other is the plugin that reads the configuration.

Each output corresponds to [`OutputConfig`](../../../trpc_sdk/abc/_log.py). An unknown key raises `TypeError` during construction.

- `writer`: the registered name of a `LOG_WRITER`, such as `console` or `file`.
- `level`: `trace` / `debug` / `info` / `warning` / `error` / `fatal`. Both `trace` and `debug` land on logging's `DEBUG`. The default is `debug`.
- `formatter`: an empty string or `"json"` uses the text layout. `"noop"` and `"full_json"` use the formatter classes in [`_format.py`](../../../trpc_sdk/log/_format.py). An unknown name falls back to `"json"`.
- `format_config`: [`FormatConfig`](../../../trpc_sdk/abc/_log.py). What a writer actually uses is `fmt` and `time_fmt`. The only percent fields that can be compiled into a native loguru template are `asctime`, `levelname`, `filename`, `lineno`, `process`, `message`, and `%(msecs)03d`. Other fields or modifiers fall back to the formatter callback.
- `writer_config`: [`WriterConfig`](../../../trpc_sdk/abc/_log.py). The built-in `FileWriter` reads only `filename`, `log_path`, `max_size`, `max_backups`, and `compress`:
  - The path is `log_path + "/" + filename`. When `log_path` is empty, only `filename` is used.
  - `max_size` is in megabytes. It is multiplied by `1000000` and passed to loguru's `rotation`. A value of `0` does not rotate.
  - `max_backups` is passed to loguru's `retention`. A value of `0` does not limit how many files are kept.
  - `compress: true` enables `gz` compression.
  - `max_age`, `roll_type`, and `time_unit` on `WriterConfig` are not read by `FileWriter`.
- `caller_skip` is on `OutputConfig` and defaults to `2`. Current built-in writers do not read it.
- `capture_third_party` must be the YAML boolean `true` or `false`. A quoted `"false"` is a non-empty string and is treated as true.
- When `third_party_level` is omitted, it follows `level` of this output.

## How a Writer Is Used

When a log binding is created, [`_LoguruSinkManager.create_binding`](../../../trpc_sdk/log/_loguru.py) looks up the registered `PluginType.LOG_WRITER` plugin by `output_config.writer`:

1. If no plugin with that name is found, the code prints `cannot find writer plugin` and skips this output.
2. If the instance subclasses `LoggerWriterPlugin`, it calls `create_log_sink(output_config)`.
3. `create_log_sink` calls the subclass's `_get_sink_kwargs(output_config)`. A return value of `None` skips this output as well.

A custom writer subclasses `LoggerWriterPlugin` and implements `_get_sink_kwargs`. This class is not exported from the `trpc_sdk.log` package. Import it from `trpc_sdk.log._writer`.

## Example

The built-in [`ConsoleWriter`](../../../trpc_sdk/log/_writer.py) is the template. `_common_sink_kwargs(output_config)` already puts in `level`, `format`, and `enqueue=True`. The subclass adds `sink` and the other arguments `loguru.logger.add` needs:

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

To write your own writer, change the registered name in the decorator, for example to `"stderr"`, and import that module from the service entry.

Then set `writer` to that registered name. One logger can write to several outputs, so the value under `default` is a list:

```yaml
plugins:
  log:
    default:
      - writer: console
        level: info
```

After the configuration is loaded, log calls from `trpc_sdk.log` such as `info(...)` or `get_logger("default")` write to the configured output plugins.
