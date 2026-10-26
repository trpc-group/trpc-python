# 怎么开发一个配置插件

配置插件负责把一份配置读成 Python 对象。扩展点是数据源和解码器，业务代码用 `ConfigLoader` 把二者拼起来。下面用框架自带的 `file` 和 `yaml` 说明。

插件在 import 时通过 `@register_plugin` 注册。`import trpc_sdk.config` 会注册 `file`、`yaml`、`json`、`noop`。自己的插件模块要在调用加载器之前被 import。

## 两条加载路径

框架启动文件和业务自己的配置文件走的不是同一条路。

`TrpcConfig.load` 读取 `trpc_python.yaml` 时写死了数据源 `file` 和解码器 `yaml`。换一个数据源或解码器，不会作用在这份启动文件上。文件里的 `plugins` 段会被整块放进 `get_plugins_config()`，框架不会按插件类型去创建 config 插件。

业务配置用 `ConfigLoader`：先调用 `provider.load(path)` 拿到 bytes，再交给 `decoder.decode`。`load` 如果是协程，加载器会自动 await。

## 解码器

解码器把 bytes 变成 dict，注册类型是 `PluginType.CONFIG_DECODER`。内置注册名是 `yaml`、`json`、`noop`。`with_codec` 传入的是这个注册名：

```python
from trpc_sdk.config import with_codec

with_codec("yaml")
```

`noop` 原样返回 bytes。这种结果没有按 key 取值的接口，`loader.get` 不能和它一起用。

解码器选项里的 `code` 是文本编码，默认 `utf-8`。框架读启动文件时传的就是这个字段。

自定义解码器实现 `decode` 和 `set_options`。内置的 `yaml`、`json` 已经覆盖常见格式，下面只演示注册方式：

```python
import json
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseConfigDecoder
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.CONFIG_DECODER, "my_decoder")
class MyDecoder(BaseConfigDecoder):
    """Decode JSON bytes into a dict."""

    def __init__(self):
        super().__init__()
        self._options = None

    def decode(self, config_data: bytes, options: Optional[Dict] = None) -> Dict:
        encoding = "utf-8"
        chosen = options or self._options
        if chosen and chosen.get("code"):
            encoding = chosen["code"]
        return json.loads(config_data.decode(encoding))

    def set_options(self, options: Dict) -> None:
        self._options = options
```

使用时写成 `with_codec("my_decoder")`。

## 数据源

数据源按路径或 key 返回原始 bytes，注册类型是 `PluginType.CONFIG_PROVIDER`。内置 `FileConfigProvider` 的注册名是 `file`。`load` 的第一个参数为空时，它改读 `options["file_path"]`。

`FileConfigProvider.hot_reload` 直接返回，本地文件插件没有热更新。

自定义数据源实现 `load`、`set_options` 和 `hot_reload`。`ConfigLoader.load` 调用时只传入 path，所以内容要能按这个 path 取到。`hot_reload` 的约定是：内容变化后，用新的 raw bytes 调用 callback。`ConfigLoader.hot_reload` 会再解码，并换成新的 `config_data`。暂时不需要热更新时，这个方法留空即可。

```python
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseConfigProvider
from trpc_sdk.abc import PluginType
from trpc_sdk.config import ProviderError
from trpc_sdk.plugin import register_plugin

_STORE = {
    "application.yaml": b"server:\n  name: demo\n",
}


@register_plugin(PluginType.CONFIG_PROVIDER, "memory")
class MemoryConfigProvider(BaseConfigProvider):
    """Return config bytes stored in the process."""

    def __init__(self):
        super().__init__()
        self._options = None

    def load(self, file_path: str, options: Optional[Dict] = None) -> bytes:
        if not file_path and options:
            file_path = options.get("file_path")
        data = _STORE.get(file_path)
        if data is None:
            raise ProviderError("memory", f"missing config {file_path}")
        return data

    def hot_reload(self, key: str, callback: Callable, options: Optional[Dict] = None) -> None:
        # 需要热更新时，在内容变化后用新的 bytes 调用 callback。
        return

    def set_options(self, options: Dict) -> None:
        self._options = options
```

读本地文件时把 `"memory"` 换成 `"file"`。`FileConfigProvider` 就是这个注册名对应的类。

## 业务侧加载

```python
from trpc_sdk.config import ConfigLoaderFactory
from trpc_sdk.config import with_codec
from trpc_sdk.config import with_provider

loader = ConfigLoaderFactory.sync_load(
    "application.yaml",
    with_provider("file"),
    with_codec("yaml"),
)
server = loader.get("server", {})
```

`sync_load` 供同步代码使用。已经在事件循环里时，用 `await ConfigLoaderFactory.load(...)`。`get` 要在 `load` 完成之后调用，key 不存在时返回第二个参数。

`BaseConfig` 是框架配置解析器 `TrpcConfigParser` 的基类。业务插件实现数据源或解码器即可。
