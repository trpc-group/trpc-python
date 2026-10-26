English | [中文](config.zh_CN.md)

# How to Write a Config Plugin

A config plugin reads a piece of configuration into a Python object. The extension points are the data source and the decoder. Business code joins the two with `ConfigLoader`. The sections below use the framework's built-in `file` and `yaml`.

A plugin registers through `@register_plugin` at import time. `import trpc_sdk.config` registers `file`, `yaml`, `json`, and `noop`. Your own plugin module must be imported before the loader is called.

## Two Load Paths

The framework startup file and a business configuration file do not take the same path.

When `TrpcConfig.load` reads `trpc_python.yaml`, the data source `file` and the decoder `yaml` are hard-coded. Switching to another data source or decoder does not apply to this startup file. The `plugins` section in the file is placed as a whole into `get_plugins_config()`. The framework does not create config plugins by plugin type.

Business configuration uses `ConfigLoader`: first call `provider.load(path)` to get bytes, then hand them to `decoder.decode`. If `load` is a coroutine, the loader awaits it automatically.

## Decoder

A decoder turns bytes into a dict. The registration type is `PluginType.CONFIG_DECODER`. The built-in registered names are `yaml`, `json`, and `noop`. What `with_codec` takes is that registered name:

```python
from trpc_sdk.config import with_codec

with_codec("yaml")
```

`noop` returns the bytes unchanged. That result has no interface for reading by key, so `loader.get` cannot be used with it.

`code` in the decoder options is the text encoding, default `utf-8`. That is the field the framework passes when it reads the startup file.

A custom decoder implements `decode` and `set_options`. The built-in `yaml` and `json` already cover common formats. The snippet below only shows how to register:

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

Use it as `with_codec("my_decoder")`.

## Data Source

A data source returns raw bytes by path or key. The registration type is `PluginType.CONFIG_PROVIDER`. The registered name of the built-in `FileConfigProvider` is `file`. When the first argument of `load` is empty, it reads `options["file_path"]` instead.

`FileConfigProvider.hot_reload` returns immediately. The local-file plugin has no hot reload.

A custom data source implements `load`, `set_options`, and `hot_reload`. `ConfigLoader.load` passes only the path, so the content must be reachable by that path. The contract of `hot_reload` is: after the content changes, call the callback with the new raw bytes. `ConfigLoader.hot_reload` decodes again and replaces `config_data`. When hot reload is not needed yet, leave this method empty.

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
        # When hot reload is needed, call callback with the new bytes after the content changes.
        return

    def set_options(self, options: Dict) -> None:
        self._options = options
```

To read a local file, replace `"memory"` with `"file"`. `FileConfigProvider` is the class for that registered name.

## Loading from Business Code

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

`sync_load` is for synchronous code. When you are already inside an event loop, use `await ConfigLoaderFactory.load(...)`. Call `get` after `load` finishes. When the key does not exist, it returns the second argument.

`BaseConfig` is the base class of the framework configuration parser `TrpcConfigParser`. A business plugin only needs to implement a data source or a decoder.
