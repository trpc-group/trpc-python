English | [中文](admin.zh_CN.md)

# Admin Commands

Admin is an HTTP management service the root process starts independently of the business ports. It is used to query the framework version and the runtime configuration, inspect or change the log level at runtime, and collect `rpcz` call-chain diagnostics.

When the configuration file contains a `server.admin` section, `serve()` imports `trpc_sdk.admin` automatically. The root process binds and starts this HTTP service only after `fork`, and only when both `ip` and `port` are set and non-empty. If either is missing, Admin does not start.

```yaml
server:
  admin:
    ip: 127.0.0.1
    port: 9028
```

**How it works**:
- After each Worker child process starts, it opens an internal WebSocket to the root process (path `/_/ws`). That connection receives log-level changes issued by the root process and aggregates `rpcz` data. Task child processes do not open this connection.
- If `admin.ip` is `0.0.0.0`, a Worker rewrites the dial-back address to `127.0.0.1`.

> Call the management APIs below on the IP and port configured under `server.admin`, not on a business `server.service` port. On success, the JSON field `errorcode` is `0`.

## Built-in Commands

| HTTP method | Path | Description |
| --- | --- | --- |
| `GET` | `/cmds` | List the HTTP paths of every registered management command (internal paths that start with `/_/` are excluded) |
| `GET` | `/version` | Return the framework version `{"version": ...}`. The value comes from `trpc_sdk.version` |
| `GET` | `/cmds/config` | Return the full configuration dict parsed when the process started |
| `GET` | `/cmds/loglevel?logger=default` | Read the current level name of the given `logger`. Omitting `logger` queries `default` |
| `PUT` | `/cmds/loglevel` | Change the log level at runtime. Pass form fields `logger` and `value` (`value` is one of `trace`, `debug`, `info`, `warning`, `error`, `fatal`) |
| `GET` | `/cmds/rpcz` | Return a summary list of recent `rpcz` spans. The count is `global.rpcz.print_spans_num` |
| `GET` | `/cmds/rpcz?span_id=123` | Fetch one span by `span_id`. Returns `errorcode: -1` when the id is not a valid integer or the span is not found |
| `GET` | `/cmds/debug/language` | Return the current language `{"language": "python"}` |

Notes:
- **Change the log level at runtime**: After `PUT /cmds/loglevel`, the root process broadcasts the new level over WebSocket to every connected Worker. When `logger` is `default`, the root process also updates its own root logger.
- **Enable `rpcz`**: To use `/cmds/rpcz`, also add the filter name `rpcz` to `server.filter` or `client.filter` in the YAML. Sampling parameters are under `global.rpcz`. See [Framework Configuration](framework_conf.md) and [How to Write a Filter Plugin](../developer_guide/develop_plugins/filter.md).

Common `curl` examples:

```bash
curl http://127.0.0.1:9028/version
curl http://127.0.0.1:9028/cmds/loglevel?logger=default
curl -X PUT http://127.0.0.1:9028/cmds/loglevel -d "logger=default&value=debug"
curl http://127.0.0.1:9028/cmds/rpcz
```

## Register a Custom Command

To add a custom HTTP management API, or to push a custom command to every Worker, register the hooks before `serve()` (before the process `fork`s):

- **`@on_master_start`**: Called when the root process starts Admin. The signature is `func(routes, configer)`. It receives the root process's `aiohttp` route table and the configuration dispatcher.
- **`@on_worker_start`**: Called when each Worker starts. The signature is `func(configer)`. It receives the configuration receiver on the Worker side.

When the root process needs to sync state to every Worker:

1. On the root process, register a data-provider function with `configer.reg_cfg(key)`, then call `await configer.config({key: arg})`. That call runs the provider and pushes the return value to every Worker over WebSocket.
2. On the Worker, register an async receiver of the same name with `configer.reg_cfg(key)`, `async def handler(ws, value)`, to handle the value that arrives.

Example:

```python
from aiohttp import web

from trpc_sdk.admin import on_master_start
from trpc_sdk.admin import on_worker_start


@on_master_start
def admin_master(routes, configer):
    @routes.get("/hello")
    async def hello(request):
        msg = "Hello World!"
        await configer.config({"test": msg})
        return web.Response(text=msg)

    @configer.reg_cfg("test")
    async def provide(arg):
        return arg


@on_worker_start
def admin_worker(configer):
    @configer.reg_cfg("test")
    async def accept(ws, arg):
        ...
```

A custom route added with `@routes.get("/hello")` appears automatically in the list returned by `GET /cmds`. Internal paths that start with `/_/` are filtered out.
