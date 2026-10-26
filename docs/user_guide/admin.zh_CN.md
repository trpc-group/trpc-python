# 管理命令（Admin）

Admin 是根进程在业务端口之外独立启动的 HTTP 管理服务，用于查询框架版本、运行配置、查看或动态调整日志级别，以及采集 `rpcz` 调用链诊断数据。

当配置文件中包含 `server.admin` 段时，`serve()` 会自动导入 `trpc_sdk.admin`。只有在 `ip` 和 `port` 同时配置且非空时，根进程才会在完成 `fork` 后绑定并启动该 HTTP 服务；若缺少任一项，则不会启动 Admin 服务。

```yaml
server:
  admin:
    ip: 127.0.0.1
    port: 9028
```

**工作原理说明**：
- 每个 Worker 子进程启动后，会向根进程建立一条内部 WebSocket 长连接（路径为 `/_/ws`），用于接收根进程下发的日志级别修改指令及汇总 `rpcz` 数据（Task 子进程不建立此连接）。
- 若 `admin.ip` 配置为 `0.0.0.0`，Worker 子进程回连根进程时会自动将目标地址替换为 `127.0.0.1`。

> 注意：调用下文的管理接口时，请使用 `server.admin` 配置的 IP 和端口，而非业务 `server.service` 的端口。接口执行成功时，返回的 JSON 中 `errorcode` 字段为 `0`。

## 内置管理命令

| HTTP 方法 | 路径 | 功能说明 |
| --- | --- | --- |
| `GET` | `/cmds` | 列出当前已注册的所有管理命令 HTTP 路径（以 `/_/` 开头的内部路径除外） |
| `GET` | `/version` | 返回框架版本号 `{"version": ...}`，值来自 `trpc_sdk.version` |
| `GET` | `/cmds/config` | 返回进程启动时解析到的完整配置字典 |
| `GET` | `/cmds/loglevel?logger=default` | 查询指定 `logger` 的当前日志级别名称；省略 `logger` 参数时默认查询 `default` |
| `PUT` | `/cmds/loglevel` | 动态修改日志级别。通过表单传入 `logger` 与 `value`（可选值：`trace`、`debug`、`info`、`warning`、`error`、`fatal`） |
| `GET` | `/cmds/rpcz` | 获取近期 `rpcz` Span 摘要列表，返回条数由 `global.rpcz.print_spans_num` 控制 |
| `GET` | `/cmds/rpcz?span_id=123` | 按 `span_id` 查询单条 Span 详情；若 ID 不是合法整数或未找到，返回 `errorcode: -1` |
| `GET` | `/cmds/debug/language` | 返回当前语言标识 `{"language": "python"}` |

补充说明：
- **动态调整日志级别**：调用 `PUT /cmds/loglevel` 后，根进程会通过 WebSocket 将新级别广播给所有已连接的 Worker 子进程；当 `logger` 为 `default` 时，根进程自身的 root logger 级别也会一并更新。
- **启用 `rpcz`**：若需使用 `/cmds/rpcz`，还需在 YAML 配置中将拦截器名 `rpcz` 加入 `server.filter` 或 `client.filter`。采样率等参数在 `global.rpcz` 下配置，详见 [框架配置](framework_conf.zh_CN.md) 与 [过滤器插件](../developer_guide/develop_plugins/filter.zh_CN.md)。

常用 `curl` 调用示例：

```bash
curl http://127.0.0.1:9028/version
curl http://127.0.0.1:9028/cmds/loglevel?logger=default
curl -X PUT http://127.0.0.1:9028/cmds/loglevel -d "logger=default&value=debug"
curl http://127.0.0.1:9028/cmds/rpcz
```

## 注册自定义管理命令

若需扩展自定义的 HTTP 管理接口或向所有 Worker 下发自定义指令，需在调用 `serve()`（即进程 `fork`）之前注册钩子函数：

- **`@on_master_start`**：在根进程启动 Admin 服务时调用，签名为 `func(routes, configer)`，接收根进程的 `aiohttp` 路由表与配置下发器。
- **`@on_worker_start`**：在每个 Worker 子进程启动时调用，签名为 `func(configer)`，接收 Worker 侧的配置接收器。

当根进程需要向所有 Worker 同步状态时：
1. 在根进程中通过 `configer.reg_cfg(key)` 注册一个数据提供函数，随后调用 `await configer.config({key: arg})` 触发该函数并将返回结果通过 WebSocket 推送给所有 Worker。
2. 在 Worker 进程中通过同名的 `configer.reg_cfg(key)` 注册异步接收函数 `async def handler(ws, value)` 处理收到的数据。

示例代码：

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

通过 `@routes.get("/hello")` 新增的自定义路由会自动出现在 `GET /cmds` 的返回列表中（以 `/_/` 开头的内部路径会被自动过滤）。
