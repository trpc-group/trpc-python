# 怎么开发一个过滤器插件

过滤器包住一次 RPC 的业务处理。服务端和客户端都继承类 `BaseFilter`，差别在注册类型和在 yaml 配置的位置。

过滤器拿到的 `req` 是业务对象，拿不到原始二进制。在一元RPC请求情况下，服务端在进入链之前已经完成解码、解压和反序列化；客户端把序列化、编码、传输和解码包在链的最里面。二进制怎么进协议头、怎么还原成业务对象，见[协议插件](protocol.zh_CN.md)。

插件在 import 时通过 `@register_plugin` 注册。自己的模块要在服务入口被 import，装饰器才会执行。

## 执行顺序

![服务端与客户端过滤器链](images/filter-chain.png)

请求从左到右，响应从右到左。服务端 `filter` 列表的第一项是 Server-Filter-1，最后一项最靠近业务。客户端同样，Client-Filter-1 是 `client.filter` 的第一项。图里的 Server / Client 方块是业务处理，不是两台机器。

## 过滤器在框架中的调用链路

服务端一元调用在 `Service.handle` 里进入链。解码、解压、反序列化之后调用 `filters.handle(ctx, req, handler.impl_func, timeout)`。HTTP 路由走 `handle_http`，同样调用这条链，中间跳过 protobuf 反序列化，`req` 是上下文上已经放好的 HTTP 请求。

客户端一元调用在 `Client.invoke` 里进入链。选择节点在链外面：`select_node` 完成之后才 `opts.filters.handle(ctx, req, partial(_invoke_backend, ...))`。链里包含序列化、编码、传输和解码逻辑。

`FilterChain.handle` 先把业务协程包成 `_call_handle`，再按 `reversed(self.filters)` 套上每个 `filter.run`。名单里靠前的过滤器在最外层：它的前置逻辑最先执行，后置逻辑最后执行。执行顺序可以参考上一节的图。

注意：

- 业务 `func(ctx, req)` 只返回响应对象。
- 每个过滤器的 `run` 返回 `(rsp, error)`，类型是 `FilterReturnType`
- `next_handle` 没有参数。继续往下写 `rsp, err = await next_handle()`

`_call_handle` 会接住业务函数抛出的异常，所以过滤器的 `await next_handle()` 能看到 `err`：

- `TrpcException` 变成 `(None, err)`，`err` 就是抛出的异常
- 超时或取消：服务端是 `ServerTimeoutError`，客户端是 `ClientTimeoutError`

过滤器自己抛出的异常不进上面的转换，会抛到服务端 `Service.handle` 或客户端 `invoke`。服务端把它编码成错误响应。客户端在上报选点结果之后重新抛出。

## 实现一个过滤器

基类是 `BaseFilter`，继承 `BasePlugin`。`server_meta_data` 的键是 `str`，值是 `bytes`，对应协议里 `trans_info` 的 `map<string, bytes>`。

```python
from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.SERVER_FILTER_COMPONENT, "auth")
class AuthServerFilter(BaseFilter):
    """Reject a request before the handler runs."""

    async def run(self, ctx: TrpcContext, req, next_handle) -> FilterReturnType:
        message = get_message(ctx)
        token = message.server_meta_data.get("token")
        if token != b"ok":
            return None, UnknownError("denied")
        rsp, err = await next_handle()
        return rsp, err

    async def run_stream(self, ctx, req, next_handle):
        raise NotImplementedError("AuthServerFilter does not wrap async-generator handlers")
```

客户端过滤器是同一个类，注册类型换成 `PluginType.CLIENT_FILTER_COMPONENT`。`Options.set_metadata` 写入的也是 `str` 键和 `bytes` 值，进链之前已经并进 `message.client_meta_data`。要读这次调用带出去的字段，放在 `await next_handle()` 之前。返回之后，解码会用下游回包里的 `trans_info` 覆盖 `client_meta_data`。

下面这个过滤器在调用前后各取一次时间，然后原样返回 `(rsp, err)`。

```python
import time

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import FilterReturnType
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.CLIENT_FILTER_COMPONENT, "timing")
class TimingClientFilter(BaseFilter):
    """Record elapsed time around one outbound call."""

    async def run(self, ctx: TrpcContext, req, next_handle) -> FilterReturnType:
        start = time.monotonic()
        rsp, err = await next_handle()
        logger.info("client call took %.3f s", time.monotonic() - start)
        return rsp, err

    async def run_stream(self, ctx, req, next_handle):
        raise NotImplementedError("TimingClientFilter does not wrap async-generator handlers")
```

- 只在这一次请求里用、不上线：`ctx.with_meta_data(key, value)`、`get_meta_data`、`pop_meta_data`。`rpcz` 把 span 放在这里。
- 随 RPC 走的 metadata：`get_message(ctx)` 之后的 `server_meta_data` 和 `client_meta_data`。

## 在YAML中配置

yaml 键是 `filter`，解析后的 Python 字段叫 `filters`。值是注册名，不是类名。

### 服务端

`create_server` 先装 `server.filter`，再把 `server.service[].filter` 追加到后面。装完之后去重，每个过滤器只保留第一次出现的位置：两边都配了的名字以 `server.filter` 中的位置为准，同一个列表里的重复名字也只装一次。

找不到名字时 `get_server_filter` 返回 `None`，`_add_server_filters` 抛出 `PluginInitError`，服务起不来。

```yaml
server:
  filter:
    - auth
    - audit
  service:
    - name: trpc.app.server.Greeter
      protocol: trpc
      network: tcp
      filter:
        - audit_service
      enable_stream_filter: false
```

这条链的请求顺序是 `auth` → `audit` → `audit_service` → 业务，返回时相反。`auth` 在最外层，对应图里的 Server-Filter-1。

### 客户端

- 某个调用端没有写 `filter` 时，字段仍是 `None`，整份使用客户端级列表
- 两边都是列表时，排成客户端级在前、该调用端在后。不去重，同一个名字会进链两次
- 没有出现在 `client.service` 里的调用端，用的是客户端级这份 `filter`

`load_client_config` 按名单调用 `get_client_filter`，再 `add_filter`。`add_filter` 遇到空对象就跳过。写了一个未注册的名字时，链里少这一环。启动时不会抛 `PluginInitError`。

```yaml
client:
  filter:
    - auth
  service:
    - name: trpc.app.server.Greeter
      filter:
        - audit
      enable_stream_filter: false
```

这条链的请求顺序是 `auth` → `audit` → 调用端执行，返回时相反。`auth` 在最外层，对应图里的 Client-Filter-1。

代码里还可以 `Options.set_filter` 和 `set_filter_list`。它们在读完 yaml 之后执行，默认追加到链尾。`override=True` 时换成这次传入的列表。
