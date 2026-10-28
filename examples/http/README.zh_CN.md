# HTTP 示例

本示例在一个进程里起两个 HTTP 服务。`127.0.0.1:9002` 上是 protobuf RPC，服务名 `trpc.trpc_python_group.helloworld.Greeter`。`127.0.0.1:9003` 上是普通 HTTP，服务名 `trpc.trpc_python_group.helloworld.Normal`。`SayHello` 回复 `hello: ` 加上请求里的 `msg`，`SayHi` 回复 `hi: ` 加上请求里的 `msg`。服务端 timeout 是 5000 毫秒。YAML 没有 `processmodel`。

## 关键特性

- **HTTP RPC**：`SayHello` / `SayHi` 走 HTTP，正文是 protobuf。客户端打在 `9002`
- **未绑定 service 的路由**：`/normal_http` 和 `/route_with_rsp_header` 没写 `service_name`，两个 HTTP 服务都会挂上
- **绑定到一个服务名**：`/bind_service_name` 的 `service_name` 是 `trpc.trpc_python_group.helloworld.Normal`，只在 `9003`
- **静态文件**：导入时把仓库的 `docs/` 挂到 `/static`。客户端不请求这条路径

| service name | 地址 | 协议 |
| --- | --- | --- |
| `trpc.trpc_python_group.helloworld.Greeter` | `127.0.0.1:9002` | `tcp` / `http` |
| `trpc.trpc_python_group.helloworld.Normal` | `127.0.0.1:9003` | `tcp` / `http` |

`/normal_http` 和 `/bind_service_name` 返回 JSON `{"data": "json"}`。`/bind_service_name` 另外设置响应头 `url_path`，值是请求路径。`/route_with_rsp_header` 的正文是 `user-defined test`，响应头 `test` 是 `data`，同样有 `url_path`。装饰器没写 `method`，默认任意方法。示例客户端发的是 POST。

导入 [normal/_server.py](normal/_server.py) 时打印 `=========== register http ==================`。

关键文件：

- [trpc_main.py](trpc_main.py)：`--conf` / `-c`，默认是旁边 [conf/trpc_python.yaml](conf/trpc_python.yaml) 的绝对路径。`register_GreeterServicer_server(..., fix_rpc_name=True)` 之后 `serve`
- [rpc/greeter.py](rpc/greeter.py)：`SayHello`、`SayHi`
- [normal/_server.py](normal/_server.py)：三条路由和 `/static`
- [client/_client.py](client/_client.py)：`HTTP_TEST` 里的 RPC 和 POST
- [test_client.py](test_client.py)：`asyncio.run(test_http_client())`
- [conf/trpc_python.yaml](conf/trpc_python.yaml)：两个 HTTP service。`tls_key` / `tls_cert` 是注释，本目录没有证书文件

调用关系：

```text
test_client.py
├── 127.0.0.1:9002  trpc.trpc_python_group.helloworld.Greeter
│   ├── SayHello / SayHi
│   ├── POST /normal_http
│   └── POST /route_with_rsp_header
└── 127.0.0.1:9003  trpc.trpc_python_group.helloworld.Normal
    ├── POST /normal_http
    ├── POST /bind_service_name
    └── POST /route_with_rsp_header
```

## 关键代码解释

- [trpc_main.py](trpc_main.py) 在导入业务代码之前，把 `rpc/stub`、`rpc`、`normal` 的绝对路径放进 `sys.path`。`examples/http` 这一层只在本文件被 import、`__name__` 不是 `__main__` 时才加。直接跑 `python3 trpc_main.py` 时，解释器还会把脚本所在目录放在 `sys.path` 最前面。服务要导入的 `normal` 和 `greeter` 走的是上面的绝对路径。
- [test_client.py](test_client.py) 写的是 `from client import test_http_client`。在 `examples/http` 下执行 `python3 test_client.py` 时，脚本目录就是 `sys.path[0]`，才能找到这个包。`python -m examples.http.test_client` 会去仓库根上找顶层的 `client`。
- RPC 代理名是 `trpc.trpc_python_group.helloworld.Greeter`，`set_protocol("http")`，序列化是 PB，timeout `3000` 毫秒。普通 HTTP 用 `trpc_sdk.http.Client.post`，正文是 `text`，`stream=False`。
- `set_short_conn(True)` 是注释。`HTTP_TEST` 用 `asyncio.gather` 并发，注释里说明这种并发不要开短连接。
- `test_async_say_hello` 和 `test_http_client_post` 的默认参数里有 `8001`。`HTTP_TEST` 实际传入的是 `9002` 和 `9003`。
- `/bind_service_name` 以装饰器为准：`service_name="trpc.trpc_python_group.helloworld.Normal"`。同文件上方的注释仍写着 `Greeter`。客户端里有一行被注释掉的 `9003/bind_service_name`，旁边写着绑定在 `9002`。正在执行的 `HTTP_TEST` 打的是 `9003/bind_service_name`，和装饰器一致。
- `get_addr` 打印上下文上的本端和对端地址。HTTP `post` 会把地址抄回调用方的 context。RPC stub 用的是一份克隆，`SayHello` / `SayHi` 返回后这两项可能仍是空的。

## 环境要求

- Python >= 3.8
- macOS 或 Linux
- 本机 `9002`、`9003` 空闲
- 在仓库根目录安装：`pip install -e .`。安装说明见上级 [README.zh_CN.md](../../README.zh_CN.md)

本目录没有 `requirements.txt`。路由模块导入 `aiohttp`，`pip install -e .` 会装上它。

## 运行步骤

在 `examples/http`：

```bash
python3 trpc_main.py
python3 trpc_main.py --conf=conf/trpc_python.yaml
```

另一个终端，当前目录仍是 `examples/http`，服务已经在听：

```bash
python3 test_client.py
```

按路由看单次请求：

```bash
curl -i -X POST http://127.0.0.1:9002/normal_http -d 'text'
curl -i -X POST http://127.0.0.1:9003/normal_http -d 'text'
curl -i -X POST http://127.0.0.1:9003/bind_service_name -d 'text'
curl -i -X POST http://127.0.0.1:9002/route_with_rsp_header -d 'text'
curl -i -X POST http://127.0.0.1:9003/route_with_rsp_header -d 'text'
```

## 运行结果

`test_client.py` 打印 `=== test_http_client start ===`，然后是每次调用的正文，最后打印 `=== test_http_client end ===`。这些协程是并发的，中间几行的顺序不固定。

断言：

- `SayHello` 的请求 `msg` 是 `hello`，回复 `msg` 是 `hello: hello`
- `SayHi` 的回复 `msg` 是 `hi: hello`
- 上面的 POST 状态码都是 `200`
- `/route_with_rsp_header` 的正文是 `user-defined test`，响应头 `test` 是 `data`
- `/normal_http` 和 `/bind_service_name` 只检查状态码，不比较 JSON 正文

`HTTP_TEST` 里 `127.0.0.1:9003/route_with_rsp_header` 写了两次，这一路会打两遍。任一断言失败会抛 `AssertionError`，进程以非 0 退出。

服务启动时打印 `conf_path:` 和实际使用的 YAML 路径。导入路由时打印 `=========== register http ==================`。

## 适用场景

- 确认 protobuf RPC 可以跑在 HTTP 上
- 确认没写 `service_name` 的路由在 `9002` 和 `9003` 上都能进
- 确认写了 `Normal` 的 `/bind_service_name` 由 `9003` 提供

## 需要注意什么

1. **在 `examples/http` 下启动客户端**。`test_client.py` 按脚本所在目录找 `client` 包。
2. **`/bind_service_name` 只挂在 `9003`**。客户端测的是这一端口。
3. **端口**。`9002` 和 `9003` 都要空闲。
4. **并发用长连接**。`HTTP_TEST` 是 `asyncio.gather`。短连接那一行保持注释。
5. **日志文件**。YAML 里的 `./hello.log` 相对进程当前目录。
6. **`/static`**。前缀指向仓库的 `docs/`。`test_client.py` 不请求它。
