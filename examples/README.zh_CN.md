# 示例

下面是 `examples` 里每份说明的入口。环境是 Python `>=3.8`，macOS 或 Linux。安装步骤见仓库 [README.zh_CN.md](../README.zh_CN.md)，在仓库根目录执行 `pip install -e .`。

## 顶层

- [helloworld](helloworld/README.zh_CN.md)：一元 Greeter，`127.0.0.1:8000`，另有用户协议 UDP `8090`
- [hellostream](hellostream/README.zh_CN.md)：一元、客户端流、服务端流、双向流，TCP `8000`–`8040`
- [hellosse](hellosse/README.zh_CN.md)：HTTP SSE `GET /sse`，`8080`–`8084`
- [hellotrt](hellotrt/README.zh_CN.md)：CPU ResNet50，`SayHello` 用本循环会话，`SayHi` 用 task 批处理，`8000` / `8110`–`8140`
- [http](http/README.zh_CN.md)：HTTP RPC 与 `@route`，`9002` / `9003`
- [plugin](plugin/README.zh_CN.md)：用户自定义插件，`SayHello` 带上 `plugins.business.token`，`127.0.0.1:8200`

<a id="helloworld-features"></a>

## helloworld 特性

说明见 [helloworld/features/README.zh_CN.md](helloworld/features/README.zh_CN.md)。

- [用户协议](helloworld/features/README.zh_CN.md#user-protocol)：UDP `protocol: user`，默认服务核对 `8090`
- [管理命令](helloworld/features/README.zh_CN.md#admin)：管理命令，merge / peer / separate
- [HTTP 路由](helloworld/features/README.zh_CN.md#http)：往默认服务上挂 HTTP 路由
- [单进程多线程 HTTP](helloworld/features/README.zh_CN.md#http-thread)：单进程多线程上的 HTTP
- [线程模型](helloworld/features/README.zh_CN.md#thread)：单进程 merge 与 separate-thread
- [peer](helloworld/features/README.zh_CN.md#peer)：多进程 merge，每个 worker 自己监听
- [separate](helloworld/features/README.zh_CN.md#separate)：多进程 separate，I/O 进程转发

<a id="hellostream-features"></a>

## hellostream 特性

- [过滤器](hellostream/features/filter/README.zh_CN.md)：流式示例上的过滤器样例，当前没有接入
