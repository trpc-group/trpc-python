# 1.0.1 (2026-11-16)

## REFACTOR

- 调整代码目录：客户端、流式客户端、名字服务、连接池、压缩类型。
- 序列化：移除 `JceSerializer` 与 `WupSerializer`。
- 代码风格格式化。


# 1.0.0 (2026-10-30)

## FEATURES

- 发布 tRPC-Python 1.0.0 版本。
- 跨语言互通：基于 Protocol Buffers 轻松实现多语言微服务间的高性能通信。
- 多通信协议：原生支持多种通信协议，方便与不同框架进行互联，支持客户端流、服务端流和双向流。
- 进程模型由 `global.processmodel` 选择，支持单进程与多进程，IO 和 Handle merge 与 separate模式。
- 基于独立 Task 进程结构承载在线推理、批量计算等高计算负载场景。
- 丰富插件生态：提供大量对接业界微服务组件的插件（比如 Promethues 等），开箱即用，方便用户构建高可用的服务治理体系。
- 可扩展插件架构：基于框架插件化的设计，用户可以进行二次开发来扩展框架能力，比如：RPC 请求参数校验、鉴权等。

## DOCUMENTS

- 用户使用指南、开发者指南。
- README、社区规范、许可证等开源社区文档。

## EXAMPLES

- helloworld：一元 Greeter。
- hellostream：一元、客户端流、服务端流、双向流。
- hellosse：HTTP SSE。
- hellotrt：CPU ResNet50 推理。
- http：HTTP RPC 与 `@route`。
- helloworld 特性：用户协议、管理命令、HTTP 路由、单进程多线程 HTTP、单进程线程模型、多进程 merge、多进程 separate。

## CI

- GitHub Actions 覆盖 lint、test、release 和 CLA。
