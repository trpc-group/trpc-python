# 超时控制

在一次完整的 RPC 调用链路上，可能同时存在三项超时预算（单位均为**毫秒**）。框架会自动选取其中最紧迫（数值最小）的一项作为有效超时，并将剩余可用时间写入发往下游的协议帧中：

1. **上游传入预算**：请求帧头中携带的 `timeout`。当前服务默认会继承该预算；若在服务端配置中设置了 `service.disable_request_timeout: true`，则会忽略上游传入的值，仅使用当前服务自身的 `service.timeout`。
2. **本服务处理预算**：`server.service[].timeout`。当与上游传入预算同时存在时，取两者中较小的一个；设为 `0` 表示本服务这一层不设处理上限。
3. **下游客户端调用预算**：`client.service[].timeout`，或代码中通过 `Options.set_timeout` 指定的超时（代码配置优先级高于 YAML）。设为 `0` 表示本次客户端调用不额外设置截止时间。

配置示例：

```yaml
server:
  check_timeout: false
  queue_wait_timeout: 0
  queue_wait_policy: 0
  service:
    - name: trpc.app.server.service
      timeout: 1000
      disable_request_timeout: false

client:
  timeout: 1000
  service:
    - name: trpc.app.server.downstream
      timeout: 500
```

> 注意：顶层的 `client.timeout` 仅作为兜底值，在按被调服务名找不到对应的 `client.service[]` 配置时才会生效；若某条 `client.service[]` 配置省略了 `timeout` 字段，则该服务的超时保持为 `0`。

## 客户端超时计算与责任归属

当客户端发起调用且配置的超时大于 `0` 时，框架会将其与当前上下文的截止时间 `ctx.deadline` 进行比较，取更早到期的那一个：

- **下游超时 vs 上游超时归属**：
  - 若客户端自身配置的超时更早到期（或上游未设置 `deadline`），随后发生的超时将被判定为**下游调用超时**。
  - 若上游传入的 `deadline` 更早到期，则超时归属为**当前服务自身超时**，在向名字服务 `Selector` 上报调用结果时，不会将其计作下游节点的失败。
  - 若在发起调用前计算出的剩余可用时间已小于或等于 `0`，调用会在发送网络请求前直接中断，并抛出 `ClientTimeoutError`（错误码 `101`）。
- **读取剩余超时与传递上下文**：
  - 业务代码可通过 `ctx.get_timeout_ms()`、`get_timeout_us()`、`get_timeout_sec()` 读取当前上下文的超时时间（返回值 `<= 0` 表示无限制）。
  - 桩代码默认开启了上下文克隆（`need_clone_ctx=True`），因此调用方手头的 `ctx` 不会被单次下游调用修改。若希望下游调用共享当前请求链路的剩余超时预算，请直接将服务端 Handler 接收到的 `ctx` 传给客户端调用方法。

## 服务端排队超时检查

当服务端配置开启 `check_timeout: true` 时，框架会在请求进入业务 Handler 之前，检查该请求在队列中已经等待了多长时间：

- **策略 `0`（`queue_wait_policy: 0`，固定阈值丢弃）**：
  - 一旦排队等待时间达到 `queue_wait_timeout`，框架直接返回 `ServerTimeoutError`（错误码 `21`），不再执行业务 Handler。
  - 若 `queue_wait_timeout` 配置为 `0`，则使用当前请求合并后的总超时预算作为排队等待上限（若合并后的总超时也为 `0`，则不执行限制）。
- **策略 `1`（`queue_wait_policy: 1`，结合滑动窗口过载丢弃）**：
  - 除等待时间达到阈值外，还要求滑动窗口判定当前处于过载状态，才会丢弃该请求。
  - 滑动窗口参数在 `server.queue_cb` 下配置：`ratio`（超时比例阈值，默认 `0.1`）、`threshold`（最小样本数阈值，默认 `100`）、`interval`（统计窗口时长，默认 `60000` 毫秒）。

服务端在业务处理阶段发生超时同样抛出 `ServerTimeoutError`（错误码 `21`）。虽然协议枚举中定义了全链路超时错误码 `24` 和 `102`，但当前框架本进程抛出的超时异常统一使用错误码 `21`（服务端）和 `101`（客户端），完整码表详见 [错误码说明](error_code.zh_CN.md)。

## 连接空闲超时（Idle Timeout）

`server.check_idle_timeout` 与 `server.service[].idletime` 用于控制 TCP 连接在多长时间内没有任何新数据收发时自动断开（单位为毫秒，大于 `0` 时生效），它们控制的是连接生命周期，而非单次 RPC 的处理预算。

> 注意：一旦某个 `Service` 注册了流式 RPC 方法，框架会自动关闭该 `Service` 传输层的空闲连接超时检查，防止长耗时或低频通信的流式长连接被误断开。
