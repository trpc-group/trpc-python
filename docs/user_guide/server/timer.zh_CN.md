# 定时器使用指南

在 tRPC-Python 中，定时器（`TimerTask`）是挂载在服务进程事件循环上的后台周期性任务。它属于进程内部任务，不占用 `server.service` 监听端点，也不涉及网络协议（`protocol`）。

业务代码通过 `@register_timer_task` 装饰器在模块导入（`import`）阶段登记定时任务；当 Worker 进程完成端口绑定并开始监听后，框架会自动根据登记记录启动对应的定时器（接口定义见 [`trpc_sdk.utils`](../../../trpc_sdk/utils/_timer.py)）。

> **注意**：由于 `@register_timer_task` 在模块导入时执行注册，请务必在调用 `server.serve()` **之前**先 `import` 定义了定时任务的模块。

## 1. 定义与注册定时任务

自定义定时任务需继承 `TimerTask` 基类，并实现异步方法 `async def handle(self)`：

```python
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import register_timer_task


@register_timer_task()
class EverySecond(TimerTask):
    async def handle(self):
        ...


@register_timer_task(interval=70, name="worker:0")
class OnFirstWorker(TimerTask):
    async def handle(self):
        ...


@register_timer_task(type=TimerType.ONCE, interval=5)
class Once(TimerTask):
    async def handle(self):
        ...


@register_timer_task(type=TimerType.REPEAT_WITH_CRON, spec="*/1 * * * * *")
class EveryCronSecond(TimerTask):
    async def handle(self):
        ...
```

## 2. 调度类型（`TimerType`）

`@register_timer_task` 通过 `type` 参数支持三种调度策略：

| `TimerType` 枚举值 | 调度行为说明 |
| --- | --- |
| `REPEAT_WITH_SECOND`（默认） | 周期性间隔执行：每隔 `interval` 秒调用一次 `handle()`（`interval` 默认值为 `1`） |
| `ONCE` | 单次延迟执行：启动后等待 `interval` 秒调用一次 `handle()`，随后结束 |
| `REPEAT_WITH_CRON` | Cron 表达式调度：按 `spec` 字符串周期性触发。`spec` 包含 6 个字段，依次为 **`秒 分 时 日 月 星期`**（例如 `0 */5 * * * *` 表示每 5 分钟的第 0 秒触发） |

## 3. 多进程下的执行器绑定与进程模型

在多进程部署时，每个承载定时器的进程都拥有一个定时器执行器（Executor），其名称格式为 `worker:<进程序号>`（如 `worker:0`、`worker:1`）。

通过 `@register_timer_task(name=...)` 的 `name` 参数，可以控制定时任务在哪些进程上运行：
- **`name` 为空（默认）**：所有定时器执行器都会运行该任务（即每个工作进程各自跑一份）。
- **指定 `name`（如 `name="worker:0"`）**：仅名称匹配的那个执行器（第 0 号进程）会运行该任务，适合只需在单节点/单进程执行的汇总或清理任务。

不同 [进程模型](overview.zh_CN.md) 下定时器执行器的分布规则如下：

- **单进程 / 多进程合并模式（`merge` / `peer`）**：每个工作进程各自包含一个定时器执行器（命名为 `worker:0`、`worker:1`……），在完成端口监听绑定后启动。未指定 `name` 的任务会在每个进程中各运行一份。
- **多进程分离模式（`separate`）**：定时器**仅在后端业务子进程（rpc task）中运行**，主 I/O 进程不运行定时器，从而避免同一任务在主进程与子进程间重复执行。未指定 `name` 的任务会在每个业务子进程中各运行一份。
