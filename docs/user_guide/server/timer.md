English | [中文](timer.zh_CN.md)

# Timer

A timer (`TimerTask`) is a periodic background task on the service process event loop. It is internal to the process. It does not occupy a `server.service` listen endpoint, and it has no network `protocol`.

Business code registers the task with `@register_timer_task` when the module is imported. After a worker process binds its port and starts listening, the framework starts the matching timers from that registry. The interface is in [`trpc_sdk.utils`](../../../trpc_sdk/utils/_timer.py).

> `@register_timer_task` runs at import time. Import the module that defines the timer **before** `server.serve()`.

## 1. Defining and Registering a Timer

Subclass `TimerTask` and implement `async def handle(self)`:

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

## 2. Schedule Types (`TimerType`)

`type` on `@register_timer_task` selects one of three schedules:

| `TimerType` | Behavior |
| --- | --- |
| `REPEAT_WITH_SECOND` (default) | Periodic interval. Call `handle()` every `interval` seconds. `interval` defaults to `1` |
| `ONCE` | Run once. After startup, wait `interval` seconds, call `handle()` once, then stop |
| `REPEAT_WITH_CRON` | Cron schedule. Fire on the `spec` string. `spec` has 6 fields: **second, minute, hour, day, month, weekday**. For example, `0 */5 * * * *` fires at second 0 of every 5th minute |

## 3. Which Process Runs the Timer

In a multi-process deployment, each process that hosts timers has one executor. Its name is `worker:<index>`, such as `worker:0` or `worker:1`.

`name` on `@register_timer_task(name=...)` chooses which processes run the task:

- **`name` empty (default).** Every timer executor runs the task. Each worker process runs its own copy.
- **`name` set, such as `name="worker:0"`.** Only the executor with that name runs it (process 0). Use this for a summary or cleanup task that should run on one process.

How executors are placed depends on the [process model](overview.md):

- **Single-process / multi-process merge (`merge` / `peer`).** Each worker process has one timer executor, named `worker:0`, `worker:1`, and so on. It starts after the listen address is bound. A task with an empty `name` runs once on each process.
- **Multi-process separate (`separate`).** Timers run **only in the backend business child processes (rpc task)**. The main I/O process does not run them, so the same task is not executed again in the parent and the child. A task with an empty `name` runs once on each business child process.
