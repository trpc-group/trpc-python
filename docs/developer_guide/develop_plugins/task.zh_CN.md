# 怎么开发一个任务插件

在 tRPC-Python 中，任务插件（Task）是一组专门用于后台批处理计算的子进程。处理网络请求的 Worker 进程可以将耗时阻塞或适合凑批（batching）执行的请求（如模型推理、CPU 密集型计算）发送给任务子进程，任务子进程按批调用用户定义的处理函数后再将结果异步返回给 Worker。

使用任务插件前，需要注意它与常规插件的几点区别：
- **不继承 `BasePlugin`**：任务插件没有 `init()` / `start()` 生命周期方法，而是通过 [`register_task`](../../../trpc_sdk/plugin/_registry.py) 函数登记（内部固定使用 `PluginType.TASK`，但 `PluginSpec.cls` 为 `None`，因此调用 `get_plugin(PluginType.TASK, name)` 不会返回可调用对象）。
- **无 `plugins.task` 配置**：任务是否启用完全取决于模块导入时是否执行了 `register_task`。定义任务的模块必须在服务入口调用 `serve()` 之前被 `import`。
- **与其他进程/定时器的区分**：定时任务请使用 `TimerTask`；`multi_process` + `separate` 模式下的 RPC 子进程属于网络工作进程；本文介绍的 `register_task` 专用于批处理计算子进程或者其他计算密集型的任务。

整体数据流转如下：

```mermaid
flowchart LR
  worker[Worker 处理协程] --> send["send_to_task"]
  send --> queue[任务进程请求队列]
  queue --> consumer[TaskConsumer 消费者线程]
  consumer --> handler["handler(batch)"]
  handler --> rspQueue[响应队列]
  rspQueue --> future[Worker 端 Future]
```

## 1. 注册批处理任务

注册任务需使用公开的数据类 [`trpc_sdk.abc.TaskInfo`](../../../trpc_sdk/abc/_task_info.py) 与注册函数 `trpc_sdk.plugin.register_task`：

```python
from trpc_sdk.abc import TaskInfo
from trpc_sdk.plugin import register_task


def handler(reqs):
    return [req.upper() for req in reqs]


def initializer():
    return None


register_task(
    "normal_task",
    TaskInfo(
        name="normal_task",
        handler=handler,
        initializer=initializer,
        batch_size=5,
    ),
)
```

> `register_task` 会自动将 `task_info.name` 覆盖为第一个参数传入的名称，建议两处保持一致。

### `handler` 编写契约

[`TaskConsumer`](../../../trpc_sdk/task/_consumer.py) 对批处理函数 `handler` 有以下要求：

1. **输入与输出等长且一一对应**：入参 `reqs` 是一个 `list`，返回值也必须是一个**长度相同**的 `list`，且下标与入参严格对应。如果返回值不是 `list`、长度不一致，或者 `handler` 执行过程中抛出异常，该批次内的所有调用方都会收到 `TaskHandleError`。
2. **支持同步函数或异步协程函数**：消费者线程会通过 `inspect.iscoroutinefunction` 自动判断并选择同步或异步执行入口，且线程内部自带独立的事件循环（请勿在 `handler` 中引用 Worker 进程的事件循环）。
3. **直接接收业务对象**：批次列表中的每个元素就是调用 `send_to_task` 时传入的 `req` 对象。框架会自动通过任务 codec 完成跨进程序列化与反序列化，业务函数无需手动处理字节流。

### `initializer` 与 `TaskInfo` 参数说明

- **`handler`（必填）**：批处理执行函数。
- **`initializer`（可选）**：在每个任务子进程 `fork` 启动后、开始从管道接收请求前调用一次（详见 [`TaskProcess._prepare_to_serve`](../../../trpc_sdk/runtime/_process/_task_process.py)）。非常适合在此处加载深度学习模型或初始化进程内资源。注意它运行在任务子进程而非 Worker 进程中；若抛出异常，该任务子进程将无法启动。
- **`batch_size`**：当前任务的单批次最大请求数。若设为 `<= 0`，则回退使用全局服务配置 `server.task_batch_size`。
- **`task_num`**：启动的任务子进程数量，默认为 `1`。当启动多个子进程时，实际进程名形如 `normal_task:0`、`normal_task:1`，但业务调用 `send_to_task` 时仍统一填写注册名 `"normal_task"`，由框架负责选择连接。
- **`pause_heartbeat`**：默认为 `False`。设为 `True` 时，[`build_task_infos`](../../../trpc_sdk/runtime/_process/_task_process.py) 会在 `fork` 前暂停名字服务心跳，待子进程 `initializer` 执行完毕后再恢复心跳。仅当需要等待任务子进程初始化完成后才对外暴露服务可发现状态时才需开启；常规场景保持默认 `False` 即可。

> 只有 Worker 侧的进程可以向 Task 发送任务，Task 进程之间不能互相调用。

## 2. 任务子进程的启动流程

当服务启动时，框架按以下步骤拉起并连接任务子进程：

1. **登记任务**：导入服务入口模块时，`register_task` 将任务信息写入注册表（可通过 `get_registered_tasks()` 查看）。
2. **构建进程拓扑**：调用 `serve()` 后，在 `fork` 前进入 `_spawn_children()`，由 `build_task_infos()` 根据每个任务的 `task_num` 生成进程记录，并按需执行 `pause_registry_heartbeat()`。
3. **创建进程间管道**：`TaskIpcMesh` 在 `fork` 前预先创建好 Worker 进程到各个 Task 进程的 `socketpair` 通信管道。
4. **启动子进程与初始化插件**：`fork` 出 `TaskProcess` 子进程后，子进程先执行 `task_init(queue_size)` 初始化底层队列，再调用 `init_task_plugins()`（仅初始化标记为 `PluginInitPos.TASK` 且非 `USER_DEFINED` 的插件）。
5. **运行消费者**：子进程关闭无关的文件描述符，执行 `initializer()`，随后根据 `server.task_consumer_num` 启动对应数量的 `TaskConsumer` 线程，并在主事件循环上运行 `consume_response` 回传结果。同时，Worker 进程在自己的服务事件循环上调用 `register_task_conn` 注册任务连接。

## 3. 在业务代码中提交任务

在 Worker 的 RPC 处理函数中，直接调用并 `await` [`send_to_task(ctx, task_name, req)`](../../../trpc_sdk/task/_task.py) 即可将请求发往任务子进程：

```python
from trpc_sdk.task import send_to_task


async def SayHello(self, ctx, request):
    rsp = await send_to_task(ctx, "normal_task", request.message)
    return rsp
```

- **凑批机制**：`TaskConsumer` 每次从队列中最多取出 `batch_size` 条请求组装成一个列表传给 `handler`（`batch_size` 是上限，若当前只有 1 条请求也会单独成批处理）。当队列为空时，消费者线程会休眠 `task_wait_interval` 毫秒后再检查。
- **常见异常说明**：
  - `RuntimeError`：当前线程未绑定任务连接表。`send_to_task` 必须在 Worker 的服务事件循环（service loop）上调用；当配置了 `handle_thread_num > 0` 时，独立的 I/O 线程上没有任务管道，不能直接调用。
  - `TaskHandleError`：指定的 `task_name` 未注册（解析到的连接为 `None`），或者任务子进程中的 `handler` 执行报错 / 返回列表长度不匹配。

## 4. 服务级配置项（`trpc_python.yaml`）

任务队列与消费者线程的全局参数配置在 `server` 节点下（而非 `plugins` 下），默认值定义在 [`trpc_sdk/config/_types.py`](../../../trpc_sdk/config/_types.py) 与 [`_constants.py`](../../../trpc_sdk/config/_constants.py) 中：

```yaml
server:
  task_batch_size: 1
  task_wait_interval: 5
  task_queue_size: 1000
  task_consumer_num: 1
```

各字段说明：
- `task_batch_size`：全局默认批次上限，默认为 `1`。**仅当某个任务的 `TaskInfo.batch_size <= 0` 时生效**；若注册任务时显式指定了正数（如上面示例中的 `batch_size=5`），则以 `TaskInfo.batch_size` 为准。
- `task_wait_interval`：任务队列为空时消费者线程的休眠等待时间，默认为 `5`（单位：毫秒）。
- `task_queue_size`：每个任务子进程的请求队列容量，默认为 `1000`。
- `task_consumer_num`：每个任务子进程内启动的 `TaskConsumer` 消费者线程数，默认为 `1`。
